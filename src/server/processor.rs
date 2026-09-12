use std::collections::HashMap;

use actix_web::{
    HttpRequest,
    http::header::CONTENT_TYPE,
    web::{Bytes, Data},
};
use async_trait::async_trait;
use serde::Deserialize;

use crate::{
    XepakError,
    auth::{
        AuthorizeProcessor, SimpleAuthenticationProcessor, token::TokenAuthenticationProcessor,
    },
    cfg::ResourceRef,
    schema::validate_with_schema,
    script_lua::LuaPreProcessor,
    server::{CONTENT_TYPE_CBOR, RequestInput, XepakAppData, is_req_body_allowed},
    xepak_data::XepakValue,
};

pub const PRIORITY_FIRST: u16 = 30_000;

pub const PRIORITY_NORMAL: u16 = 20_000;

pub const PRIORITY_LAST: u16 = 1000;

const PRIOTIRY_QUERY_ARGS: u16 = PRIORITY_FIRST + 10000;

/// Define request processors variants.
#[derive(Clone, Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum PreProcessor {
    /// Referenece to shared pre-processor
    Ref {
        id: String,
    },

    /// Extracts body argurments to request object.
    ParseBodyArgs {
        /// If those top level keys were not in request - set them as nulls
        #[serde(default)]
        null_if_abscent: Vec<String>,
    },

    SimpleAuthentication {
        /// Allows anonymous authentication
        #[serde(default)]
        anonymous_auth: bool,
    },

    TokenAuthentication {
        #[serde(default)]
        data_source: String,

        /// Query in a form: `SELECT id,roles FROM ... WHERE ... key = {{api-key}}`
        query: String,

        #[serde(default)]
        cache_ttl_sec: u16,
    },

    Authorize {
        rules: String,
    },

    LuaScript {
        script: String,
    },
}

pub fn init_required_pre_processors() -> Vec<Box<dyn PreProcessorHandler>> {
    vec![
        Box::new(QueryArgsProcessor {}),
        Box::new(InputArgsValidator {}),
    ]
}

pub fn build_pre_processor(
    app: &XepakAppData,
    parent_rref: &ResourceRef,
    position: u16,
    specs: &PreProcessor,
    shared: &HashMap<String, PreProcessor>,
) -> Result<Box<dyn PreProcessorHandler>, XepakError> {
    // TODO: rref will be used later for pre-processors LUA cache
    match specs {
        PreProcessor::Ref { id } => {
            if let Some(sspecs) = shared.get(id) {
                if let PreProcessor::Ref { .. } = sspecs {
                    Err(XepakError::Cfg(
                        "Ref types are not allowed in shared pre-processors".to_string(),
                    ))
                } else {
                    build_pre_processor(app, parent_rref, position, sspecs, shared)
                }
            } else {
                Err(XepakError::Cfg(format!(
                    "Can't find pre-processor by reference: \"{id}\""
                )))
            }
        }
        PreProcessor::ParseBodyArgs { null_if_abscent } => {
            Ok(Box::new(BodyToArgsProcessor::new(null_if_abscent.clone())))
        }
        PreProcessor::SimpleAuthentication { anonymous_auth } => Ok(Box::new(
            SimpleAuthenticationProcessor::new(position, *anonymous_auth),
        )),
        PreProcessor::Authorize { rules } => {
            Ok(Box::new(AuthorizeProcessor::new(position, rules.as_ref())?))
        }
        PreProcessor::TokenAuthentication {
            data_source,
            query,
            cache_ttl_sec,
        } => Ok(Box::new(TokenAuthenticationProcessor::new(
            position,
            query.clone(),
            data_source.clone(),
            *cache_ttl_sec,
        ))),
        PreProcessor::LuaScript { script } => Ok(Box::new(LuaPreProcessor::new(
            app,
            parent_rref.nested(position),
            position,
            script,
        )?)),
    }
}

/// Shared interface for all pre-processors.
#[async_trait(?Send)]
pub trait PreProcessorHandler: Send + Sync {
    /// Handler with higher priority will be processed first
    fn priority(&self) -> u16;

    async fn handle(
        &self,
        req: &HttpRequest,
        state: &Data<XepakAppData>,
        body: &Bytes,
        input: &mut RequestInput,
    ) -> Result<(), XepakError>;
}

/// Adjust processor priority related to it's position in the ordered list.
/// The bigger order - the lower priority is.
#[inline]
pub fn adjust_priority(priority: u16, position: u16) -> u16 {
    if priority < position {
        return 0;
    }
    priority - position
}

/// Execute validation logic for all input arguments according to schema.
/// Has lowest priority, should be executed after all other pre-processors
/// that could enrich input with K/V arguments.
/// The core notion here is that we validate not a source data
/// but its parsed K/V representation.
pub struct InputArgsValidator {}

#[async_trait(?Send)]
impl PreProcessorHandler for InputArgsValidator {
    fn priority(&self) -> u16 {
        PRIORITY_LAST
    }

    async fn handle(
        &self,
        _req: &HttpRequest,
        _state: &Data<XepakAppData>,
        _body: &Bytes,
        input: &mut RequestInput,
    ) -> Result<(), XepakError> {
        validate_with_schema(&input.schema, &input.path_args.lock().unwrap())?;
        validate_with_schema(&input.schema, &input.args.lock().unwrap())?;
        Ok(())
    }
}

/// Handle arguments from query string.
/// Skip query string args for POST/PUT requests (basically anything that have request body)
pub struct QueryArgsProcessor {}

#[async_trait(?Send)]
impl PreProcessorHandler for QueryArgsProcessor {
    fn priority(&self) -> u16 {
        PRIOTIRY_QUERY_ARGS
    }

    async fn handle(
        &self,
        req: &HttpRequest,
        _state: &Data<XepakAppData>,
        _body: &Bytes,
        input: &mut RequestInput,
    ) -> Result<(), XepakError> {
        if is_req_body_allowed(req) {
            return Ok(());
        }
        let qstring = req.uri().query().unwrap_or_default();
        let query_args =
            if let Ok(qa) = serde_urlencoded::from_str::<HashMap<String, XepakValue>>(qstring) {
                qa
            } else {
                tracing::warn!("Can't decode query string from URL");
                Default::default()
            };

        for (k, v) in query_args {
            input.set_arg_with_schema(k, v, true)?;
        }

        Ok(())
    }
}

/// Deserialize request body to K/V arguments.
#[derive(Clone)]
pub struct BodyToArgsProcessor {
    null_if_abscent: Vec<String>,
}

impl BodyToArgsProcessor {
    pub fn new(null_if_abscent: Vec<String>) -> Self {
        Self { null_if_abscent }
    }

    pub fn handle_cbor_body(
        &self,
        body: &Bytes,
        input: &mut RequestInput,
    ) -> Result<(), XepakError> {
        if body.is_empty() {
            return Ok(());
        }

        let req_body: XepakValue = cbor2::from_slice(body)
            .map_err(|e| XepakError::Input(format!("Wrong CBOR format: {e}")))?;

        if !req_body.is_map() {
            return Err(XepakError::Input(
                "CBOR request body must be a map/dict".to_string(),
            ));
        }

        let req_body_map = req_body.as_map()?;

        for (key, value) in req_body_map {
            let xvalue = if value.is_tuple() || value.is_map() {
                return Err(XepakError::Input(format!(
                    "(๑•ᗝ•)૭ Root CBOR must NOT have any nested arrays or objects. See \"{key}\" property."
                )));
            } else {
                value.clone()
            };

            input.set_arg_with_schema(key, xvalue, true)?;
        }

        Ok(())
    }

    pub fn handle_json_body(
        &self,
        body: &Bytes,
        input: &mut RequestInput,
    ) -> Result<(), XepakError> {
        if body.is_empty() {
            return Ok(());
        }

        let json_request: serde_json::Value = serde_json::from_slice(body)
            .map_err(|e| XepakError::Input(format!("Wrong JSON format: {e}")))?;

        let Some(json_object) = json_request.as_object() else {
            return Err(XepakError::Input(
                "JSON request body only allowed to be an object".to_string(),
            ));
        };

        for (key, value) in json_object {
            let xvalue = if value.is_array() || value.is_object() {
                return Err(XepakError::Input(format!(
                    "(๑•ᗝ•)૭ Root JSON must NOT have any nested arrays or objects. See \"{key}\" property."
                )));
            } else {
                value.try_into()?
            };

            input.set_arg_with_schema(key.clone(), xvalue, true)?;
        }

        Ok(())
    }
}

#[async_trait(?Send)]
impl PreProcessorHandler for BodyToArgsProcessor {
    async fn handle(
        &self,
        req: &HttpRequest,
        _state: &Data<XepakAppData>,
        body: &Bytes,
        input: &mut RequestInput,
    ) -> Result<(), XepakError> {
        if !is_req_body_allowed(req) {
            return Ok(());
        }

        // Everything that is not excplicitly defined as CBOR is handled as JSON
        let cbor_body = if let Some(accept) = req.headers().get(CONTENT_TYPE)
            && accept.eq(CONTENT_TYPE_CBOR)
        {
            true
        } else {
            false
        };

        if cbor_body {
            self.handle_cbor_body(body, input)?;
        } else {
            self.handle_json_body(body, input)?;
        }

        for key in &self.null_if_abscent {
            if !input.has_any_arg(key) {
                input
                    .args
                    .lock()
                    .unwrap()
                    .insert(key.clone(), XepakValue::Null);
            }
        }

        Ok(())
    }

    fn priority(&self) -> u16 {
        PRIORITY_NORMAL
    }
}
