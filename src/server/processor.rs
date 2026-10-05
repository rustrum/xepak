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
    script_lua::LuaPreProcessor,
    server::{CONTENT_TYPE_CBOR, RequestInput, XepakAppData, is_req_body_allowed, to_input_error},
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
    vec![Box::new(QueryArgsProcessor {})]
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

        let qa_value = XepakValue::from(query_args);

        input.get_args = input.apply_schema_to("GET", qa_value, true)?;

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

        let body_value: XepakValue = cbor2::from_slice(body)
            .map_err(|e| XepakError::Input(format!("Wrong CBOR format: {e}")))?;

        input.body_args = input
            .apply_schema_to("", body_value, true)
            .map_err(to_input_error)?;

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

        let body_value: XepakValue = serde_json::from_slice(body)
            .map_err(|e| XepakError::Input(format!("Wrong JSON format: {e}")))?;

        input.body_args = input
            .apply_schema_to("", body_value, true)
            .map_err(to_input_error)?;

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
