use std::collections::HashMap;

use actix_web::{
    HttpRequest,
    http::{Method, header::CONTENT_TYPE},
    web::{Bytes, Data},
};
use serde::Deserialize;

use crate::{
    XepakError,
    auth::{AuthorizeProcessor, SimpleAuthenticationProcessor},
    schema::validate_with_schema,
    server::{CONTENT_TYPE_CBOR, RequestInput, XepakAppData},
    xepak_data::XepakValue,
};

pub const PRIORITY_FIRST: u16 = 60_000;

pub const PRIORITY_NORMAL: u16 = 30_000;

pub const PRIORITY_LAST: u16 = 1000;

/// Define request processors variants.
#[derive(Clone, Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum PreProcessor {
    /// Referenece to shared pre-processor
    Ref {
        id: String,
    },

    /// Extracts body argurments to request object.
    ParseBodyArgs,

    SimpleAuthentication {
        /// Allows anonymous authentication
        #[serde(default)]
        anonymous_auth: bool,
    },

    Authorize {
        rules: String,
    },
}

pub fn init_required_pre_processors() -> Vec<Box<dyn PreProcessorHandler>> {
    vec![
        Box::new(QueryArgsProcessor {}),
        Box::new(InputArgsValidator {}),
    ]
}

pub fn build_pre_processor(
    position: u16,
    specs: &PreProcessor,
    shared: &HashMap<String, PreProcessor>,
) -> Result<Box<dyn PreProcessorHandler>, XepakError> {
    match specs {
        PreProcessor::Ref { id } => {
            if let Some(sspecs) = shared.get(id) {
                if let PreProcessor::Ref { .. } = sspecs {
                    Err(XepakError::Cfg(
                        "Ref types are not allowed in shared pre-processors".to_string(),
                    ))
                } else {
                    build_pre_processor(position, sspecs, shared)
                }
            } else {
                Err(XepakError::Cfg(format!(
                    "Can't find pre-processor by reference: \"{id}\""
                )))
            }
        }
        PreProcessor::ParseBodyArgs => Ok(Box::new(BodyToArgsProcessor::default())),
        PreProcessor::SimpleAuthentication { anonymous_auth } => Ok(Box::new(
            SimpleAuthenticationProcessor::new(position, *anonymous_auth),
        )),
        PreProcessor::Authorize { rules } => {
            Ok(Box::new(AuthorizeProcessor::new(position, rules.as_ref())?))
        }
    }
}

pub trait PreProcessorHandler: Send + Sync {
    /// Handler with higher priority will be processed first
    fn priority(&self) -> u16;

    fn handle(
        &self,
        req: &HttpRequest,
        state: &Data<XepakAppData>,
        body: &Bytes,
        input: &mut RequestInput,
    ) -> Result<(), XepakError>;
}

#[inline]
pub fn adjust_priority(current: u16, order: u16) -> u16 {
    if current > order {
        return 0;
    }
    current - order
}

/// Execute validation logic for all input arguments according to schema.
pub struct InputArgsValidator {}
impl PreProcessorHandler for InputArgsValidator {
    fn priority(&self) -> u16 {
        PRIORITY_LAST
    }

    fn handle(
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

/// Handle arguments from query string arguments.
/// Skip query string args POST/PUT requests (basically anything that have request body)
pub struct QueryArgsProcessor {}

impl PreProcessorHandler for QueryArgsProcessor {
    fn priority(&self) -> u16 {
        PRIORITY_FIRST + 1000
    }

    fn handle(
        &self,
        req: &HttpRequest,
        _state: &Data<XepakAppData>,
        _body: &Bytes,
        input: &mut RequestInput,
    ) -> Result<(), XepakError> {
        if req.method() == Method::PUT || req.method() == Method::POST {
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

#[derive(Default)]
pub struct BodyToArgsProcessor {}

impl BodyToArgsProcessor {
    pub fn handle_cbor_body(
        &self,
        _body: &Bytes,
        _input: &mut RequestInput,
    ) -> Result<(), XepakError> {
        todo!("Implement CBOR parsing")
    }

    pub fn handle_json_body(
        &self,
        body: &Bytes,
        input: &mut RequestInput,
    ) -> Result<(), XepakError> {
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

impl PreProcessorHandler for BodyToArgsProcessor {
    fn handle(
        &self,
        req: &HttpRequest,
        _state: &Data<XepakAppData>,
        body: &Bytes,
        input: &mut RequestInput,
    ) -> Result<(), XepakError> {
        if req.method() != Method::POST && req.method() != Method::PUT {
            return Ok(());
        }

        let cbor_body = if let Some(accept) = req.headers().get(CONTENT_TYPE)
            && accept.eq(CONTENT_TYPE_CBOR)
        {
            true
        } else {
            false
        };

        if cbor_body {
            self.handle_cbor_body(body, input)
        } else {
            self.handle_json_body(body, input)
        }
    }

    fn priority(&self) -> u16 {
        PRIORITY_NORMAL
    }
}
