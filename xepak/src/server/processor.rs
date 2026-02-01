use std::collections::HashMap;

use actix_web::{
    HttpRequest,
    http::{Method, header::CONTENT_TYPE},
    web::{Bytes, Data},
};
use serde::Deserialize;

use crate::{
    XepakError,
    server::{CONTENT_TYPE_CBOR, RequestArgs, XepakAppData},
    types::{Schema, XepakValue},
};

/// Define request processors variants.
#[derive(Clone, Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum PreProcessor {
    BodyToArgs,
}

pub trait PreProcessorHandler {
    /// Handler with higher priority will be processed first
    fn priority(&self) -> i16 {
        0
    }

    fn handle(
        &self,
        req: &HttpRequest,
        state: &Data<XepakAppData>,
        body: &Bytes,
        input: &mut RequestArgs,
    ) -> Result<(), XepakError>;
}

/// Handle arguments from query string arguments.
/// Skip query string args POST/PUT requests (basically anything that have request body)
pub struct QueryArgsProcessor {}

impl PreProcessorHandler for QueryArgsProcessor {
    fn handle(
        &self,
        req: &HttpRequest,
        _state: &Data<XepakAppData>,
        _body: &Bytes,
        input: &mut RequestArgs,
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
            input.set_arg_validate(k, v)?;
        }

        Ok(())
    }
}
pub struct BodyToArgsProcessor {}

impl BodyToArgsProcessor {
    pub fn handle_cbor_body(
        &self,
        body: &Bytes,
        input: &mut RequestArgs,
    ) -> Result<(), XepakError> {
        todo!("Implement CBOR parsing")
    }

    pub fn handle_json_body(
        &self,
        body: &Bytes,
        input: &mut RequestArgs,
    ) -> Result<(), XepakError> {
        let json_request: serde_json::Value = serde_json::from_slice(body)
            .map_err(|e| XepakError::Input(format!("Wrong JSON format: {e}")))?;

        let Some(json_object) = json_request.as_object() else {
            return Err(XepakError::Input(
                "JSON request body only allowed to be an object".to_string(),
            ));
        };

        for (key, value) in json_object {
            let xvalue = if value.is_null() {
                XepakValue::Null
            } else {
                return Err(XepakError::Input(format!(
                    "(๑•̀ᗝ•́)૭ Root JSON must NOT have any nested arrays or objects. See \"{key}\" property."
                )));
            };

            input.set_arg_validate(key.clone(), xvalue)?;
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
        input: &mut RequestArgs,
    ) -> Result<(), XepakError> {
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
}
