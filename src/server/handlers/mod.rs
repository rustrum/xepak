// mod mcp;
mod rest;
// mod rpc;

// pub use mcp::*;
pub use rest::*;
// pub use rpc::*;

use actix_web::{
    HttpRequest, HttpResponse, HttpResponseBuilder,
    body::BoxBody,
    http::{StatusCode, header::CONTENT_TYPE},
    web::{Bytes, Data},
};

use crate::{
    server::{CONTENT_TYPE_CBOR, CONTENT_TYPE_JSON, LIMIT_HEADER, OFFSET_HEADER, XepakAppData},
    xepak_data::XepakValue,
};

pub type EndpointHandlerArgs = (HttpRequest, Data<XepakAppData>, Bytes);

fn to_json_response(
    code: StatusCode,
    data: &XepakValue,
    limit: usize,
    offset: usize,
) -> HttpResponse<BoxBody> {
    match data.to_json() {
        Ok(body) => {
            let mut resp = HttpResponseBuilder::new(code);
            resp.append_header((CONTENT_TYPE, CONTENT_TYPE_JSON));
            if limit > 0 {
                resp.append_header((LIMIT_HEADER, limit.to_string()));
            }
            if offset > 0 {
                resp.append_header((OFFSET_HEADER, offset.to_string()));
            }

            resp.body(body)
        }
        Err(e) => {
            tracing::error!("Can't serialize response: {e}");
            HttpResponse::InternalServerError().body(format!("{e}"))
        }
    }
}

fn to_cbor_response(
    code: StatusCode,
    data: &XepakValue,
    limit: usize,
    offset: usize,
) -> HttpResponse<BoxBody> {
    match data.to_cbor_vec() {
        Ok(body) => {
            let mut resp = HttpResponseBuilder::new(code);
            resp.append_header((CONTENT_TYPE, CONTENT_TYPE_CBOR));
            if limit > 0 {
                resp.append_header((LIMIT_HEADER, limit.to_string()));
            }
            if offset > 0 {
                resp.append_header((OFFSET_HEADER, offset.to_string()));
            }

            resp.body(body)
        }
        Err(e) => {
            tracing::error!("Can't serialize response: {e}");
            HttpResponse::InternalServerError().body(format!("{e}"))
        }
    }
}
