// mod mcp;
mod rest;
mod rpc;

// pub use mcp::*;
pub use rest::*;
pub use rpc::*;

use actix_web::{
    HttpRequest, HttpResponse, HttpResponseBuilder,
    body::BoxBody,
    http::{StatusCode, header::CONTENT_TYPE},
    web::{Bytes, Data},
};

use crate::{
    XepakError,
    cfg::ResourceRef,
    script_lua::{execute_lua_script, init_lua_env_fn},
    server::{
        CONTENT_TYPE_CBOR, CONTENT_TYPE_JSON, LIMIT_HEADER, OFFSET_HEADER, RequestInput,
        XepakAppData,
        cfg::ResourceSpecs,
        processor::{
            PreProcessor, PreProcessorHandler, build_pre_processor, init_required_pre_processors,
        },
    },
    storage::{ResourceRequest, SqlxRequestArgs, Storage},
    xepak_data::XepakValue,
};

pub type EndpointHandlerArgs = (HttpRequest, Data<XepakAppData>, Bytes);

/// This is just a validation to fail early if LUA syntax incorrect
pub fn validate_resource(app: &XepakAppData, resource: &ResourceSpecs) -> Result<(), XepakError> {
    match resource {
        ResourceSpecs::QueryScriptLua { script, .. } | ResourceSpecs::DataScript { script, .. } => {
            init_lua_env_fn(app, script)?;
        }
        _ => {}
    }
    Ok(())
}

pub fn build_pre_processors(
    app: &XepakAppData,
    rref: ResourceRef,
    pre_processors: &[PreProcessor],
    ignore_default_pre_processors: bool,
) -> Result<Vec<Box<dyn PreProcessorHandler>>, XepakError> {
    let mut processors: Vec<Box<dyn PreProcessorHandler>> = init_required_pre_processors();

    let mut order = 0u16;
    // Default pre processors
    if !ignore_default_pre_processors {
        for specs in &app.default_pre_processors {
            order += 1;
            processors.push(build_pre_processor(
                app,
                &rref,
                order,
                specs,
                &app.shared_pre_processors,
            )?);
        }
    }
    // Pre processors for current handler
    for specs in pre_processors {
        order += 1;
        processors.push(build_pre_processor(
            app,
            &rref,
            order,
            specs,
            &app.shared_pre_processors,
        )?);
    }

    // Here PP order could change (depends on the basic priority each handlers had)
    processors.sort_by_key(|b| std::cmp::Reverse(b.priority()));

    Ok(processors)
}

async fn handle_resource(
    input: &RequestInput,
    state: &Data<XepakAppData>,
    cache_key: &str,
    resource: &ResourceSpecs,
    single_record_response: bool,
) -> Result<XepakValue, XepakError> {
    match resource {
        ResourceSpecs::Query { data_source, query } => {
            let Some(ds) = state.get_data_source(data_source) else {
                return Err(XepakError::Cfg(format!(
                    "Data source does not exists \"{data_source}\""
                )));
            };

            let rr = ResourceRequest::new(query, input);
            run_query(ds, rr, single_record_response).await
        }

        ResourceSpecs::QueryScriptLua {
            data_source,
            script,
        } => {
            let Some(ds) = state.get_data_source(data_source) else {
                return Err(XepakError::Cfg(format!(
                    "Data source does not exists \"{data_source}\""
                )));
            };

            let query =
                execute_lua_script::<String>(state, input.clone(), cache_key, script).await?;

            let rr = ResourceRequest::new(&query, input);
            run_query(ds, rr, single_record_response).await
        }
        ResourceSpecs::DataScript { script, .. } => {
            execute_lua_script(state, input.clone(), cache_key, script).await
        }
    }
}

async fn run_query<RA: SqlxRequestArgs>(
    ds: &Storage,
    request: ResourceRequest<'_, RA>,
    query_one: bool,
) -> Result<XepakValue, XepakError> {
    if query_one {
        ds.query_one(request)
            .await
            .map(|v| v.unwrap_or(XepakValue::Null))
    } else {
        ds.query(request).await.map(Into::into)
    }
}

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
