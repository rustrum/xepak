use std::{pin::Pin, sync::Arc};

use actix_web::{Handler, HttpResponse, dev::HttpServiceFactory, web};
use serde::{Deserialize, Serialize};

use crate::{
    cfg::ResourceRef, server::{cfg::{EndpointRpcSpecs, EndpointSpecs}, handlers::EndpointHandlerArgs, processor::PreProcessorHandler},
};

pub const JSON_RPC_VERSION: &str = "2.0";

#[derive(Clone)]
pub struct RpcHandler {
    _rref: ResourceRef,
    ep: Arc<EndpointRpcSpecs>,
    resource_fn_key: String,
    pre_processors: Arc<Vec<Box<dyn PreProcessorHandler>>>,
}

impl RpcHandler {
    fn parse(&self) {}
}

impl Handler<EndpointHandlerArgs> for RpcHandler {
    type Output = HttpResponse;
    type Future = Pin<Box<dyn Future<Output = Self::Output> + 'static>>;

    fn call(&self, (req, state, body): EndpointHandlerArgs) -> Self::Future {
        unimplemented!();
        // tracing::debug!("Handler CALL called for {:?}", self.ep);
        // let this = self.clone();
        // Box::pin(async move { this.handle(req, state, body).await })
    }
}

impl HttpServiceFactory for RpcHandler {
    fn register(self, config: &mut actix_web::dev::AppService) {
        unimplemented!();
        let name = format!("Entrypoint: {}", self.ep.uri);
        tracing::debug!("Registering [{:?}]: {name}", std::thread::current().id());

        web::resource(self.ep.uri.clone())
            .route(web::route().to(self))
            // .route(web::route().to(move |req, state, body| {
            //     let h = self.clone();
            //     async move { h.handle(req, state, body).await }
            // }))
            // .route(web::route().to(self))
            .register(config);

        // web::resource("/user/list")
        //     // .route(web::route().to(self))
        //     // .route(web::route().to(move |req, state, body| {
        //     //     let h = self.clone();
        //     //     async move { h.handle(req, state, body).await }
        //     // }))
        //     // .route(web::route().to(self))
        //     .register(config);
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(untagged)]
pub enum RpcId {
    Number(i64),
    String(String),
    Null,
}

/// JSON-RPC 2.0 request envelope.
#[derive(Deserialize)]
struct JsonRpcRequest<T> {
    jsonrpc: String,
    id: Option<RpcId>,
    method: String,
    params: Option<T>,
}

#[derive(Serialize)]
struct JsonRpcResponse<T> {
    jsonrpc: String,
    id: Option<RpcId>,
    result: Option<T>,
}

#[derive(Serialize)]
struct JsonRpcError {
    jsonrpc: String,
    id: Option<RpcId>,
    error: JsonRpcErrorPayload,
}

#[derive(Serialize)]
struct JsonRpcErrorPayload {
    code: isize,
    message: String,
    data: Option<String>,
}
