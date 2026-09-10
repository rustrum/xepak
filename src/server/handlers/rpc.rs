use std::{pin::Pin, sync::Arc};

use actix_web::{Handler, HttpResponse, dev::HttpServiceFactory, web};

use crate::{
    cfg::{EndpointSpecs, ResourceRef},
    server::{handlers::EndpointHandlerArgs, processor::PreProcessorHandler},
};

#[derive(Clone)]
pub struct RpcHandler {
    _rref: ResourceRef,
    ep: Arc<EndpointSpecs>,
    resource_fn_key: String,
    processors: Arc<Vec<Box<dyn PreProcessorHandler>>>,
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
