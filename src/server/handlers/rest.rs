use actix_web::{
    Handler, HttpRequest, HttpResponse,
    dev::HttpServiceFactory,
    http::{Method, StatusCode, header::ACCEPT},
    web::{self, Bytes, Data},
};
use std::{pin::Pin, sync::Arc};

use super::{EndpointHandlerArgs, build_pre_processors, to_cbor_response, to_json_response};
use crate::{
    XepakError,
    cfg::ResourceRef,
    server::{
        CONTENT_TYPE_CBOR, RequestInput, XepakAppData,
        cfg::EndpointSpecs,
        handlers::{handle_resource, validate_resource},
        processor::PreProcessorHandler,
        to_error_object,
    },
    xepak_data::{XepakType, XepakValue},
};

#[derive(Clone)]
pub struct EndpointHandler {
    _rref: ResourceRef,
    ep: Arc<EndpointSpecs>,
    script_cache_key: String,
    processors: Arc<Vec<Box<dyn PreProcessorHandler>>>,
}

impl EndpointHandler {
    pub fn new(
        rref: ResourceRef,
        ep: EndpointSpecs,
        app: &XepakAppData,
    ) -> Result<Self, XepakError> {
        validate_resource(app, &ep.resource)?;

        let pre_processors = build_pre_processors(
            app,
            rref.nested("pp"),
            &ep.pre_processors,
            ep.pre_processors_ignore_default,
        )?;
        Ok(Self {
            script_cache_key: rref.nested("resource-fn").to_string(),
            _rref: rref,
            ep: Arc::new(ep),

            // handler_lua: Arc::new(handler_lua),
            processors: Arc::new(pre_processors),
        })
    }

    fn validate_method_allowed(&self, req: &HttpRequest) -> Result<(), XepakError> {
        let am = &self.ep.allow_methods;
        if am.is_empty() && req.method() == Method::GET {
            return Ok(());
        }
        if am.contains(req.method()) {
            return Ok(());
        }

        Err(XepakError::Input(format!(
            "(๑•ᗝ•)૭ Request method {} not allowed!",
            req.method()
        )))
    }

    async fn handle_request(
        &self,
        req: HttpRequest,
        state: Data<XepakAppData>,
        body: Bytes,
    ) -> HttpResponse {
        tracing::debug!("Handler called for {:?}", self.ep.uri);

        let mut ri = match self.pre_process_request(&req, &state, &body).await {
            Ok(result) => result,
            Err(err) => {
                let (status_code, data) = to_error_object(err);
                return self.data_to_response(&req, None, status_code, &data);
            }
        };
        // Maybe it should be in processors
        ri.parse_offset_limit(&self.ep.offset_arg, &self.ep.limit_arg, self.ep.fetch_limit);

        let resource_exec = handle_resource(
            &ri,
            &state,
            &self.script_cache_key,
            &self.ep.resource,
            self.ep.single_record_response,
        )
        .await;

        let data = match resource_exec {
            Ok(d) => d,
            Err(err) => {
                let (status_code, data) = to_error_object(err);
                return self.data_to_response(&req, None, status_code, &data);
            }
        };

        // Apply output schema if configured
        let data = match self.apply_output_schema(data) {
            Ok(d) => d,
            Err(err) => {
                let (status_code, data) = to_error_object(err);
                return self.data_to_response(&req, Some(&ri), status_code, &data);
            }
        };

        self.build_response(&req, &ri, data)
    }

    fn apply_output_schema(&self, data: XepakValue) -> Result<XepakValue, XepakError> {
        if self.ep.schema.output.is_empty() {
            return Ok(data);
        }
        let schema = &self.ep.schema.output;
        crate::schema::apply_schema(schema, data, "output", false, true).map_err(Into::into)
    }

    async fn pre_process_request(
        &self,
        req: &HttpRequest,
        state: &Data<XepakAppData>,
        body: &Bytes,
    ) -> Result<RequestInput, XepakError> {
        self.validate_method_allowed(req)?;

        let mut input = RequestInput::new(
            self.ep.schema.input.clone(),
            self.ep.strict_schema,
            req.method().to_string(),
            &self.ep.uri,
            req.path(),
        );

        for p in self.processors.as_ref() {
            p.handle(req, state, body, &mut input).await?;
        }

        Ok(input)
    }

    fn data_to_response(
        &self,
        req: &HttpRequest,
        input: Option<&RequestInput>,
        status_code: StatusCode,
        data: &XepakValue,
    ) -> HttpResponse {
        let cbor_response = if let Some(accept) = req.headers().get(ACCEPT)
            && accept.eq(CONTENT_TYPE_CBOR)
        {
            true
        } else {
            false
        };

        // TODO should normalize headers output instead providing limit/offset
        let (limit, offset) = match input {
            Some(inp) => (inp.get_limit(), inp.get_offset()),
            None => (0, 0),
        };

        if cbor_response {
            to_cbor_response(status_code, data, limit, offset)
        } else {
            to_json_response(status_code, data, limit, offset)
        }
    }
    fn build_response(
        &self,
        req: &HttpRequest,
        input: &RequestInput,
        data: XepakValue,
    ) -> HttpResponse {
        match data.get_type() {
            XepakType::Null => {
                let (status_code, err_data) = to_error_object(XepakError::NotFound(format!(
                    "Record not found at URI: {}",
                    req.uri()
                )));

                self.data_to_response(req, Some(input), status_code, &err_data)
            }
            XepakType::Dict | XepakType::Tuple => {
                self.data_to_response(req, Some(input), StatusCode::OK, &data)
            }
            _ => {
                let (status_code, err_data) = to_error_object(XepakError::NotConsistent(format!(
                    "Return data types could be only Array|Map|Null not {}",
                    data.get_type()
                )));
                self.data_to_response(req, Some(input), status_code, &err_data)
            }
        }
    }
}

impl Handler<EndpointHandlerArgs> for EndpointHandler {
    type Output = HttpResponse;
    type Future = Pin<Box<dyn Future<Output = Self::Output> + 'static>>;

    fn call(&self, (req, state, body): EndpointHandlerArgs) -> Self::Future {
        tracing::debug!("Calling REST endpoint CALL function with {:?}", self.ep);
        let this = self.clone();
        Box::pin(async move { this.handle_request(req, state, body).await })
    }
}

impl HttpServiceFactory for EndpointHandler {
    fn register(self, config: &mut actix_web::dev::AppService) {
        let name = format!("Entrypoint: {}", self.ep.uri);
        tracing::debug!("Registering [{:?}]: {name}", std::thread::current().id());

        web::resource(self.ep.uri.clone())
            .route(web::route().to(self))
            .register(config);
    }
}
