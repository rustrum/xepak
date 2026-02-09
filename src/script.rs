use std::{collections::HashMap, sync::Arc};

use actix_web::web::Data;
use rhai::{
    AST, CustomType, Dynamic, Engine, EvalAltResult, NativeCallContext, ParseError, Position, Scope,
};
use sqlx_core::logger;
use tokio::runtime::Handle;

use crate::{
    XepakError,
    server::{RequestInput, XepakAppData},
    storage::ResourceRequest,
    types::XepakValue,
};

#[derive(Debug, Clone)]
pub struct RhaiRequestContext {
    args: RequestInput,
}

impl From<RequestInput> for RhaiRequestContext {
    fn from(args: RequestInput) -> Self {
        Self { args }
    }
}

impl RhaiRequestContext {
    pub fn has_arg(&mut self, arg_name: &str) -> bool {
        self.args.has_any_arg(arg_name)
    }

    pub fn get_arg(&mut self, arg_name: &str) -> Dynamic {
        match self.args.get_arg_value(arg_name).cloned() {
            Some(v) => xepak_to_dynamic(&v),
            None => Dynamic::UNIT,
        }
    }
}

impl CustomType for RhaiRequestContext {
    fn build(mut builder: rhai::TypeBuilder<Self>) {
        builder.with_name("RhaiRequestContext");
        builder.with_fn("has_arg", Self::has_arg);
        builder.with_fn("get_arg", Self::get_arg);
    }
}

#[derive(Default, Debug, Clone)]
pub struct RhaiQueryBuilder {
    query: Vec<String>,
}

impl RhaiQueryBuilder {
    pub fn new_str(q: String) -> Self {
        Self { query: vec![q] }
    }

    pub fn add(&mut self, part: String) {
        self.query.push(part);
    }

    pub fn add_joined_parts(
        &mut self,
        prefix: String,
        parts: rhai::Array,
        separator: String,
        suffix: String,
    ) {
        if parts.is_empty() {
            return;
        }

        if !prefix.is_empty() {
            self.query.push(prefix);
        }

        let parts: Vec<String> = parts.iter().map(|v| v.to_string()).collect();

        self.query.push(parts.join(&format!(" {separator} ")));

        if !suffix.is_empty() {
            self.query.push(suffix);
        }
    }

    pub fn build(&mut self) -> String {
        self.query.join(" ")
    }
}

impl CustomType for RhaiQueryBuilder {
    fn build(mut builder: rhai::TypeBuilder<Self>) {
        builder.with_name("RhaiQueryBuilder");
        builder.with_fn("query_builder", Self::default);
        builder.with_fn("query_builder", Self::new_str);
        builder.with_fn("add", Self::add);
        builder.with_fn("add_joined_parts", Self::add_joined_parts);
        builder.with_fn("build", Self::build);
    }
}

impl CustomType for XepakValue {
    fn build(mut builder: rhai::TypeBuilder<Self>) {
        builder.with_name("XepakValue");
        builder.with_fn("as_dynamic", |obj: &mut XepakValue| xepak_to_dynamic(obj));
        // builder.with_fn("is_null", |obj: &mut XepakValue| obj.is_null());
        // builder.with_fn("as_bool", |obj: &mut XepakValue| obj.as_bool());
        // builder.with_fn("as_int", |obj: &mut XepakValue| obj.as_int());
        // builder.with_fn("as_float", |obj: &mut XepakValue| obj.as_float());
        // builder.with_fn("as_string", |obj: &mut XepakValue| obj.as_string());
    }
}

// pub fn init_rhai_script(script: &str) -> Result<(Engine, AST), XepakError> {
//     let rhai = build_rhai_engine();
//     let ast = build_rhai_ast(&rhai, script)?;
//     Ok((rhai, ast))
// }

pub fn build_rhai_ast(rhai: &Engine, script: &str) -> Result<AST, ParseError> {
    let ast = rhai.compile(script)?;
    Ok(ast)
}

pub fn build_rhai_engine(state: &XepakAppData) -> Engine {
    let handle = Handle::current();

    let mut rhai = Engine::new();

    rhai.on_print(|s| {
        tracing::info!("RHAI: {s}");
    });

    rhai.on_debug(|s, src, pos| {
        tracing::debug!("RHAI: {} @ {pos:?} > {s}", src.unwrap_or_default());
    });

    rhai.build_type::<RhaiRequestContext>();
    rhai.build_type::<RhaiQueryBuilder>();
    rhai.build_type::<XepakValue>();
    rhai.register_fn("error_input", error_input);
    rhai.register_fn("error_server", error_server);
    rhai.register_fn("error_not_found", error_not_found);
    rhai.register_fn("error_forbidden", error_forbidden);

    let f_state = state.clone();
    let f_handle = handle.clone();
    rhai.register_fn(
        "storage_query",
        move |ctx: NativeCallContext, query: &str, args: rhai::Map| {
            storage_query(&f_state, &f_handle, "", query, args)
                .map_err(|err| to_eval_alt_result_ctx(err, Some(ctx)))
        },
    );

    let f_state = state.clone();
    let f_handle = handle.clone();
    rhai.register_fn(
        "storage_query_one",
        move |ctx: NativeCallContext, query: &str, args: rhai::Map| {
            storage_query_one(&f_state, &f_handle, "", query, args)
                .map_err(|err| to_eval_alt_result_ctx(err, Some(ctx)))
        },
    );

    let f_state = state.clone();
    let f_handle = handle.clone();
    rhai.register_fn(
        "storage_query_value",
        move |ctx: NativeCallContext, query: &str, args: rhai::Map| {
            storage_query_value(&f_state, &f_handle, "", query, args)
                .map_err(|err| to_eval_alt_result_ctx(err, Some(ctx)))
        },
    );

    rhai
}

fn prepare_args(args_in: rhai::Map) -> Result<HashMap<String, XepakValue>, XepakError> {
    let mut args = HashMap::new();

    for (k, v) in args_in {
        args.insert(k.to_string(), dynamic_to_xepak(&v)?);
    }

    Ok(args)
}

pub fn storage_query(
    state: &XepakAppData,
    handle: &Handle,
    ds_name: &str,
    query: &str,
    dyn_args: rhai::Map,
) -> Result<Dynamic, XepakError> {
    let Some(ds) = state.get_data_source(ds_name) else {
        return Err(XepakError::Cfg(format!(
            "Data source does not exists \"{ds_name}\""
        )));
    };

    let args = RequestInput::new_in_script(prepare_args(dyn_args)?, 0, 0);

    let rr = ResourceRequest::new(query, &args);

    let result = handle
        .block_on(async { ds.query(rr).await })?
        .into_iter()
        .map(|row| {
            row.into_iter()
                .map(|(k, v)| (k, xepak_to_dynamic(&v)))
                .collect::<HashMap<String, Dynamic>>()
        })
        .collect();

    Ok(result)
}

pub fn storage_query_one(
    state: &XepakAppData,
    handle: &Handle,
    ds_name: &str,
    query: &str,
    dyn_args: rhai::Map,
) -> Result<Dynamic, XepakError> {
    let Some(ds) = state.get_data_source(ds_name) else {
        return Err(XepakError::Cfg(format!(
            "Data source does not exists \"{ds_name}\""
        )));
    };

    let args = RequestInput::new_in_script(prepare_args(dyn_args)?, 0, 0);

    let rr = ResourceRequest::new(query, &args);

    let result = handle.block_on(async { ds.query_one(rr).await })?.map(|v| {
        v.into_iter()
            .map(|(k, v)| (k.into(), xepak_to_dynamic(&v)))
            .collect::<rhai::Map>()
    });

    Ok(result.map(Dynamic::from_map).unwrap_or(Dynamic::UNIT))
}

pub fn storage_query_value(
    state: &XepakAppData,
    handle: &Handle,
    ds_name: &str,
    query: &str,
    dyn_args: rhai::Map,
) -> Result<Dynamic, XepakError> {
    let Some(ds) = state.get_data_source(ds_name) else {
        return Err(XepakError::Cfg(format!(
            "Data source does not exists \"{ds_name}\""
        )));
    };

    let args = RequestInput::new_in_script(prepare_args(dyn_args)?, 0, 0);

    let rr = ResourceRequest::new(query, &args);

    let result = handle.block_on(async { ds.query_value(rr).await })?;

    tracing::debug!("Result {result:?}");

    Ok(xepak_to_dynamic(&result))
}

pub async fn execute_script_blocking(
    state: Data<XepakAppData>,
    uri: String,
    rhai: Arc<Option<Engine>>,
    ast: Arc<Option<AST>>,
    input: RequestInput,
) -> Result<Dynamic, XepakError> {
    tokio::task::spawn_blocking(move || {
        let Some(rhai) = rhai.as_ref() else {
            return Err(XepakError::Unexpected(format!(
                "Script engine must exists for handler {uri}"
            )));
        };
        let Some(ast) = ast.as_ref() else {
            return Err(XepakError::Unexpected(format!(
                "Query script AST must already exists for handler {uri}"
            )));
        };
        let mut scope = Scope::new();
        scope.set_value("ctx", RhaiRequestContext::from(input));

        match rhai.eval_ast_with_scope::<Dynamic>(&mut scope, ast) {
            Ok(result) => Ok(result),
            Err(e) => {
                return Err(if let EvalAltResult::ErrorRuntime(ref value, pos) = *e {
                    if let Some(xerror) = value.clone().try_cast::<XepakError>() {
                        // no need to log here, this could be an expected behavior
                        if !xerror.is_expectable() {
                            tracing::error!("Script {pos}: {xerror}");
                        }
                        xerror
                    } else {
                        tracing::error!("Script {pos}: {e}");
                        Arc::new(*e).into()
                    }
                } else {
                    tracing::error!("Script execution: {e}");
                    Arc::new(*e).into()
                });
            }
        }
    })
    .await
    .map_err(XepakError::other)?
}

pub fn xepak_to_dynamic(value: &XepakValue) -> Dynamic {
    match value {
        XepakValue::Null => Dynamic::UNIT,
        XepakValue::Boolean(v) => Dynamic::from_bool(*v),
        XepakValue::Integer(v) => Dynamic::from_int(*v as i64),
        XepakValue::Float(v) => Dynamic::from_float(*v),
        XepakValue::Text(v) => Dynamic::from(v.clone()),
    }
}

pub fn dynamic_to_xepak(v: &Dynamic) -> Result<XepakValue, XepakError> {
    let r = if v.is_unit() {
        XepakValue::Null
    } else if v.is_bool() {
        XepakValue::Boolean(
            v.as_bool()
                .map_err(|e| XepakError::Unexpected(e.to_string()))?,
        )
    } else if v.is_char() || v.is_string() {
        XepakValue::Text(v.to_string())
    } else if v.is_int() {
        XepakValue::Integer(
            v.as_int()
                .map_err(|e| XepakError::Unexpected(e.to_string()))? as i128,
        )
    } else if v.is_float() {
        XepakValue::Float(
            v.as_float()
                .map_err(|e| XepakError::Unexpected(e.to_string()))?,
        )
    } else {
        return Err(XepakError::Convert(format!(
            "{} not compatible with XepakValue",
            v.type_name()
        )));
    };

    Ok(r)
}

pub fn error_input(ctx: NativeCallContext, message: String) -> Result<(), Box<EvalAltResult>> {
    let err = XepakError::Input(message.to_string());
    Err(EvalAltResult::ErrorRuntime(Dynamic::from(err), ctx.call_position()).into())
}

pub fn error_forbidden(ctx: NativeCallContext, message: String) -> Result<(), Box<EvalAltResult>> {
    let err = XepakError::Forbidden(message.to_string());
    Err(EvalAltResult::ErrorRuntime(Dynamic::from(err), ctx.call_position()).into())
}

pub fn error_not_found(ctx: NativeCallContext, message: String) -> Result<(), Box<EvalAltResult>> {
    let err = XepakError::NotFound(message);
    Err(EvalAltResult::ErrorRuntime(Dynamic::from(err), ctx.call_position()).into())
}

pub fn error_server(ctx: NativeCallContext, message: String) -> Result<(), Box<EvalAltResult>> {
    let err = XepakError::WeScrewed(message);
    Err(EvalAltResult::ErrorRuntime(Dynamic::from(err), ctx.call_position()).into())
}

pub fn to_eval_alt_result(err: XepakError) -> Box<EvalAltResult> {
    to_eval_alt_result_ctx(err, None)
}

pub fn to_eval_alt_result_ctx(
    err: XepakError,
    ctx: Option<NativeCallContext>,
) -> Box<EvalAltResult> {
    let pos = ctx.map(|c| c.call_position()).unwrap_or(Position::NONE);
    Box::new(EvalAltResult::ErrorRuntime(Dynamic::from(err), pos))
}
