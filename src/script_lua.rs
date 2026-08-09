use std::{cell::RefCell, collections::HashMap, sync::Arc};

use actix_web::web::Data;
use mlua::{
    Error as LuaError, ExternalError, FromLua, FromLuaMulti, Function, IntoLua, Lua, Table,
    UserData, UserDataMethods, Value,
};

use crate::{
    XepakError,
    server::{RequestInput, XepakAppData},
    storage::ResourceRequest,
    xepak_data::XepakValue,
};

thread_local! {
    static LUA_CACHE: RefCell<Option<Lua>> = const { RefCell::new(None) };
}

/// Creates new Lua instance or returns current one from thread_local cache.
pub fn lua_load_engine(app_state: &XepakAppData) -> Result<Lua, XepakError> {
    LUA_CACHE.with(|cell| {
        if let Some(cached) = (*cell.borrow()).as_ref() {
            return Ok(cached.clone());
        }

        let lua = build_lua_engine(app_state)?;

        let mut slot = cell.borrow_mut();
        *slot = Some(lua.clone());

        Ok(lua)
    })
}

/// App data is stored inside each Lua VM.
struct LuaAppData {
    state: XepakAppData,
}

impl LuaAppData {
    fn load(lua: &Lua) -> mlua::Result<mlua::AppDataRef<'_, LuaAppData>> {
        lua.app_data_ref::<LuaAppData>()
            .ok_or_else(|| LuaError::runtime("LuaAppData not initialized (o_O)."))
    }
}

#[derive(Debug, Clone)]
struct LuaRequestContext {
    input: RequestInput,
}

impl From<RequestInput> for LuaRequestContext {
    fn from(input: RequestInput) -> Self {
        Self { input }
    }
}

impl LuaRequestContext {
    fn load_input(_: &Lua, this: &Self, _: ()) -> mlua::Result<RequestInput> {
        Ok(this.input.clone())
    }

    fn has_arg(_: &Lua, this: &Self, arg_name: String) -> mlua::Result<bool> {
        Ok(this.input.has_any_arg(&arg_name))
    }

    fn get_arg(lua: &Lua, this: &Self, arg_name: String) -> mlua::Result<Value> {
        match this.input.get_arg_value(&arg_name) {
            Some(v) => v.into_lua(lua),
            None => Ok(Value::Nil),
        }
    }

    fn set_arg(lua: &Lua, this: &mut Self, (arg_name, value): (String, Value)) -> mlua::Result<()> {
        let xvalue = XepakValue::from_lua(value, lua)?;
        this.input
            .set_arg_with_schema(arg_name, xvalue, true)
            .map_err(ExternalError::into_lua_err)
    }
}

impl UserData for LuaRequestContext {
    fn add_methods<M: UserDataMethods<Self>>(methods: &mut M) {
        methods.add_method("load_input", Self::load_input);
        methods.add_method("has_arg", Self::has_arg);
        methods.add_method("get_arg", Self::get_arg);
        methods.add_method_mut("set_arg", Self::set_arg);
    }
}

impl UserData for RequestInput {}

#[derive(Default, Clone)]
struct LuaQueryBuilder {
    query: Vec<String>,
}

impl LuaQueryBuilder {
    fn new(_: &Lua, init: Option<String>) -> mlua::Result<Self> {
        Ok(match init {
            Some(q) => Self { query: vec![q] },
            None => Self::default(),
        })
    }

    fn add(_: &Lua, this: &mut Self, part: String) -> mlua::Result<()> {
        this.query.push(part);
        Ok(())
    }

    fn add_joined_parts(
        _: &Lua,
        this: &mut Self,
        (prefix, parts_table, separator, suffix): (String, Table, String, String),
    ) -> mlua::Result<()> {
        if parts_table.is_empty() {
            return Ok(());
        }

        if !prefix.is_empty() {
            this.query.push(prefix);
        }

        let parts: Vec<String> = parts_table
            .sequence_values::<String>()
            .collect::<mlua::Result<_>>()?;

        this.query.push(parts.join(&format!(" {separator} ")));

        if !suffix.is_empty() {
            this.query.push(suffix);
        }
        Ok(())
    }

    fn build(_: &Lua, this: &Self, _: ()) -> mlua::Result<String> {
        Ok(this.query.join(" "))
    }
}

impl UserData for LuaQueryBuilder {
    fn add_methods<M: UserDataMethods<Self>>(methods: &mut M) {
        methods.add_method_mut("add", Self::add);
        methods.add_method_mut("add_joined_parts", Self::add_joined_parts);
        methods.add_method("build", Self::build);
    }
}

impl IntoLua for XepakValue {
    fn into_lua(self, lua: &Lua) -> mlua::Result<Value> {
        match self {
            XepakValue::Null => Ok(Value::Nil),
            XepakValue::Boolean(v) => Ok(Value::Boolean(v)),
            XepakValue::Integer(v) => Ok(Value::Integer(v as i64)),
            XepakValue::Float(v) => Ok(Value::Number(v)),
            XepakValue::Text(v) => Ok(Value::String(lua.create_string(&v)?)),
            XepakValue::Blob(v) => Ok(Value::String(lua.create_string(&v)?)),
            XepakValue::Map(v) => {
                let t = lua.create_table()?;
                for (k, xv) in v {
                    t.set(k, xv.into_lua(lua)?)?;
                }
                Ok(Value::Table(t))
            }
            XepakValue::Tuple(v) => {
                let t = lua.create_table()?;
                for xv in v {
                    t.push(xv.into_lua(lua)?)?;
                }
                Ok(Value::Table(t))
            }
        }
    }
}

impl FromLua for XepakValue {
    fn from_lua(value: Value, _: &Lua) -> mlua::Result<Self> {
        match value {
            Value::Nil => Ok(XepakValue::Null),
            Value::Boolean(v) => Ok(XepakValue::Boolean(v)),
            Value::Integer(v) => Ok(XepakValue::Integer(v as i128)),
            Value::Number(v) => Ok(XepakValue::Float(v)),
            Value::String(s) => Ok(XepakValue::Text(
                s.to_str()
                    .map_err(|e| LuaError::runtime(e.to_string()))?
                    .to_owned(),
            )),
            // TODO lua handles bytes as string so it is not possible to explicitly convert back to Blob
            other => Err(LuaError::runtime(format!(
                "{} not compatible with XepakValue",
                other.type_name()
            ))),
        }
    }
}

/// Compiles a Lua script into a Function.
pub fn build_lua_function(lua: &Lua, script: &str) -> Result<Function, XepakError> {
    Ok(lua.load(script).into_function()?)
}

/// Creates a Lua VM and registers all globals.
pub fn build_lua_engine(app_state: &XepakAppData) -> Result<Lua, XepakError> {
    let lua = Lua::new();

    // This state can be accesses from any rust function
    lua.set_app_data(LuaAppData {
        state: app_state.clone(),
    });

    lua.globals()
        .set("query_builder", lua.create_function(LuaQueryBuilder::new)?)?;

    lua.globals()
        .set("log_info", lua.create_function(log_info)?)?;
    lua.globals()
        .set("log_debug", lua.create_function(log_debug)?)?;
    lua.globals()
        .set("error_input", lua.create_function(error_input)?)?;
    lua.globals()
        .set("error_not_found", lua.create_function(error_not_found)?)?;
    lua.globals()
        .set("error_forbidden", lua.create_function(error_forbidden)?)?;
    lua.globals()
        .set("error_server", lua.create_function(error_server)?)?;

    register_db_functions(&lua)?;

    Ok(lua)
}

fn register_db_functions(lua: &Lua) -> Result<(), XepakError> {
    lua.globals()
        .set("storage_query", lua.create_async_function(storage_query)?)?;
    lua.globals().set(
        "storage_query_one",
        lua.create_async_function(storage_query_one)?,
    )?;
    lua.globals().set(
        "storage_query_value",
        lua.create_async_function(storage_query_value)?,
    )?;
    Ok(())
}

//
// Storage query functions
//

fn prepare_args(lua: &Lua, args: Value) -> mlua::Result<RequestInput> {
    match args {
        Value::Table(t) => {
            let mut map = HashMap::new();
            for pair in t.pairs::<String, Value>() {
                let (k, v) = pair?;
                map.insert(k, XepakValue::from_lua(v, lua)?);
            }
            Ok(RequestInput::new_in_script(map, 0, 0))
        }
        Value::UserData(ud) => {
            let ri = ud.borrow::<RequestInput>()?;
            Ok(ri.clone())
        }
        _ => Err(LuaError::runtime(
            "storage function: second argument must be a table or RequestInput",
        )),
    }
}

/// Return multiple rows query result as a table.
async fn storage_query(lua: Lua, (query, args): (String, Value)) -> mlua::Result<Table> {
    let input = prepare_args(&lua, args)?;
    let state = {
        let app = LuaAppData::load(&lua)?;
        app.state.clone()
    };
    let Some(ds) = state.get_data_source("") else {
        return Err(XepakError::Cfg("Data source does not exist".to_string()).into_lua_err());
    };
    let rr = ResourceRequest::new(&query, &input);
    let rows = ds.query(rr).await.map_err(ExternalError::into_lua_err)?;
    rows_to_lua_table(&lua, rows)
}

/// Returns single row from DB or a LUA nill value.
async fn storage_query_one(lua: Lua, (query, args): (String, Value)) -> mlua::Result<Value> {
    let input = prepare_args(&lua, args)?;
    let state = {
        let app = LuaAppData::load(&lua)?;
        app.state.clone()
    };
    let Some(ds) = state.get_data_source("") else {
        return Err(XepakError::Cfg("Data source does not exist".to_string()).into_lua_err());
    };
    let rr = ResourceRequest::new(&query, &input);
    match ds
        .query_one(rr)
        .await
        .map_err(ExternalError::into_lua_err)?
    {
        None => Ok(Value::Nil),
        Some(r) => r.into_lua(&lua),
    }
}

///Returns only a single value from a DB query.
async fn storage_query_value(lua: Lua, (query, args): (String, Value)) -> mlua::Result<Value> {
    let input = prepare_args(&lua, args)?;
    let state = {
        let app = LuaAppData::load(&lua)?;
        app.state.clone()
    };
    let Some(ds) = state.get_data_source("") else {
        return Err(XepakError::Cfg("Data source does not exist".to_string()).into_lua_err());
    };
    let rr = ResourceRequest::new(&query, &input);
    ds.query_value(rr)
        .await
        .map_err(ExternalError::into_lua_err)?
        .into_lua(&lua)
}

fn rows_to_lua_table(lua: &Lua, rows: Vec<XepakValue>) -> mlua::Result<Table> {
    let result = lua.create_table()?;
    for (i, row) in rows.into_iter().enumerate() {
        result.set(i + 1, row.into_lua(lua)?)?;
    }
    Ok(result)
}

/// Execute LUA script in async way.
/// Each request gets its own Lua VM so concurrent requests on the same actix worker
/// thread cannot share or corrupt the `ctx` global. Analogous to execute_script_blocking
/// for rhai but fully async — no blocking threads or handle.block_on calls.
pub async fn execute_lua_script<R>(
    _state: Data<XepakAppData>,
    uri: String,
    lua_env: Arc<Option<(Lua, Function)>>,
    input: RequestInput,
) -> Result<R, XepakError>
where
    R: FromLuaMulti + 'static,
{
    tokio::task::spawn_local(async move {
        let Some((lua, lua_fn)) = lua_env.as_ref() else {
            return Err(XepakError::Unexpected(format!(
                "Query script AST must already exists for handler {uri}"
            )));
        };

        let request_env = lua.create_table()?;
        request_env.set("ctx", LuaRequestContext::from(input))?;

        let globals = lua.globals();
        let meta = lua.create_table()?;
        meta.set("__index", globals)?;
        request_env.set_metatable(Some(meta))?;

        let isolated_fn = lua_fn.clone();
        isolated_fn.set_environment(request_env)?;

        // TODO: redo error handling later (may add line numbers logs if possible)
        match isolated_fn.call_async::<R>(()).await {
            Ok(result) => Ok(result),
            Err(e) => {
                Err(if let Some(xerror) = extract_xepak_from_lua_error(&e) {
                    // no need to log here, this could be an expected behavior
                    if !xerror.is_expectable() {
                        tracing::error!("Lua script error: {xerror}");
                    }
                    xerror
                } else {
                    tracing::error!("Lua scrip error: {e}");
                    XepakError::LuaScript(e.to_string())
                })
            }
        }
    })
    .await
    .map_err(XepakError::other)?
}

fn log_info(_: &Lua, msg: String) -> mlua::Result<()> {
    tracing::info!("LUA: {msg}");
    Ok(())
}

fn log_debug(_: &Lua, msg: String) -> mlua::Result<()> {
    tracing::debug!("LUA: {msg}");
    Ok(())
}

fn error_input(_: &Lua, message: String) -> mlua::Result<()> {
    Err(XepakError::Input(message).into_lua_err())
}

fn error_forbidden(_: &Lua, message: String) -> mlua::Result<()> {
    Err(XepakError::Forbidden(message).into_lua_err())
}

fn error_not_found(_: &Lua, message: String) -> mlua::Result<()> {
    Err(XepakError::NotFound(message).into_lua_err())
}

fn error_server(_: &Lua, message: String) -> mlua::Result<()> {
    Err(XepakError::WeScrewed(message).into_lua_err())
}

fn extract_xepak_from_lua_error(e: &LuaError) -> Option<XepakError> {
    match e {
        LuaError::ExternalError(arc) => arc.downcast_ref::<XepakError>().cloned(),
        LuaError::CallbackError { cause, .. } => extract_xepak_from_lua_error(cause),
        _ => None,
    }
}

impl From<LuaError> for XepakError {
    fn from(e: LuaError) -> Self {
        extract_xepak_from_lua_error(&e).unwrap_or_else(|| XepakError::LuaScript(e.to_string()))
    }
}

pub fn quick_table_is_map(table: &Table) -> bool {
    let tlen = table.len().unwrap_or(0);
    tlen == 0 && table.pairs::<Value, Value>().next().is_some()
}

pub fn quick_table_is_array(table: &Table) -> bool {
    let tlen = table.len().unwrap_or(0);
    tlen > 0 || (tlen == 0 && table.pairs::<Value, Value>().next().is_none())
}

// fn is_pure_sequence(table: &Table) -> Result<bool> {
//     let mut count = 0usize;
//     let mut max_index = 0usize;
//
//     table.len()
//     for entry in table.pairs::<Value, Value>() {
//         let (key, _) = entry?;
//
//         let Some(index) = positive_array_index(&key) else {
//             return Ok(false);
//         };
//
//         count += 1;
//         max_index = max_index.max(index);
//     }
//
//     // For a pure sequence like { "a", "b", "c" },
//     // count == max_index == 3.
//     //
//     // For { [1] = "a", [3] = "c" },
//     // count == 2, max_index == 3, so it is not a pure sequence.
//     Ok(count == max_index)
// }
//
// fn positive_array_index(value: &Value) -> Option<usize> {
//     match value {
//         Value::Integer(i) if *i >= 1 => (*i).try_into().ok(),
//
//         Value::Number(n) => {
//             let n = *n;
//
//             if n >= 1.0 && n.fract() == 0.0 && n <= usize::MAX as f64 {
//                 Some(n as usize)
//             } else {
//                 None
//             }
//         }
//
//         _ => None,
//     }
// }
