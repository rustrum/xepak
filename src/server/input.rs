use std::{
    borrow::Cow,
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex},
};

use crate::{
    XepakError,
    schema::{Schema, apply_schema, convert_with_schema},
    storage::{SqlxRequestArgs, StorageRequestArgs},
    xepak_data::{XepakDataError, XepakValue, sql::SqlxBindable},
};

pub const ARG_NAME_LIMIT: &str = "-limit-";

pub const ARG_NAME_OFFSET: &str = "-offset-";

/// Contains aggregated/formatted input from request that will be used to query resource.
/// Input is updated/extended during processors execution.
/// Also it could be updated from resource script before executing output query.
#[derive(Debug, Clone)]
pub struct RequestInput {
    /// Input schema
    pub(crate) schema: Schema,

    /// If true - fail on keyargs not defined in the schema
    strict_schema: bool,

    /// HTTP request method (GET, POST, etc.)
    pub(crate) method: String,

    /// Arguments parsed from URI template
    /// Always stored as [`XepakValue::Dict`] if values exist.
    pub(crate) path_args: XepakValue,

    /// Arguments from query string.
    /// Always stored as [`XepakValue::Dict`] if values exist.
    pub(crate) get_args: XepakValue,

    /// Arguments parsed from POST/PUT request body
    pub(crate) body_args: XepakValue,

    /// Final input args storage with schema applied
    args: Arc<Mutex<HashMap<String, XepakValue>>>,

    /// Authentication data for current request.
    /// Should be provided by an appropriate pre-processor.
    pub(crate) auth: Arc<Option<(XepakValue, HashSet<String>)>>,
}

impl RequestInput {
    /// Meant to be executed while handling query string in main logic.
    pub fn new(
        schema: Schema,
        strict_schema: bool,
        method: String,
        uri_pattern: &str,
        req_path: &str,
    ) -> Self {
        // Todo return result that will validate path_args against schema

        let mut path = actix_router::Path::new(req_path);

        let resource = actix_router::ResourceDef::new(uri_pattern);
        resource.capture_match_info(&mut path);

        let path_args: HashMap<String, XepakValue> = path
            .iter()
            .map(|(k, v)| (k.to_string(), XepakValue::Text(v.to_string())))
            .collect();

        RequestInput {
            schema,
            strict_schema,
            auth: Arc::new(None),
            method,
            path_args: path_args.into(),
            get_args: XepakValue::Null,
            body_args: XepakValue::Null,
            args: Arc::new(Mutex::new(Default::default())),
        }
    }

    /// To use in other parts of code not linked with main request processing flow
    /// like script or processors.
    /// It will be unaware of HTTP request context.
    pub fn new_simple(args: HashMap<String, XepakValue>) -> Self {
        RequestInput {
            auth: Arc::new(None),
            schema: Schema::default(),
            strict_schema: false,
            method: String::new(),
            path_args: XepakValue::Null,
            get_args: XepakValue::Null,
            body_args: XepakValue::Null,
            args: Arc::new(Mutex::new(args)),
        }
    }

    /// Return only custom argument value.
    /// Custom args could be set by pre-processor or inside Lua.
    pub fn get_arg(&self, key: &str) -> Option<XepakValue> {
        let args_lock = self.args.lock().expect("Lock is poisoned, can't recover");
        args_lock.get(key).cloned()
    }

    /// Set custom argument value.
    /// Returns previos value if it was set.
    pub fn set_arg(&self, key: &str, value: XepakValue) -> Option<XepakValue> {
        self.args
            .lock()
            .expect("Lock is poisoned, can't recover")
            .insert(key.to_string(), value)
    }

    pub fn has_any_arg(&self, arg_name: &str) -> bool {
        if self
            .args
            .lock()
            .expect("Lock is poisoned, can't recover")
            .contains_key(arg_name)
        {
            return true;
        }
        if self.path_args.dict_contains(arg_name) {
            return true;
        }
        if self.has_body_arg(arg_name) {
            return true;
        }
        self.get_args.dict_contains(arg_name)
    }

    pub fn get_arg_value(&self, name: &str) -> Option<Cow<'_, XepakValue>> {
        // Custom defined args has higher priority
        let args_lock = self.args.lock().expect("Lock is poisoned, can't recover");
        if args_lock.contains_key(name) {
            return args_lock.get(name).cloned().map(Cow::Owned);
        }

        // URI path args have priority over other types
        let path_arg = self.path_args.dict_get(name);
        if path_arg.is_some() {
            return path_arg.map(Cow::Borrowed);
        }

        let body_arg = self.get_body_arg(name);
        if body_arg.is_some() {
            return body_arg.map(Cow::Borrowed);
        }

        let get_arg = self.get_args.dict_get(name);
        if get_arg.is_some() {
            return get_arg.map(Cow::Borrowed);
        }

        None
    }

    fn has_body_arg(&self, key: &str) -> bool {
        if let Some(len) = self.body_args.tuple_len() {
            return if let Some(idx) = self.key_to_usize(key) {
                idx < len
            } else {
                false
            };
        }
        self.body_args.dict_contains(key)
    }

    fn get_body_arg(&self, key: &str) -> Option<&XepakValue> {
        if self.body_args.is_tuple() {
            return if let Some(idx) = self.key_to_usize(key) {
                self.body_args.tuple_get(idx)
            } else {
                None
            };
        }
        self.body_args.dict_get(key)
    }

    fn key_to_usize(&self, key: &str) -> Option<usize> {
        // let's keep it simple for now
        key.parse().ok()
    }

    pub(crate) fn apply_schema_to(
        &self,
        key_ref: &str,
        value: XepakValue,
        validate: bool,
    ) -> Result<XepakValue, XepakDataError> {
        if self.schema.is_empty() {
            // Empty schema - nothing to apply
            Ok(value)
        } else {
            apply_schema(&self.schema, value, key_ref, self.strict_schema, validate)
        }
    }

    /// Set top level named argument value and apply schema conversion to it if any defined.
    /// Strict [`Schema`] rules will apply only if `enforce_schema = true`.
    pub fn set_named_arg_with_schema(
        &mut self,
        name: String,
        value: XepakValue,
        enforce_schema: bool,
    ) -> Result<(), XepakError> {
        let arg_name = name.as_str();
        let value = if self.strict_schema && enforce_schema {
            let schema = self.schema.get_arg_schema_strict(arg_name)?;
            convert_with_schema(schema, arg_name, value)?
        } else {
            if let Some(schema) = self.schema.get_arg_schema(arg_name) {
                convert_with_schema(schema, arg_name, value)?
            } else {
                value
            }
        };

        self.args
            .lock()
            .expect("Lock is poisoned, can't recover")
            .insert(name, value);
        Ok(())
    }

    pub fn set_auth(&mut self, id: String, roles: HashSet<String>) {
        self.auth = Arc::new(Some((XepakValue::Text(id), roles)))
    }

    pub fn is_authenticated(&self) -> bool {
        self.auth.is_some()
    }

    pub fn get_auth(&self) -> Option<&(XepakValue, HashSet<String>)> {
        self.auth.as_ref().as_ref()
    }
}

impl StorageRequestArgs for RequestInput {}

impl SqlxRequestArgs for RequestInput {
    fn bind_arg<'a, DB>(
        &'a self,
        arg_name: &str,
        query: sqlx::query::Query<'a, DB, <DB as sqlx::Database>::Arguments>,
    ) -> Result<sqlx::query::Query<'a, DB, <DB as sqlx::Database>::Arguments>, XepakError>
    where
        DB: sqlx::Database,
        // XepakValue wrapped types
        Option<String>: SqlxBindable<'a, DB>,
        bool: SqlxBindable<'a, DB>,
        i64: SqlxBindable<'a, DB>,
        f64: SqlxBindable<'a, DB>,
        String: SqlxBindable<'a, DB>,
        Vec<u8>: SqlxBindable<'a, DB>,
    {
        let Some(value) = self.get_arg_value(arg_name) else {
            return Err(XepakError::Input(format!(
                "Can't bind argument '{arg_name}' - does not exist in request."
            )));
        };
        Ok(match value {
            Cow::Borrowed(v) => v.bind_sqlx(query),
            Cow::Owned(v) => v.bind_sql_move(query),
        })
    }
}
