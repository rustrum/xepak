use std::{
    borrow::Cow,
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex},
};

use crate::{
    XepakError,
    schema::{Schema, apply_schema, convert_with_schema},
    storage::{SqlxRequestArgs, StorageRequestArgs},
    xepak_data::{XepakDataError, XepakValue},
};

/// Contains aggregated/formatted input from request that will be used to querying resource.
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
    /// Always stored as s [`XepakValue::Dict`] if values exists.
    pub(crate) path_args: XepakValue,

    /// Arguments from querystring.
    /// Always stored as s [`XepakValue::Dict`] if values exists.
    pub(crate) get_args: XepakValue,

    /// Arguments parsed from POST/PUT request body
    pub(crate) body_args: XepakValue,

    /// Final input args storage with schema applied
    pub(crate) args: Arc<Mutex<HashMap<String, XepakValue>>>,

    /// Authentication data for current request.
    /// Shold be provided by an appropriate pre-processor.
    pub(crate) auth: Arc<Option<(XepakValue, HashSet<String>)>>,

    limit: usize,

    offset: usize,
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
            limit: 0,
            offset: 0,
        }
    }

    /// To use in other parts of code not linked with main request processing flow
    /// like script or processors.
    /// It will be unaware of HTTP request context.
    pub fn new_simple(args: HashMap<String, XepakValue>, limit: usize, offset: usize) -> Self {
        RequestInput {
            auth: Arc::new(None),
            schema: Schema::default(),
            strict_schema: false,
            method: String::new(),
            path_args: XepakValue::Null,
            get_args: XepakValue::Null,
            body_args: XepakValue::Null,
            args: Arc::new(Mutex::new(args)),
            limit,
            offset,
        }
    }

    pub fn has_any_arg(&self, arg_name: &str) -> bool {
        if self.args.lock().unwrap().contains_key(arg_name) {
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
        let args_lock = self.args.lock().unwrap();
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
        // lets keep it simple for now
        key.parse().ok()
    }

    pub fn get_limit(&self) -> usize {
        self.limit
    }

    pub fn get_offset(&self) -> usize {
        self.offset
    }

    /// Will try to parse limit/offset from existing arguments if possible.
    /// Output debug message if parsing failed.
    pub fn parse_offset_limit(&mut self, offset_arg: &str, limit_arg: &str, limit_max: usize) {
        if !limit_arg.is_empty() {
            self.limit = self.parse_usize_from(limit_arg).unwrap_or(limit_max);
            if self.limit > limit_max {
                self.limit = limit_max;
            }
        }
        if !offset_arg.is_empty() {
            self.offset = self.parse_usize_from(offset_arg).unwrap_or_default();
        }
    }

    fn parse_usize_from(&self, arg_name: &str) -> Option<usize> {
        let value = self.get_arg_value(arg_name)?;

        let ivalue = match value.as_int() {
            Ok(v) => v,
            Err(e) => {
                tracing::debug!("Can't get int from arg {arg_name}: {e}");
                return None;
            }
        };

        if ivalue < 0 || ivalue > usize::MAX as i128 {
            tracing::debug!("Value not in range for arg {arg_name}: {ivalue}");
            return None;
        }

        Some(ivalue as usize)
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

        self.args.lock().unwrap().insert(name, value);
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

impl StorageRequestArgs for RequestInput {
    fn get_rows_limit(&self) -> usize {
        self.get_limit()
    }

    fn get_rows_offset(&self) -> usize {
        self.get_offset()
    }
}

impl SqlxRequestArgs for RequestInput {
    fn bind_arg<'a>(
        &'a self,
        arg_name: &str,
        query: sqlx::query::Query<'a, sqlx::Any, sqlx::any::AnyArguments>,
    ) -> Result<sqlx::query::Query<'a, sqlx::Any, sqlx::any::AnyArguments>, XepakError> {
        let Some(value) = self.get_arg_value(arg_name) else {
            return Err(XepakError::Input(format!(
                "Can't bind argument '{arg_name}' - does not exists in request."
            )));
        };
        Ok(match value {
            Cow::Borrowed(v) => v.bind_sqlx(query),
            Cow::Owned(v) => v.bind_sql_move(query),
        })
    }
}
