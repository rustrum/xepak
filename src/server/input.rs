use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex},
};

use crate::{
    XepakError,
    schema::{Schema, convert_with_schema},
    storage::{SqlxRequestArgs, StorageRequestArgs},
    xepak_data::XepakValue,
};

/// Contains aggregated/formatted input from request that will be used to querying resource.
/// Input is updated/extended during processors execution.
/// Also it could be updated from resource script before executing output query.
#[derive(Debug, Clone)]
pub struct RequestInput {
    pub(crate) schema: Schema,

    /// If true - fail on non existing args
    strict_schema: bool,

    /// Arguments parsed from URI (higher priority)
    pub(crate) path_args: Arc<Mutex<HashMap<String, XepakValue>>>,

    /// Final input args storage with schema applied
    pub(crate) args: Arc<Mutex<HashMap<String, XepakValue>>>,

    /// Authentication data for current request.
    /// Shold be provided by an appropriate pre-processor.
    pub(crate) auth: Arc<Option<(XepakValue, HashSet<String>)>>,

    limit: usize,

    offset: usize,
}

impl RequestInput {
    pub fn new(schema: Schema, strict_schema: bool, uri_pattern: &str, req_path: &str) -> Self {
        // Todo return result that will validate path_args against schema

        let mut path = actix_router::Path::new(req_path);

        let resource = actix_router::ResourceDef::new(uri_pattern);
        resource.capture_match_info(&mut path);

        let path_args = path
            .iter()
            .map(|(k, v)| (k.to_string(), XepakValue::Text(v.to_string())))
            .collect();

        RequestInput {
            schema,
            strict_schema,
            auth: Arc::new(None),
            path_args: Arc::new(Mutex::new(path_args)),
            args: Arc::new(Mutex::new(Default::default())),
            limit: 0,
            offset: 0,
        }
    }

    /// Used when script is building input for nested queries
    pub fn new_in_script(args: HashMap<String, XepakValue>, limit: usize, offset: usize) -> Self {
        RequestInput {
            auth: Arc::new(None),
            schema: Schema::default(),
            strict_schema: false,
            path_args: Arc::new(Mutex::new(Default::default())),
            args: Arc::new(Mutex::new(args)),
            limit,
            offset,
        }
    }

    pub fn has_any_arg(&self, arg_name: &str) -> bool {
        if self.path_args.lock().unwrap().contains_key(arg_name) {
            return true;
        }
        self.args.lock().unwrap().contains_key(arg_name)
    }

    pub fn get_arg_value(&self, argument: &str) -> Option<XepakValue> {
        let path_arg = self.path_args.lock().unwrap().get(argument).cloned();
        if path_arg.is_none() {
            self.args.lock().unwrap().get(argument).cloned()
        } else {
            path_arg
        }
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

    /// Set argument value and apply schema conversion to it if any defined.
    /// Strict [`Schema`] rules will apply only if `enforce_schema = true`,
    /// this is needed to avoid schema.
    pub fn set_arg_with_schema(
        &mut self,
        name: String,
        value: XepakValue,
        enforce_schema: bool,
    ) -> Result<(), XepakError> {
        let value = convert_with_schema(
            &self.schema,
            name.as_str(),
            value,
            self.strict_schema && enforce_schema,
        )?;
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

        Ok(value.bind_sqlx(query))
    }
}
