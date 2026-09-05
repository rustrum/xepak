use std::collections::HashMap;

use async_trait::async_trait;

use crate::{
    XepakError,
    auth::API_KEY_HEADER,
    server::{
        RequestInput,
        processor::{PRIORITY_NORMAL, PreProcessorHandler, adjust_priority},
    },
    storage::ResourceRequest,
    xepak_data::XepakValue,
};

/// Authorize based on a plain token saved in the storage.
pub struct TokenAuthenticationProcessor {
    priority: u16,

    /// Datasource to run query against
    data_source: String,

    /// Querystring that must return data in appropriate format.
    /// Query must return single row and follow next format
    /// `SELECT id,roles FROM ... WHERE ... key = {{api-key}}`
    /// where `roles` is a string with comma delimited role names.
    query: String,

    /// If non zero - cache authentication for N seconds
    cache_time_sec: usize,
}

impl TokenAuthenticationProcessor {
    pub fn new(position: u16, query: String, ds: String, cache_sec: u16) -> Self {
        Self {
            priority: adjust_priority(PRIORITY_NORMAL, position),
            query,
            data_source: ds,
            cache_time_sec: cache_sec as usize,
        }
    }

    fn build_auth_cache_key(api_key: &str) -> String {
        format!("storage-token-auth-key-{}", api_key)
    }

    fn process_cache(
        cache_key: &str,
        cache_value: Option<XepakValue>,
        input: &mut crate::server::RequestInput,
    ) -> bool {
        let Some(value) = cache_value else {
            return false;
        };

        let XepakValue::Tuple(v) = value else {
            tracing::warn!("Wrong cache value type for {cache_key}");
            return false;
        };

        if v.len() != 2 {
            tracing::warn!("Wrong cache value tuple size for {cache_key}");
            return false;
        }

        if let (XepakValue::Text(user_id), XepakValue::Tuple(roles)) = (&v[0], &v[1]) {
            input.set_auth(
                user_id.clone(),
                roles.iter().map(|r| r.as_string()).collect(),
            );
            return true;
        } else {
            tracing::warn!("Wrong cache value inner types key {cache_key}");
        }
        false
    }

    fn process_db_query(response: Option<XepakValue>) -> Result<(String, Vec<String>), XepakError> {
        let Some(result) = response else {
            return Err(XepakError::Forbidden(
                "Wrong API key or something".to_string(),
            ));
        };

        let XepakValue::Map(row) = result else {
            return Err(XepakError::Unexpected(
                "Wrong row shape returned from DB".to_string(),
            ));
        };

        let user_id = row
            .get("id")
            .ok_or_else(|| XepakError::Unexpected("Missing 'id' column".to_string()))?
            .as_string();

        let roles_str = row
            .get("roles")
            .ok_or_else(|| XepakError::Unexpected("Missing 'roles' column".to_string()))?
            .as_string();

        // Split comma-separated roles string to Vec<String>
        let roles: Vec<String> = roles_str
            .split(',')
            .map(|r| r.trim().to_uppercase())
            .filter(|r| !r.is_empty())
            .collect();

        Ok((user_id, roles))
    }
}

#[async_trait(?Send)]
impl PreProcessorHandler for TokenAuthenticationProcessor {
    fn priority(&self) -> u16 {
        self.priority
    }

    async fn handle(
        &self,
        req: &actix_web::HttpRequest,
        state: &actix_web::web::Data<crate::server::XepakAppData>,
        _body: &actix_web::web::Bytes,
        input: &mut crate::server::RequestInput,
    ) -> Result<(), XepakError> {
        let api_key = match req.headers().get(API_KEY_HEADER) {
            Some(val) => val
                .to_str()
                .map_err(|_| XepakError::Input("Invalid API key provided".to_string()))?,
            None => return Err(XepakError::Forbidden("No API key".to_string())),
        };

        let cache_key = Self::build_auth_cache_key(api_key);
        if Self::process_cache(&cache_key, state.cache_get(&cache_key).await, input) {
            return Ok(());
        }

        let Some(ds) = state.get_data_source(&self.data_source) else {
            return Err(XepakError::Cfg(format!(
                "Data source '{}' not found",
                self.data_source
            )));
        };

        let mut args = HashMap::new();
        args.insert("api-key".to_string(), XepakValue::Text(api_key.to_string()));
        let ri = RequestInput::new_simple(args, 0, 0);
        let rr = ResourceRequest::new(&self.query, &ri);

        let (user_id, roles) = Self::process_db_query(ds.query_one(rr).await?)?;

        input.set_auth(user_id.clone(), roles.clone().into_iter().collect());

        let cache_value = crate::xepak_data::XepakValue::Tuple(vec![
            XepakValue::Text(user_id),
            XepakValue::Tuple(roles.into_iter().map(XepakValue::Text).collect()),
        ]);

        state
            .cache_insert_ttl(cache_key, cache_value, self.cache_time_sec as u16)
            .await;

        Ok(())
    }
}
