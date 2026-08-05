pub mod rules;
pub mod token;

use std::collections::HashSet;

use serde::Deserialize;
use sqlx_core::HashMap;

use crate::{
    XepakError,
    auth::rules::{AuthRules, RulesParser},
    server::processor::{PRIORITY_NORMAL, PreProcessorHandler, adjust_priority},
};

pub type ShallowAuthRegistry = HashMap<String, (String, HashSet<String>)>;

pub const API_KEY_HEADER: &str = "x-api-key";

/// The most simple toml based auth configuration.
#[derive(Clone, Debug, Deserialize)]
pub struct ShallowAuthSpecs {
    id: String,

    key: String,

    #[serde(default)]
    from_env: bool,

    #[serde(default)]
    roles: Vec<String>,
}

impl ShallowAuthSpecs {
    fn put_to_registry(&self, registry: &mut ShallowAuthRegistry) -> Result<(), XepakError> {
        let api_key = if self.from_env {
            match std::env::var(&self.key) {
                Ok(v) => v,
                Err(err) => {
                    return Err(XepakError::Cfg(format!(
                        "Can't load API key from ENV variable \"{}\" {}",
                        self.key, err
                    )));
                }
            }
        } else {
            self.key.clone()
        };

        let roles = self.roles.iter().map(|v| v.to_uppercase()).collect();

        registry.insert(api_key, (self.id.clone(), roles));

        Ok(())
    }
}

pub fn auth_specs_to_registry(
    specs: &[ShallowAuthSpecs],
) -> Result<ShallowAuthRegistry, XepakError> {
    let mut registry: ShallowAuthRegistry = Default::default();

    for s in specs {
        s.put_to_registry(&mut registry)?;
    }

    Ok(registry)
}

/// Authenticates requests using [`SimpleAuthSpecs`].
pub struct ShallowAuthenticationProcessor {
    priority: u16,
    /// TODO: Maybe should remove this flag?
    _allow_no_auth: bool,
}

impl ShallowAuthenticationProcessor {
    pub fn new(position: u16, allow_no_auth: bool) -> Self {
        Self {
            priority: adjust_priority(PRIORITY_NORMAL, position),
            _allow_no_auth: allow_no_auth,
        }
    }
}

impl PreProcessorHandler for ShallowAuthenticationProcessor {
    fn priority(&self) -> u16 {
        self.priority
    }

    fn handle(
        &self,
        req: &actix_web::HttpRequest,
        state: &actix_web::web::Data<crate::server::XepakAppData>,
        _body: &actix_web::web::Bytes,
        input: &mut crate::server::RequestInput,
    ) -> Result<(), XepakError> {
        if input.is_authenticated() {
            tracing::warn!("Already authenticated! Why?");
            return Ok(());
        }

        let not_auth_err = Err(XepakError::Forbidden("Not authenticated".to_string()));

        // get API key value from headers
        let Some(api_key_value) = req.headers().get(API_KEY_HEADER) else {
            return not_auth_err;
        };

        let api_key = api_key_value
            .to_str()
            .map_err(|e| XepakError::Input(format!("Wrong {API_KEY_HEADER} value: {e}")))?;

        tracing::debug!("API key found: {api_key}");

        // check in registry if key exists or error
        let Some((auth_id, auth_roles)) = state.get_auth_data(api_key) else {
            return not_auth_err;
        };

        tracing::debug!("Authenticate user: {auth_id}");
        input.set_auth(auth_id.to_string(), auth_roles.clone());

        Ok(())
    }
}

/// Provides authorization for already authenticated requests.
///
pub struct AuthorizeProcessor {
    priority: u16,
    rules: Option<AuthRules>,
}

impl AuthorizeProcessor {
    pub fn new(position: u16, rules_expr: &str) -> Result<Self, XepakError> {
        let rules = if rules_expr.trim().is_empty() {
            None
        } else {
            Some(RulesParser::new(rules_expr).parse()?.normalize())
        };

        Ok(Self {
            priority: adjust_priority(PRIORITY_NORMAL, position),
            rules,
        })
    }
}

impl PreProcessorHandler for AuthorizeProcessor {
    fn handle(
        &self,
        _req: &actix_web::HttpRequest,
        _state: &actix_web::web::Data<crate::server::XepakAppData>,
        _body: &actix_web::web::Bytes,
        input: &mut crate::server::RequestInput,
    ) -> Result<(), crate::XepakError> {
        let Some((id, roles)) = input.get_auth() else {
            return Err(XepakError::Forbidden("Not authenticated".to_string()));
        };

        let id = id.as_string();

        let Some(rules) = &self.rules else {
            // If no access checks provided it means we only require authenticated requests
            tracing::debug!("Allowed! No access checks for authenticated id:{id}");
            return Ok(());
        };

        if !rules.is_allowed(&id, roles) {
            return Err(XepakError::Forbidden(format!(
                "Not authorized to perform request! Auth id: {id}"
            )));
        }

        Ok(())
    }

    fn priority(&self) -> u16 {
        self.priority
    }
}
