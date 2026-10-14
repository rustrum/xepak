pub mod rules;
pub mod storage;

use std::collections::HashSet;

use actix_web::http::Method;
use async_trait::async_trait;

use crate::{
    XepakError,
    auth::rules::{AuthRules, RulesParser},
    server::processor::{PRIORITY_NORMAL, PreProcessorHandler, adjust_priority},
};

pub const API_KEY_HEADER: &str = "x-api-key";

pub const AUTH_BEARER_HEADER: &str = "Authorization";

pub fn extract_api_key(req: &actix_web::HttpRequest) -> Result<Option<&str>, XepakError> {
    let mut is_bearer = false;

    // get API key value from headers
    let mut api_key_value = req.headers().get(API_KEY_HEADER);
    // POST only method is used for RPC/MCP servers
    // Thus can check for a bearer here
    if api_key_value.is_none() && req.method() == Method::POST {
        tracing::warn!("No key searching bearer");
        let bearer_header = req.headers().get(AUTH_BEARER_HEADER);
        if bearer_header.is_some() {
            api_key_value = bearer_header;
            is_bearer = true;
        }
    }

    let api_key_opt = api_key_value
        .map(|v| {
            v.to_str()
                .map_err(|e| XepakError::Input(format!("Wrong {API_KEY_HEADER} value format: {e}")))
        })
        .transpose()?;

    let Some(mut api_key) = api_key_opt else {
        return Ok(Option::None);
    };

    if is_bearer {
        let Some((prefix, key)) = api_key.split_once(char::is_whitespace) else {
            tracing::debug!(
                "Can't split Authorization header by space. No \"Bearer KEY\" part exists"
            );
            return Err(XepakError::Input("Invalid auth header".to_string()));
        };
        if prefix.to_uppercase() == "BEARER" {
            api_key = key.trim();
        } else {
            tracing::debug!("Auhorization header first part is not \"Bearer\"");
            return Err(XepakError::Input("Invalid auth header".to_string()));
        }
    }

    Ok(Some(api_key))
}
/// Authenticate client based on API key in the HTTP headers.
pub struct ConfigAuthenticationProcessor {
    priority: u16,
    /// Allow anonymous authentication
    anonymous_auth: bool,
}

impl ConfigAuthenticationProcessor {
    const ANON_AUTH_ERROR: &str = "Authentication failed! API key is not provided.";

    const AUTH_ERROR: &str = "Authentication failed!";

    pub fn new(position: u16, anonymous_auth: bool) -> Self {
        Self {
            priority: adjust_priority(PRIORITY_NORMAL, position),
            anonymous_auth,
        }
    }

    fn try_anonymous_auth(
        &self,
        state: &crate::server::XepakAppData,
        input: &mut crate::server::RequestInput,
    ) -> Result<(), XepakError> {
        if !self.anonymous_auth {
            return Err(XepakError::Forbidden(Self::ANON_AUTH_ERROR.to_string()));
        }
        let auth_roles = match state.get_auth_data("") {
            Some((_, roles)) => roles.clone(),
            None => HashSet::new(),
        };

        tracing::debug!(
            "Anonymous user authenticated with roles: {}",
            auth_roles
                .iter()
                .map(|v| v.to_string())
                .collect::<Vec<_>>()
                .join(", ")
        );
        input.set_auth("".to_string(), auth_roles);
        Ok(())
    }
}

#[async_trait(?Send)]
impl PreProcessorHandler for ConfigAuthenticationProcessor {
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
        if input.is_authenticated() {
            tracing::warn!("Already authenticated! Why?");
            return Ok(());
        }

        let api_key = extract_api_key(req)?;

        let Some(api_key) = api_key else {
            return self.try_anonymous_auth(state, input);
        };

        // check in registry if key exists or error
        let Some((auth_id, auth_roles)) = state.get_auth_data(api_key) else {
            return Err(XepakError::Forbidden(Self::AUTH_ERROR.to_string()));
        };

        tracing::debug!("User authenticated: {auth_id}");
        input.set_auth(auth_id.to_string(), auth_roles.clone());

        Ok(())
    }
}

/// Provides authorization for already authenticated requests.
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

#[async_trait(?Send)]
impl PreProcessorHandler for AuthorizeProcessor {
    async fn handle(
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
