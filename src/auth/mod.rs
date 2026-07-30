pub mod rules;

use std::collections::HashSet;

use serde::Deserialize;
use sqlx_core::HashMap;

use crate::{XepakError, auth::rules::RulesParser, server::processor::PreProcessorHandler};

pub type SimpleAuthRegistry = HashMap<String, (String, HashSet<String>)>;

pub const API_KEY_HEADER: &str = "x-api-key";

#[derive(Clone, Debug, Deserialize)]
pub struct SimpleAuthSpecs {
    id: String,

    key: String,

    #[serde(default)]
    from_env: bool,

    #[serde(default)]
    roles: Vec<String>,
}

impl SimpleAuthSpecs {
    fn put_to_registry(&self, registry: &mut SimpleAuthRegistry) -> Result<(), XepakError> {
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

pub fn auth_specs_to_registry(specs: &[SimpleAuthSpecs]) -> Result<SimpleAuthRegistry, XepakError> {
    let mut registry: SimpleAuthRegistry = Default::default();

    for s in specs {
        s.put_to_registry(&mut registry)?;
    }

    Ok(registry)
}

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum CheckAuthConf {
    Role {
        #[serde(default)]
        v: String,
    },

    Id {
        v: String,
    },

    /// Combine nested conditions via logical AND (all must succeed)
    And {
        nested: Vec<CheckAuthConf>,
    },

    /// Combine nested conditions via logical OR (at least one must succeed)
    Or {
        nested: Vec<CheckAuthConf>,
    },
}

impl PartialEq for CheckAuthConf {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (CheckAuthConf::Role { v: a_v }, CheckAuthConf::Role { v: b_v }) => a_v == b_v,
            (CheckAuthConf::Id { v: a_v }, CheckAuthConf::Id { v: b_v }) => a_v == b_v,
            (CheckAuthConf::And { nested: a_n }, CheckAuthConf::And { nested: b_n }) => {
                a_n.len() == b_n.len() && a_n.iter().zip(b_n).all(|(a, b)| a == b)
            }
            (CheckAuthConf::Or { nested: a_n }, CheckAuthConf::Or { nested: b_n }) => {
                a_n.len() == b_n.len() && a_n.iter().zip(b_n).all(|(a, b)| a == b)
            }
            _ => false,
        }
    }
}

impl Eq for CheckAuthConf {}

impl CheckAuthConf {
    /// We just make all roles uppercase
    fn normalize(&self) -> Self {
        match self {
            CheckAuthConf::Role { v } => Self::Role {
                v: v.clone().to_uppercase(),
            },
            CheckAuthConf::Id { .. } => self.clone(),
            CheckAuthConf::And { nested } => Self::And {
                nested: nested.iter().cloned().map(|v| v.normalize()).collect(),
            },
            CheckAuthConf::Or { nested } => Self::And {
                nested: nested.iter().cloned().map(|v| v.normalize()).collect(),
            },
        }
    }

    fn is_allowed(&self, id: &str, roles: &HashSet<String>) -> bool {
        match self {
            CheckAuthConf::Role { v } => roles.contains(v),
            CheckAuthConf::Id { v } => id == v,
            CheckAuthConf::And { nested } => {
                let mut check = true;
                for c in nested {
                    if !c.is_allowed(id, roles) {
                        check = false;
                        break;
                    }
                }
                check
            }
            CheckAuthConf::Or { nested } => {
                let mut check = false;
                for c in nested {
                    if c.is_allowed(id, roles) {
                        check = true;
                        break;
                    }
                }
                check
            }
        }
    }
}

pub struct SimpleAuthenticationProcessor {
    /// TODO: Maybe should remove this flag?
    _allow_no_auth: bool,
}

impl SimpleAuthenticationProcessor {
    pub fn new(allow_no_auth: bool) -> Self {
        Self {
            _allow_no_auth: allow_no_auth,
        }
    }

    pub fn new_boxed(allow_no_auth: bool) -> Box<Self> {
        Box::new(Self::new(allow_no_auth))
    }
}

impl PreProcessorHandler for SimpleAuthenticationProcessor {
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

pub struct AuthorizeProcessor {
    checks: Option<CheckAuthConf>,
}

impl AuthorizeProcessor {
    pub fn new(rules_expr: &str) -> Result<Self, XepakError> {
        let checks = if rules_expr.trim().is_empty() {
            None
        } else {
            Some(RulesParser::new(rules_expr).parse()?.normalize())
        };

        Ok(Self { checks })
    }

    pub fn new_boxed(rules_expr: &str) -> Result<Box<Self>, XepakError> {
        Ok(Box::new(Self::new(rules_expr)?))
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

        let Some(check) = &self.checks else {
            // If no access checks provided it means we only require authenticated requests
            tracing::debug!("Allowed! No access checks for authenticated id:{id}");
            return Ok(());
        };

        if !check.is_allowed(&id, roles) {
            return Err(XepakError::Forbidden(format!(
                "Not authorized to perform request! Auth id: {id}"
            )));
        }

        Ok(())
    }
}
