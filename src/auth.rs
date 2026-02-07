use std::collections::HashSet;

use serde::Deserialize;
use sqlx_core::HashMap;

use crate::{XepakError, server::processor::PreProcessorHandler};

pub type SimpleAuthRegistry = HashMap<String, (String, HashSet<String>)>;

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

pub struct SimpleAuthProcessor {
    checks: Vec<CheckAuthConf>,
}

impl SimpleAuthProcessor {
    pub fn new(checks: &[CheckAuthConf]) -> Self {
        let normalized = if checks.len() > 1 {
            vec![
                CheckAuthConf::Or {
                    nested: checks.to_vec(),
                }
                .normalize(),
            ]
        } else {
            checks.to_vec()
        };

        Self { checks: normalized }
    }
}

impl PreProcessorHandler for SimpleAuthProcessor {
    fn handle(
        &self,
        req: &actix_web::HttpRequest,
        state: &actix_web::web::Data<crate::server::XepakAppData>,
        body: &actix_web::web::Bytes,
        input: &mut crate::server::RequestArgs,
    ) -> Result<(), crate::XepakError> {
        // get API key value from headers

        // if not throw error

        // check in registry if key exists or error

        // Get meta data for key and run is_allowed 

        // if fail throw error
    }
}
