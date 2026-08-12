use std::collections::{HashMap, HashSet};

use serde::{Deserialize, Serialize};

use crate::{XepakError, xepak_data::XepakValue};

pub type AuthRegistry = HashMap<String, (String, HashSet<String>)>;

#[derive(Clone)]
pub struct AppRegistry {
    pub auth: AuthRegistry,
    pub secrets: HashMap<String, String>,
    pub data: HashMap<String, XepakValue>,
}

impl TryFrom<HashMap<String, toml::Value>> for AppRegistry {
    type Error = XepakError;

    fn try_from(mut value: HashMap<String, toml::Value>) -> Result<Self, Self::Error> {
        let auth = match value.remove("auth") {
            Some(v) => {
                let specs = Vec::<AuthRegistrySpecs>::deserialize(v)
                    .map_err(|e| XepakError::Cfg(format!("Wrong registry.auth format {}", e)))?;
                tracing::debug!("Auth registry {specs:?}");
                let mut registry: AuthRegistry = Default::default();
                for s in specs {
                    s.put_to_registry(&mut registry)?;
                }
                registry
            }
            None => Default::default(),
        };

        let secrets = match value.remove("secrets") {
            Some(v) => {
                let secrets_conf = HashMap::<String, SecretsRegistrySpec>::deserialize(v)
                    .map_err(|e| XepakError::Cfg(format!("Wrong registry.secrets format {}", e)))?;
                tracing::debug!("Secrets registry {secrets_conf:?}");

                let mut secrets = HashMap::new();
                for (k, v) in secrets_conf {
                    let value = match v {
                        SecretsRegistrySpec::Text { value } => value,
                        SecretsRegistrySpec::Env { name } => std::env::var(&name)
                            .map(|v| v.trim().to_string())
                            .map_err(|err| {
                                XepakError::Cfg(format!(
                                    "Can't load API key from ENV variable \"{}\" {}",
                                    name, err
                                ))
                            })?,
                    };
                    secrets.insert(k, value);
                }
                secrets
            }
            None => Default::default(),
        };

        let mut data = HashMap::new();
        for (k, v) in value {
            let xv = XepakValue::deserialize(v)
                .map_err(|e| XepakError::Cfg(format!("Can't deserilize registry {}", e)))?;
            data.insert(k, xv);
        }

        Ok(Self {
            auth,
            secrets,
            data,
        })
    }
}

/// Structure for the configurartion registry "auth" section.
#[derive(Clone, Debug, Deserialize)]
pub struct AuthRegistrySpecs {
    id: String,

    key: String,

    #[serde(default)]
    from_env: bool,

    #[serde(default)]
    roles: Vec<String>,
}

impl AuthRegistrySpecs {
    pub fn put_to_registry(&self, registry: &mut AuthRegistry) -> Result<(), XepakError> {
        let api_key = from_env_or_string(self.from_env, self.key.clone())?;

        let roles = self.roles.iter().map(|v| v.to_uppercase()).collect();

        // With current check we could have only one empty id record with empty key
        if api_key.is_empty() && !self.id.trim().is_empty() {
            return Err(XepakError::Cfg(format!(
                "Empty API key allowed only for anonymous auth! ID must be empty not \"{}\"",
                self.id
            )));
        }

        registry.insert(api_key, (self.id.trim().to_string(), roles));

        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum SecretsRegistrySpec {
    Text { value: String },
    Env { name: String },
}
fn from_env_or_string(load_from_env: bool, value: String) -> Result<String, XepakError> {
    if !load_from_env {
        return Ok(value.trim().to_string());
    }
    std::env::var(&value)
        .map(|v| v.trim().to_string())
        .map_err(|err| {
            XepakError::Cfg(format!(
                "Can't load API key from ENV variable \"{}\" {}",
                value, err
            ))
        })
}
