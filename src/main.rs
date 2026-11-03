use std::{io, path::PathBuf, sync::Arc};

use tracing::level_filters::LevelFilter;
use xepak::{
    XepakError,
    cfg::{XepakConf, load_conf_file, load_specs_from_dir},
    server::init_server,
};

/// Port to run application on
const ENV_LISTEN: &str = "XEPAK_LISTEN";

/// Enables JSON logging if defined with: 1|y|true
const ENV_LOG_JSON: &str = "XEPAK_LOG_JSON";

/// Path to config file
const ENV_CONFIG_PATH: &str = "XEPAK_CONFIG";

#[actix_web::main]
#[allow(clippy::expect_fun_call)]
async fn main() -> Result<(), XepakError> {
    let args = parse_cli_args().map_err(Arc::new)?;

    let args = update_from_env(args);

    init_logger(&args)?;

    let Some(file_path) = &args.config_file else {
        return Err(XepakError::Cfg(
            "Configuration file was not provided".to_string(),
        ));
    };
    tracing::debug!("Configuration file: {file_path}");

    let xepak_conf = load_conf_file(file_path)?;
    let xepak_conf = args.apply_to_config(xepak_conf);

    tracing::debug!("Conf: {xepak_conf:?}");

    let conf_dir = PathBuf::from(&file_path)
        .parent()
        .expect(format!("Can't get a dir for path {file_path}").as_str())
        .to_path_buf();

    let specs_dir = if xepak_conf.specs_dir.is_relative() {
        conf_dir.join(&xepak_conf.specs_dir)
    } else {
        xepak_conf.specs_dir.clone()
    };

    let xepak_specs = load_specs_from_dir(specs_dir)?;

    tracing::debug!("Specs: {xepak_specs:?}");

    let server = init_server(conf_dir, xepak_conf, xepak_specs).await?;

    server.await.map_err(Arc::new)?;

    Ok(())
}

fn init_logger(args: &AppArgs) -> Result<(), XepakError> {
    let filter = if let Some(log_level) = &args.log {
        tracing_subscriber::EnvFilter::builder()
            .with_default_directive(tracing::level_filters::LevelFilter::WARN.into())
            .parse(log_level)
            .map_err(|e| XepakError::Cfg(e.to_string()))?
    } else {
        tracing_subscriber::EnvFilter::builder()
            .with_default_directive(LevelFilter::INFO.into())
            .from_env_lossy()
    };

    if matches!(args.log_json, Some(true)) {
        tracing_subscriber::fmt()
            .json()
            .with_env_filter(filter)
            .init();
    } else {
        tracing_subscriber::fmt().with_env_filter(filter).init();
    }
    Ok(())
}

#[derive(Default)]
struct AppArgs {
    listen: Option<String>,
    log: Option<String>,
    log_json: Option<bool>,
    config_file: Option<String>,
}

impl AppArgs {
    /// Overwrites [`XepakConf`] with [`AppArgs`] values.
    fn apply_to_config(&self, mut config: XepakConf) -> XepakConf {
        if let Some(listen) = &self.listen {
            config.server.listen = listen.clone();
        }
        config
    }
}

fn parse_cli_args() -> io::Result<AppArgs> {
    let cli = getopt3::new(getopt3::hideBin(std::env::args()), "jl:");
    match cli {
        Ok(opts) => {
            let mut args: AppArgs = Default::default();

            // LOG filter
            if let Some(log_str) = opts.options.get(&'l') {
                args.log = Some(log_str.clone())
            }

            // LOG in JSON
            if opts.has('j') {
                args.log_json = Some(true);
            }

            args.config_file = opts.arguments.first().cloned();

            Ok(args)
        }
        Err(e) => Err(io::Error::new(io::ErrorKind::InvalidInput, e)),
    }
}

fn update_from_env(mut args: AppArgs) -> AppArgs {
    if args.listen.is_none() {
        args.listen = std::env::var(ENV_LISTEN).ok();
    }

    if args.log_json.is_none() {
        let env_val = std::env::var(ENV_LOG_JSON)
            .ok()
            .unwrap_or_default()
            .to_lowercase();
        args.log_json = match env_val.trim() {
            "1" | "y" | "yes" | "true" | "on" => Some(true),
            _ => Some(false),
        };
    }
    if args.config_file.is_none() {
        args.config_file = std::env::var(ENV_CONFIG_PATH).ok();
    }
    args
}
