use std::{io, path::PathBuf};

use xepak::{
    XepakError,
    cfg::{XepakConf, load_conf_file, load_specs_from_dir},
    server::init_server,
};

const ENV_PORT: &str = "XEPAK_PORT";

const ENV_PATH: &str = "XEPAK_CONFIG";

#[actix_web::main]
async fn main() -> Result<(), XepakError> {
    let args = parse_cli_args()?;

    let args = update_from_env(args);

    let filter = if let Some(log_level) = args.log {
        tracing_subscriber::EnvFilter::builder()
            .with_default_directive(tracing::level_filters::LevelFilter::ERROR.into())
            .parse(log_level)
            .map_err(|e| XepakError::Cfg(e.to_string()))?
    } else {
        tracing_subscriber::EnvFilter::from_default_env()
    };

    tracing_subscriber::fmt().with_env_filter(filter).init();

    let Some(file_path) = args.config_file else {
        return Err(XepakError::Cfg(
            "Configuration file was not provided".to_string(),
        ));
    };
    tracing::debug!("Configuration file: {file_path}");

    let xepak_conf = load_conf_file(&file_path)?;
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

    let server = init_server(xepak_conf, xepak_specs).await?;

    server.await?;

    Ok(())
}

#[derive(Default)]
struct AppArgs {
    /// TODO: maybe port is not required here
    port: Option<u16>,
    log: Option<String>,
    config_file: Option<String>,
}

fn parse_cli_args() -> io::Result<AppArgs> {
    let cli = getopt3::new(getopt3::hideBin(std::env::args()), "p:l:");
    match cli {
        Ok(opts) => {
            let mut args: AppArgs = Default::default();
            if let Some(port_str) = opts.options.get(&'p') {
                let port_num = port_str
                    .parse::<u16>()
                    .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
                args.port = Some(port_num);
            }

            if let Some(log_str) = opts.options.get(&'l') {
                args.log = Some(log_str.clone())
            }

            args.config_file = opts.arguments.iter().next().cloned();

            Ok(args)
        }
        Err(e) => Err(io::Error::new(io::ErrorKind::InvalidInput, e)),
    }
}

fn update_from_env(mut args: AppArgs) -> AppArgs {
    if args.port.is_none() {
        if let Some(pn) = std::env::var(ENV_PORT).ok().map(|p| p.parse::<u16>()) {
            match pn {
                Ok(p) => args.port = Some(p),
                Err(e) => tracing::error!("Can't parse port number: {}", e),
            }
        }
    }

    if args.config_file.is_none() {
        args.config_file = std::env::var(ENV_PATH).ok();
    }
    args
}
