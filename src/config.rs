use std::{
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
};

use anyhow::{Context, Result, bail};
use serde::Deserialize;

#[derive(Debug, Clone)]
pub struct Config {
    pub services: BTreeMap<String, ServiceConfig>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ServiceConfig {
    pub command: String,
    #[serde(default)]
    pub working_dir: Option<PathBuf>,
    #[serde(default)]
    pub ready_probe_http: Option<String>,
    #[serde(default)]
    pub ready_probe_log_line_contains: Option<String>,
    #[serde(default)]
    pub env: BTreeMap<String, String>,
}

impl Config {
    pub fn load(path: &Path) -> Result<Self> {
        let contents = fs::read_to_string(path)
            .with_context(|| format!("failed to read {}", path.display()))?;
        let services: BTreeMap<String, ServiceConfig> = toml::from_str(&contents)
            .with_context(|| format!("invalid TOML in {}", path.display()))?;
        if services.is_empty() {
            bail!("{} does not define any services", path.display());
        }

        for (name, service) in &services {
            if name.trim().is_empty() {
                bail!("service names must not be empty");
            }
            if service.command.trim().is_empty() {
                bail!("service `{name}` has an empty command");
            }
            if let Some(working_dir) = &service.working_dir {
                if working_dir.as_os_str().is_empty() {
                    bail!("service `{name}` has an empty working_dir");
                }
            }
            if service.ready_probe_http.is_some() && service.ready_probe_log_line_contains.is_some()
            {
                bail!("service `{name}` must use only one readiness probe type in this version");
            }
            for key in service.env.keys() {
                if key.trim().is_empty() {
                    bail!("service `{name}` contains an empty env key");
                }
            }
        }

        Ok(Self { services })
    }

    pub fn max_name_width(&self) -> usize {
        self.services
            .keys()
            .map(|name| name.len())
            .max()
            .unwrap_or_default()
    }
}

#[cfg(test)]
mod tests {
    use super::Config;
    use std::path::Path;

    #[test]
    fn parses_dynamic_service_tables() {
        let file = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(
            file.path(),
            r#"
[abc]
command = "cargo run -p abc"
working_dir = "forge_web"
ready_probe_http = "http://localhost:1234/health"

[abc.env]
PORT = "1234"

[db]
command = "postgres -D ./pgdata"
ready_probe_log_line_contains = "ready to accept connections"
"#,
        )
        .unwrap();

        let config = Config::load(file.path()).unwrap();
        assert_eq!(config.services.len(), 2);
        assert_eq!(config.max_name_width(), 3);
        assert_eq!(
            config.services["abc"].working_dir.as_deref(),
            Some(Path::new("forge_web"))
        );
        assert_eq!(config.services["abc"].env["PORT"], "1234");
    }
}
