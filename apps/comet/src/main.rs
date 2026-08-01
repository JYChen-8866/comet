//! comet — local note-agent app: headed by default, `comet headless` runs the
//! engine alone. No account, cloud, or self-update surface.

mod daemon;

use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(name = "comet", about = "Local note-taking agent")]
struct Cli {
    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Subcommand)]
enum Command {
    /// Run the engine without a UI.
    Headless,
    /// Manage the headless engine as a background service (launchd / systemd --user).
    Daemon {
        #[command(subcommand)]
        command: DaemonCommand,
    },
}

#[derive(Subcommand)]
enum DaemonCommand {
    /// Install, enable, and start the service (captures COMET_* env).
    Install,
    /// Stop and remove the service.
    Uninstall,
    /// Start the installed service.
    Start,
    /// Stop the service.
    Stop,
    /// Restart the service.
    Restart,
    /// Show the service manager's view of the daemon.
    Status,
}

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    if !matches!(cli.command, Some(Command::Headless)) {
        let default_filter = "info,loro_internal=warn,loro=warn";
        tracing_subscriber::fmt()
            .with_env_filter(
                tracing_subscriber::EnvFilter::try_from_default_env()
                    .unwrap_or_else(|_| default_filter.into()),
            )
            .init();
    }

    match cli.command {
        Some(Command::Headless) => {
            let runtime = tokio::runtime::Runtime::new()?;
            runtime.block_on(async {
                let engine = comet_engine::Engine::new(engine_config_from_env());
                engine.run().await
            })
        }
        Some(Command::Daemon { command }) => match command {
            DaemonCommand::Install => daemon::install(&engine_config_from_env().data_dir),
            DaemonCommand::Uninstall => daemon::uninstall(),
            DaemonCommand::Start => daemon::start(),
            DaemonCommand::Stop => daemon::stop(),
            DaemonCommand::Restart => daemon::restart(),
            DaemonCommand::Status => daemon::status(),
        },
        None => {
            let config = engine_config_from_env();
            comet_ui::run_app(comet_ui::UiConfig {
                data_dir: config.data_dir,
                ipc_port: config.ipc_port,
                default_harness: config.default_harness,
            });
            Ok(())
        }
    }
}

/// The env-resolved engine configuration shared by `headless` and the headed app.
fn engine_config_from_env() -> comet_engine::EngineConfig {
    comet_engine::EngineConfig {
        data_dir: std::env::var_os("COMET_DATA_DIR")
            .map(std::path::PathBuf::from)
            .unwrap_or_else(dirs_data_dir),
        ipc_port: std::env::var("COMET_IPC_PORT")
            .ok()
            .and_then(|p| p.parse().ok())
            .unwrap_or(27654),
        default_harness: harness_from_env(),
    }
}

/// `COMET_HARNESS` (kebab-case id) picks the default harness for chats without a
/// config row. Only the built-in mock is available in the local build.
fn harness_from_env() -> comet_engine::HarnessId {
    match std::env::var("COMET_HARNESS").as_deref().map(str::trim) {
        Ok("mock") => comet_engine::HarnessId::Mock,
        _ => comet_engine::HarnessId::Mock,
    }
}

fn dirs_data_dir() -> std::path::PathBuf {
    let home = std::env::var_os("HOME").expect("HOME not set");
    std::path::PathBuf::from(home).join(".comet-native")
}
