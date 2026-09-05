use anyhow::{Result, anyhow};
use collections::HashMap;
use gpui::{App, AppContext as _, AsyncApp, BackgroundExecutor, Context, Entity, Task, WeakEntity};

use futures::{FutureExt, future::Shared};
use itertools::Itertools as _;
use language::LanguageName;
use remote::{Interactive, RemoteClient};
use rpc::{
    AnyProtoClient, TypedEnvelope,
    proto::{self, REMOTE_SERVER_PROJECT_ID},
};
use settings::{Settings, SettingsLocation};
use std::{
    borrow::Cow,
    fmt,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};
use task::{Shell, ShellBuilder, ShellKind, SpawnInTerminal, TaskId, shell_to_proto};
use terminal::{
    RemotePtyHandle, RemotePtyTransport, RemoteTerminalOptions, Terminal, TerminalBuilder,
    TerminalMode, insert_zed_terminal_env, terminal_settings::TerminalSettings,
};
// `util`'s root re-exports of the shell helpers are native-only; `util::shell` itself
// compiles for wasm.
use util::{
    ResultExt as _,
    command::new_std_command,
    maybe,
    rel_path::RelPath,
    shell::{get_default_system_shell, get_system_shell},
};

use crate::{Project, ProjectPath};

pub struct Terminals {
    /// Every terminal created by this project, remote PTYs included.
    pub(crate) local_handles: Vec<WeakEntity<terminal::Terminal>>,
    /// Terminals whose PTY lives on the remote server, keyed by the id the
    /// server assigned them.
    pub(crate) remote: HashMap<u64, RemoteTerminalEntry>,
    /// The server's `ListTerminals` inventory, fetched once per fresh session by
    /// [`Project::fetch_remote_terminal_inventory`]. Entries are taken out by
    /// [`Project::restore_remote_terminal`]; whatever is left is closed by
    /// [`Project::close_unrestored_remote_terminals`] (D4).
    pub(crate) restorable: HashMap<u64, proto::TerminalInfo>,
    /// The in-flight (or completed) `ListTerminals` fetch, shared so every
    /// restore path awaits the same request rather than racing it. `TerminalPanel::load`
    /// and a center-pane `TerminalView::deserialize` both start on this, in
    /// whichever order the workspace happens to restore them. Cleared by the reap
    /// so a later fresh session fetches again.
    pub(crate) inventory_fetch: Option<Shared<Task<()>>>,
}

/// A terminal of this project whose PTY runs inside the remote server.
pub(crate) struct RemoteTerminalEntry {
    /// `None` between `SpawnTerminalResponse` and `TerminalBuilder::subscribe`;
    /// output pushed into the handle queues in its channel until then.
    pub(crate) terminal: Option<WeakEntity<Terminal>>,
    pub(crate) handle: RemotePtyHandle,
}

/// Largest `TerminalInput` payload put on the wire. A bracketed paste of tens of
/// megabytes would otherwise blow past the 16 MiB frame ceiling (D3).
pub(crate) const INPUT_CHUNK: usize = 64 * 1024;

/// The server does not have (or has finished) the terminal a restored tab asked
/// for, so the tab is dropped. Expected after a sandbox stop, not an error.
#[derive(Debug)]
pub struct RemoteTerminalGone(pub u64);

impl fmt::Display for RemoteTerminalGone {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "remote terminal {} is no longer available", self.0)
    }
}

impl std::error::Error for RemoteTerminalGone {}

impl Project {
    pub fn active_entry_directory(&self, cx: &App) -> Option<PathBuf> {
        let entry_id = self.active_entry()?;
        let worktree = self.worktree_for_entry(entry_id, cx)?;
        let worktree = worktree.read(cx);
        let entry = worktree.entry_for_id(entry_id)?;

        let absolute_path = worktree.absolutize(entry.path.as_ref());
        if entry.is_dir() {
            Some(absolute_path)
        } else {
            absolute_path.parent().map(|p| p.to_path_buf())
        }
    }

    pub fn active_project_directory(&self, cx: &App) -> Option<Arc<Path>> {
        self.active_entry()
            .and_then(|entry_id| self.worktree_for_entry(entry_id, cx))
            .into_iter()
            .chain(self.worktrees(cx))
            .find_map(|tree| tree.read(cx).root_dir())
    }

    pub fn first_project_directory(&self, cx: &App) -> Option<PathBuf> {
        let worktree = self.worktrees(cx).next()?;
        let worktree = worktree.read(cx);
        if worktree.root_entry()?.is_dir() {
            Some(worktree.abs_path().to_path_buf())
        } else {
            None
        }
    }

    pub fn create_terminal_task(
        &mut self,
        spawn_task: SpawnInTerminal,
        cx: &mut Context<Self>,
    ) -> Task<Result<Entity<Terminal>>> {
        let is_via_remote = self.remote_client.is_some();

        let path: Option<Arc<Path>> = if let Some(cwd) = &spawn_task.cwd {
            if is_via_remote {
                Some(Arc::from(cwd.as_ref()))
            } else {
                let cwd = cwd.to_string_lossy();
                let tilde_substituted = shellexpand::tilde(&cwd);
                Some(Arc::from(Path::new(tilde_substituted.as_ref())))
            }
        } else {
            self.active_project_directory(cx)
        };

        let mut settings_location = None;
        if let Some(path) = path.as_ref()
            && let Some((worktree, _)) = self.find_worktree(path, cx)
        {
            settings_location = Some(SettingsLocation {
                worktree_id: worktree.read(cx).id(),
                path: RelPath::empty(),
            });
        }
        let settings = TerminalSettings::get(settings_location, cx).clone();
        let detect_venv = settings.detect_venv.as_option().is_some();

        let terminal_mode = TerminalMode::task(spawn_task.clone());

        let local_path = if is_via_remote { None } else { path.clone() };
        let remote_client = self.remote_client.clone();
        let remote_pty = self.supports_remote_pty(cx);
        let shell = match &remote_client {
            Some(remote_client) => remote_client
                .read(cx)
                .shell()
                .unwrap_or_else(get_default_system_shell),
            None => get_system_shell(),
        };
        let path_style = self.path_style(cx);
        let shell_kind = ShellKind::new(&shell, path_style.is_windows());

        // Prepare a task for resolving the environment
        let env_task =
            self.resolve_directory_environment(&shell, path.clone(), remote_client.clone(), cx);

        // Scope the toolchain lookup to the worktree the terminal is being
        // spawned in. Previously this iterated the active editor's worktree
        // and then every visible worktree, so a Python toolchain persisted
        // for worktree A would leak into a terminal opened in worktree B and
        // inject (e.g.) `conda activate base` into a shell that has no
        // business with conda.
        let project_path_contexts: Vec<ProjectPath> = path
            .as_ref()
            .and_then(|p| self.find_worktree(p, cx))
            .map(|(worktree, relative_path)| ProjectPath {
                worktree_id: worktree.read(cx).id(),
                path: relative_path,
            })
            .into_iter()
            .collect();
        let toolchains = project_path_contexts
            .into_iter()
            .filter(|_| detect_venv)
            .map(|p| self.active_toolchain(p, LanguageName::new_static("Python"), cx))
            .collect::<Vec<_>>();
        let lang_registry = self.languages.clone();
        cx.spawn(async move |project, cx| {
            let mut env = env_task.await.unwrap_or_default();
            env.extend(settings.env);

            let activation_script = maybe!(async {
                for toolchain in toolchains {
                    let Some(toolchain) = toolchain.await else {
                        continue;
                    };
                    let language = lang_registry
                        .language_for_name(&toolchain.language_name.0)
                        .await
                        .ok();
                    let lister = language?.toolchain_lister()?;
                    let future =
                        cx.update(|cx| lister.activation_script(&toolchain, shell_kind, cx));
                    return Some(future.await);
                }
                None
            })
            .await
            .unwrap_or_default();

            let builder = project
                .update(cx, move |this, cx| {
                    let format_to_run = |spawn_task: &SpawnInTerminal| {
                        format_task_for_activation(
                            spawn_task,
                            shell_kind,
                            &shell,
                            path_style.is_windows(),
                        )
                    };

                    if let Some(remote_client) = remote_client.clone().filter(|_| remote_pty) {
                        let to_run =
                            (!activation_script.is_empty()).then(|| format_to_run(&spawn_task));
                        let mut env = env;
                        env.extend(spawn_task.env.clone());
                        let shell = if activation_script.is_empty() {
                            match spawn_task.command.clone() {
                                Some(program) => Shell::WithArguments {
                                    program,
                                    args: spawn_task.args.clone(),
                                    title_override: None,
                                },
                                None => Shell::System,
                            }
                        } else {
                            let separator = shell_kind.sequential_commands_separator();
                            let activation_script =
                                activation_script.join(&format!("{separator} "));
                            let to_run = to_run.expect("activation command was formatted");
                            let arg = format!("{activation_script}{separator} {to_run}");
                            Shell::WithArguments {
                                program: shell.clone(),
                                args: shell_kind.args_for_shell(true, arg),
                                title_override: None,
                            }
                        };
                        return anyhow::Ok(this.spawn_remote_terminal(
                            remote_client,
                            path,
                            Some(spawn_task.id.clone()),
                            RemoteTerminalOptions {
                                // The remote server spawns in `path`; the client
                                // keeps no local working directory for it.
                                working_directory: None,
                                mode: terminal_mode,
                                shell,
                                env,
                                cursor_shape: settings.cursor_shape,
                                alternate_scroll: settings.alternate_scroll,
                                max_scroll_history_lines: settings.max_scroll_history_lines,
                                path_hyperlink_regexes: settings.path_hyperlink_regexes,
                                path_hyperlink_timeout: Duration::from_millis(
                                    settings.path_hyperlink_timeout_ms,
                                ),
                                window_id: cx.entity_id().as_u64(),
                                path_style,
                                title_override: Some(spawn_task.label.clone()),
                                // The activation script is already part of the
                                // command the server runs.
                                activation_script: Vec::new(),
                            },
                            cx,
                        ));
                    }

                    let (shell, env) = {
                        let to_run =
                            (!activation_script.is_empty()).then(|| format_to_run(&spawn_task));
                        env.extend(spawn_task.env);
                        match remote_client {
                            Some(remote_client) => match activation_script.clone() {
                                activation_script if !activation_script.is_empty() => {
                                    let separator = shell_kind.sequential_commands_separator();
                                    let activation_script =
                                        activation_script.join(&format!("{separator} "));
                                    let to_run = to_run.expect("activation command was formatted");

                                    let arg = format!("{activation_script}{separator} {to_run}");
                                    let args = shell_kind.args_for_shell(true, arg);
                                    let shell = remote_client
                                        .read(cx)
                                        .shell()
                                        .unwrap_or_else(get_default_system_shell);

                                    create_remote_shell(
                                        Some((&shell, &args)),
                                        env,
                                        path,
                                        remote_client,
                                        cx,
                                    )?
                                }
                                _ => create_remote_shell(
                                    spawn_task
                                        .command
                                        .as_ref()
                                        .map(|command| (command, &spawn_task.args)),
                                    env,
                                    path,
                                    remote_client,
                                    cx,
                                )?,
                            },
                            None => match activation_script.clone() {
                                activation_script if !activation_script.is_empty() => {
                                    let separator = shell_kind.sequential_commands_separator();
                                    let activation_script =
                                        activation_script.join(&format!("{separator} "));
                                    let to_run = to_run.expect("activation command was formatted");

                                    let arg = format!("{activation_script}{separator} {to_run}");
                                    let args = shell_kind.args_for_shell(true, arg);

                                    (
                                        Shell::WithArguments {
                                            program: shell,
                                            args,
                                            title_override: None,
                                        },
                                        env,
                                    )
                                }
                                _ => (
                                    if let Some(program) = spawn_task.command {
                                        Shell::WithArguments {
                                            program,
                                            args: spawn_task.args,
                                            title_override: None,
                                        }
                                    } else {
                                        Shell::System
                                    },
                                    env,
                                ),
                            },
                        }
                    };
                    anyhow::Ok(TerminalBuilder::new(
                        local_path.map(|path| path.to_path_buf()),
                        terminal_mode,
                        shell,
                        env,
                        settings.cursor_shape,
                        settings.alternate_scroll,
                        settings.max_scroll_history_lines,
                        settings.path_hyperlink_regexes,
                        Duration::from_millis(settings.path_hyperlink_timeout_ms),
                        is_via_remote,
                        cx.entity_id().as_u64(),
                        cx,
                        activation_script,
                        path_style,
                    ))
                })??
                .await?;
            project.update(cx, move |this, cx| this.adopt_terminal_builder(builder, cx))
        })
    }

    pub fn create_terminal_shell(
        &mut self,
        cwd: Option<PathBuf>,
        cx: &mut Context<Self>,
    ) -> Task<Result<Entity<Terminal>>> {
        self.create_terminal_shell_internal(cwd, false, cx)
    }

    /// Creates a local terminal even if the project is remote.
    /// In remote projects: opens in Zed's launch directory (bypasses SSH).
    /// In local projects: opens in the project directory (same as regular terminals).
    pub fn create_local_terminal(
        &mut self,
        cx: &mut Context<Self>,
    ) -> Task<Result<Entity<Terminal>>> {
        let working_directory = if self.remote_client.is_some() {
            // Remote project: don't use remote paths, let shell use Zed's cwd
            None
        } else {
            // Local project: use project directory like normal terminals
            self.active_project_directory(cx).map(|p| p.to_path_buf())
        };
        self.create_terminal_shell_internal(working_directory, true, cx)
    }

    /// Internal method for creating terminal shells.
    /// If force_local is true, creates a local terminal even if the project has a remote client.
    /// This allows "breaking out" to a local shell in remote projects.
    fn create_terminal_shell_internal(
        &mut self,
        cwd: Option<PathBuf>,
        force_local: bool,
        cx: &mut Context<Self>,
    ) -> Task<Result<Entity<Terminal>>> {
        let path = cwd.map(|p| Arc::from(&*p));
        let is_via_remote = !force_local && self.remote_client.is_some();

        let mut settings_location = None;
        if let Some(path) = path.as_ref()
            && let Some((worktree, _)) = self.find_worktree(path, cx)
        {
            settings_location = Some(SettingsLocation {
                worktree_id: worktree.read(cx).id(),
                path: RelPath::empty(),
            });
        }
        let settings = TerminalSettings::get(settings_location, cx).clone();
        let detect_venv = settings.detect_venv.as_option().is_some();
        let local_path = if is_via_remote { None } else { path.clone() };

        // See create_terminal_task: scope the toolchain lookup to the
        // worktree the terminal is opened in, not the active editor's
        // worktree or other visible worktrees.
        let project_path_contexts: Vec<ProjectPath> = path
            .as_ref()
            .and_then(|p| self.find_worktree(p, cx))
            .map(|(worktree, relative_path)| ProjectPath {
                worktree_id: worktree.read(cx).id(),
                path: relative_path,
            })
            .into_iter()
            .collect();
        let toolchains = project_path_contexts
            .into_iter()
            .filter(|_| detect_venv)
            .map(|p| self.active_toolchain(p, LanguageName::new_static("Python"), cx))
            .collect::<Vec<_>>();
        let remote_client = if force_local {
            None
        } else {
            self.remote_client.clone()
        };
        let remote_pty = self.supports_remote_pty(cx);
        // In the browser there is no local shell to break out to.
        #[cfg(target_family = "wasm")]
        if force_local && remote_pty {
            return Task::ready(Err(anyhow!(
                "local terminals are not available in the browser"
            )));
        }
        let shell = match &remote_client {
            Some(remote_client) => remote_client
                .read(cx)
                .shell()
                .unwrap_or_else(get_default_system_shell),
            None => settings.shell.program(),
        };
        let env_shell = match &remote_client {
            Some(_) => shell.clone(),
            None => get_system_shell(),
        };

        let path_style = self.path_style(cx);

        // Prepare a task for resolving the environment
        let env_task =
            self.resolve_directory_environment(&env_shell, path.clone(), remote_client.clone(), cx);

        let lang_registry = self.languages.clone();
        cx.spawn(async move |project, cx| {
            let shell_kind = ShellKind::new(&shell, path_style.is_windows());
            let mut env = env_task.await.unwrap_or_default();
            env.extend(settings.env);

            let activation_script = maybe!(async {
                for toolchain in toolchains {
                    let Some(toolchain) = toolchain.await else {
                        continue;
                    };
                    let language = lang_registry
                        .language_for_name(&toolchain.language_name.0)
                        .await
                        .ok();
                    let lister = language?.toolchain_lister()?;
                    let future =
                        cx.update(|cx| lister.activation_script(&toolchain, shell_kind, cx));
                    return Some(future.await);
                }
                None
            })
            .await
            .unwrap_or_default();

            let builder = project
                .update(cx, move |this, cx| {
                    if let Some(remote_client) = remote_client.clone().filter(|_| remote_pty) {
                        return anyhow::Ok(this.spawn_remote_terminal(
                            remote_client,
                            path.clone(),
                            None,
                            RemoteTerminalOptions {
                                // A server path: persisted so the tab can be
                                // recreated in the same directory (D28).
                                working_directory: path.as_ref().map(|path| path.to_path_buf()),
                                mode: TerminalMode::interactive(),
                                // The server resolves its own login shell.
                                shell: Shell::System,
                                env,
                                cursor_shape: settings.cursor_shape,
                                alternate_scroll: settings.alternate_scroll,
                                max_scroll_history_lines: settings.max_scroll_history_lines,
                                path_hyperlink_regexes: settings.path_hyperlink_regexes,
                                path_hyperlink_timeout: Duration::from_millis(
                                    settings.path_hyperlink_timeout_ms,
                                ),
                                window_id: cx.entity_id().as_u64(),
                                path_style,
                                title_override: None,
                                activation_script,
                            },
                            cx,
                        ));
                    }

                    let (shell, env) = {
                        match remote_client {
                            Some(remote_client) => {
                                create_remote_shell(None, env, path, remote_client, cx)?
                            }
                            None => (settings.shell, env),
                        }
                    };
                    anyhow::Ok(TerminalBuilder::new(
                        local_path.map(|path| path.to_path_buf()),
                        TerminalMode::interactive(),
                        shell,
                        env,
                        settings.cursor_shape,
                        settings.alternate_scroll,
                        settings.max_scroll_history_lines,
                        settings.path_hyperlink_regexes,
                        Duration::from_millis(settings.path_hyperlink_timeout_ms),
                        is_via_remote,
                        cx.entity_id().as_u64(),
                        cx,
                        activation_script,
                        path_style,
                    ))
                })??
                .await?;
            project.update(cx, move |this, cx| this.adopt_terminal_builder(builder, cx))
        })
    }

    pub fn clone_terminal(
        &mut self,
        terminal: &Entity<Terminal>,
        cx: &mut Context<'_, Project>,
        cwd: Option<PathBuf>,
    ) -> Task<Result<Entity<Terminal>>> {
        // We cannot clone the task's terminal, as it will effectively re-spawn the task, which might not be desirable.
        // For now, create a new shell instead.
        // A task terminal would re-spawn its task, and a remote terminal has no
        // local process to clone: open a fresh shell instead.
        if terminal.read(cx).task().is_some() || terminal.read(cx).is_remote_pty() {
            return self.create_terminal_shell(cwd, cx);
        }
        let local_path = if self.is_via_remote_server() {
            None
        } else {
            cwd
        };

        let builder = terminal.read(cx).clone_builder(cx, local_path);
        cx.spawn(async |project, cx| {
            let builder = builder.await?;
            project.update(cx, |project, cx| {
                project.adopt_terminal_builder(builder, cx)
            })
        })
    }

    pub fn terminal_settings<'a>(
        &'a self,
        path: &'a Option<PathBuf>,
        cx: &'a App,
    ) -> &'a TerminalSettings {
        let mut settings_location = None;
        if let Some(path) = path.as_ref()
            && let Some((worktree, _)) = self.find_worktree(path, cx)
        {
            settings_location = Some(SettingsLocation {
                worktree_id: worktree.read(cx).id(),
                path: RelPath::empty(),
            });
        }
        TerminalSettings::get(settings_location, cx)
    }

    pub fn exec_in_shell(
        &self,
        command: String,
        cx: &mut Context<Self>,
    ) -> Task<Result<smol::process::Command>> {
        // `build_command` needs a local process to run; in the browser there is
        // none, and the remote server runs the shell for us elsewhere.
        #[cfg(target_family = "wasm")]
        if self.supports_remote_pty(cx) {
            return Task::ready(Err(anyhow!(
                "shell commands are not available in the browser"
            )));
        }
        let path = self.first_project_directory(cx);
        let remote_client = self.remote_client.clone();
        let settings = self.terminal_settings(&path, cx).clone();
        let shell = remote_client
            .as_ref()
            .and_then(|remote_client| remote_client.read(cx).shell())
            .map(Shell::Program)
            .unwrap_or(Shell::System);
        let is_windows = self.path_style(cx).is_windows();
        let builder = ShellBuilder::new(&shell, is_windows).non_interactive();
        let (command, args) = builder.build(Some(command), &Vec::new());

        let env_task = self.resolve_directory_environment(
            &shell.program(),
            path.as_ref().map(|p| Arc::from(&**p)),
            remote_client.clone(),
            cx,
        );

        cx.spawn(async move |project, cx| {
            let mut env = env_task.await.unwrap_or_default();
            env.extend(settings.env);

            project.update(cx, move |_, cx| {
                match remote_client {
                    Some(remote_client) => {
                        let command_template = remote_client.read(cx).build_command(
                            Some(command),
                            &args,
                            &env,
                            None,
                            None,
                            Interactive::Yes,
                        )?;
                        let mut command = new_std_command(command_template.program);
                        command.args(command_template.args);
                        command.envs(command_template.env);
                        Ok(command)
                    }
                    None => {
                        let mut command = new_std_command(command);
                        command.args(args);
                        command.envs(env);
                        if let Some(path) = path {
                            command.current_dir(path);
                        }
                        Ok(command)
                    }
                }
                .map(|mut process| {
                    util::set_pre_exec_to_start_new_session(&mut process);
                    smol::process::Command::from(process)
                })
            })?
        })
    }

    pub fn local_terminal_handles(&self) -> &Vec<WeakEntity<terminal::Terminal>> {
        &self.terminals.local_handles
    }

    fn resolve_directory_environment(
        &self,
        shell: &str,
        path: Option<Arc<Path>>,
        remote_client: Option<Entity<RemoteClient>>,
        cx: &mut App,
    ) -> Shared<Task<Option<HashMap<String, String>>>> {
        if let Some(path) = &path {
            let shell = Shell::Program(shell.to_string());
            self.environment
                .update(cx, |project_env, cx| match &remote_client {
                    Some(remote_client) => project_env.remote_directory_environment(
                        &shell,
                        path.clone(),
                        remote_client.clone(),
                        cx,
                    ),
                    None => project_env.local_directory_environment(&shell, path.clone(), cx),
                })
        } else {
            Task::ready(None).shared()
        }
    }

    /// True when this project's remote connection hosts PTYs itself, so terminals
    /// run inside the remote server instead of in a locally spawned `ssh`-style
    /// process (D27). False for local, ssh, wsl and docker projects.
    pub fn supports_remote_pty(&self, cx: &App) -> bool {
        self.remote_client
            .as_ref()
            .is_some_and(|remote_client| remote_client.read(cx).supports_remote_pty())
    }

    fn remote_pty_client(&self, cx: &App) -> Option<AnyProtoClient> {
        let remote_client = self.remote_client.as_ref()?.read(cx);
        remote_client
            .supports_remote_pty()
            .then(|| remote_client.proto_client())
    }

    /// Spawns a terminal whose PTY lives in the remote server and returns its
    /// builder. The terminal starts detached: the `AttachTerminal` that starts the
    /// stream is sent in the same update that registers the terminal, so no
    /// output can arrive before there is somewhere to put it.
    fn spawn_remote_terminal(
        &mut self,
        remote_client: Entity<RemoteClient>,
        working_directory: Option<Arc<Path>>,
        task_id: Option<TaskId>,
        mut options: RemoteTerminalOptions,
        cx: &mut Context<Self>,
    ) -> Task<Result<TerminalBuilder>> {
        let remote_client = remote_client.read(cx);
        let client = remote_client.proto_client();
        let host = remote_client.connection_options().display_name();
        insert_zed_terminal_env(&mut options.env, &release_channel::AppVersion::global(cx));
        let title = options
            .title_override
            .clone()
            .unwrap_or_else(|| format!("{host} — Terminal"));
        options.title_override = Some(title.clone());

        // The local path opens at these bounds too; the first layout resizes it.
        let terminal_bounds = terminal::TerminalBounds::default();
        let cols = terminal_bounds.num_columns() as u32;
        let rows = terminal_bounds.num_lines() as u32;

        let (program, args) = match &options.shell {
            Shell::System => (None, None),
            Shell::Program(program) => (Some(program.clone()), None),
            Shell::WithArguments { program, args, .. } => {
                (Some(program.clone()), Some(args.clone()))
            }
        };
        let request = client.request(proto::SpawnTerminal {
            project_id: REMOTE_SERVER_PROJECT_ID,
            working_directory: working_directory
                .as_ref()
                .map(|path| path.to_string_lossy().into_owned()),
            shell: Some(shell_to_proto(options.shell.clone())),
            env: options.env.clone().into_iter().collect(),
            cols,
            rows,
            task_id: task_id.map(|task_id| task_id.0),
            title: Some(title),
        });

        cx.spawn(async move |project, cx| {
            let terminal_id = match request.await {
                Ok(response) => response.terminal_id,
                Err(error) => {
                    // Rendered as `FailedToSpawnTerminal`, like a local spawn
                    // failure: a missing directory or program comes back here.
                    return Err(anyhow!(terminal::TerminalError {
                        directory: working_directory.map(|path| path.to_path_buf()),
                        program,
                        args,
                        title_override: options.title_override.clone(),
                        source: std::io::Error::other(format!("{error:#}")),
                    }));
                }
            };
            project.update(cx, |this, cx| {
                let (builder, handle) = TerminalBuilder::new_remote(
                    options,
                    Arc::new(ProtoPtyTransport {
                        client: client.clone(),
                        terminal_id,
                        executor: cx.background_executor().clone(),
                    }),
                    cx.background_executor(),
                );
                this.terminals.remote.insert(
                    terminal_id,
                    RemoteTerminalEntry {
                        terminal: None,
                        handle,
                    },
                );
                let attach = client.request(proto::AttachTerminal {
                    project_id: REMOTE_SERVER_PROJECT_ID,
                    terminal_id,
                    from_offset: 0,
                    cols,
                    rows,
                });
                cx.background_spawn(async move {
                    attach.await.log_err();
                })
                .detach();
                builder
            })
        })
    }

    /// Subscribes a freshly built terminal, tracks it and makes sure both the
    /// local-handle list and the remote-terminal map forget it when it is
    /// dropped.
    fn adopt_terminal_builder(
        &mut self,
        builder: TerminalBuilder,
        cx: &mut Context<Self>,
    ) -> Entity<Terminal> {
        let terminal_handle = cx.new(|cx| builder.subscribe(cx));
        let remote_terminal_id = terminal_handle.read(cx).remote_terminal_id();
        if let Some(remote_terminal_id) = remote_terminal_id
            && let Some(entry) = self.terminals.remote.get_mut(&remote_terminal_id)
        {
            entry.terminal = Some(terminal_handle.downgrade());
        }

        self.terminals
            .local_handles
            .push(terminal_handle.downgrade());

        let id = terminal_handle.entity_id();
        cx.observe_release(&terminal_handle, move |project, _terminal, cx| {
            if let Some(remote_terminal_id) = remote_terminal_id {
                project.terminals.remote.remove(&remote_terminal_id);
            }
            let handles = &mut project.terminals.local_handles;

            if let Some(index) = handles
                .iter()
                .position(|terminal| terminal.entity_id() == id)
            {
                handles.remove(index);
                cx.notify();
            }
        })
        .detach();

        terminal_handle
    }

    /// Fetches the server's inventory of terminals so a fresh session can decide
    /// which ones to reattach and which to reap (D4). A failed request leaves the
    /// inventory empty, which restores nothing and closes nothing.
    pub fn fetch_remote_terminal_inventory(&mut self, cx: &mut Context<Self>) -> Task<()> {
        if let Some(shared) = self.terminals.inventory_fetch.clone() {
            return cx.spawn(async move |_, _| shared.await);
        }
        let Some(client) = self.remote_pty_client(cx) else {
            return Task::ready(());
        };
        let request = client.request(proto::ListTerminals {
            project_id: REMOTE_SERVER_PROJECT_ID,
        });
        let shared = cx
            .spawn(async move |project, cx| {
                let response = match request.await {
                    Ok(response) => response,
                    Err(error) => {
                        log::warn!("failed to list remote terminals: {error:#}");
                        return;
                    }
                };
                project
                    .update(cx, |this, _| {
                        this.terminals.restorable = response
                            .terminals
                            .into_iter()
                            .map(|info| (info.terminal_id, info))
                            .collect();
                    })
                    .log_err();
            })
            .shared();
        self.terminals.inventory_fetch = Some(shared.clone());
        // Drive to completion even if the returned task is dropped, so the two
        // restore paths that await a clone always make progress.
        cx.spawn({
            let shared = shared.clone();
            async move |_, _| shared.await
        })
        .detach();
        cx.spawn(async move |_, _| shared.await)
    }

    /// Reattaches a persisted terminal after a fresh session, replaying the
    /// server's scrollback into it (D4).
    ///
    /// Returns `Err(RemoteTerminalGone)` when the server no longer has the
    /// terminal, it already exited, or it was restored once already; the caller
    /// drops the persisted tab in that case.
    pub fn restore_remote_terminal(
        &mut self,
        terminal_id: u64,
        working_directory: Option<PathBuf>,
        title: Option<String>,
        cx: &mut Context<Self>,
    ) -> Result<Entity<Terminal>> {
        let Some(client) = self.remote_pty_client(cx) else {
            return Err(anyhow!(RemoteTerminalGone(terminal_id)));
        };
        let Some(info) = self.terminals.restorable.remove(&terminal_id) else {
            return Err(anyhow!(RemoteTerminalGone(terminal_id)));
        };
        if self.terminals.remote.contains_key(&terminal_id) {
            return Err(anyhow!(RemoteTerminalGone(terminal_id)));
        }
        if info.exit.is_some() {
            client
                .send(proto::CloseTerminal {
                    project_id: REMOTE_SERVER_PROJECT_ID,
                    terminal_id,
                })
                .log_err();
            return Err(anyhow!(RemoteTerminalGone(terminal_id)));
        }

        let working_directory = working_directory.or_else(|| info.cwd.as_ref().map(PathBuf::from));
        let path_style = self.path_style(cx);
        let settings = self.terminal_settings(&working_directory, cx).clone();
        let terminal_bounds = terminal::TerminalBounds::default();
        let cols = terminal_bounds.num_columns() as u32;
        let rows = terminal_bounds.num_lines() as u32;

        let (builder, handle) = TerminalBuilder::new_remote(
            RemoteTerminalOptions {
                working_directory,
                mode: TerminalMode::interactive(),
                shell: Shell::System,
                env: HashMap::default(),
                cursor_shape: settings.cursor_shape,
                alternate_scroll: settings.alternate_scroll,
                max_scroll_history_lines: settings.max_scroll_history_lines,
                path_hyperlink_regexes: settings.path_hyperlink_regexes,
                path_hyperlink_timeout: Duration::from_millis(settings.path_hyperlink_timeout_ms),
                window_id: cx.entity_id().as_u64(),
                path_style,
                title_override: title.or(Some(info.title)),
                // Activation scripts ran when the terminal was first spawned.
                activation_script: Vec::new(),
            },
            Arc::new(ProtoPtyTransport {
                client: client.clone(),
                terminal_id,
                executor: cx.background_executor().clone(),
            }),
            cx.background_executor(),
        );
        self.terminals.remote.insert(
            terminal_id,
            RemoteTerminalEntry {
                terminal: None,
                handle,
            },
        );
        let attach = client.request(proto::AttachTerminal {
            project_id: REMOTE_SERVER_PROJECT_ID,
            terminal_id,
            // The whole ring is replayed, so the tab comes back with its scrollback.
            from_offset: 0,
            cols,
            rows,
        });
        cx.background_spawn(async move {
            attach.await.log_err();
        })
        .detach();

        Ok(self.adopt_terminal_builder(builder, cx))
    }

    /// Closes every terminal in the inventory that nobody restored: exited
    /// shells, tabs that were closed without the server hearing about it, and
    /// terminals of a client that never came back (D4).
    pub fn close_unrestored_remote_terminals(&mut self, cx: &mut Context<Self>) {
        let Some(client) = self.remote_pty_client(cx) else {
            self.terminals.restorable.clear();
            self.terminals.inventory_fetch = None;
            return;
        };
        let restorable = std::mem::take(&mut self.terminals.restorable);
        for (terminal_id, _) in restorable {
            // Never close a terminal this client is actively using: one spawned
            // or restored before the `ListTerminals` response returned (an
            // agent/debugger terminal at workspace open, or a center-pane tab
            // that restored it) is both in the inventory and in `remote`.
            if self.terminals.remote.contains_key(&terminal_id) {
                continue;
            }
            client
                .send(proto::CloseTerminal {
                    project_id: REMOTE_SERVER_PROJECT_ID,
                    terminal_id,
                })
                .log_err();
        }
        // A later fresh session (never within one client's lifetime) fetches
        // the inventory again.
        self.terminals.inventory_fetch = None;
    }

    /// Resumes every remote terminal's output stream after a reconnect, from the
    /// offset each terminal has already parsed. Idempotent: replayed chunks the
    /// terminal already has are dropped by offset.
    pub(crate) fn reattach_remote_terminals(&mut self, cx: &mut Context<Self>) {
        let Some(client) = self.remote_pty_client(cx) else {
            return;
        };
        let attachments = self
            .terminals
            .remote
            .iter()
            .filter_map(|(terminal_id, entry)| {
                let terminal = entry.terminal.as_ref()?.upgrade()?;
                let terminal = terminal.read(cx);
                if !terminal.has_active_pty_resources() {
                    return None;
                }
                let bounds = terminal.last_content.terminal_bounds;
                Some((
                    *terminal_id,
                    entry.handle.clone(),
                    terminal.remote_next_offset().unwrap_or(0),
                    bounds.num_columns() as u32,
                    bounds.num_lines() as u32,
                ))
            })
            .collect::<Vec<_>>();

        for (terminal_id, handle, from_offset, cols, rows) in attachments {
            let request = client.request(proto::AttachTerminal {
                project_id: REMOTE_SERVER_PROJECT_ID,
                terminal_id,
                from_offset,
                cols,
                rows,
            });
            cx.background_spawn(async move {
                if let Err(error) = request.await {
                    // The server no longer has this terminal: tell the terminal so
                    // it stops pretending to be live.
                    log::warn!("failed to reattach remote terminal {terminal_id}: {error:#}");
                    handle.push_lost();
                }
            })
            .detach();
        }
    }

    pub(crate) async fn handle_terminal_output(
        this: Entity<Self>,
        envelope: TypedEnvelope<proto::TerminalOutput>,
        mut cx: AsyncApp,
    ) -> Result<()> {
        // No `.await` before the update: `ChannelClient` dispatches incoming
        // messages in arrival order, so doing the work synchronously is what keeps
        // output chunks in order.
        this.update(&mut cx, |this, _| {
            let payload = envelope.payload;
            match this.terminals.remote.get(&payload.terminal_id) {
                Some(entry) => {
                    entry
                        .handle
                        .push_output(payload.offset, payload.data, payload.reset);
                }
                None => log::debug!(
                    "dropping output for unknown remote terminal {}",
                    payload.terminal_id
                ),
            }
        });
        Ok(())
    }

    pub(crate) async fn handle_terminal_exited(
        this: Entity<Self>,
        envelope: TypedEnvelope<proto::TerminalExited>,
        mut cx: AsyncApp,
    ) -> Result<()> {
        this.update(&mut cx, |this, _| {
            let payload = envelope.payload;
            let exit = payload.exit.unwrap_or_default();
            match this.terminals.remote.get(&payload.terminal_id) {
                Some(entry) => {
                    entry.handle.push_exit(exit.code, exit.signal);
                }
                None => log::debug!(
                    "dropping exit for unknown remote terminal {}",
                    payload.terminal_id
                ),
            }
        });
        Ok(())
    }
}

/// Splits terminal input into messages the wire accepts.
fn chunk_input(data: &[u8]) -> impl Iterator<Item = &[u8]> {
    // `chunks` panics on a zero size and yields nothing for an empty slice, but a
    // keystroke is never empty and `INPUT_CHUNK` is a constant.
    data.chunks(INPUT_CHUNK)
}

/// The production [`RemotePtyTransport`]: every call is one message to the remote
/// server, addressed to `REMOTE_SERVER_PROJECT_ID` (the only id
/// `HeadlessProject` subscribes under).
struct ProtoPtyTransport {
    client: AnyProtoClient,
    terminal_id: u64,
    executor: BackgroundExecutor,
}

impl RemotePtyTransport for ProtoPtyTransport {
    fn terminal_id(&self) -> u64 {
        self.terminal_id
    }

    fn input(&self, data: Cow<'static, [u8]>) {
        for chunk in chunk_input(&data) {
            self.client
                .send(proto::TerminalInput {
                    project_id: REMOTE_SERVER_PROJECT_ID,
                    terminal_id: self.terminal_id,
                    data: chunk.to_vec(),
                })
                .log_err();
        }
    }

    fn resize(&self, cols: u16, rows: u16) {
        self.client
            .send(proto::ResizeTerminal {
                project_id: REMOTE_SERVER_PROJECT_ID,
                terminal_id: self.terminal_id,
                cols: cols as u32,
                rows: rows as u32,
            })
            .log_err();
    }

    fn ack(&self, offset: u64) {
        self.client
            .send(proto::AckTerminalOutput {
                project_id: REMOTE_SERVER_PROJECT_ID,
                terminal_id: self.terminal_id,
                offset,
            })
            .log_err();
    }

    fn resync(&self, from_offset: u64, cols: u16, rows: u16) {
        // The response is informational: the replayed output (and, for a terminal
        // that exited meanwhile, a re-sent `TerminalExited`) arrives through the
        // ordinary handlers.
        let request = self.client.request(proto::AttachTerminal {
            project_id: REMOTE_SERVER_PROJECT_ID,
            terminal_id: self.terminal_id,
            from_offset,
            cols: cols as u32,
            rows: rows as u32,
        });
        self.executor
            .spawn(async move {
                request.await.log_err();
            })
            .detach();
    }

    fn close(&self) {
        self.client
            .send(proto::CloseTerminal {
                project_id: REMOTE_SERVER_PROJECT_ID,
                terminal_id: self.terminal_id,
            })
            .log_err();
    }
}

fn create_remote_shell(
    spawn_command: Option<(&String, &Vec<String>)>,
    mut env: HashMap<String, String>,
    working_directory: Option<Arc<Path>>,
    remote_client: Entity<RemoteClient>,
    cx: &mut App,
) -> Result<(Shell, HashMap<String, String>)> {
    insert_zed_terminal_env(&mut env, &release_channel::AppVersion::global(cx));

    let (program, args) = match spawn_command {
        Some((program, args)) => (Some(program.clone()), args),
        None => (None, &Vec::new()),
    };

    let command = remote_client.read(cx).build_command(
        program,
        args.as_slice(),
        &env,
        working_directory.map(|path| path.display().to_string()),
        None,
        Interactive::Yes,
    )?;

    log::debug!("Connecting to a remote server: {:?}", command.program);
    let host = remote_client.read(cx).connection_options().display_name();

    Ok((
        Shell::WithArguments {
            program: command.program,
            args: command.args,
            title_override: Some(format!("{} — Terminal", host)),
        },
        command.env,
    ))
}

fn format_task_for_activation(
    spawn_task: &SpawnInTerminal,
    shell_kind: ShellKind,
    shell: &str,
    is_windows: bool,
) -> String {
    if let Some(command) = &spawn_task.command {
        let command = shell_kind.prepend_command_prefix(command);
        let command = shell_kind.try_quote_prefix_aware(&command);
        let args = spawn_task
            .args
            .iter()
            .enumerate()
            .filter_map(|(index, arg)| {
                quote_prepared_task_arg_for_activation(
                    spawn_task, shell_kind, arg, index, is_windows,
                )
            });

        command.into_iter().chain(args).join(" ")
    } else {
        // todo: this breaks for remotes to windows
        format!("exec {shell} -l")
    }
}

fn quote_prepared_task_arg_for_activation<'a>(
    spawn_task: &SpawnInTerminal,
    shell_kind: ShellKind,
    arg: &'a str,
    index: usize,
    is_windows: bool,
) -> Option<Cow<'a, str>> {
    if spawn_task.shell.shell_kind(is_windows) == ShellKind::Cmd
        && index >= 2
        && spawn_task
            .args
            .get(index - 2)
            .is_some_and(|arg| arg.eq_ignore_ascii_case("/S"))
        && spawn_task
            .args
            .get(index - 1)
            .is_some_and(|arg| arg.eq_ignore_ascii_case("/C"))
    {
        // The /C argument is already a cmd command string from prepare_task_for_spawn.
        // Quoting it again for venv activation makes cmd see the quotes as literals.
        return quote_cmd_command_arg_for_outer_shell(arg, shell_kind).map(Cow::Owned);
    }

    shell_kind.try_quote(arg)
}

fn quote_cmd_command_arg_for_outer_shell(arg: &str, shell_kind: ShellKind) -> Option<String> {
    match shell_kind {
        ShellKind::PowerShell | ShellKind::Pwsh => Some(format!("'{}'", arg.replace('\'', "''"))),
        ShellKind::Cmd => Some(arg.to_string()),
        ShellKind::Posix
        | ShellKind::Csh
        | ShellKind::Tcsh
        | ShellKind::Fish
        | ShellKind::Nushell
        | ShellKind::Rc
        | ShellKind::Xonsh
        | ShellKind::Elvish => shell_kind.try_quote(arg).map(Cow::into_owned),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pretty_assertions::assert_eq;

    fn prepared_cmd_task(command_arg: &str) -> SpawnInTerminal {
        SpawnInTerminal {
            command: Some("cmd.exe".to_string()),
            args: vec!["/S".to_string(), "/C".to_string(), command_arg.to_string()],
            shell: Shell::Program("cmd.exe".to_string()),
            ..SpawnInTerminal::default()
        }
    }

    #[test]
    fn formats_prepared_cmd_task_for_powershell_activation() {
        let task = prepared_cmd_task("\"echo Hi there\"");

        assert_eq!(
            format_task_for_activation(&task, ShellKind::PowerShell, "powershell.exe", true),
            "&cmd.exe /S /C '\"echo Hi there\"'"
        );
    }

    #[test]
    fn formats_prepared_cmd_task_for_cmd_activation() {
        let task = prepared_cmd_task("\"echo Hi there\"");

        assert_eq!(
            format_task_for_activation(&task, ShellKind::Cmd, "cmd.exe", true),
            "cmd.exe /S /C \"echo Hi there\""
        );
    }

    #[test]
    fn formats_prepared_cmd_task_with_shell_args_for_activation() {
        let task = SpawnInTerminal {
            command: Some("cmd.exe".to_string()),
            args: vec![
                "/D".to_string(),
                "/S".to_string(),
                "/C".to_string(),
                "\"echo Hi there\"".to_string(),
            ],
            shell: Shell::WithArguments {
                program: "cmd.exe".to_string(),
                args: vec!["/D".to_string()],
                title_override: None,
            },
            ..SpawnInTerminal::default()
        };

        assert_eq!(
            format_task_for_activation(&task, ShellKind::PowerShell, "powershell.exe", true),
            "&cmd.exe /D /S /C '\"echo Hi there\"'"
        );
    }

    #[test]
    fn formats_prepared_cmd_task_with_single_quote_for_powershell_activation() {
        let task = prepared_cmd_task("\"echo It's fine\"");

        assert_eq!(
            format_task_for_activation(&task, ShellKind::PowerShell, "powershell.exe", true),
            "&cmd.exe /S /C '\"echo It''s fine\"'"
        );
    }

    #[test]
    fn formats_non_cmd_task_for_activation() {
        let task = SpawnInTerminal {
            command: Some("cargo".to_string()),
            args: vec!["test".to_string(), "some test".to_string()],
            shell: Shell::System,
            ..SpawnInTerminal::default()
        };

        assert_eq!(
            format_task_for_activation(&task, ShellKind::PowerShell, "powershell.exe", true),
            "&cargo test 'some test'"
        );
    }
}

/// Client-side tests for the remote terminal protocol, driven against a fake
/// proto server: no `HeadlessProject`, no PTY, just the messages.
#[cfg(test)]
mod remote_terminal_tests {
    use super::*;
    use client::{Client, UserStore};
    use clock::FakeSystemClock;
    use fs::FakeFs;
    use gpui::TestAppContext;
    use http_client::FakeHttpClient;
    use language::LanguageRegistry;
    use node_runtime::NodeRuntime;
    use remote::RemoteClientEvent;
    use settings::SettingsStore;
    use std::sync::atomic::{AtomicBool, Ordering};

    /// The server half of the terminal protocol, as far as the client can see
    /// it: records every message the client sends and answers requests from
    /// canned state.
    #[derive(Default)]
    struct FakeTerminalServer {
        spawns: Vec<proto::SpawnTerminal>,
        attaches: Vec<proto::AttachTerminal>,
        inputs: Vec<proto::TerminalInput>,
        acks: Vec<proto::AckTerminalOutput>,
        resizes: Vec<proto::ResizeTerminal>,
        closes: Vec<proto::CloseTerminal>,
        list_requests: usize,
        /// What `ListTerminals` answers.
        inventory: Vec<proto::TerminalInfo>,
        /// The id handed to the next `SpawnTerminal`.
        next_terminal_id: u64,
        /// When set, every `AttachTerminal` fails as if the id were unknown.
        reject_attach: bool,
    }

    impl FakeTerminalServer {
        fn install(
            server_session: &AnyProtoClient,
            server_cx: &mut TestAppContext,
        ) -> Entity<Self> {
            let fake = server_cx.new(|_| Self {
                next_terminal_id: 7,
                ..Self::default()
            });
            server_session.subscribe_to_entity(REMOTE_SERVER_PROJECT_ID, &fake);
            server_session
                .add_request_handler::<proto::Ping, _, _, _>(fake.downgrade(), |_, _, _| async {
                    Ok(proto::Ack {})
                });
            server_session.add_entity_request_handler::<proto::SpawnTerminal, Self, _, _>(
                |this, envelope, mut cx| async move {
                    this.update(&mut cx, |this, _| {
                        let terminal_id = this.next_terminal_id;
                        this.next_terminal_id += 1;
                        this.spawns.push(envelope.payload);
                        Ok(proto::SpawnTerminalResponse { terminal_id })
                    })
                },
            );
            server_session.add_entity_request_handler::<proto::AttachTerminal, Self, _, _>(
                |this, envelope, mut cx| async move {
                    this.update(&mut cx, |this, _| {
                        let request = envelope.payload;
                        this.attaches.push(request.clone());
                        if this.reject_attach {
                            anyhow::bail!("unknown terminal {}", request.terminal_id);
                        }
                        Ok(proto::AttachTerminalResponse {
                            replayed_from: request.from_offset,
                            end_offset: request.from_offset,
                            exit: None,
                        })
                    })
                },
            );
            server_session.add_entity_request_handler::<proto::ListTerminals, Self, _, _>(
                |this, _envelope, mut cx| async move {
                    this.update(&mut cx, |this, _| {
                        this.list_requests += 1;
                        Ok(proto::ListTerminalsResponse {
                            terminals: this.inventory.clone(),
                        })
                    })
                },
            );
            server_session.add_entity_message_handler::<proto::TerminalInput, Self, _, _>(
                |this, envelope, mut cx| async move {
                    this.update(&mut cx, |this, _| this.inputs.push(envelope.payload));
                    Ok(())
                },
            );
            server_session.add_entity_message_handler::<proto::AckTerminalOutput, Self, _, _>(
                |this, envelope, mut cx| async move {
                    this.update(&mut cx, |this, _| this.acks.push(envelope.payload));
                    Ok(())
                },
            );
            server_session.add_entity_message_handler::<proto::ResizeTerminal, Self, _, _>(
                |this, envelope, mut cx| async move {
                    this.update(&mut cx, |this, _| this.resizes.push(envelope.payload));
                    Ok(())
                },
            );
            server_session.add_entity_message_handler::<proto::CloseTerminal, Self, _, _>(
                |this, envelope, mut cx| async move {
                    this.update(&mut cx, |this, _| this.closes.push(envelope.payload));
                    Ok(())
                },
            );
            fake
        }

        fn attach_offsets(&self) -> Vec<(u64, u64)> {
            self.attaches
                .iter()
                .map(|attach| (attach.terminal_id, attach.from_offset))
                .collect()
        }

        fn closed_ids(&self) -> Vec<u64> {
            self.closes.iter().map(|close| close.terminal_id).collect()
        }
    }

    /// A remote project over the mock transport. `remote_pty` selects whether
    /// the mock claims to host PTYs (the WebSocket transport) or not (ssh-style).
    async fn build_remote_project(
        remote_pty: bool,
        cx: &mut TestAppContext,
        server_cx: &mut TestAppContext,
    ) -> (Entity<Project>, Entity<FakeTerminalServer>, AnyProtoClient) {
        zlog::init_test();
        cx.update(|cx| {
            release_channel::init(semver::Version::new(0, 0, 0), cx);
            if !cx.has_global::<SettingsStore>() {
                let settings_store = SettingsStore::test(cx);
                cx.set_global(settings_store);
            }
        });
        server_cx.update(|cx| release_channel::init(semver::Version::new(0, 0, 0), cx));

        let (opts, server_session, connect_guard) = if remote_pty {
            RemoteClient::fake_server_with_remote_pty(cx, server_cx)
        } else {
            RemoteClient::fake_server(cx, server_cx)
        };
        let fake = FakeTerminalServer::install(&server_session, server_cx);
        drop(connect_guard);
        let remote_client = RemoteClient::connect_mock(opts, cx).await;

        let client = cx.update(|cx| {
            Client::new(
                Arc::new(FakeSystemClock::new()),
                FakeHttpClient::with_404_response(),
                cx,
            )
        });
        let user_store = cx.new(|cx| UserStore::new(client.clone(), cx));
        let languages = Arc::new(LanguageRegistry::test(cx.executor()));
        let fs = FakeFs::new(cx.executor());
        cx.update(|cx| Project::init(&client, cx));
        let project = cx.update(|cx| {
            Project::remote(
                remote_client,
                client,
                NodeRuntime::unavailable(),
                user_store,
                languages,
                fs,
                false,
                cx,
            )
        });
        (project, fake, server_session)
    }

    /// Runs both sides until every queued message and task has been processed.
    ///
    /// The empty updates flush effects, which is when GPUI releases dropped
    /// entities (and so runs `Drop for Terminal`); the clock advance fires the
    /// terminal's output-batching timer, which `run_until_parked` alone never
    /// does (the deterministic scheduler only moves time when asked).
    fn settle(cx: &mut TestAppContext, server_cx: &mut TestAppContext) {
        for _ in 0..4 {
            cx.update(|_| {});
            server_cx.update(|_| {});
            cx.run_until_parked();
            server_cx.run_until_parked();
            cx.executor().advance_clock(Duration::from_millis(5));
            cx.run_until_parked();
            server_cx.run_until_parked();
        }
    }

    async fn create_shell(
        project: &Entity<Project>,
        working_directory: &str,
        cx: &mut TestAppContext,
        server_cx: &mut TestAppContext,
    ) -> Entity<Terminal> {
        let terminal = project
            .update(cx, |project, cx| {
                project.create_terminal_shell(Some(PathBuf::from(working_directory)), cx)
            })
            .await
            .expect("remote shell spawns");
        settle(cx, server_cx);
        terminal
    }

    fn output(terminal_id: u64, offset: u64, data: &[u8]) -> proto::TerminalOutput {
        proto::TerminalOutput {
            project_id: REMOTE_SERVER_PROJECT_ID,
            terminal_id,
            offset,
            data: data.to_vec(),
            reset: false,
        }
    }

    fn content(terminal: &Entity<Terminal>, cx: &TestAppContext) -> String {
        terminal.read_with(cx, |terminal, _| terminal.get_content())
    }

    /// The id a `RemoteTerminalGone` error names, if that is what `result` is.
    fn gone_id(result: Result<Entity<Terminal>>) -> Option<u64> {
        result
            .err()
            .and_then(|error| error.downcast::<RemoteTerminalGone>().ok())
            .map(|gone| gone.0)
    }

    #[test]
    fn remote_terminal_input_is_chunked_at_64k() {
        let data = vec![b'x'; (1 << 20) + 5];
        let chunks: Vec<&[u8]> = chunk_input(&data).collect();
        assert_eq!(chunks.len(), 17);
        assert!(chunks[..16].iter().all(|chunk| chunk.len() == INPUT_CHUNK));
        assert_eq!(chunks[16].len(), 5);
        assert_eq!(chunks.concat(), data);
    }

    #[gpui::test]
    async fn remote_terminal_shell_spawns_attaches_and_streams(
        cx: &mut TestAppContext,
        server_cx: &mut TestAppContext,
    ) {
        let (project, fake, server_session) = build_remote_project(true, cx, server_cx).await;
        assert!(cx.read(|cx| project.read(cx).supports_remote_pty(cx)));

        let terminal = create_shell(&project, "/workspaces/app", cx, server_cx).await;

        terminal.read_with(cx, |terminal, _| {
            assert!(terminal.is_remote_pty());
            assert_eq!(terminal.remote_terminal_id(), Some(7));
            assert_eq!(
                terminal.remote_working_directory(),
                Some(Path::new("/workspaces/app"))
            );
            assert!(terminal.has_active_pty_resources());
            assert!(terminal.title(true).ends_with(" — Terminal"));
        });
        fake.read_with(server_cx, |fake, _| {
            assert_eq!(fake.spawns.len(), 1);
            let spawn = &fake.spawns[0];
            assert_eq!(spawn.project_id, REMOTE_SERVER_PROJECT_ID);
            assert_eq!(spawn.working_directory.as_deref(), Some("/workspaces/app"));
            assert_eq!(spawn.shell, Some(shell_to_proto(Shell::System)));
            assert_eq!(spawn.task_id, None);
            assert!(
                spawn
                    .title
                    .as_deref()
                    .is_some_and(|title| title.ends_with(" — Terminal")),
                "{:?}",
                spawn.title
            );
            assert_eq!(spawn.env.get("ZED_TERM").map(String::as_str), Some("true"));
            assert!(spawn.cols > 0 && spawn.rows > 0);
            // Spawn is detached; the attach that starts the stream follows at once.
            assert_eq!(fake.attach_offsets(), vec![(7, 0)]);
            assert_eq!(fake.attaches[0].cols, spawn.cols);
            assert_eq!(fake.attaches[0].rows, spawn.rows);
        });

        // Output is parsed into the grid and acknowledged.
        server_session.send(output(7, 0, b"$ hello")).unwrap();
        settle(cx, server_cx);
        assert!(content(&terminal, cx).contains("hello"));
        terminal.read_with(cx, |terminal, _| {
            assert_eq!(terminal.remote_next_offset(), Some(7));
        });
        fake.read_with(server_cx, |fake, _| {
            assert_eq!(fake.acks.last().map(|ack| ack.offset), Some(7));
        });

        // Keystrokes go to the server terminal.
        terminal.update(cx, |terminal, _| terminal.input(b"ls\r".to_vec()));
        settle(cx, server_cx);
        fake.read_with(server_cx, |fake, _| {
            assert_eq!(fake.inputs.len(), 1);
            assert_eq!(fake.inputs[0].terminal_id, 7);
            assert_eq!(fake.inputs[0].data, b"ls\r");
        });

        project.read_with(cx, |project, _| {
            assert_eq!(project.local_terminal_handles().len(), 1);
            assert!(project.terminals.remote.contains_key(&7));
        });

        // Closing the tab closes the server terminal once and forgets it.
        drop(terminal);
        settle(cx, server_cx);
        fake.read_with(server_cx, |fake, _| assert_eq!(fake.closed_ids(), vec![7]));
        project.read_with(cx, |project, _| {
            assert!(project.local_terminal_handles().is_empty());
            assert!(project.terminals.remote.is_empty());
        });
    }

    #[gpui::test]
    async fn remote_terminal_task_reports_exit_code(
        cx: &mut TestAppContext,
        server_cx: &mut TestAppContext,
    ) {
        let (project, fake, server_session) = build_remote_project(true, cx, server_cx).await;

        let terminal = project
            .update(cx, |project, cx| {
                project.create_terminal_task(
                    SpawnInTerminal {
                        id: TaskId("task-1".to_string()),
                        label: "t".to_string(),
                        full_label: "t".to_string(),
                        command: Some("sh".to_string()),
                        args: vec!["-c".to_string(), "exit 4".to_string()],
                        show_summary: true,
                        ..SpawnInTerminal::default()
                    },
                    cx,
                )
            })
            .await
            .expect("remote task spawns");
        settle(cx, server_cx);

        terminal.read_with(cx, |terminal, _| {
            assert!(terminal.is_remote_pty());
            assert!(terminal.task().is_some());
            assert_eq!(terminal.remote_terminal_id(), Some(7));
            assert_eq!(terminal.title_override(), Some("t"));
        });
        fake.read_with(server_cx, |fake, _| {
            let spawn = &fake.spawns[0];
            assert_eq!(spawn.task_id.as_deref(), Some("task-1"));
            assert_eq!(spawn.title.as_deref(), Some("t"));
            assert_eq!(
                spawn.shell,
                Some(shell_to_proto(Shell::WithArguments {
                    program: "sh".to_string(),
                    args: vec!["-c".to_string(), "exit 4".to_string()],
                    title_override: None,
                }))
            );
            assert_eq!(fake.attach_offsets(), vec![(7, 0)]);
        });

        server_session.send(output(7, 0, b"failing\r\n")).unwrap();
        server_session
            .send(proto::TerminalExited {
                project_id: REMOTE_SERVER_PROJECT_ID,
                terminal_id: 7,
                exit: Some(proto::TerminalExit {
                    code: Some(4),
                    signal: None,
                }),
                end_offset: 9,
            })
            .unwrap();
        settle(cx, server_cx);

        let status = terminal
            .update(cx, |terminal, cx| terminal.wait_for_completed_task(cx))
            .await;
        assert_eq!(status.and_then(|status| status.code()), Some(4));
        terminal.read_with(cx, |terminal, _| {
            assert!(!terminal.has_active_pty_resources());
            assert!(terminal.get_content().contains("failing"));
        });

        // The server keeps an exited terminal (with its scrollback) until it is
        // told to close it, so dropping the tab sends exactly one CloseTerminal
        // and the server frees the entry - it does not leak until a reload.
        drop(terminal);
        settle(cx, server_cx);
        fake.read_with(server_cx, |fake, _| assert_eq!(fake.closed_ids(), vec![7]));
    }

    #[gpui::test]
    async fn remote_terminal_reattaches_after_reconnect(
        cx: &mut TestAppContext,
        server_cx: &mut TestAppContext,
    ) {
        let (project, fake, server_session) = build_remote_project(true, cx, server_cx).await;
        let terminal = create_shell(&project, "/", cx, server_cx).await;

        server_session.send(output(7, 0, b"0123456789")).unwrap();
        settle(cx, server_cx);
        terminal.read_with(cx, |terminal, _| {
            assert_eq!(terminal.remote_next_offset(), Some(10));
        });

        let remote_client = project.read_with(cx, |project, _| project.remote_client().unwrap());
        let reconnected = Arc::new(AtomicBool::new(false));
        let _subscription = cx.update(|cx| {
            let reconnected = reconnected.clone();
            cx.subscribe(&remote_client, move |_, event, _| {
                if matches!(event, RemoteClientEvent::Reconnected) {
                    reconnected.store(true, Ordering::SeqCst);
                }
            })
        });
        remote_client
            .update(cx, |client, cx| client.simulate_disconnect(cx))
            .detach();
        settle(cx, server_cx);
        assert!(reconnected.load(Ordering::SeqCst));

        // The stream resumes from what the grid already has, not from zero.
        fake.read_with(server_cx, |fake, _| {
            assert_eq!(fake.attach_offsets(), vec![(7, 0), (7, 10)]);
        });

        // A replay overlapping the grid only contributes its new bytes.
        server_session.send(output(7, 5, b"56789abcde")).unwrap();
        settle(cx, server_cx);
        terminal.read_with(cx, |terminal, _| {
            assert_eq!(terminal.remote_next_offset(), Some(15));
            assert!(terminal.get_content().contains("0123456789abcde"));
            assert!(terminal.has_active_pty_resources());
        });
    }

    #[gpui::test]
    async fn remote_terminal_attach_failure_after_reconnect_marks_lost(
        cx: &mut TestAppContext,
        server_cx: &mut TestAppContext,
    ) {
        let (project, fake, _server_session) = build_remote_project(true, cx, server_cx).await;
        let terminal = create_shell(&project, "/", cx, server_cx).await;

        fake.update(server_cx, |fake, _| fake.reject_attach = true);
        let remote_client = project.read_with(cx, |project, _| project.remote_client().unwrap());
        remote_client
            .update(cx, |client, cx| client.simulate_disconnect(cx))
            .detach();
        settle(cx, server_cx);

        fake.read_with(server_cx, |fake, _| {
            assert_eq!(fake.attach_offsets(), vec![(7, 0), (7, 0)]);
        });
        terminal.read_with(cx, |terminal, _| {
            assert!(terminal.get_content().contains("no longer available"));
            assert!(!terminal.has_active_pty_resources());
        });

        // A lost terminal sends nothing more, and is not closed when dropped.
        terminal.update(cx, |terminal, _| terminal.input(b"ls\r".to_vec()));
        settle(cx, server_cx);
        fake.read_with(server_cx, |fake, _| assert!(fake.inputs.is_empty()));
        drop(terminal);
        settle(cx, server_cx);
        fake.read_with(server_cx, |fake, _| assert!(fake.closes.is_empty()));
    }

    #[gpui::test]
    async fn remote_terminal_fresh_session_restores_and_reaps(
        cx: &mut TestAppContext,
        server_cx: &mut TestAppContext,
    ) {
        let (project, fake, server_session) = build_remote_project(true, cx, server_cx).await;
        fake.update(server_cx, |fake, _| {
            fake.inventory = vec![
                proto::TerminalInfo {
                    terminal_id: 7,
                    title: "seven".to_string(),
                    cwd: Some("/workspaces/seven".to_string()),
                    cols: 80,
                    rows: 24,
                    ..Default::default()
                },
                proto::TerminalInfo {
                    terminal_id: 9,
                    title: "nine".to_string(),
                    exit: Some(proto::TerminalExit {
                        code: Some(0),
                        signal: None,
                    }),
                    ..Default::default()
                },
                proto::TerminalInfo {
                    terminal_id: 11,
                    title: "eleven".to_string(),
                    cwd: Some("/workspaces/eleven".to_string()),
                    ..Default::default()
                },
                proto::TerminalInfo {
                    terminal_id: 13,
                    title: "thirteen".to_string(),
                    ..Default::default()
                },
            ];
        });

        project
            .update(cx, |project, cx| {
                project.fetch_remote_terminal_inventory(cx)
            })
            .await;
        settle(cx, server_cx);
        fake.read_with(server_cx, |fake, _| assert_eq!(fake.list_requests, 1));
        project.read_with(cx, |project, _| {
            assert_eq!(project.terminals.restorable.len(), 4);
        });

        // A live terminal is reattached from offset 0, with the server's title and cwd.
        let seven = project
            .update(cx, |project, cx| {
                project.restore_remote_terminal(7, None, None, cx)
            })
            .expect("terminal 7 is restorable");
        settle(cx, server_cx);
        seven.read_with(cx, |terminal, _| {
            assert!(terminal.is_remote_pty());
            assert_eq!(terminal.remote_terminal_id(), Some(7));
            assert_eq!(terminal.title_override(), Some("seven"));
            assert_eq!(
                terminal.remote_working_directory(),
                Some(Path::new("/workspaces/seven"))
            );
            assert!(terminal.has_active_pty_resources());
        });
        fake.read_with(server_cx, |fake, _| {
            assert!(fake.spawns.is_empty());
            assert_eq!(fake.attach_offsets(), vec![(7, 0)]);
            assert!(fake.closes.is_empty());
        });
        project.read_with(cx, |project, _| {
            assert!(project.terminals.remote.contains_key(&7));
            assert_eq!(project.local_terminal_handles().len(), 1);
        });

        // The replayed scrollback lands in the restored grid.
        server_session
            .send(output(7, 0, b"old scrollback"))
            .unwrap();
        settle(cx, server_cx);
        assert!(content(&seven, cx).contains("old scrollback"));

        // The persisted title and working directory win over the server's.
        let eleven = project
            .update(cx, |project, cx| {
                project.restore_remote_terminal(
                    11,
                    Some(PathBuf::from("/persisted")),
                    Some("custom".to_string()),
                    cx,
                )
            })
            .expect("terminal 11 is restorable");
        eleven.read_with(cx, |terminal, _| {
            assert_eq!(terminal.title_override(), Some("custom"));
            assert_eq!(
                terminal.remote_working_directory(),
                Some(Path::new("/persisted"))
            );
        });

        // A terminal is restorable once; an exited one is closed on the spot;
        // an unknown one is simply gone.
        assert_eq!(
            gone_id(project.update(cx, |project, cx| {
                project.restore_remote_terminal(7, None, None, cx)
            })),
            Some(7)
        );
        assert_eq!(
            gone_id(project.update(cx, |project, cx| {
                project.restore_remote_terminal(9, None, None, cx)
            })),
            Some(9)
        );
        assert_eq!(
            gone_id(project.update(cx, |project, cx| {
                project.restore_remote_terminal(42, None, None, cx)
            })),
            Some(42)
        );
        settle(cx, server_cx);
        fake.read_with(server_cx, |fake, _| assert_eq!(fake.closed_ids(), vec![9]));

        // Whatever nobody restored is reaped; restored terminals are untouched.
        project.update(cx, |project, cx| {
            project.close_unrestored_remote_terminals(cx)
        });
        settle(cx, server_cx);
        fake.read_with(server_cx, |fake, _| {
            assert_eq!(fake.closed_ids(), vec![9, 13])
        });
        project.read_with(cx, |project, _| {
            assert!(project.terminals.restorable.is_empty());
            assert_eq!(project.local_terminal_handles().len(), 2);
        });
        seven.read_with(cx, |terminal, _| {
            assert!(terminal.has_active_pty_resources())
        });
    }

    #[gpui::test]
    async fn remote_terminal_unknown_ids_are_ignored(
        cx: &mut TestAppContext,
        server_cx: &mut TestAppContext,
    ) {
        let (project, _fake, server_session) = build_remote_project(true, cx, server_cx).await;
        let terminal = create_shell(&project, "/", cx, server_cx).await;

        server_session.send(output(0xdead, 0, b"stray")).unwrap();
        server_session
            .send(proto::TerminalExited {
                project_id: REMOTE_SERVER_PROJECT_ID,
                terminal_id: 0xdead,
                exit: Some(proto::TerminalExit {
                    code: Some(1),
                    signal: None,
                }),
                end_offset: 5,
            })
            .unwrap();
        settle(cx, server_cx);

        terminal.read_with(cx, |terminal, _| {
            assert!(!terminal.get_content().contains("stray"));
            assert!(terminal.has_active_pty_resources());
            assert_eq!(terminal.remote_next_offset(), Some(0));
        });
        project.read_with(cx, |project, _| {
            assert_eq!(project.local_terminal_handles().len(), 1);
        });
    }

    #[cfg(unix)]
    #[gpui::test]
    async fn remote_terminal_ssh_style_connection_keeps_build_command(
        cx: &mut TestAppContext,
        server_cx: &mut TestAppContext,
    ) {
        let (project, fake, _server_session) = build_remote_project(false, cx, server_cx).await;
        assert!(!cx.read(|cx| project.read(cx).supports_remote_pty(cx)));

        // The mock transport's `build_command` names a `mock` program that cannot
        // be spawned locally, which is exactly what the ssh path would try.
        let result = project
            .update(cx, |project, cx| {
                project.create_terminal_shell(Some(PathBuf::from("/")), cx)
            })
            .await;
        settle(cx, server_cx);
        let error = result.err().expect("the `mock` program cannot be spawned");
        assert!(format!("{error:#}").contains("mock"), "{error:#}");
        fake.read_with(server_cx, |fake, _| {
            assert!(fake.spawns.is_empty());
            assert!(fake.attaches.is_empty());
        });
    }
}
