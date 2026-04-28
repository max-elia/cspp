from __future__ import annotations

import json
import math
import os
import shlex
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from webserver_runtime import WEBSERVER_ROOT


PROJECT_ROOT = Path(__file__).resolve().parents[1]
RUNTIME_CONFIG_PATH = PROJECT_ROOT / "configs" / "cspp_runtimes.json"
RUNTIME_STATE_ROOT = WEBSERVER_ROOT / "state" / "runtime_queues"
DEFAULT_LOCAL_EXPORT_ROOT = WEBSERVER_ROOT / "exports"


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def ensure_runtime_state_dirs() -> Path:
    RUNTIME_STATE_ROOT.mkdir(parents=True, exist_ok=True)
    return RUNTIME_STATE_ROOT


def _read_json(path: Path, default: Any = None) -> Any:
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return default


def _write_json(path: Path, payload: Any) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=True), encoding="utf-8")
    return path


def queue_state_path(runtime_id: str) -> Path:
    ensure_runtime_state_dirs()
    return RUNTIME_STATE_ROOT / f"{runtime_id}.json"


def load_runtime_config() -> dict[str, Any]:
    payload = _read_json(RUNTIME_CONFIG_PATH, {}) or {}
    project_root = PROJECT_ROOT.resolve()
    runtimes = payload.get("runtimes")
    if not isinstance(runtimes, list) or not runtimes:
        runtimes = []
    valid_runtimes: list[dict[str, Any]] = []
    seen_ids: set[str] = set()
    for raw in runtimes:
        if not isinstance(raw, dict):
            continue
        runtime_id = str(raw.get("id") or "").strip()
        if not runtime_id or runtime_id in seen_ids:
            continue
        seen_ids.add(runtime_id)
        kind = str(raw.get("kind") or "local").strip().lower() or "local"
        poll_interval_sec = _safe_int(raw.get("poll_interval_sec")) or (10 if kind == "local" else 60)
        valid_runtimes.append(
            {
                "id": runtime_id,
                "label": str(raw.get("label") or runtime_id),
                "kind": kind,
                "project_root": str((project_root / str(raw.get("project_root") or PROJECT_ROOT)).resolve())
                if not Path(str(raw.get("project_root") or PROJECT_ROOT)).expanduser().is_absolute()
                else str(Path(str(raw.get("project_root") or PROJECT_ROOT)).expanduser().resolve()),
                "export_root": str((project_root / str(raw.get("export_root") or DEFAULT_LOCAL_EXPORT_ROOT)).resolve())
                if not Path(str(raw.get("export_root") or DEFAULT_LOCAL_EXPORT_ROOT)).expanduser().is_absolute()
                else str(Path(str(raw.get("export_root") or DEFAULT_LOCAL_EXPORT_ROOT)).expanduser().resolve()),
                "activation_command": str(raw.get("activation_command") or "source init_env.sh"),
                "prepare_commands": [str(item) for item in (raw.get("prepare_commands") or []) if str(item).strip()],
                "poll_interval_sec": max(5, poll_interval_sec),
                "source_sync_path": str(raw.get("source_sync_path") or "src/"),
                "host": raw.get("host"),
                "user": raw.get("user"),
                "password_env": raw.get("password_env"),
                "ssh_key_path_env": raw.get("ssh_key_path_env"),
                "remote_project_root": raw.get("remote_project_root"),
                "remote_export_root": raw.get("remote_export_root"),
                "tags": [str(item) for item in (raw.get("tags") or []) if str(item).strip()],
            }
        )

    if not any(runtime.get("id") == "local" for runtime in valid_runtimes):
        valid_runtimes.insert(
            0,
            {
                "id": "local",
                "label": "Local",
                "kind": "local",
                "project_root": str(PROJECT_ROOT),
                "export_root": str(DEFAULT_LOCAL_EXPORT_ROOT),
                "activation_command": "source init_env.sh",
                "prepare_commands": [],
                "poll_interval_sec": 10,
                "source_sync_path": "src/",
                "host": None,
                "user": None,
                "password_env": None,
                "ssh_key_path_env": None,
                "remote_project_root": None,
                "remote_export_root": None,
                "tags": ["default"],
            },
        )

    return {
        "schema_version": int(payload.get("schema_version") or 1),
        "runtimes": valid_runtimes,
    }


def list_runtimes() -> list[dict[str, Any]]:
    return list(load_runtime_config().get("runtimes") or [])


def get_runtime(runtime_id: str | None) -> dict[str, Any]:
    requested = str(runtime_id or "local").strip() or "local"
    for runtime in list_runtimes():
        if runtime.get("id") == requested:
            return runtime
    raise KeyError(f"Unknown runtime: {requested}")


def load_queue_state(runtime_id: str) -> dict[str, Any]:
    payload = _read_json(queue_state_path(runtime_id), {}) or {}
    return {
        "runtime_id": runtime_id,
        "active_run_id": payload.get("active_run_id"),
        "queued_run_ids": [str(item) for item in (payload.get("queued_run_ids") or []) if str(item).strip()],
        "updated_at": payload.get("updated_at"),
    }


def save_queue_state(runtime_id: str, *, active_run_id: str | None, queued_run_ids: list[str]) -> dict[str, Any]:
    payload = {
        "runtime_id": runtime_id,
        "active_run_id": active_run_id,
        "queued_run_ids": list(queued_run_ids),
        "updated_at": now_iso(),
    }
    _write_json(queue_state_path(runtime_id), payload)
    return payload


def _safe_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except Exception:
        return None


def _safe_int(value: Any) -> int | None:
    parsed = _safe_float(value)
    if parsed is None:
        return None
    return int(parsed)


def usable_cores_for(system_cores: int | None) -> int | None:
    if system_cores is None:
        return None
    return max(1, int(math.floor(max(1, int(system_cores)) / 2)))


def _ssh_prefix(runtime: dict[str, Any]) -> list[str]:
    host = str(runtime.get("host") or "").strip()
    user = str(runtime.get("user") or "").strip()
    if not host or not user:
        raise ValueError(f"SSH runtime '{runtime.get('id')}' is missing host/user")

    key_env = str(runtime.get("ssh_key_path_env") or "").strip()
    key_path = os.environ.get(key_env, "").strip() if key_env else ""
    prefix: list[str] = []
    if key_path:
        prefix = ["ssh", "-i", key_path, "-o", "StrictHostKeyChecking=no", f"{user}@{host}"]
        return prefix

    password_env = str(runtime.get("password_env") or "").strip()
    password = os.environ.get(password_env, "").strip() if password_env else ""
    if password:
        return [
            "sshpass",
            "-p",
            password,
            "ssh",
            "-o",
            "StrictHostKeyChecking=no",
            f"{user}@{host}",
        ]
    return ["ssh", "-o", "StrictHostKeyChecking=no", f"{user}@{host}"]


def _rsync_ssh_transport(runtime: dict[str, Any]) -> list[str]:
    host = str(runtime.get("host") or "").strip()
    user = str(runtime.get("user") or "").strip()
    if not host or not user:
        raise ValueError(f"SSH runtime '{runtime.get('id')}' is missing host/user")

    key_env = str(runtime.get("ssh_key_path_env") or "").strip()
    key_path = os.environ.get(key_env, "").strip() if key_env else ""
    if key_path:
        return ["ssh", "-i", key_path, "-o", "StrictHostKeyChecking=no"]
    return ["ssh", "-o", "StrictHostKeyChecking=no"]


def _remote_shell(runtime: dict[str, Any], command: str) -> subprocess.CompletedProcess[str]:
    prefix = _ssh_prefix(runtime)
    return subprocess.run(prefix + [command], text=True, capture_output=True, check=False)


def _quote_remote_path(path: str) -> str:
    """shlex-quote a remote path while preserving a leading ``~`` / ``~/``.

    ``shlex.quote`` wraps its input in single quotes, and bash does not expand
    ``~`` (or ``$HOME``) inside single quotes. For remote paths that start with
    a tilde we instead emit ``"$HOME"``/``"$HOME/"`` (double-quoted so ``$HOME``
    expands at runtime) concatenated with the single-quoted remainder. Bash
    treats adjacent quoted strings as a single word, so the result is safe to
    drop into any shell command.
    """
    text = str(path or "").strip()
    if not text:
        return shlex.quote(text)
    if text == "~":
        return '"$HOME"'
    if text.startswith("~/"):
        remainder = text[2:]
        return '"$HOME/"' + shlex.quote(remainder) if remainder else '"$HOME/"'
    return shlex.quote(text)


def _local_shell(command: str, *, cwd: str | Path | None = None) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["/bin/zsh", "-lc", command],
        cwd=str(cwd) if cwd else None,
        text=True,
        capture_output=True,
        check=False,
    )


def _resolved_activation_command(activation_command: str, root: str | Path) -> str:
    activation = str(activation_command or "source init_env.sh").strip() or "source init_env.sh"
    if activation == "source init_env.sh":
        root_path = str(root).rstrip("/")
        return f"source {_quote_remote_path(f'{root_path}/init_env.sh')}"
    return activation


def _probe_local(runtime: dict[str, Any]) -> dict[str, Any]:
    project_root = Path(str(runtime.get("project_root") or PROJECT_ROOT)).expanduser().resolve()
    activation = _resolved_activation_command(str(runtime.get("activation_command") or "source init_env.sh"), project_root)
    if not project_root.exists():
        return {
            "runtime_id": runtime["id"],
            "status": "unreachable",
            "ready": False,
            "checked_at": now_iso(),
            "error": f"project root not found: {project_root}",
        }

    result = _local_shell(
        f"cd {shlex.quote(str(project_root))} && {activation} && python src/run.py list >/dev/null && python -c 'import os; print(os.cpu_count() or 1)'"
    )
    if result.returncode != 0:
        error = (result.stderr or result.stdout or "").strip() or f"probe failed with exit code {result.returncode}"
        return {
            "runtime_id": runtime["id"],
            "status": "error",
            "ready": False,
            "checked_at": now_iso(),
            "error": error,
        }

    last_line = (result.stdout or "").strip().splitlines()[-1] if (result.stdout or "").strip() else ""
    system_cores = _safe_int(last_line) or max(1, int(os.cpu_count() or 1))
    return {
        "runtime_id": runtime["id"],
        "status": "ready",
        "ready": True,
        "checked_at": now_iso(),
        "system_cores": system_cores,
        "usable_cores": usable_cores_for(system_cores),
        "error": None,
    }


def _probe_ssh(runtime: dict[str, Any]) -> dict[str, Any]:
    remote_root = str(runtime.get("remote_project_root") or "").strip()
    activation = _resolved_activation_command(str(runtime.get("activation_command") or "source init_env.sh"), remote_root)
    if not remote_root:
        return {
            "runtime_id": runtime["id"],
            "status": "error",
            "ready": False,
            "checked_at": now_iso(),
            "error": "remote_project_root is not configured",
        }

    inner = (
        f"cd {_quote_remote_path(remote_root)} && {activation} && "
        "python src/run.py list >/dev/null && "
        "python -c 'import os; print(os.cpu_count() or 1)'"
    )
    command = f"bash -lc {shlex.quote(inner)}"
    result = _remote_shell(runtime, command)
    if result.returncode != 0:
        error = (result.stderr or result.stdout or "").strip() or f"probe failed with exit code {result.returncode}"
        return {
            "runtime_id": runtime["id"],
            "status": "error",
            "ready": False,
            "checked_at": now_iso(),
            "error": error,
        }

    last_line = (result.stdout or "").strip().splitlines()[-1] if (result.stdout or "").strip() else ""
    system_cores = _safe_int(last_line)
    return {
        "runtime_id": runtime["id"],
        "status": "ready",
        "ready": True,
        "checked_at": now_iso(),
        "system_cores": system_cores,
        "usable_cores": usable_cores_for(system_cores),
        "error": None,
    }


def probe_runtime(runtime_id: str, *, repair: bool = False) -> dict[str, Any]:
    runtime = get_runtime(runtime_id)
    if runtime.get("kind") == "ssh":
        probe = _probe_ssh(runtime)
    else:
        probe = _probe_local(runtime)
    if probe.get("ready") or not repair:
        return probe
    prepare_runtime(runtime_id)
    if runtime.get("kind") == "ssh":
        return _probe_ssh(runtime)
    return _probe_local(runtime)


def _run_prepare_commands_local(runtime: dict[str, Any]) -> None:
    project_root = Path(str(runtime.get("project_root") or PROJECT_ROOT)).expanduser().resolve()
    for command in runtime.get("prepare_commands") or []:
        result = _local_shell(f"cd {shlex.quote(str(project_root))} && {command}")
        if result.returncode != 0:
            message = (result.stderr or result.stdout or "").strip()
            raise RuntimeError(message or f"prepare command failed: {command}")


def _run_prepare_commands_ssh(runtime: dict[str, Any]) -> None:
    remote_root = str(runtime.get("remote_project_root") or "").strip()
    for command in runtime.get("prepare_commands") or []:
        inner = f"cd {_quote_remote_path(remote_root)} && {command}"
        result = _remote_shell(runtime, f"bash -lc {shlex.quote(inner)}")
        if result.returncode != 0:
            message = (result.stderr or result.stdout or "").strip()
            raise RuntimeError(message or f"prepare command failed: {command}")


def prepare_runtime(runtime_id: str) -> None:
    runtime = get_runtime(runtime_id)
    if runtime.get("kind") == "ssh":
        _run_prepare_commands_ssh(runtime)
        return
    _run_prepare_commands_local(runtime)


def _rsync_command(runtime: dict[str, Any], source: str, target: str, *, delete: bool = False) -> list[str]:
    cmd: list[str] = []
    key_env = str(runtime.get("ssh_key_path_env") or "").strip()
    key_path = os.environ.get(key_env, "").strip() if key_env else ""
    password_env = str(runtime.get("password_env") or "").strip()
    password = os.environ.get(password_env, "").strip() if password_env else ""
    if password and not key_path:
        cmd.extend(["sshpass", "-p", password])
    cmd.extend(["rsync", "-az"])
    if delete:
        cmd.append("--delete")
    cmd.extend(["-e", " ".join(_rsync_ssh_transport(runtime))])
    cmd.extend([source, target])
    return cmd


def sync_source_to_runtime(runtime_id: str) -> None:
    runtime = get_runtime(runtime_id)
    if runtime.get("kind") != "ssh":
        return
    remote_root = str(runtime.get("remote_project_root") or "").strip()
    local_root = Path(str(runtime.get("project_root") or PROJECT_ROOT)).expanduser().resolve()
    source_sync_path = str(runtime.get("source_sync_path") or "src/").strip() or "src/"
    source_path = local_root / source_sync_path
    if not source_path.exists():
        raise FileNotFoundError(f"Source sync path not found: {source_path}")
    remote_host = f"{runtime['user']}@{runtime['host']}:{remote_root.rstrip('/')}/{source_sync_path.rstrip('/')}/"
    result = subprocess.run(
        _rsync_command(runtime, f"{source_path}/", remote_host, delete=True),
        text=True,
        capture_output=True,
        check=False,
    )
    if result.returncode != 0:
        message = (result.stderr or result.stdout or "").strip()
        raise RuntimeError(message or "source sync failed")


def sync_run_to_runtime(runtime_id: str, local_run_dir: str | Path, remote_run_dir: str) -> None:
    runtime = get_runtime(runtime_id)
    if runtime.get("kind") != "ssh":
        return
    result = subprocess.run(
        _rsync_command(runtime, f"{Path(local_run_dir).expanduser().resolve()}/", f"{runtime['user']}@{runtime['host']}:{remote_run_dir}/", delete=True),
        text=True,
        capture_output=True,
        check=False,
    )
    if result.returncode != 0:
        message = (result.stderr or result.stdout or "").strip()
        raise RuntimeError(message or "run sync failed")


def sync_run_from_runtime(runtime_id: str, remote_run_dir: str, local_run_dir: str | Path) -> None:
    runtime = get_runtime(runtime_id)
    if runtime.get("kind") != "ssh":
        return
    result = subprocess.run(
        _rsync_command(runtime, f"{runtime['user']}@{runtime['host']}:{remote_run_dir}/", f"{Path(local_run_dir).expanduser().resolve()}/", delete=False),
        text=True,
        capture_output=True,
        check=False,
    )
    if result.returncode != 0:
        message = (result.stderr or result.stdout or "").strip()
        raise RuntimeError(message or "result sync failed")


def remote_run_paths(runtime_id: str, remote_run_id: str) -> dict[str, str]:
    runtime = get_runtime(runtime_id)
    remote_export_root = str(runtime.get("remote_export_root") or runtime.get("export_root") or "").strip()
    if not remote_export_root:
        raise ValueError(f"Runtime '{runtime_id}' is missing remote export root")
    remote_run_dir = f"{remote_export_root.rstrip('/')}/runs/{remote_run_id}"
    remote_state_dir = f"{remote_run_dir}/state"
    return {
        "remote_run_dir": remote_run_dir,
        "remote_state_dir": remote_state_dir,
        "remote_pid_path": f"{remote_state_dir}/runtime_job.pid",
        "remote_exit_path": f"{remote_state_dir}/runtime_job.exit_code",
        "remote_log_path": f"{remote_state_dir}/runtime_job.log",
    }


def _remote_command_for_runtime(
    command: list[str],
    *,
    remote_root: str,
    remote_user: str | None,
    local_run_dir: str | Path,
    remote_run_dir: str,
) -> list[str]:
    local_root = str(PROJECT_ROOT.resolve())
    local_run_dir_text = str(Path(local_run_dir).expanduser().resolve())
    remote_user_text = str(remote_user or "").strip()

    def _absolute_remote_path(value: str) -> str:
        text = str(value).rstrip("/")
        if text == "~":
            return f"/home/{remote_user_text}" if remote_user_text else text
        if text.startswith("~/"):
            if remote_user_text:
                return f"/home/{remote_user_text}/{text[2:]}"
            return text
        return text

    remote_root_clean = _absolute_remote_path(remote_root)
    remote_run_dir_clean = _absolute_remote_path(remote_run_dir)
    normalized: list[str] = []
    for index, item in enumerate(command):
        text = str(item)
        if index == 0:
            executable_name = Path(text).name.lower()
            if executable_name.startswith("python"):
                normalized.append("python")
                continue
        if text == local_run_dir_text or text.startswith(local_run_dir_text + "/"):
            suffix = text[len(local_run_dir_text) :]
            normalized.append(f"{remote_run_dir_clean}{suffix}")
            continue
        if text == local_root or text.startswith(local_root + "/"):
            suffix = text[len(local_root) :]
            normalized.append(f"{remote_root_clean}{suffix}")
            continue
        normalized.append(text)
    return normalized


def start_remote_run(
    runtime_id: str,
    *,
    local_run_dir: str | Path,
    remote_run_id: str,
    command: list[str],
    extra_env: dict[str, str] | None = None,
) -> dict[str, Any]:
    runtime = get_runtime(runtime_id)
    remote_root = str(runtime.get("remote_project_root") or "").strip()
    remote_export_root = str(runtime.get("remote_export_root") or runtime.get("export_root") or "").strip()
    if not remote_root:
        raise ValueError(f"Runtime '{runtime_id}' is missing remote roots")
    if not remote_export_root:
        raise ValueError(f"Runtime '{runtime_id}' is missing remote export root")
    paths = remote_run_paths(runtime_id, remote_run_id)
    remote_run_dir = paths["remote_run_dir"]
    remote_state_dir = paths["remote_state_dir"]
    remote_pid_path = paths["remote_pid_path"]
    remote_exit_path = paths["remote_exit_path"]
    remote_log_path = paths["remote_log_path"]
    sync_source_to_runtime(runtime_id)
    sync_run_to_runtime(runtime_id, local_run_dir, remote_run_dir)

    # Tilde-prefixed paths must stay tilde-aware; other values (e.g. numeric
    # thread counts) go through plain shlex quoting.
    tilde_env_keys = {"THESIS_EXPORT_ROOT", "RUN_DIR"}
    env_pairs = {
        "THESIS_EXPORT_ROOT": remote_export_root,
        "RUN_DIR": remote_run_dir,
        **(extra_env or {}),
    }

    def _quote_env_value(key: str, value: str) -> str:
        return _quote_remote_path(value) if key in tilde_env_keys else shlex.quote(value)

    env_prefix = " ".join(
        f"{key}={_quote_env_value(key, str(value))}"
        for key, value in env_pairs.items()
        if str(value).strip()
    )
    remote_command_items = _remote_command_for_runtime(
        command,
        remote_root=remote_root,
        remote_user=str(runtime.get("user") or ""),
        local_run_dir=local_run_dir,
        remote_run_dir=remote_run_dir,
    )
    joined_command = " ".join(shlex.quote(item) for item in remote_command_items)
    activation_command = _resolved_activation_command(str(runtime.get("activation_command") or "source init_env.sh"), remote_root)
    inner_command = (
        f"cd {_quote_remote_path(remote_root)} && {activation_command} && {env_prefix} {joined_command}; "
        f"rc=$?; mkdir -p {_quote_remote_path(remote_state_dir)}; echo $rc > {_quote_remote_path(remote_exit_path)}"
    )
    outer_command = (
        f"mkdir -p {_quote_remote_path(remote_state_dir)} && "
        f"rm -f {_quote_remote_path(remote_exit_path)} && "
        f"nohup /bin/bash -lc {shlex.quote(inner_command)} "
        f"> {_quote_remote_path(remote_log_path)} 2>&1 < /dev/null & "
        f"echo $! > {_quote_remote_path(remote_pid_path)}"
    )
    remote_command = f"bash -lc {shlex.quote(outer_command)}"
    result = _remote_shell(runtime, remote_command)
    if result.returncode != 0:
        message = (result.stderr or result.stdout or "").strip()
        raise RuntimeError(message or "remote start failed")
    return {
        "remote_run_dir": remote_run_dir,
        "remote_pid_path": remote_pid_path,
        "remote_exit_path": remote_exit_path,
        "remote_log_path": remote_log_path,
    }


def read_remote_run_status(runtime_id: str, *, remote_pid_path: str, remote_exit_path: str) -> dict[str, Any]:
    runtime = get_runtime(runtime_id)
    exit_q = _quote_remote_path(remote_exit_path)
    pid_q = _quote_remote_path(remote_pid_path)
    inner = (
        f"if [ -f {exit_q} ]; then "
        f'printf "EXIT %s\\n" "$(cat {exit_q})"; '
        f'elif [ -f {pid_q} ] && kill -0 "$(cat {pid_q})" 2>/dev/null; then '
        'printf "RUNNING\\n"; '
        "else "
        'printf "UNKNOWN\\n"; '
        "fi"
    )
    command = f"bash -lc {shlex.quote(inner)}"
    result = _remote_shell(runtime, command)
    output = (result.stdout or "").strip()
    if result.returncode != 0:
        return {"state": "error", "error": (result.stderr or output or "").strip() or "status check failed"}
    if output.startswith("EXIT "):
        return {"state": "finished", "returncode": _safe_int(output.split(" ", 1)[1]) or 0}
    if output == "RUNNING":
        return {"state": "running"}
    return {"state": "unknown"}


def stop_remote_run(runtime_id: str, *, remote_pid_path: str) -> None:
    runtime = get_runtime(runtime_id)
    pid_q = _quote_remote_path(remote_pid_path)
    inner = (
        f"if [ -f {pid_q} ]; then "
        f"pid=$(cat {pid_q}); "
        'kill "$pid" 2>/dev/null || true; '
        "fi"
    )
    command = f"bash -lc {shlex.quote(inner)}"
    _remote_shell(runtime, command)


def delete_remote_run_dir(runtime_id: str, remote_run_dir: str) -> None:
    """Remove a run directory on a remote runtime.

    Safety: the path must sit under the runtime's configured ``remote_export_root``
    so a bad value can't ``rm -rf`` something unrelated. Missing directories are
    silently ignored so delete is idempotent.
    """
    text = str(remote_run_dir or "").strip()
    if not text:
        return
    runtime = get_runtime(runtime_id)
    kind = str(runtime.get("kind") or "local").lower()
    if kind == "local":
        return
    export_root = str(runtime.get("remote_export_root") or "").strip().rstrip("/")
    if not export_root:
        return
    normalized = text.rstrip("/")
    if not (normalized == export_root or normalized.startswith(export_root + "/")):
        raise ValueError(
            f"refusing to delete remote path outside export root: {text!r} (export_root={export_root!r})"
        )
    path_q = _quote_remote_path(normalized)
    inner = f"rm -rf -- {path_q}"
    command = f"bash -lc {shlex.quote(inner)}"
    _remote_shell(runtime, command)
