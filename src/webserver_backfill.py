from __future__ import annotations

from webserver_backend import storage


def main() -> None:
    storage.ensure_webserver_dirs()
    for instance_root in sorted(path for path in storage.WEBSERVER_INSTANCE_ROOT.iterdir() if path.is_dir()):
        storage.rebuild_instance_artifact_manifest(instance_root.name)
    for run_root in sorted(path for path in storage.WEBSERVER_RUN_ROOT.iterdir() if path.is_dir()):
        if not storage.sync_manifest_path(run_root.name).exists():
            job = storage.read_pipeline_job(run_root.name)
            storage.write_sync_manifest(
                run_root.name,
                {
                    "runtime_id": job.get("runtime_id"),
                    "runtime_kind": job.get("runtime_kind"),
                    "sync_status": job.get("sync_status"),
                    "last_sync_at": job.get("last_sync_at"),
                    "remote_run_dir": job.get("remote_run_dir"),
                    "remote_pid_path": job.get("remote_pid_path"),
                    "remote_exit_path": job.get("remote_exit_path"),
                    "stop_requested": str(job.get("status") or "").strip().lower() == "stopped",
                },
            )
        storage.rebuild_run_artifact_manifest(run_root.name)
    storage.list_runs_from_manifests()
    storage.list_instances_from_manifests()
    print("webserver manifest backfill completed")


if __name__ == "__main__":
    main()
