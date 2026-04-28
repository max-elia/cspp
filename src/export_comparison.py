from __future__ import annotations


def refresh_run_catalog() -> None:
    """Compatibility hook for the public CLI.

    The private thesis repository maintained cross-run experiment catalogs.
    The public repository keeps runs self-contained, so there is nothing to
    refresh.
    """
    return None
