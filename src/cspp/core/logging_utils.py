"""
Shared logging utilities for CSPP cluster optimization.

Provides consistent formatting and progress reporting across all steps.
"""

import sys
from datetime import datetime


# ANSI color codes for terminal output
class Colors:
    HEADER = '\033[95m'
    BLUE = '\033[94m'
    CYAN = '\033[96m'
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    RED = '\033[91m'
    BOLD = '\033[1m'
    RESET = '\033[0m'


def supports_color():
    """Check if the terminal supports color output."""
    return hasattr(sys.stdout, 'isatty') and sys.stdout.isatty()


USE_COLOR = supports_color()


def color(text, color_code):
    """Apply color to text if supported."""
    if USE_COLOR:
        return f"{color_code}{text}{Colors.RESET}"
    return text


def timestamp():
    """Return current timestamp string."""
    return datetime.now().strftime('%H:%M:%S')


def format_duration(seconds):
    """Format duration in human-readable form."""
    if seconds < 60:
        return f"{seconds:.1f}s"
    elif seconds < 3600:
        return f"{seconds/60:.1f}min"
    else:
        hours = int(seconds // 3600)
        minutes = int((seconds % 3600) // 60)
        return f"{hours}h {minutes}min"


def format_gap(gap_value):
    """Format MIP gap as percentage."""
    if gap_value is None:
        return "N/A"
    return f"{gap_value:.2%}"


def format_cost(cost):
    """Format cost value."""
    if cost is None:
        return "N/A"
    return f"{cost:,.2f} EUR"


def format_status(status):
    """Format Gurobi status code to human-readable string."""
    status_map = {
        1: "LOADED",
        2: "OPTIMAL",
        3: "INFEASIBLE",
        4: "INF_OR_UNBD",
        5: "UNBOUNDED",
        6: "CUTOFF",
        7: "ITER_LIMIT",
        8: "NODE_LIMIT",
        9: "TIME_LIMIT",
        10: "SOLUTION_LIMIT",
        11: "INTERRUPTED",
        12: "NUMERIC",
        13: "SUBOPTIMAL",
        14: "INPROGRESS",
        15: "USER_OBJ_LIMIT",
    }
    return status_map.get(status, f"STATUS_{status}")


# === Header/Section Printing ===

def print_header(title, width=80):
    """Print a major section header."""
    print()
    print(color("=" * width, Colors.BOLD))
    print(color(f" {title}", Colors.BOLD + Colors.CYAN))
    print(color("=" * width, Colors.BOLD))


def print_subheader(title, width=80):
    """Print a subsection header."""
    print()
    print(color(f"--- {title} ---", Colors.YELLOW))


def print_step_header(step_num, step_name, total_steps=3):
    """Print a step header for the pipeline."""
    print()
    print(color("=" * 80, Colors.BOLD))
    print(color(f" STEP {step_num}/{total_steps}: {step_name}", Colors.BOLD + Colors.CYAN))
    print(color("=" * 80, Colors.BOLD))
    print(f"  Started at {timestamp()}")


def print_config(config_dict, title="Configuration"):
    """Print configuration parameters."""
    print_subheader(title)
    max_key_len = max(len(str(k)) for k in config_dict.keys())
    for key, value in config_dict.items():
        print(f"  {key:<{max_key_len}} : {value}")


# === Progress Reporting ===

def print_cluster_start(cluster_id, n_customers, cluster_num=None, total_clusters=None):
    """Print cluster optimization start message."""
    progress = ""
    if cluster_num is not None and total_clusters is not None:
        progress = f" [{cluster_num}/{total_clusters}]"
    print()
    print(color(f"CLUSTER {cluster_id}{progress}: {n_customers} customers", Colors.BOLD))
    print(f"  Started at {timestamp()}")


def print_cluster_result(cluster_id, time_sec, gap, iterations, timeout, objective=None):
    """Print cluster optimization result."""
    status_color = Colors.GREEN if not timeout else Colors.YELLOW
    status_text = "COMPLETED" if not timeout else "TIMEOUT"

    print(f"  {color(status_text, status_color)} in {format_duration(time_sec)}")
    print(f"  Gap: {format_gap(gap)} | Iterations: {iterations}")
    if objective is not None:
        print(f"  Objective: {format_cost(objective)}")


def print_scenario_start(scenario, scenario_num=None, total_scenarios=None):
    """Print scenario evaluation start."""
    progress = ""
    if scenario_num is not None and total_scenarios is not None:
        progress = f" [{scenario_num}/{total_scenarios}]"
    print()
    print(color(f"Scenario {scenario}{progress}", Colors.BOLD))


def print_scenario_result(scenario, cost, status, n_routes=None, n_clusters_solved=None):
    """Print scenario evaluation result."""
    status_colors = {
        "OPTIMAL_OR_FEASIBLE": Colors.GREEN,
        "TIME_LIMIT_INCUMBENT": Colors.YELLOW,
        "TIME_LIMIT_NO_INCUMBENT": Colors.RED,
        "INFEASIBLE": Colors.RED,
        "NO_SOLUTION": Colors.RED,
    }
    status_color = status_colors.get(status, Colors.RESET)

    parts = [f"  Status: {color(status, status_color)}"]
    parts.append(f"Cost: {format_cost(cost)}")
    if n_routes is not None:
        parts.append(f"Routes: {n_routes}")
    if n_clusters_solved is not None:
        parts.append(f"Clusters: {n_clusters_solved}")

    print(" | ".join(parts))


def print_optimization_progress(runtime, best_obj, best_bound, gap, prefix=""):
    """Print optimization progress (for use in callbacks)."""
    gap_str = f"{gap:.2%}" if gap is not None else "N/A"
    best_str = f"{best_obj:,.1f}" if best_obj is not None and best_obj < 1e10 else "N/A"
    bound_str = f"{best_bound:,.1f}" if best_bound is not None and best_bound > -1e10 else "N/A"

    print(f"\r  {prefix}T={runtime:.0f}s | Best: {best_str} | Bound: {bound_str} | Gap: {gap_str}   ", end="", flush=True)


def print_second_stage_result(cluster_id, status, cost, gap, runtime, is_global=False):
    """Print second-stage solve result for a cluster or global."""
    label = "Global" if is_global else f"Cluster {cluster_id}"
    status_str = format_status(status)

    if cost is not None:
        print(f"  {label}: {status_str} | Cost: {format_cost(cost)} | Gap: {format_gap(gap)} | Time: {format_duration(runtime)}")
    else:
        print(f"  {label}: {color(status_str, Colors.RED)} | Time: {format_duration(runtime)}")


# === Summary Printing ===

def print_summary_table(headers, rows, title=None):
    """Print a formatted summary table."""
    if title:
        print_subheader(title)

    # Calculate column widths
    col_widths = [len(h) for h in headers]
    for row in rows:
        for i, cell in enumerate(row):
            col_widths[i] = max(col_widths[i], len(str(cell)))

    # Print header
    header_line = " | ".join(f"{h:<{col_widths[i]}}" for i, h in enumerate(headers))
    print(f"  {header_line}")
    print(f"  {'-' * len(header_line)}")

    # Print rows
    for row in rows:
        row_line = " | ".join(f"{str(cell):<{col_widths[i]}}" for i, cell in enumerate(row))
        print(f"  {row_line}")


def print_final_summary(total_time, n_scenarios, feasible_costs, worst_scenario=None):
    """Print final pipeline summary."""
    print_header("FINAL SUMMARY")
    print(f"  Total runtime: {format_duration(total_time)}")
    print(f"  Scenarios evaluated: {n_scenarios}")

    if feasible_costs:
        avg_cost = sum(feasible_costs.values()) / len(feasible_costs)
        min_cost = min(feasible_costs.values())
        max_cost = max(feasible_costs.values())
        print(f"  Feasible scenarios: {len(feasible_costs)}/{n_scenarios}")
        print(f"  Cost range: {format_cost(min_cost)} - {format_cost(max_cost)}")
        print(f"  Average cost: {format_cost(avg_cost)}")
        if worst_scenario is not None:
            print(f"  Worst scenario: {worst_scenario} ({format_cost(feasible_costs[worst_scenario])})")
    else:
        print(color("  No feasible solutions found!", Colors.RED))


def print_output_paths(paths_dict):
    """Print output file paths."""
    print_subheader("Output Files")
    for name, path in paths_dict.items():
        print(f"  {name}: {path}")
