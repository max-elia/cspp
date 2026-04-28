from typing import Dict, Iterable, List, Set, Tuple


def build_cluster_arc_set(
    customer_ids: Iterable[int],
    final_clusters: Dict[int, int],
    depot: int = 0,
):
    """
    Build the clustering-stage arc set with:
    - depot <-> every customer
    - complete directed arcs inside each cluster
    """
    customer_list = [int(c) for c in customer_ids]

    cluster_members: Dict[int, List[int]] = {}
    for cid in customer_list:
        cluster_members.setdefault(final_clusters[cid], []).append(cid)

    arcs: Set[Tuple[int, int]] = set()

    # Depot links
    for cid in customer_list:
        arcs.add((depot, cid))
        arcs.add((cid, depot))

    # Intra-cluster complete directed arcs
    for members in cluster_members.values():
        for i in members:
            for j in members:
                if i != j:
                    arcs.add((i, j))

    # Safety: no self-loops
    arcs = {(u, v) for (u, v) in arcs if u != v}

    depot_arcs = sum(1 for (u, v) in arcs if u == depot or v == depot)
    customer_arcs = len(arcs) - depot_arcs
    intra_arcs = 0
    for (u, v) in arcs:
        if u == depot or v == depot:
            continue
        if final_clusters.get(u) == final_clusters.get(v):
            intra_arcs += 1

    n_customers = len(customer_list)
    full_arcs = n_customers * (n_customers - 1) + 2 * n_customers
    reduction_pct = (1 - (len(arcs) / full_arcs)) * 100 if full_arcs > 0 else 0.0

    stats = {
        "depot_arcs": depot_arcs,
        "customer_customer_arcs": customer_arcs,
        "intra_cluster_arcs": intra_arcs,
        "total_arcs": len(arcs),
        "full_arcs": full_arcs,
        "reduction_pct": reduction_pct,
        "n_customers": n_customers,
    }

    return arcs, cluster_members, stats


def build_global_arc_set(customer_ids: Iterable[int], depot: int = 0) -> Set[Tuple[int, int]]:
    customer_list = [int(c) for c in customer_ids]
    arcs: Set[Tuple[int, int]] = set()

    for cid in customer_list:
        arcs.add((depot, cid))
        arcs.add((cid, depot))

    for i in customer_list:
        for j in customer_list:
            if i != j:
                arcs.add((i, j))

    return arcs
