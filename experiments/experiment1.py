import sys
import os
import random
import matplotlib.pyplot as plt

# 确保可以 import src 内的代码
sys.path.append(os.path.join(os.path.dirname(__file__), "..", "src"))

from core.network import Network


# successor-only routing helper
def successor_only_lookup(net: Network, start_id: int, key_id: int):
    hops = 0
    current = start_id
    visited = set()



    while True:
        if current in visited:
            return None, hops  # topology broken or loop
        visited.add(current)

        proc = net.processors[current]
        succ = proc.successor_id
        if succ is None:
            return None, hops

        hops += 1

        # found responsible node
        if net._in_range(key_id, current, succ):
            return succ, hops

        current = succ


def run_experiment():
    m_bits = 20
    sizes = [8, 16, 32, 64]

    results = []  # collect all output lines here
    results.append("=== Experiment 1: Finger Routing vs Successor-Only ===")

    avg_finger_hops_per_size = []
    avg_succ_hops_per_size = []

    for n in sizes:
        # 1. Generate node labels
        node_labels = [f"{i}" for i in range(n)]
        key_labels = [f"{i}" for i in range(200)]

        net = Network(m_bits)
        net.init_from_labels(node_labels, key_labels)

        # pick one start node
        start_label = node_labels[0]
        start_id = net.get_internal_node_id(start_label)

        # randomly choose keys for the experiment
        sample_keys = random.sample(key_labels, 30)

        finger_hops = []
        succ_hops = []

        for key in sample_keys:
            key_id = net.get_internal_key_id(key)

            # finger-based lookup (use existing route_find_key_from)
            result = net.route_find_key_from(start_label, key)
            finger_hops.append(len(result["path_external"]))

            # successor-only lookup
            _, hops = successor_only_lookup(net, start_id, key_id)
            succ_hops.append(hops)

        avg_finger = sum(finger_hops) / len(finger_hops)
        avg_succ = sum(succ_hops) / len(succ_hops)

        results.append(f"Network Size = {n}")
        results.append(f"  Finger routing avg hops: {avg_finger:.2f}")
        results.append(f"  Successor-only avg hops: {avg_succ:.2f}")
        results.append("")

        avg_finger_hops_per_size.append(avg_finger)
        avg_succ_hops_per_size.append(avg_succ)


    print("\n".join(results))

    plot_results(
        sizes,
        avg_finger_hops_per_size,
        avg_succ_hops_per_size,
    )

def plot_results(sizes, finger_avg, succ_avg):
    plt.figure(figsize=(7, 4.5))

    plt.plot(sizes, finger_avg, marker="o", label="Finger routing")
    plt.plot(sizes, succ_avg, marker="s", label="Successor-only routing")

    plt.yscale("log")
    plt.xlabel("Network size (number of nodes)")
    plt.ylabel("Average lookup hops (log scale)")
    plt.title("Experiment 1: Finger vs Successor-only Routing")
    plt.grid(True, which="both", linestyle="--", alpha=0.4)
    plt.legend()


    plt.tight_layout()
    plt.savefig("experiment1_lookup_performance.png", dpi=300)
    plt.savefig("experiment1_lookup_performance.pdf")
    print("Saved figure to experiment1_lookup_performance.png / .pdf")


if __name__ == "__main__":
    run_experiment()
