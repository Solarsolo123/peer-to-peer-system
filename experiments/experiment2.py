import sys
import os
import random
import matplotlib.pyplot as plt
#random.seed(26)

# 确保可以 import src 内的代码
sys.path.append(os.path.join(os.path.dirname(__file__), "..", "src"))

from core.network import Network


def build_network(num_nodes: int, m_bits: int = 20, num_keys: int = 300) -> Network:
    """
    构建一个带有 num_nodes 个节点和 num_keys 个键的 Chord 网络。
    """
    node_labels = [f"{i}" for i in range(num_nodes)]
    key_labels = [f"{i}" for i in range(num_keys)]

    net = Network(m_bits)
    net.init_from_labels(node_labels, key_labels)
    return net


def crash_some_nodes(net: Network, crash_ratio: float) -> int:
    """
    在当前网络中随机 crash 一部分节点。

    返回实际 crash 的节点数量。
    """
    all_labels = list(net.node_label_to_id.keys())
    num_nodes = len(all_labels)
    num_to_crash = max(1, int(num_nodes * crash_ratio))

    crash_candidates = random.sample(all_labels, num_to_crash)

    for label in crash_candidates:
        net.crash_processor(label)

    return num_to_crash


def collect_live_keys(net: Network, all_key_labels, max_samples: int):
    """
    从所有 key 中挑选“理想负责节点仍然 alive”的 key，用于实验。
    避免那些本来就落在 crashed 节点上的 key，因为这些 key 在真实系统中也无法访问。
    """
    live_keys = []

    for key in all_key_labels:
        key_str = str(key)
        key_id = net._hash_to_id(key_str)
        ideal_id = net._find_responsible_node_id(key_id)
        ideal_proc = net.processors[ideal_id]
        if ideal_proc.alive:
            live_keys.append(key)

    if not live_keys:
        return []

    if len(live_keys) <= max_samples:
        return live_keys
    return random.sample(live_keys, max_samples)


def run_experiment():
    m_bits = 20
    sizes = [16, 32, 64, 128, 256, 512, 1024]
    num_keys = 500
    crash_ratio = 0.05
    probes_per_size = 30  # 每个网络规模下测试多少个 key

    lines = []
    lines.append("=== Experiment 2: Crash Recovery and Lookup Stability ===")
    lines.append(
        f"(crash_ratio = {crash_ratio}, probes_per_size = {probes_per_size})"
    )
    lines.append("")

    header = (
        "Size | Crashed | Alive | Consistent% | Avg retries | "
        "Max retries | P(retries>1)"
    )
    lines.append(header)
    lines.append("-" * len(header))

    size_list = []
    consistent_list = []
    avg_retries_list = []
    prob_gt1_list = []

    for n in sizes:
        # 1. 构建网络
        net = build_network(num_nodes=n, m_bits=m_bits, num_keys=num_keys)

        # 2. 随机 crash 一部分节点
        crashed = crash_some_nodes(net, crash_ratio=crash_ratio)
        alive_ids = [nid for nid, p in net.processors.items() if p.alive]
        alive = len(alive_ids)

        # 3. 挑选理想负责节点仍然 alive 的 key
        all_key_labels = [f"{i}" for i in range(num_keys)]
        sample_keys = collect_live_keys(
            net, all_key_labels=all_key_labels, max_samples=probes_per_size
        )
        if not sample_keys:
            lines.append(
                f"{n:4d} | {crashed:7d} | {alive:5d} | "
                f"{0:10.1f} | {0:11.2f} | {0:11d} | {0:11.1f}"
            )
            continue

        # 4. 对这些 key 执行查找，并统计 retries 和一致性
        consistent_count = 0
        retries_list = []
        retries_gt1 = 0

        # 起始节点 label（如果这个节点 crash 了，route_find_key_from 会自动换一个 alive 的）
        start_label = "0"

        for key in sample_keys:
            result = net.route_find_key_from(start_label, key)
            if result["consistent"]:
                consistent_count += 1
            r = result["retries"]
            retries_list.append(r)
            if r > 1:
                retries_gt1 += 1

        total = len(sample_keys)
        consistent_ratio = 100.0 * consistent_count / total
        avg_retries = sum(retries_list) / total
        max_retries = max(retries_list)
        prob_retries_gt1 = 100.0 * retries_gt1 / total

        lines.append(
            f"{n:4d} | {crashed:7d} | {alive:5d} | "
            f"{consistent_ratio:10.1f} | {avg_retries:11.2f} | "
            f"{max_retries:11d} | {prob_retries_gt1:11.1f}"
        )
        size_list.append(n)
        consistent_list.append(consistent_ratio)
        avg_retries_list.append(avg_retries)
        prob_gt1_list.append(prob_retries_gt1)

    print("\n".join(lines))
    # 图 1：平均 retries 随网络规模变化（x 轴 log scale）
    plt.figure()
    plt.plot(size_list, avg_retries_list, marker="o")
    plt.xscale("log", base=2)
    plt.xlabel("Network size (number of nodes)")
    plt.ylabel("Average retries")
    plt.title("Experiment 2: Average retries vs network size")
    plt.grid(True, which="both", linestyle="--", linewidth=0.5)
    plt.tight_layout()
    plt.savefig("exp2_avg_retries.png")

    # 图 2：一致性比例随网络规模变化
    plt.figure()
    plt.plot(size_list, consistent_list, marker="o")
    plt.xscale("log", base=2)
    plt.xlabel("Network size (number of nodes)")
    plt.ylabel("Consistent lookups (%)")
    plt.title("Experiment 2: Consistent lookups vs network size")
    plt.grid(True, which="both", linestyle="--", linewidth=0.5)
    plt.tight_layout()
    plt.savefig("exp2_consistent_ratio.png")

    # 图 3：需要多次 retries 的比例
    plt.figure()
    plt.plot(size_list, prob_gt1_list, marker="o")
    plt.xscale("log", base=2)
    plt.xlabel("Network size (number of nodes)")
    plt.ylabel("P(retries > 1) (%)")
    plt.title("Experiment 2: Probability of retries > 1")
    plt.grid(True, which="both", linestyle="--", linewidth=0.5)
    plt.tight_layout()
    plt.savefig("exp2_prob_retries_gt1.png")



if __name__ == "__main__":
    run_experiment()
