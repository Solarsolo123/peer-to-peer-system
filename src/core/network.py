from __future__ import annotations
import hashlib, random
from typing import Any, Dict, List, Optional, Set
from src.core.processor import Processor


class Network:
    """
    Global view of the P2P / Chord-style ring.

    Responsibilities:
      - hold all processors (internal Chord IDs)
      - maintain mapping between external labels and internal IDs
      - build the ring (successor / predecessor)
      - assign keys to processors based on Chord rule
      - later: handle find/add/end/crash operations
    """

    def __init__(self, m_bits: int) -> None:
        self.m_bits = m_bits
        self.max_id = 2 ** m_bits

        # internal_id -> Processor
        self.processors: Dict[int, Processor] = {}

        # cached sorted list of internal node IDs
        self._sorted_node_ids: List[int] = []

        # mappings for nodes
        # external label is stored as string for consistency
        self.node_label_to_id: Dict[str, int] = {}
        self.node_id_to_label: Dict[int, str] = {}

        # mappings for keys
        self.key_label_to_id: Dict[str, int] = {}
        # one key_id may correspond to multiple external labels
        self.key_id_to_labels: Dict[int, Set[str]] = {}

    # ------------------------------------------------------------------
    # public initialization API
    # ------------------------------------------------------------------

    def init_from_labels(
        self, node_labels: List[Any], key_labels: List[Any]
    ) -> None:
        """
        Initialize the network from external labels.

        Args:
            node_labels: external identifiers for nodes (any type, will be str())
            key_labels: external identifiers for keys (any type, will be str())

        Steps:
          1) map node labels to internal IDs using SHA-1 and create processors
          2) build successor / predecessor ring on internal IDs
          3) map key labels to internal IDs and assign to responsible processors
        """
        if not node_labels:
            raise ValueError("Cannot initialize network with empty node label list")

        # 1) create processors from external node labels
        self._create_processors_from_labels(node_labels)

        # 2) build ring based on internal node IDs
        self._build_ring()

        # 3) assign keys based on external key labels
        self._assign_keys_from_labels(key_labels)

        # 4) build finger tables
        self.build_finger_tables()

    # ------------------------------------------------------------------
    # hashing and mapping
    # ------------------------------------------------------------------

    def _hash_to_id(self, label: str) -> int:
        """
        Map an external label to an internal Chord ID using SHA-1,
        then mod 2^m_bits.
        """
        h = hashlib.sha1(label.encode("utf-8")).hexdigest()
        h_int = int(h, 16)
        return h_int % self.max_id

    def _create_processors_from_labels(self, node_labels: List[Any]) -> None:
        """
        Create Processor objects from external node labels.

        External labels are converted to strings and hashed to internal IDs.
        If two different labels map to the same internal ID, this method raises
        a ValueError to avoid silent collisions.
        """
        self.processors.clear()
        self.node_label_to_id.clear()
        self.node_id_to_label.clear()

        # use set to remove duplicate labels
        seen_labels: Set[str] = set()
        for raw_label in node_labels:
            label = str(raw_label)
            print("label=",label)
            if label in seen_labels:
                continue
            seen_labels.add(label)

            internal_id = self._hash_to_id(label)
            print("internal_id=", internal_id)
            if internal_id in self.processors:
                existing_label = self.node_id_to_label[internal_id]
                raise ValueError(
                    f"Hash collision for nodes: '{existing_label}' and '{label}' "
                    f"both map to ID {internal_id}"
                )

            proc = Processor(label=label,node_id=internal_id)
            self.processors[internal_id] = proc
            self.node_label_to_id[label] = internal_id
            self.node_id_to_label[internal_id] = label

        if not self.processors:
            raise ValueError("No processors created from node labels")

        # cache sorted internal IDs
        self._sorted_node_ids = sorted(self.processors.keys())

    def _assign_keys_from_labels(self, key_labels: List[Any]) -> None:
        """
        Map external key labels to internal key IDs and assign them to processors.

        External labels are converted to strings, hashed to internal IDs,
        and then assigned using the Chord rule:
          responsible node is the first node with id >= key_id,
          or the smallest node id if none.
        """
        self.key_label_to_id.clear()
        self.key_id_to_labels.clear()

        if not self._sorted_node_ids:
            return

        for raw_label in key_labels:
            label = str(raw_label)
            key_id = self._hash_to_id(label)

            self.key_label_to_id[label] = key_id
            if key_id not in self.key_id_to_labels:
                self.key_id_to_labels[key_id] = set()
            self.key_id_to_labels[key_id].add(label)

            node_id = self._find_responsible_node_id(key_id)
            proc = self.processors[node_id]
            proc.add_key(label)

    def print_state_external(self) -> None:
        """
        Print network state using external labels for nodes and keys.
        """
        for internal_id in self.get_sorted_node_ids():
            proc = self.get_processor(internal_id)
            if proc is None:
                continue

            node_label = proc.label
            external_keys = proc.keys

            print(f"Node {node_label} (internal_id={internal_id})")
            print(f"status: {proc.alive}")
            print(f"  successor: {self.node_id_to_label.get(proc.successor_id, proc.successor_id)}")
            print(f"  predecessor: {self.node_id_to_label.get(proc.predecessor_id, proc.predecessor_id)}")
            print(f"  keys (external): {external_keys}")
            print(f"  fingertable: {proc.finger_table}")
            print()


    def add_processor(self, processor_label: Any):
        if len(self.processors) == self.max_id:
            return {
                "success": False,
                "error": "Creation failed. The number of processors is maximum. "
                         "No more processor can be added.",
                "moved_keys": [],
            }
        label = str(processor_label)
        print("label=", label)
        internal_id = self._hash_to_id(label)
        print("internal_id=", internal_id)
        if internal_id in self.processors:
            existing_label = self.node_id_to_label[internal_id]
            raise ValueError(
                f"Hash collision for nodes: '{existing_label}' and '{label}' "
                f"both map to ID {internal_id}"
            )

        proc = Processor(label=label, node_id=internal_id)
        self.processors[internal_id] = proc
        self.node_label_to_id[label] = internal_id
        self.node_id_to_label[internal_id] = label
        self._sorted_node_ids = sorted(self.processors.keys())

        idx = self._sorted_node_ids.index(internal_id)
        pre = self._sorted_node_ids[idx - 1]
        succ = self._sorted_node_ids[(idx + 1) % len(self._sorted_node_ids)]
        moved_keys = []

        for key in list(self.processors[succ].keys):
            key_id = self.key_label_to_id[key]
            if self._in_range(key_id, pre, internal_id):
                self.processors[succ].keys.remove(key)
                self.processors[internal_id].keys.add(key)
                moved_keys.append(key)

        self._build_ring()
        # fingertable的形式可以改一下
        self.build_finger_tables()

        if moved_keys:
            return {
                "success": True,
                "error": None,
                "old_processor": self.processors[succ].label,
                "old_processorid": self.processors[succ].node_id,
                "new_processor": label,
                "moved_keys": sorted(k for k in moved_keys),
            }
        else:
            print("No keys moved.")
            return {
                "success": True,
                "error": None,
                "new_processor": label,
                "moved_keys": None,
            }

    def end_processor(self, processor_label: Any):
        if len(self.processors) == 0:
            return {
                "success": False,
                "error": "End processor failed. The number of processors is 0. "
                         "No more processor can be ended.",
                "moved_keys": [],
            }
        label = str(processor_label)
        print("label=", label)
        internal_id = self._hash_to_id(label)
        print("internal_id=", internal_id)
        if internal_id not in self.processors:
            raise ValueError(
                f"No processor founded: '{label}' "
            )

        proc = self.processors[internal_id]

        idx = self._sorted_node_ids.index(internal_id)
        pre = self._sorted_node_ids[idx - 1]
        succ_id = self._sorted_node_ids[(idx + 1) % len(self._sorted_node_ids)]
        moved_labels = sorted(proc.keys)

        succ = self.processors[succ_id]
        for l in moved_labels:
            succ.add_key(l)
        proc.keys.clear()

        # delete this processor, rebuild ring / finger tables
        del self.processors[internal_id]
        del self.node_label_to_id[label]
        del self.node_id_to_label[internal_id]
        self._sorted_node_ids = sorted(self.processors.keys())
        self._build_ring()
        self.build_finger_tables()


        if moved_labels:
            # print(f"Keys moved to new processor {label}:")
            # print(f"  {sorted(k for k in moved_labels)}")
            return {
                "success": True,
                "error": None,
                "new_processor": succ.label,
                "new_processorid": succ.node_id,
                "old_processorid": internal_id,
                "moved_keys": sorted(k for k in moved_labels),
            }
        else:
            print("No keys moved.")
            return {
                "success": True,
                "error": None,
                "new_processor": succ.label,
                "new_processorid": succ.node_id,
                "old_processorid": internal_id,
                "moved_keys": None,
            }

    def crash_processor(self, processor_label: Any):
        """
        Mark a processor as crashed (alive=False).
        The crashed node remains in processors[], but is ignored by stabilize().

        Crashed ≠ End. Crash does NOT move keys.
        Keys remain inaccessible until stabilize() repairs the ring.
        """
        label = str(processor_label)
        if label not in self.node_label_to_id:
            return {
                "success": False,
                "error": f"Cannot crash: processor '{label}' not found.",
            }

        internal_id = self.node_label_to_id[label]
        proc = self.processors[internal_id]
        proc.alive = False

        return {
            "success": True,
            "error": None,
            "crashed": label,
        }

    # ------------------------------------------------------------------
    # ring construction and key placement
    # ------------------------------------------------------------------

    def _build_ring(self) -> None:
        """
        Set successor and predecessor pointers for all processors
        based on the sorted internal node IDs.
        """
        ids = sorted(self.processors.keys())
        self._sorted_node_ids = ids

        n = len(ids)
        if n == 0:
            return

        for i, node_id in enumerate(ids):
            succ_id = ids[(i + 1) % n]
            pred_id = ids[(i - 1) % n]
            proc = self.processors[node_id]
            proc.set_successor(succ_id)
            proc.set_predecessor(pred_id)

    def _find_responsible_node_id(self, key_id: int) -> int:
        """
        Given an internal key ID, find the internal node ID responsible for it.

        Chord rule:
          responsible node is the first node_id >= key_id,
          if none, wrap around to the smallest node_id.
        """
        ids = self._sorted_node_ids
        # linear search is enough for now; can be optimized to binary search later
        for node_id in ids:
            if node_id >= key_id:
                return node_id
        return ids[0]

    def _in_range(self, key, pre, new):
        if pre < new:
            return pre < key <= new
        else:
            # wrap around case
            return key > pre or key <= new

    # ------------------------------------------------------------------
    # finger tables and Chord-style routing
    # ------------------------------------------------------------------

    def build_finger_tables(self) -> None:
        """
        Build finger tables for all processors using the current ring.

        For each node n and each k in [0, m_bits),
        finger[k] = successor of (n + 2^k) mod 2^m.
        """
        # 只用 alive 的节点构造 finger table
        alive_ids = [nid for nid, p in self.processors.items() if p.alive]
        alive_ids.sort()

        if not alive_ids:
            return

        def find_alive_successor(start: int) -> int:
            # 在 alive_ids 里找到第一个 >= start 的节点
            for nid in alive_ids:
                if nid >= start:
                    return nid
            # 如果没有，则 wrap 到最小的
            return alive_ids[0]

        for node_id in alive_ids:
            proc = self.processors[node_id]
            proc.finger_table = []
            for k in range(self.m_bits):
                start = (node_id + (1 << k)) % self.max_id
                succ_id = find_alive_successor(start)
                proc.finger_table.append(succ_id)

        # # 对于 crashed 节点，可以选择清空 finger_table（避免 show 时误导）
        # for nid, p in self.processors.items():
        #     if not p.alive:
        #         p.finger_table = []

    def _closest_preceding_finger_alive(self, current_id: int, key_id: int) -> int:
        proc = self.processors[current_id]
        if not proc.finger_table:
            return current_id

        for finger_id in reversed(proc.finger_table):
            if finger_id is None:
                continue
            f = self.processors.get(finger_id)
            if f is None or not f.alive:
                continue
            if self._in_range(f.node_id, current_id, key_id):
                return f.node_id

        #return current_id
        return None

    def _find_successor_via_routing(self, start_id: int, key_id: int) -> Optional[int]:
        """
        Starting from start_id, route using Chord-style finger table
        to find the alive successor responsible for key_id.

        This roughly follows the same logic as route_find_key_from(),
        but only returns the final successor ID (or None on failure).
        """
        if not self.processors:
            return None

        # 如果起点不存在或已挂，选一个 alive 节点作为起点
        if start_id not in self.processors or not self.processors[start_id].alive:
            alive_ids = [nid for nid, p in self.processors.items() if p.alive]
            if not alive_ids:
                return None
            start_id = alive_ids[0]

        current_id = start_id
        visited: Set[int] = set()
        max_hops = len(self.processors) * 2 if self.processors else 0

        for _ in range(max_hops):
            if current_id in visited:
                # 回到了之前访问过的节点，说明拓扑有问题，停止
                break
            visited.add(current_id)

            cur_proc = self.processors[current_id]

            # 找到当前节点的第一个 alive successor（跳过 crashed 的）
            succ_id = cur_proc.successor_id
            hops = 0
            while succ_id is not None and succ_id in self.processors and not self.processors[succ_id].alive:
                succ_id = self.processors[succ_id].successor_id
                hops += 1
                if hops > len(self.processors):
                    succ_id = None
                    break

            if succ_id is None or succ_id not in self.processors:
                # successor 不可用，停止
                break

            # 如果 key_id 落在 (current, succ]，succ 就是负责节点
            if self._in_range(key_id, current_id, succ_id):
                return succ_id

            # 否则按 closest-preceding-finger 选择下一跳
            next_id = self._closest_preceding_finger_alive(current_id, key_id)

            # finger 帮不上忙，就至少往 successor 方向走一步
            if next_id is None or next_id == current_id:
                next_id = succ_id

            current_id = next_id

        return None

    def _notify(self, successor_id: int, potential_pred_id: int) -> None:
        """
        successor.notify(potential_predecessor)

        successor 会根据 Chord 论文规则更新自己的 predecessor：
          - 如果当前 predecessor 为空，或
          - potential_predecessor 更接近自己（在 (predecessor, self] 区间）
        """
        succ = self.processors.get(successor_id)
        if succ is None or not succ.alive:
            return

        n = self.processors.get(potential_pred_id)
        if n is None or not n.alive:
            return

        # 当前 predecessor
        cur_pred_id = succ.predecessor_id

        if cur_pred_id is None:
            succ.predecessor_id = potential_pred_id
            return

        # 如果 potential_pred 在 (cur_pred, succ]，则更新
        if self._in_range(potential_pred_id, cur_pred_id, succ.node_id):
            succ.predecessor_id = potential_pred_id

    def _stabilize_one_node(self, node_id: int) -> None:
        """
        对某一个 alive 节点执行一次 Chord stabilize 协议。

        伪代码：
          n = this node
          x = successor.predecessor
          if x in (n, successor): successor = x
          successor.notify(n)
        """
        n = self.processors.get(node_id)
        if n is None or not n.alive:
            return

        succ_id = n.successor_id
        if succ_id is None:
            return

        # 跳过 crash 的 successor（用 successor.successor 一直往前跳）
        # 注意最多跳 len(nodes) 次，防止死循环
        hops = 0
        while True:
            succ = self.processors.get(succ_id)
            if succ is None:
                return
            if succ.alive:
                break
            succ_id = succ.successor_id
            hops += 1
            if hops > len(self.processors):
                return

        n.successor_id = succ_id
        succ = self.processors[succ_id]

        # x = successor.predecessor
        x_id = succ.predecessor_id
        if x_id is not None:
            x = self.processors.get(x_id)
            if x is not None and x.alive:
                # 如果 x 在 (n, succ] 区间内，则更新 successor = x
                if self._in_range(x.node_id, n.node_id, succ.node_id):
                    n.successor_id = x.node_id
                    succ = x  # 新 successor

        # 通知 successor：我可能是你的 predecessor
        self._notify(succ.node_id, n.node_id)

    def tick_once(self, max_nodes: int = 0) -> None:
        """
        时间前进一格（逻辑时钟），这一格中随机选一些 alive 节点执行 stabilize。

        max_nodes = 0 表示“这一 tick 里所有 alive 节点都执行一次 stabilize”
        max_nodes > 0 表示“随机选最多 max_nodes 个节点执行”
        """
        alive_ids = [nid for nid, p in self.processors.items() if p.alive]
        if not alive_ids:
            return

        if max_nodes <= 0 or max_nodes >= len(alive_ids):
            chosen = alive_ids
        else:
            chosen = random.sample(alive_ids, max_nodes)

        for nid in chosen:
            self._stabilize_one_node(nid)

        # stabilize 之后，可以顺手重建 finger table，让路由更快收敛
        self.build_finger_tables()

    def route_find_key_from(self, start_label: Any, key_label: Any) -> Dict[str, Any]:
        """
        从 start_label 节点出发，模拟 Chord finger table 查找 key_label。
        内部会用 ideal结果检查是否一致，不一致时自动执行一次全网 stabilize 并重试一次。

        返回:
          {
            "path_external": [...],
            "responsible_node": Optional[str],
            "stored": bool,
            "consistent": bool,   # routing_end vs ideal 是否一致
            "retries": int        # 为了得到最终结果做了几次 routing
          }
        """
        key_str = str(key_label)
        key_id = self._hash_to_id(key_str)

        def _route_once() -> (List[int], Optional[int]):
            # 起点
            start_str = str(start_label)
            start_id = self.node_label_to_id.get(start_str)
            if start_id is None or not self.processors[start_id].alive:
                alive_ids = [nid for nid, p in self.processors.items() if p.alive]
                if not alive_ids:
                    return [], None
                start_id = alive_ids[0]

            path: List[int] = []
            current_id = start_id
            visited: Set[int] = set()
            max_hops = len(self.processors) * 2 if self.processors else 0

            for _ in range(max_hops):
                if current_id in visited:
                    # 已经绕回来了，说明拓扑有问题，停止
                    break
                visited.add(current_id)

                path.append(current_id)
                cur_proc = self.processors[current_id]

                # 找 successor（跳过 crashed 的）
                succ_id = cur_proc.successor_id
                hops = 0
                while succ_id is not None and not self.processors[succ_id].alive:
                    succ_id = self.processors[succ_id].successor_id
                    hops += 1
                    if hops > len(self.processors):
                        succ_id = None
                        break

                if succ_id is None:
                    # 没 successor，路由终止
                    break

                # ① key 在 (current, succ] 上 → succ 是负责节点，结束
                if self._in_range(key_id, current_id, succ_id):
                    path.append(succ_id)
                    current_id = succ_id
                    break

                # ② 否则找 “closest preceding finger”
                next_id = self._closest_preceding_finger_alive(current_id, key_id)

                # finger 帮不上忙 → 至少往 successor 方向走一步
                if next_id is None or next_id == current_id:
                    next_id = succ_id

                current_id = next_id

            return path, current_id

        # 第一次 routing
        path_internal, end_id = _route_once()
        retries = 1

        if end_id is None:
            return {
                "path_external": [],
                "responsible_node": None,
                "stored": False,
                "consistent": False,
                "retries": retries,
            }

        # ideal 负责节点（数学正确答案）
        ideal_id = self._find_responsible_node_id(key_id)
        ideal_proc = self.processors[ideal_id]
        ideal_alive = ideal_proc.alive

        consistent = (end_id == ideal_id and ideal_alive)

        # 如果不一致，尝试 stabilize + 再试一次
        if not consistent:
            # 全网 stabilize 一轮
            self.tick_once(max_nodes=0)
            # 再路由一次
            path_internal, end_id = _route_once()
            retries += 1
            # 再对比
            ideal_id = self._find_responsible_node_id(key_id)
            ideal_proc = self.processors[ideal_id]
            ideal_alive = ideal_proc.alive
            consistent = (end_id == ideal_id and ideal_alive)

        # 最终负责节点：我们选择 ideal 作为最终判定者
        if not ideal_alive:
            responsible_label = None
            stored = False
        else:
            responsible_label = self.node_id_to_label.get(ideal_id, str(ideal_id))
            # 注意：我们用 external key_str 检查是否真的存了这个 key
            stored = ideal_proc.has_key(key_str)

        path_external = [
            self.node_id_to_label.get(nid, str(nid)) for nid in path_internal
        ]

        return {
            "path_external": path_external,
            "responsible_node": responsible_label,
            "stored": stored,
            "consistent": consistent,
            "retries": retries,
        }

    # ------------------------------------------------------------------
    # helper accessors (useful for CLI and router)
    # ------------------------------------------------------------------

    def get_processor(self, internal_id: int) -> Optional[Processor]:
        return self.processors.get(internal_id)

    def get_sorted_node_ids(self) -> List[int]:
        return list(self._sorted_node_ids)

    def get_internal_node_id(self, external_label: Any) -> Optional[int]:
        """
        Look up the internal node ID from an external label.
        """
        return self.node_label_to_id.get(str(external_label))

    def get_internal_key_id(self, external_label: Any) -> Optional[int]:
        """
        Look up the internal key ID from an external key label.
        """
        return self.key_label_to_id.get(str(external_label))
