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

        # 2) build ring based on initial data file
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

    def add_processor(self, processor_label: Any, start_label: Any = None):
        """
        Add a new processor to the ring in a more Chord-like way:

          1) compute the new node ID from its label;
          2) starting from a given node (or some alive node), route using the
             finger tables to find the successor responsible for this ID;
          3) insert the new node between the successor and its predecessor,
             and move the corresponding keys;
          4) let stabilization gradually repair the rest of the ring.

        Finger tables are not globally rebuilt here; they can be refreshed
        later by tick_once() or other maintenance routines.
        """
        # check masimum node number
        if len(self.processors) == self.max_id:
            return {
                "success": False,
                "error": (
                    "Creation failed. The number of processors is maximum. "
                    "No more processor can be added."
                ),
                "moved_keys": [],
            }

        label = str(processor_label)
        internal_id = self._hash_to_id(label)

        # hash collision detect
        if internal_id in self.processors:
            existing_label = self.node_id_to_label[internal_id]
            raise ValueError(
                f"Hash collision for nodes: '{existing_label}' and '{label}' "
                f"both map to ID {internal_id}"
            )

        # if this is the first node
        if not self.processors:
            proc = Processor(label=label, node_id=internal_id)
            self.processors[internal_id] = proc
            self.node_label_to_id[label] = internal_id
            self.node_id_to_label[internal_id] = label
            self._sorted_node_ids = [internal_id]

            proc.set_successor(internal_id)
            proc.set_predecessor(internal_id)

            # initial fingertable
            self.build_finger_tables()

            return {
                "success": True,
                "error": None,
                "new_processor": label,
                "moved_keys": None,
            }

        # 2) Choose routing start: prefer start_label (current attached node)
        start_id: Optional[int] = None
        if start_label is not None:
            start_str = str(start_label)
            start_id = self.node_label_to_id.get(start_str)

        # If start_label is missing or invalid, pick any alive node
        if start_id is None:
            alive_ids = [nid for nid, p in self.processors.items() if p.alive]
            if alive_ids:
                start_id = alive_ids[0]

        succ_id: Optional[int] = None

        if start_id is not None:
            # 2.1 First try finger-table based routing (Chord-style)
            succ_id = self._find_successor_via_routing(start_id, internal_id)

            # 2.2 If routing fails (e.g., fingers are stale), fall back to
            #     a pure-successor-based search
            if succ_id is None:
                succ_id = self._find_successor_via_successor(start_id, internal_id)

        # 2.3 Final fallback: use the ideal responsible node based on the
        #     sorted ID list (centralized view), mostly for robustness
        if succ_id is None:
            succ_id = self._find_responsible_node_id(internal_id)

        succ = self.processors[succ_id]

        # 3) 确定 predecessor：
        #    优先使用 succ.predecessor，如果不可用则在排序列表中找一个前驱
        pred_id = succ.predecessor_id
        if pred_id is None or pred_id not in self.processors or not self.processors[pred_id].alive:
            ids = sorted(self.processors.keys())
            if succ_id in ids:
                idx = ids.index(succ_id)
                pred_id = ids[idx - 1]
            else:
                # 极端情况下直接让它自己成为前驱
                pred_id = succ_id

        pred = self.processors[pred_id]

        # 4) 创建新节点并插入全局结构
        proc = Processor(label=label, node_id=internal_id)
        self.processors[internal_id] = proc
        self.node_label_to_id[label] = internal_id
        self.node_id_to_label[internal_id] = label

        # 更新全局排序列表（用于 debug / ideal 计算）
        self._sorted_node_ids = sorted(self.processors.keys())

        # 5) 从 successor 上迁移属于 (pred, new] 范围的 keys
        moved_keys: List[str] = []
        for key in list(succ.keys):
            key_id = self.key_label_to_id[key]
            if self._in_range(key_id, pred_id, internal_id):
                succ.keys.remove(key)
                proc.keys.add(key)
                moved_keys.append(key)

        # 6) 局部更新 successor / predecessor 指针
        proc.set_successor(succ_id)
        proc.set_predecessor(pred_id)

        pred.set_successor(internal_id)
        succ.set_predecessor(internal_id)

        # 7) finger table 先不全局重建，后续 tick_once 会处理。
        #    如果希望新节点一开始有个“还行”的 finger table，可以复制 succ 的：
        proc.finger_table = list(succ.finger_table)
        self.update_fingers_for_new_node(internal_id)

        if moved_keys:
            return {
                "success": True,
                "error": None,
                "old_processor": succ.label,
                "old_processorid": succ.node_id,
                "new_processor": label,
                "moved_keys": sorted(moved_keys),
            }
        else:
            return {
                "success": True,
                "error": None,
                "new_processor": label,
                "moved_keys": None,
            }


    def end_processor(self, processor_label: Any):
        """
        Gracefully remove a processor from the ring.

        Steps:
          1) locate the node by its label;
          2) move all its keys to its successor;
          3) locally repair predecessor / successor pointers;
          4) remove the node from global structures;
          5) incrementally update finger tables that pointed to this node.
        """
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
            raise ValueError(f"No processor founded: '{label}'")

        proc = self.processors[internal_id]

        # 特殊情况：这是环中唯一节点
        if len(self.processors) == 1:
            moved_labels = sorted(proc.keys)
            proc.keys.clear()

            # 从全局结构中删除
            del self.processors[internal_id]
            del self.node_label_to_id[label]
            del self.node_id_to_label[internal_id]
            self._sorted_node_ids = []

            return {
                "success": True,
                "error": None,
                "new_processor": None,
                "new_processorid": None,
                "old_processorid": internal_id,
                "moved_keys": moved_labels or None,
            }

        # 一般情况：至少有两个节点
        pred_id = proc.predecessor_id
        succ_id = proc.successor_id

        # 如果 predecessor 丢失，用 sorted 列表兜底
        if pred_id is None or pred_id not in self.processors:
            ids = sorted(self.processors.keys())
            idx = ids.index(internal_id)
            pred_id = ids[idx - 1]

        # 如果 successor 丢失，也用 sorted 列表兜底
        if succ_id is None or succ_id not in self.processors:
            ids = sorted(self.processors.keys())
            idx = ids.index(internal_id)
            succ_id = ids[(idx + 1) % len(ids)]

        pred = self.processors[pred_id]
        succ = self.processors[succ_id]

        # 1) 把所有 key 迁移给 successor
        moved_labels = sorted(proc.keys)
        for l in moved_labels:
            succ.add_key(l)
        proc.keys.clear()

        # 2) 局部修复 predecessor / successor 指针，让 pred 和 succ 绕过这个节点
        pred.set_successor(succ_id)
        succ.set_predecessor(pred_id)

        # 3) 从全局结构删除该节点
        del self.processors[internal_id]
        del self.node_label_to_id[label]
        del self.node_id_to_label[internal_id]

        # 4) 只用于 ideal successor / debug 的排序列表
        self._sorted_node_ids = sorted(self.processors.keys())

        # 5) 增量修复 finger tables 中指向该节点的条目
        self.update_fingers_for_removed_node(internal_id)

        if moved_labels:
            return {
                "success": True,
                "error": None,
                "new_processor": succ.label,
                "new_processorid": succ.node_id,
                "old_processorid": internal_id,
                "moved_keys": moved_labels,
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
        based on the initial data file.
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

    def update_fingers_for_new_node(self, new_id: int) -> None:
        if new_id not in self.processors:
            return

        new_proc = self.processors[new_id]
        if not new_proc.alive:
            return

        for p_id, p in self.processors.items():
            if not p.alive:
                continue

            # Ensure finger_table exists and has the correct size
            if len(p.finger_table) < self.m_bits:
                p.finger_table += [p.successor_id] * (self.m_bits - len(p.finger_table))

            for i in range(self.m_bits):
                start = (p.node_id + (1 << i)) % self.max_id

                # ideal (mathematically correct) successor for this finger start
                ideal = self._find_responsible_node_id(start)

                if ideal == new_id:
                    p.finger_table[i] = new_id

    def update_fingers_for_removed_node(self, removed_id: int) -> None:
        """
        Incrementally update finger tables of existing nodes after a node is
        gracefully removed. Any finger entry that used to point to removed_id
        will be updated to its new mathematically correct successor.

        This avoids a full global rebuild of all finger tables.
        """
        if not self.processors:
            return

        # 遍历所有 still-alive 节点
        for p_id, p in self.processors.items():
            if not p.alive:
                continue

            # 确保 finger_table 长度正确
            if len(p.finger_table) < self.m_bits:
                p.finger_table += [p.successor_id] * (self.m_bits - len(p.finger_table))

            for i in range(self.m_bits):
                if p.finger_table[i] != removed_id:
                    continue

                # 对这一条 finger 重新计算理想 successor
                start = (p.node_id + (1 << i)) % self.max_id
                ideal = self._find_responsible_node_id(start)
                p.finger_table[i] = ideal

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

    def _find_successor_via_successor(self, start_id: int, key_id: int) -> Optional[int]:
        """
        Fallback successor search that only walks along successor pointers.
        As long as the successor chain is not completely broken, this will
        eventually find the alive successor responsible for key_id.

        Returns:
            internal node ID of the successor, or None if not found.
        """
        if not self.processors:
            return None

        # If the start node is invalid or crashed, pick any alive node
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
                # we looped back, topology is inconsistent
                break
            visited.add(current_id)

            cur = self.processors[current_id]

            # follow successor, skipping crashed nodes
            succ_id = cur.successor_id
            hops = 0
            while succ_id is not None and succ_id in self.processors and not self.processors[succ_id].alive:
                succ_id = self.processors[succ_id].successor_id
                hops += 1
                if hops > len(self.processors):
                    succ_id = None
                    break

            if succ_id is None or succ_id not in self.processors:
                break

            # if key_id is in (current, succ], then succ is the responsible node
            if self._in_range(key_id, current_id, succ_id):
                return succ_id

            current_id = succ_id

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
        max_nodes < 0 表示不限制
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

        # 不一致时会多轮触发部分节点的 stabilize，并在每轮之后重试路由，直到正确或达到最大尝试次数
        max_retries = 10
        while not consistent and retries < max_retries:
        #if not consistent:
            # 全网 stabilize 一轮
            #self.tick_once(max_nodes=0)
            alive_ids = [nid for nid, p in self.processors.items() if p.alive]
            k = max(1, len(alive_ids) // 2)
            print("k is:",k)
            self.tick_once(max_nodes=k)

            # 再路由一次
            path_internal, end_id = _route_once()
            retries += 1

            if end_id is None:
                # 拓扑太乱了，提前跳出；consistent 维持 False
                break
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
