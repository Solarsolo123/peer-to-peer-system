from __future__ import annotations
import hashlib, random
from typing import Any, Dict, List, Optional, Set, Tuple
from src.core.processor import Processor


class Network:
    """
    Global view of the Chord ring.

    Responsibilities:
      - hold all processors (internal Chord IDs)
      - maintain mapping between external labels and internal IDs
      - build the ring
      - assign keys to processors based on Chord rule
      - find/add/end/crash operations
    """

    def __init__(self, m_bits: int) -> None:
        self.m_bits = m_bits
        self.max_id = 2 ** m_bits

        # internal_id -> Processor
        self.processors: Dict[int, Processor] = {}

        # cached sorted list of internal node IDs
        self._sorted_node_ids: List[int] = []

        # mappings for nodes
        self.node_label_to_id: Dict[str, int] = {}
        self.node_id_to_label: Dict[int, str] = {}

        # mappings for keys
        self.key_label_to_id: Dict[str, int] = {}
        # one key_id may correspond to multiple external labels
        self.key_id_to_labels: Dict[int, Set[str]] = {}

        # size of successor list each node maintains
        self.successor_list_size: int = 3
        # total number of replicas per key (primary + secondaries)
        self.replication_factor: int = 3


    # public initialization
    def init_from_labels(self, node_labels: List[Any], key_labels: List[Any]) -> None:
        """
        Initialize the network from .json.

        Args:
            node_labels: external identifiers for nodes
            key_labels: external identifiers for keys

        Steps:
          1. map node labels to internal IDs using SHA-1 and create processors
          2. build successor / predecessor ring on internal IDs
          3. assign keys (with replication) to processors
          4. build finger tables (in initial phase, it is centralized)
        """
        if not node_labels:
            raise ValueError("Cannot initialize network with empty node label list")

        # create processors from external node labels
        self._create_processors_from_labels(node_labels)

        # build ring based on initial data file
        self._build_ring()

        # assign keys based on external key labels
        self._assign_keys_from_labels(key_labels)

        # build finger tables
        self.build_finger_tables()


    # hashing and mapping

    def _hash_to_id(self, label: str) -> int:
        """
        Map an external label to an internal Chord ID using SHA-1, then mod 2^m_bits.
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
            #print("label=",label)
            if label in seen_labels:
                continue
            seen_labels.add(label)

            internal_id = self._hash_to_id(label)
            #print("internal_id=", internal_id)
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

    def _replicate_key_to_successors(self, primary_id: int, key_label: str) -> None:
        """
        Replicate a key from its primary node to its next successors.
        Assumes primary already stored the key locally.
        """
        if primary_id not in self.processors:
            return
        primary = self.processors[primary_id]
        if not primary.alive:
            return

        replicas = 1  # primary already holds one copy
        # use successor_list as main replication target list
        for succ_id in primary.successor_list:
            if succ_id is None or succ_id not in self.processors:
                continue
            succ = self.processors[succ_id]
            if not succ.alive:
                continue
            succ.add_key(key_label)
            replicas += 1
            if replicas >= self.replication_factor:
                break

    def _assign_keys_from_labels(self, key_labels: List[Any]) -> None:
        """
        Map external key labels to internal key IDs and assign them to processors.
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
            self._replicate_key_to_successors(node_id, label)

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
            print(f"  successor_list: {[self.node_id_to_label.get(s, s) for s in proc.successor_list]}")
            print()


    # count alive nodes
    def _alive_count(self) -> int:
        return sum(1 for p in self.processors.values() if p.alive)

    def _rebuild_successor_list_for(self, node_id: int) -> None:
        """
        Recompute successor_list for one node by walking successor pointers.
        Used after stabilization or membership changes.
        """
        n = self.processors.get(node_id)
        if n is None or not n.alive:
            return

        successors: List[Optional[int]] = []
        current_id = n.successor_id
        hops = 0
        max_hops = len(self.processors)

        while current_id is not None and hops < max_hops and len(successors) < self.successor_list_size:
            if current_id not in self.processors:
                break
            p = self.processors[current_id]
            if p.alive and current_id not in successors and current_id != node_id:
                successors.append(current_id)
            current_id = p.successor_id
            hops += 1

        n.set_successor_list(successors)

    # choose the first alive successor using successor_list
    def _first_alive_successor(self, node_id: int) -> Optional[int]:
        """
        Try direct successor, then successor_list, then fall back to successor chain.
        """
        n = self.processors.get(node_id)
        if n is None:
            return None

        # direct successor
        sid = n.successor_id
        if sid is not None and sid in self.processors and self.processors[sid].alive:
            return sid

        # successor_list
        for cand in n.successor_list:
            if cand is not None and cand in self.processors and self.processors[cand].alive:
                return cand

        # follow successor chain
        succ_id = n.successor_id
        hops = 0
        max_hops = len(self.processors)
        while succ_id is not None and hops < max_hops:
            s = self.processors.get(succ_id)
            if s is None:
                break
            if s.alive:
                return succ_id
            succ_id = s.successor_id
            hops += 1

        return None

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
        # check maximum node number
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
            proc.set_successor_list([internal_id] * self.successor_list_size)

            # initial fingertable
            self.build_finger_tables()

            return {
                "success": True,
                "error": None,
                "new_processor": label,
                "moved_keys": None,
            }

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
            # First try finger-table based routing (Chord-style)
            succ_id = self._find_successor_via_routing(start_id, internal_id)

            # If routing fails (e.g., fingers are stale), fall back to a pure-successor-based search
            if succ_id is None:
                succ_id = self._find_successor_via_successor(start_id, internal_id)

        # Final fallback: use the ideal responsible node based on the sorted ID list (centralized view), mostly for robustness
        if succ_id is None:
            succ_id = self._find_responsible_node_id(internal_id)

        succ = self.processors[succ_id]

        # choose predecessor：
        pred_id = succ.predecessor_id
        # if pred_id isn't exist or predecessor is not alive,
        if pred_id is None or pred_id not in self.processors or not self.processors[pred_id].alive:
            ids = sorted(self.processors.keys())
            if succ_id in ids:
                idx = ids.index(succ_id)
                pred_id = ids[idx - 1]
            else:
                pred_id = succ_id

        pred = self.processors[pred_id]

        # create new node and insert into global structures
        proc = Processor(label=label, node_id=internal_id)
        self.processors[internal_id] = proc
        self.node_label_to_id[label] = internal_id
        self.node_id_to_label[internal_id] = label

        # update global sorted list (for debug)
        self._sorted_node_ids = sorted(self.processors.keys())

        # move keys in (pred, new] from successor to the new node
        moved_keys: List[str] = []
        for key in list(succ.keys):
            key_id = self.key_label_to_id[key]
            if self._in_range(key_id, pred_id, internal_id):
                succ.keys.remove(key)
                proc.keys.add(key)
                moved_keys.append(key)

        # update local successor / predecessor pointers
        proc.set_successor(succ_id)
        proc.set_predecessor(pred_id)

        pred.set_successor(internal_id)
        succ.set_predecessor(internal_id)

        # refresh successor lists locally
        self._rebuild_successor_list_for(pred_id)
        self._rebuild_successor_list_for(succ_id)
        self._rebuild_successor_list_for(internal_id)

        # finger table is not globally rebuilt; new node copies succ's table
        proc.finger_table = list(succ.finger_table)
        #self.update_fingers_for_new_node(internal_id)

        # re-establish replication for keys for which this new node becomes the primary responsible node
        for key in moved_keys:
            # key_id = self.key_label_to_id.get(key)
            # if key_id is None:
            #     continue
            # ideal_id = self._find_responsible_node_id(key_id)
            # if ideal_id == internal_id:
            self._replicate_key_to_successors(internal_id, key)

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
        #print("label=", label)
        internal_id = self._hash_to_id(label)
        #print("internal_id=", internal_id)

        if internal_id not in self.processors:
            raise ValueError(f"No processor founded: '{label}'")

        proc = self.processors[internal_id]


        # special case: this is the only node in the ring
        if len(self.processors) == 1:
            moved_labels = sorted(proc.keys)
            proc.keys.clear()

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

        pred_id = proc.predecessor_id
        succ_id = proc.successor_id

        # if predecessor unknown, fall back to sorted list
        if pred_id is None or pred_id not in self.processors:
            ids = sorted(self.processors.keys())
            idx = ids.index(internal_id)
            pred_id = ids[idx - 1]

        # if successor unknown, fall back to sorted list
        if succ_id is None or succ_id not in self.processors:
            ids = sorted(self.processors.keys())
            idx = ids.index(internal_id)
            succ_id = ids[(idx + 1) % len(ids)]

        pred = self.processors[pred_id]
        succ = self.processors[succ_id]

        # move all keys to successor
        moved_labels = sorted(proc.keys)
        for l in moved_labels:
            succ.add_key(l)
        proc.keys.clear()

        # repair predecessor / successor pointers to bypass this node
        pred.set_successor(succ_id)
        succ.set_predecessor(pred_id)

        # refresh successor lists for neighbors
        self._rebuild_successor_list_for(pred_id)
        self._rebuild_successor_list_for(succ_id)

        # re-replicate keys for which successor is now primary
        for l in moved_labels:
            # key_id = self.key_label_to_id.get(l)
            # if key_id is None:
            #     continue
            # ideal_id = self._find_responsible_node_id(key_id)
            # if ideal_id == succ_id:
            self._replicate_key_to_successors(succ_id, l)

        # remove node from global structures
        del self.processors[internal_id]
        del self.node_label_to_id[label]
        del self.node_id_to_label[internal_id]

        # update sorted list used for debug
        self._sorted_node_ids = sorted(self.processors.keys())

        # update finger tables entries pointing to this node
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

        Crash does NOT move keys.
        Keys remain accessible only via replicas on other nodes.
        """
        label = str(processor_label)
        if label not in self.node_label_to_id:
            return {
                "success": False,
                "error": f"Cannot crash: processor '{label}' not found.",
            }

        internal_id = self.node_label_to_id[label]
        proc = self.processors[internal_id]

        if not proc.alive:
            return {
                "success": False,
                "error": f"Processor '{label}' is already crashed.",
            }

        proc.alive = False

        return {
            "success": True,
            "error": None,
            "crashed": label,
        }

    # ring construction and key placement

    def _build_ring(self) -> None:
        """
        Set successor and predecessor pointers for all processors
        based on the initial data file.
        This method is mainly used during initialization, not for dynamic changes.
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
            succ_list: List[Optional[int]] = []
            for offset in range(1, 1 + self.successor_list_size):
                succ_list.append(ids[(i + offset) % n])
            proc.set_successor_list(succ_list)

    def _find_responsible_node_id(self, key_id: int) -> int:
        """
        Given an internal key ID, find the internal node ID responsible for it.
        """
        ids = self._sorted_node_ids
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


    # finger tables and routing

    def build_finger_tables(self) -> None:
        """
        Build finger tables for all processors using the current ring. Only used in initial phase
        """
        alive_ids = [nid for nid, p in self.processors.items() if p.alive]
        alive_ids.sort()

        if not alive_ids:
            return

        def find_alive_successor(start: int) -> int:
            for nid in alive_ids:
                if nid >= start:
                    return nid
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
        gracefully removed.
        This avoids a full global rebuild of all finger tables.
        """
        if not self.processors:
            return

        for p_id, p in self.processors.items():
            if not p.alive:
                continue

            if len(p.finger_table) < self.m_bits:
                p.finger_table += [p.successor_id] * (self.m_bits - len(p.finger_table))

            for i in range(self.m_bits):
                if p.finger_table[i] != removed_id:
                    continue

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
        Starting from start_id, route using finger table to find the alive successor responsible for key_id.

        This roughly follows the same logic as route_find_key_from(),
        but only returns the final successor ID (or None on failure).
        """
        if not self.processors:
            return None

        # if start node is invalid or crashed, pick any alive node
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
                # loop detected, topology is broken
                break
            visited.add(current_id)

            # use successor (skipping crashed) with successor_list help
            succ_id = self._first_alive_successor(current_id)

            if succ_id is None or succ_id not in self.processors:
                break

            # if key_id falls into (current, succ], succ is responsible
            if self._in_range(key_id, current_id, succ_id):
                return succ_id

            # otherwise use closest-preceding-finger as next hop
            next_id = self._closest_preceding_finger_alive(current_id, key_id)

            if next_id is None or next_id == current_id:
                next_id = succ_id

            current_id = next_id

        return None

    def _find_successor_via_successor(self, start_id: int, key_id: int) -> Optional[int]:
        """
        Fallback successor search that only walks along successor pointers.
        As long as the successor chain is not completely broken, this will
        eventually find the alive successor responsible for key_id.
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

            succ_id = self._first_alive_successor(current_id)
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
        """
        succ = self.processors.get(successor_id)
        if succ is None or not succ.alive:
            return

        n = self.processors.get(potential_pred_id)
        if n is None or not n.alive:
            return

        cur_pred_id = succ.predecessor_id

        if cur_pred_id is None:
            succ.predecessor_id = potential_pred_id
            return

        # update predecessor if potential_pred is in (cur_pred, succ]
        if self._in_range(potential_pred_id, cur_pred_id, succ.node_id):
            succ.predecessor_id = potential_pred_id

    def _stabilize_one_node(self, node_id: int) -> None:
        """
        Perform one round of Chord stabilize for a single alive node.

        for example：
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

        # skip crashed successors; stop if too many hops
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
                if self._in_range(x.node_id, n.node_id, succ.node_id):
                    n.successor_id = x.node_id
                    succ = x

        self._notify(succ.node_id, n.node_id)
        # after stabilizing successor, rebuild successor_list for n
        self._rebuild_successor_list_for(n.node_id)

    def tick_once(self, max_nodes: int = 0) -> None:
        """
        Advance logical time by one tick. In this tick, some alive nodes
        perform stabilize().

        max_nodes = 0   all alive nodes stabilize
        max_nodes > 0   randomly choose at most max_nodes alive nodes
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
        Simulate a Chord lookup for key_label starting from start_label.

        Returns:
          {
            "path_external": [...],
            "responsible_node": Optional[str],
            "stored": bool,
          }
        """
        key_str = str(key_label)
        key_id = self._hash_to_id(key_str)

        def _route_once() -> Tuple[List[int], Optional[int]]:
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
                    # loop detected, stop
                    break
                visited.add(current_id)

                path.append(current_id)
                cur_proc = self.processors[current_id]

                succ_id = self._first_alive_successor(current_id)
                if succ_id is None:
                    # no alive successor reachable, stop routing
                    break

                # if key is in (current, succ], succ is responsible
                if self._in_range(key_id, current_id, succ_id):
                    path.append(succ_id)
                    current_id = succ_id
                    break

                # keep finger-based step, fall back to successor if needed
                next_id = self._closest_preceding_finger_alive(current_id, key_id)
                if next_id is None or next_id == current_id:
                    next_id = succ_id

                current_id = next_id

            return path, current_id

        # first and only routing attempt
        path_internal, end_id = _route_once()

        path_external = [
            self.node_id_to_label.get(nid, str(nid)) for nid in path_internal
        ]

        if end_id is None:
            # no valid end node
            return {
                "path_external": path_external,
                "responsible_node": None,
                "stored": False,
            }

        end_proc = self.processors[end_id]

        stored_anywhere = False
        responsible_label: Optional[str] = None

        if end_proc.alive and end_proc.has_key(key_str):
            stored_anywhere = True
            responsible_label = end_proc.label
        elif end_proc.alive and not end_proc.has_key(key_str):
            stored_anywhere = False
            responsible_label = end_proc.label
        else: #end_proc is not alive
            for end_succ_id in end_proc.successor_list:
                if end_succ_id is None:
                    continue
                succ = self.processors[end_succ_id]
                if not succ.alive:
                    path_internal.append(end_succ_id)
                    continue
                if succ.has_key(key_str):
                    path_internal.append(end_succ_id)
                    stored_anywhere = True
                    responsible_label = succ.label
                    break

        return {
            "path_external": path_external,
            "responsible_node": responsible_label,
            "stored": stored_anywhere,
        }

    # helper accessors

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
