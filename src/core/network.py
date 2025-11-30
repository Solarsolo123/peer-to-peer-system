# src/core/network.py

from __future__ import annotations

import hashlib
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
            print(f"  successor: {self.node_id_to_label.get(proc.successor_id, proc.successor_id)}")
            print(f"  predecessor: {self.node_id_to_label.get(proc.predecessor_id, proc.predecessor_id)}")
            print(f"  keys (external): {external_keys}")
            print()

    def find_key(self, key_label: Any):
        if not self._sorted_node_ids:
            return None
        key_label = str(key_label)
        hash_key = self._hash_to_id(key_label)
        node_id = self._find_responsible_node_id(hash_key)
        proc = self.processors[node_id]
        if proc.has_key(key_label):
            return self.node_id_to_label.get(node_id, str(node_id))
        else:
            return None

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
            if self.is_in_range(key_id, pre, internal_id):
                self.processors[succ].keys.remove(key)
                self.processors[internal_id].keys.add(key)
                moved_keys.append(key)

        if moved_keys:
            print(f"Keys moved to new processor {label}:")
            print(f"  {sorted(k for k in moved_keys)}")
            return {
                "success": True,
                "error": None,
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
        succ = self._sorted_node_ids[(idx + 1) % len(self._sorted_node_ids)]
        moved_labels = sorted(proc.keys)

        for label in moved_labels:
            succ.add_key(label)
        proc.keys.clear()

        # delete this processor, rebuild ring / finger tables
        del self.processors[internal_id]
        del self.node_label_to_id[label]
        del self.node_id_to_label[internal_id]
        self._sorted_node_ids = sorted(self.processors.keys())
        self._build_ring()
        self.build_finger_tables()


        if moved_labels:
            print(f"Keys moved to new processor {label}:")
            print(f"  {sorted(k for k in moved_labels)}")
            return {
                "success": True,
                "error": None,
                "new_processor": label,
                "moved_keys": sorted(k for k in moved_labels),
            }
        else:
            print("No keys moved.")
            return {
                "success": True,
                "error": None,
                "new_processor": label,
                "moved_keys": None,
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
        ids = self._sorted_node_ids
        if not ids:
            return

        for node_id in ids:
            proc = self.processors[node_id]
            proc.finger_table = []
            for k in range(self.m_bits):
                start = (node_id + (1 << k)) % self.max_id
                succ_id = self._find_responsible_node_id(start)
                proc.finger_table.append(succ_id)

    def _closest_preceding_finger(self, current_id: int, key_id: int) -> int:
        """
        From current_id, scan its finger table from largest to smallest,
        and return the finger node_id that lies in (current_id, key_id),
        if any. If none, return current_id itself.
        """
        proc = self.processors[current_id]
        if not proc.finger_table:
            return current_id

        for finger_id in reversed(proc.finger_table):
            if finger_id is None:
                continue
            # here we assume all nodes are alive; later you can skip crashed ones
            if self._in_range(finger_id, current_id, key_id):
                return finger_id

        return current_id

    def route_find_key_from(
            self, start_label: Any, key_label: Any
    ) -> Dict[str, Any]:
        """
        Simulate Chord routing from the given start node to find the key.

        Returns:
          {
            "path_internal": [int, ...],
            "path_external": [str, ...],
            "responsible_node": str,   # external label
            "stored": bool             # whether key is actually stored there
          }
        """
        if not self._sorted_node_ids:
            return {
                "path_internal": [],
                "path_external": [],
                "responsible_node": None,
                "stored": False,
            }

        key_str = str(key_label)
        key_id = self._hash_to_id(key_str)

        # get start internal id
        start_label_str = str(start_label)
        start_id = self.node_label_to_id.get(start_label_str)
        if start_id is None:
            # if start node not found, fall back to smallest node
            start_id = self._sorted_node_ids[0]

        path_internal: List[int] = []
        current_id = start_id

        # safety limit to avoid infinite loop
        max_hops = len(self._sorted_node_ids) * 2

        for _ in range(max_hops):
            path_internal.append(current_id)
            current_proc = self.processors[current_id]
            succ_id = current_proc.successor_id
            if succ_id is None:
                break

            # if key lies between current and successor, jump to successor and stop
            if self._in_range(key_id, current_id, succ_id):
                path_internal.append(succ_id)
                current_id = succ_id
                break

            # otherwise, move to closest preceding finger
            next_id = self._closest_preceding_finger(current_id, key_id)
            if next_id == current_id:
                # cannot move forward, stop here
                break
            current_id = next_id

        # current_id should be the responsible node (according to routing)
        responsible_internal = self._find_responsible_node_id(key_id)
        responsible_label = self.node_id_to_label.get(
            responsible_internal, str(responsible_internal)
        )

        # check if key is actually stored at responsible node
        responsible_proc = self.processors[responsible_internal]
        stored = responsible_proc.has_key(key_id)

        path_external = [
            self.node_id_to_label.get(nid, str(nid)) for nid in path_internal
        ]

        return {
            "path_internal": path_internal,
            "path_external": path_external,
            "responsible_node": responsible_label,
            "stored": stored,
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
