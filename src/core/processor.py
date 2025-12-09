from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional, Set, Dict, Any, List


@dataclass
class Processor:
    """
    A processor (node) in the Chord ring.
    This class only stores local state and operates on its own keys
    and neighbor pointers. Global operations are done by network.py.
    """
    node_id: int
    label: str
    alive: bool = True

    # successor and predecessor are stored as node IDs.
    successor_id: Optional[int] = None
    predecessor_id: Optional[int] = None
    # A list of candidate successors
    successor_list: List[Optional[int]] = field(default_factory=list)

    # keys currently stored on this processor (external key labels as str).
    keys: Set[str] = field(default_factory=set)

    finger_table: List[Optional[int]] = field(default_factory=list)



    def add_key(self, key_id: str) -> None:
        """Store a key on this processor."""
        self.keys.add(key_id)

    def remove_key(self, key_id: str) -> None:
        """Remove a key from this processor if present."""
        self.keys.discard(key_id)

    def has_key(self, key_id: str) -> bool:
        """Check whether this processor currently stores the given key."""
        return key_id in self.keys

    def move_all_keys_to(self, target: Processor) -> None:
        """
        Move all keys from this processor to the target processor.
        """
        if not self.keys:
            return
        for k in self.keys:
            target.add_key(k)
        self.keys.clear()



    def set_successor(self, successor_id: Optional[int]) -> None:
        """Update the successor pointer."""
        self.successor_id = successor_id

    def set_predecessor(self, predecessor_id: Optional[int]) -> None:
        """Update the predecessor pointer."""
        self.predecessor_id = predecessor_id

    def set_successor_list(self, successors: List[Optional[int]]) -> None:
        """Replace the successor list with a new list."""
        self.successor_list = list(successors)

    def clear_neighbors(self) -> None:
        """Clear successor and predecessor pointers."""
        self.successor_id = None
        self.predecessor_id = None

    def mark_alive(self) -> None:
        """Mark this processor as alive."""
        self.alive = True

    def mark_crashed(self) -> None:
        """Mark this processor as crashed."""
        self.alive = False


    def to_dict(self) -> Dict[str, Any]:
        """
        Return a dictionary with basic information for debugging or printing.
        """
        return {
            "label": self.label,
            "id": self.node_id,
            "alive": self.alive,
            "successor": self.successor_id,
            "predecessor": self.predecessor_id,
            "keys": sorted(self.keys),
            "finger_table": self.finger_table,
            "successor_list": self.successor_list,
        }

    def __repr__(self) -> str:
        return (
            f"Processor(label={self.label}, id={self.node_id}, alive={self.alive}, "
            f"succ={self.successor_id}, pred={self.predecessor_id}, "
            f"keys={sorted(self.keys)},"
            f"succ_list_len={len(self.successor_list)})"
        )
