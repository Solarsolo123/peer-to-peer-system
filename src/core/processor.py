from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional, Set, Dict, Any, List


@dataclass
class Processor:
    """
    A processor (node) in the P2P / Chord ring.
    This class only stores local state and operates on its own keys
    and neighbor pointers. Global operations are done by Network/Router.
    """

    node_id: int
    label: str
    alive: bool = True

    # successor and predecessor are stored as node IDs.
    successor_id: Optional[int] = None
    predecessor_id: Optional[int] = None

    # keys currently stored on this processor (key IDs).
    keys: Set[str] = field(default_factory=set)

    # finger_table[k] = node_id of the successor of (n + 2^k)
    finger_table: List[Optional[int]] = field(default_factory=list)



    def add_key(self, key_id: int) -> None:
        """Store a key on this processor."""
        self.keys.add(key_id)

    def remove_key(self, key_id: int) -> None:
        """Remove a key from this processor if present."""
        self.keys.discard(key_id)

    def has_key(self, key_id: int) -> bool:
        """Check whether this processor currently stores the given key."""
        return key_id in self.keys

    def move_all_keys_to(self, target: Processor) -> None:
        """
        Move all keys from this processor to the target processor.
        After this call, self.keys will be empty.
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
        }

    def __repr__(self) -> str:
        return (
            f"Processor(label={self.label}, id={self.node_id}, alive={self.alive}, "
            f"succ={self.successor_id}, pred={self.predecessor_id}, "
            f"keys={sorted(self.keys)})"
        )
