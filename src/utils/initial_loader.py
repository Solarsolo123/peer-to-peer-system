import json
from pathlib import Path
from typing import List, Dict, Any, Tuple

def load_initial_data(path: str) -> Tuple[int, List[int], List[int]]:
    """
    load initial data from src/initialdata/xx.json
    return: m_bits,node_ids, key_ids
    """

    p = Path(path)
    with p.open("r", encoding="utf-8") as f:
        data: Dict[str, Any] = json.load(f)


    m_bits = int(data.get("m_bits", 6))
    nodes_raw = data.get("nodes", [])
    keys_raw = data.get("keys", [])

    max_id = 2 ** m_bits
    node_ids = sorted(set(int(x) for x in nodes_raw))
    if any(not (0 <= n < max_id) for n in node_ids):
        raise ValueError("Node ID out of range")
    key_ids = sorted(set(int(x) for x in keys_raw))

    return m_bits, node_ids, key_ids