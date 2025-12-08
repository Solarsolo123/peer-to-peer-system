#  A Simulator Study on Chord Protocol

This project implements a lightweight simulator for the Chord protocol. All processors run inside a single process, but each behaves as an independent logical node with its own routing state. The simulator focuses on the logical behavior of Chord rather than real networking.

---

## 1. Project Structure

```
peer-to-peer-system/
│
├── experiments/
│   ├── experiment1.py                      # Lookup performance experiment
│   └── experiment1_lookup_performance.png  # Example plot
│
│
├── src/
│   ├── core/
│   │   ├── network.py                      # Main Chord logic
│   │   └── processor.py                    # Node state
│   │
│   ├── initialdata/
│   │   └── small.json                      # Initial nodes + keys
│   │
│   ├── utils/
│   │   └── initial_loader.py               # JSON configuration loader
│   │
│   └── main.py                             # CLI entry point
│
└── README.md
```

---

## 2. Initial Data Files (`src/initialdata/`)

The simulator loads its initial configuration from a JSON file such as:

```
src/initialdata/small.json
```

The JSON file defines:
* the bit size of the identifier space used by the Chord ring,
* a list of node labels,
* a list of key labels.

After loading, the simulator:

1. hashes labels to obtain node identifiers,
2. builds the initial Chord ring,
3. assigns keys to responsible nodes,
4. constructs initial finger tables and successor lists.

You may edit `small.json` or create new files to explore different setups.

---

## 3. Running the Simulator & Basic Commands

Run the simulator from the project root:

```
python src/main.py
```

At startup:

* The JSON configuration is loaded.
* You are automatically attached to a random active node.

### 3.1 Core Commands

* **`where`**
  Show the node you are currently attached to.

* **`move to <NodeLabel>`**
  Change the current node. This does *not* modify the network.

* **`find <KeyLabel>`**
  Perform a Chord lookup starting from the current node.

* **`add <NodeLabel>`**
  Insert a new node, compute its ID, locate its successor, update neighbors, and migrate keys.

* **`end <NodeLabel>`**
  Gracefully remove a node. Its keys are moved to its successor.

* **`crash <NodeLabel>`**
  Simulate an unexpected failure. Node remains in memory but becomes inactive.

* **`show`**
  Display all nodes, successors, predecessors, finger tables, successor lists, and stored keys.

* **`step`**
  Run one stabilization round. Several nodes update their successor/predecessor pointers.

* **`quit`**
  Exit the program.

---

## 4. Usage Restrictions

* You cannot `end` or `crash` the node you are currently attached to.
  Move to another node first.

* Crashed nodes:

  * remain in global state,
  * stop participating in routing,
  * do not migrate keys immediately.

* Successor lists and key replication improve fault tolerance. A lookup can still succeed as long as one replica holder remains alive.

---

## 5. Experiments

### 5.1 Finger Routing vs Successor-Only

Run:

```
python experiments/experiment1.py
```

The script:

* builds rings of several sizes,
* issues lookup requests,
* compares routing using:

  * full finger tables,
  * only successor pointers,
* outputs hop counts in PNG.

This corresponds to Section 4.3 of the report.

---

## 6. Summary

This simulator provides a controlled environment for studying:

* key distribution,
* Chord routing paths,
* finger table behavior,
* join/leave/crash dynamics,
* stabilization and ring recovery.

It is intended as a simple and extensible tool for experimentation and for understanding structured P2P lookup systems like Chord.
