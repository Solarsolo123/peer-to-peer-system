from __future__ import annotations

import argparse, random
from multiprocessing.spawn import old_main_modules
from pathlib import Path

from src.core.network import Network
from src.utils.initial_loader import load_initial_data


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Chord-style P2P network simulator"
    )

    default_config_path = (
        Path(__file__).parent / "initialdata" / "small.json"
    )

    parser.add_argument(
        "--config",
        type=str,
        default=str(default_config_path),
        help="Path to initial data JSON file",
    )

    args = parser.parse_args()

    # load initial data
    m_bits, node_labels, key_labels = load_initial_data(args.config)

    # initialize network
    net = Network(m_bits=m_bits)
    net.init_from_labels(node_labels=node_labels, key_labels=key_labels)

    # check
    print(f"Initialized network with m_bits = {m_bits}")
    print("Internal nodes and their state:")

    # choose random alive processor for user
    alive_ids = [nid for nid, p in net.processors.items() if p.alive]
    entry_id = random.choice(alive_ids)
    current_node_label = net.node_id_to_label[entry_id]
    #current_node_label = '30'

    print(f"You are attached to node {current_node_label}")

    net.print_state_external()

    print("Enter commands. Supported:")
    print("  where                          - show current attached node")
    print("  move <nodeLabel>               - move to another alive node")
    print("  find <keyLabel>                - route a lookup for the given key")
    print("  add <processorLabel>           - create a new processor")
    print("  end <processorLabel>           - gracefully remove a processor")
    print("  crash <processorLabel>         - crash a processor (no key migration)")
    print("  show                           - print current network state")
    print("  step                           - run one full stabilization step")
    print("  quit                           - exit the program")

    while True:
        try:
            line = input("> ")
        except EOFError:
            # handle Ctrl+D or input stream closed
            print()
            break

        line = line.strip()
        if not line:
            continue

        parts = line.split()
        cmd = parts[0].lower()

        if cmd == "where":
            print(f"Current node: {current_node_label}")
            continue

        if cmd == "move":
            if len(parts) < 2:
                print("Usage: move <ProcessorID>")
                continue
            new_label = parts[1]
            if (
                    new_label in net.node_label_to_id
                    and net.processors[net.node_label_to_id[new_label]].alive
            ):
                current_node_label = new_label
                print(f"Now attached to node {current_node_label}")
            else:
                print("Cannot move: target node not alive or does not exist.")
            continue

        if cmd in ("quit", "exit"):
            print("Bye.")
            break

        # print current network state
        if cmd == "show":
            net.print_state_external()
            continue

        # run one full stabilization step
        if cmd == "step":
            net.tick_once(max_nodes=0)
            print("One full stabilization step executed.")
            continue

        if cmd == "find":
            if len(parts) < 2:
                print("Usage: find <KeyID>")
                continue
            key_label = parts[1]
            result = net.route_find_key_from(current_node_label, key_label)

            if result["stored"] != True:
                print("This key is not found in this network.")
                continue
            else:
                print("Route:", " -> ".join(result["path_external"]))
                print("Responsible:", result["responsible_node"])
                print("Stored:", result["stored"])
                print("Consistent with ideal ring:", result["consistent"])
                print("Routing retries:", result["retries"])
                net.tick_once(max_nodes=2)
                continue

        if cmd == "add":
            if len(parts) < 2:
                print("Usage: add <processorLabel>")
                continue

            proc_label = parts[1]
            result = net.add_processor(proc_label)

            if not result["success"]:
                print("[Error]", result["error"])
                continue

            print(f"Processor {result['new_processor']} created successfully.")
            if result["moved_keys"]:
                print("Old processor:",result["old_processor"])
                print("Old processor internal id:", result["old_processorid"])
                print("Keys moved to the new processor:")
                print(" ", result["moved_keys"])
            else:
                print("No keys moved.")

            net.tick_once(max_nodes=2)
            continue

        # gracefully end processor
        if cmd == "end":
            if len(parts) < 2:
                print("Usage: end <processorLabel>")
                continue

            target_label = parts[1]

            if target_label == current_node_label:
                print("You cannot end the processor you are currently attached to.")
                continue

            result = net.end_processor(target_label)

            if not result["success"]:
                print("[Error]", result["error"])
                continue

            print(f"Processor {target_label} ended successfully.")
            print("Ended processor internal id:", result["old_processorid"])
            if result["moved_keys"]:
                print("Keys moved from the ended processor:")
                print(" ", result["moved_keys"])
                print("Keys moved to new processor:",result["new_processor"])
                print("new processor internal id:", result["new_processorid"])
            else:
                print("No keys needed to be moved.")

            net.tick_once(max_nodes=2)
            continue

        # crash processor (no key migration)
        if cmd == "crash":
            if len(parts) < 2:
                print("Usage: crash <processorLabel>")
                continue

            target_label = parts[1]

            if target_label == current_node_label:
                print("You cannot crash the processor you are currently attached to.")
                continue

            result = net.crash_processor(target_label)

            if not result["success"]:
                print("[Error]", result["error"])
                continue

            print(f"Processor {target_label} crashed.")
            net.tick_once(max_nodes=2)
            continue



        # unknown command
        print(f"Unknown command: {cmd}")
        print("Supported commands: find <key>, add <processorID>, quit")

if __name__ == "__main__":
    main()
