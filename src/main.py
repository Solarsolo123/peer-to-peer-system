from __future__ import annotations

import argparse
from pathlib import Path

from core.network import Network
from utils.initial_loader import load_initial_data


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

    net.print_state_external()

    print("Enter commands. Supported:")
    print("  find <key>   - find where a key is stored")
    print("  add <processorID>         - add new processor")
    print("  quit         - exit the program")

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

        if cmd in ("quit", "exit"):
            print("Bye.")
            break

        if cmd == "find":
            if len(parts) < 2:
                print("Usage: find <key>")
                continue

            key_label = parts[1]
            node_label = net.find_key(key_label)

            if node_label is None:
                print(f"Key '{key_label}' is not stored anywhere in the system.")
            else:
                print(f"Key '{key_label}' is stored at node '{node_label}'.")
            continue

        if cmd == "add":
            if len(parts) < 2:
                print("Usage: add <ProcessorID>")
                continue

            result = net.add_processor(parts[1])

            if not result["success"]:
                print("[Error]", result["error"])
                continue

            print(f"Processor {result['new_processor']} created successfully.")

            if result["moved_keys"]:
                print("Keys moved to the new processor:")
                print(" ", result["moved_keys"])
            else:
                print("No keys moved.")
            continue



        # unknown command
        print(f"Unknown command: {cmd}")
        print("Supported commands: find <key>, add <processorID>, quit")

if __name__ == "__main__":
    main()
