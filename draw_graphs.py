import json
import sys
import typing
from collections import defaultdict

import matplotlib.pyplot as plt

if len(sys.argv) > 1:
    JSON_PATH = sys.argv[1]
else:
    raise ValueError(
        "Please provide the path to the benchmark JSON file as a command-line argument."
    )

with open(JSON_PATH, "r") as f:
    data = json.load(f)

benchmarks = data["benchmarks"]

data: dict[str, dict[str, typing.Any]] = defaultdict(dict)

for test in benchmarks:
    if "create" in test["name"]:
        op = "create"
    elif "read" in test["name"]:
        op = "read"
    elif "update" in test["name"]:
        op = "update"
    elif "delete" in test["name"]:
        op = "delete"
    else:
        raise ValueError(f"Unknown operation in test name: {test['name']}")
    try:
        query_variant = test["param"].split("-")[1]
    except IndexError:
        query_variant = "no_filter"
    data[op].setdefault(query_variant, {})[
        int(test["params"]["records_count"])
    ] = test["stats"]


def plot_all_subplots(op: str, data: dict[str, typing.Any]):
    fig, axs = plt.subplots(2, 1, figsize=(10, 10))
    fig.suptitle(f"{op.capitalize()} - Benchmark Analysis", fontsize=16)

    for query_variant, stats in data.items():
        sizes = sorted(stats.keys())
        mean_times = [stats[size]["mean"] for size in sizes]
        operations_per_second = [stats[size]["ops"] for size in sizes]
        axs[0].loglog(sizes, mean_times, marker="o", label=query_variant)
        axs[1].loglog(
            sizes, operations_per_second, marker="o", label=query_variant
        )

    axs[0].set_xlabel("Number of records")
    axs[0].set_ylabel("Execution time (seconds)")
    axs[0].set_title("Execution Time vs Data Size")
    axs[0].grid(True)
    axs[0].legend()

    axs[1].set_xlabel("Number of records")
    axs[1].set_ylabel("Operations per second (ops/sec)")
    axs[1].set_title("Operations per Second vs Data Size")
    axs[1].grid(True)
    axs[1].legend()

    plt.show()


# -------------------------------
# Generate plots (subplots per operation)
# -------------------------------
for operation, values in list(data.items()):
    plot_all_subplots(operation, values)
