import itertools
import concurrent.futures
from pathlib import Path

from experiments.simulator_experiments import run_and_analyze
from experiments.spec import Experiment, sched_specs

import pandas as pd
from tqdm import tqdm


def main():
    exp_dir = Path("../tpch-plot")
    exp_dir.mkdir(parents=True, exist_ok=True)

    # bases = {
    #     "allhard": BaseExperiment(
    #         deadline_variance=(10,25),
    #         ar_weights=(0.0006490822741, 0.0002796889911, 0.6338342785),
    #         num_invocations=220,
    #         max_cores=100,
    #         dataset_size=100,
    #     ),
    #     "mixed": BaseExperiment(
    #         deadline_variance=(10,25),
    #         ar_weights=(0.1346377367, 0.1507476164, 0.3153456339),
    #         num_invocations=220,
    #         max_cores=100,
    #         dataset_size=100,
    #     ),
    # }

    experiments = {
        f"mixed-{ar}": Experiment(
            deadline_variance=(10,25),
            ar_weights=(0.1346377367, 0.1507476164, 0.3153456339),
            num_invocations=220,
            max_cores=100,
            dataset_size=100,
            total_arrival_rate=ar,
        )
        for ar in map(lambda x: x/1000, range(10, 55, 1))
    }
    trials = [
        (exp_dir / exp / "FIFO", experiments[exp], sched_specs["FIFO"])
        for exp in experiments
    ]

    num_workers = 1
    with concurrent.futures.ThreadPoolExecutor(max_workers=num_workers) as executor:
        results = list(tqdm(executor.map(
            run_and_analyze, *zip(*trials)
        ), total=len(trials)))

    df = pd.DataFrame(results)
    df.to_csv(exp_dir / "results.csv", index=False)
    print(df)


if __name__ == "__main__":
    main()
