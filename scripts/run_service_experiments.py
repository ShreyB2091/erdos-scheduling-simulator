#!/usr/bin/env python3
import argparse
from pathlib import Path
import itertools
import logging

from experiments import Experiment, sched_specs, run_service_experiment

def main():
    logging.basicConfig(format='%(asctime)s %(message)s', level=logging.INFO)

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--spark-mirror-path",
        type=Path,
        required=True,
        help="Path to spark-mirror repository",
    )
    parser.add_argument("--output-dir", type=Path, default=Path("../service-plot"))
    args = parser.parse_args()

    experiments = {
        f"mixed-{r}": Experiment(
            deadline_variance=(10,25),
            ar_weights=[0.1346377367, 0.1507476164, 0.3153456339],
            num_invocations=220,
            max_cores=100,
            dataset_size=100,
            total_arrival_rate=r,
        )
        for r in (0.02, 0.03, 0.033, 0.035, 0.038, 0.04, 0.045, 0.05)
    }
    schedulers = {name: sched_specs[name] for name in ("TetriSched_0", "Graphene_0")}

    runs = list(itertools.product(experiments.items(), schedulers.items()))

    for i, ((expt_name, expt), (sched_name, sched)) in enumerate(runs):
        print(f"=== {expt_name} ({i+1}/{len(runs)}) ===")
        run_service_experiment(
            output_dir=args.output_dir / expt_name / sched_name,
            expt=expt,
            sched=sched,
            spark_mirror=args.spark_mirror_path,
            properties_file=args.spark_mirror_path / "conf" / "spark-dg-config.conf",
        )
        print("=== done ===\n")


if __name__ == "__main__":
    main()
