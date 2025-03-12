#!/usr/bin/env python3
import json
import re
import subprocess
import shutil
import random
import math
from datetime import date


from pathlib import Path

import ray
import numpy as np


import hyperopt as hpo
from hyperopt import hp
from ray import tune
from ray import train
from ray.tune.search.hyperopt import HyperOptSearch
from ray.tune import Trainable
from ray.train import RunConfig


def generate_search_space():
    return {
        "min_deadline_variance": 10,
        "max_deadline_variance": 25,

        "easy_ar_weight": tune.uniform(0, 1),
        "med_ar_weight": tune.uniform(0, 1),
        "hard_ar_weight": tune.uniform(0, 1),

        "arrival_rate": tune.uniform(0.022, 0.055),
        "invocations": 220,

        "tpch_max_executors_per_job": tune.choice((75, 100, 200)),
        "tpch_dataset_size": tune.choice((100, 250)),
    }


def objective(config, experiment_dir):
    output_dir = experiment_dir / str(train.get_context().get_trial_id())
    output_dir.mkdir(parents=True)

    total_ar_weight = config["easy_ar_weight"] + config["med_ar_weight"] + config["hard_ar_weight"]
    arrival_rate = config["arrival_rate"]
    easy_ar = arrival_rate * config["easy_ar_weight"] / total_ar_weight
    med_ar = arrival_rate * config["med_ar_weight"] / total_ar_weight
    hard_ar = arrival_rate * config["hard_ar_weight"] / total_ar_weight

    partitioning = generate_partition_string(query_difficulty_map[
        (config["tpch_dataset_size"], config["tpch_max_executors_per_job"])
    ])

    workload_spec_flags = [
        "--partitioning-scheme", partitioning,
        "--num-queries", config["invocations"],
        "--arrival-rates", easy_ar, med_ar, hard_ar,
        "--dataset-size", config["tpch_dataset_size"],
        "--max-cores", config["tpch_max_executors_per_job"],
        "--deadline-variance", config["min_deadline_variance"], config["max_deadline_variance"],
        "--min-task-runtime", 12,
        "--tpch-query-dag-spec", "profiles/workload/tpch/queries.yaml",
        "--profile-type", "Cloudlab",
        "--random-seed", 1234,
    ]
    

    edf_slo, edf_analysis = run_edf(output_dir, sim_flags)
    dsched_slo, dsched_analysis = run_dsched(output_dir, sim_flags)

    metric = (
        (150 * math.log(dsched_slo / 0.8) if dsched_slo < 0.8 else 0) # penalize for dsched going below 80%
        + 2 * (dsched_slo - edf_slo)  # maximize slo difference, weighted by 2
        + (edf_analysis["avg"] - edf_analysis["eff"])  # maximize util difference in edf
        + dsched_analysis["eff"] # maximize effective util in dsched
    )

    result = {
        "metric": metric,
        "edf": {
            "slo": edf_slo,
            "analysis": edf_analysis,
        },
        "dsched": {
            "slo": dsched_slo,
            "analysis": dsched_analysis,
        },
    }

    return result


# Things to configure before spawning a search:
# - variables in main
# - generate_search_space config space

def main():
    num_samples = 10000
    num_cores_per_trial = 2
    # max_concurrent_trials = 4
    search_space = generate_search_space()
    exp_name = f"config-search-{date.today().isoformat()}"

    ray.init(num_cpus=108)

    experiment_dir = (Path("../expts/ray") / exp_name).resolve()
    if experiment_dir.exists():
        # clear up previous results
        shutil.rmtree(experiment_dir)
    experiment_dir.mkdir(parents=True)

    search_alg = HyperOptSearch(metric="metric", mode="max")
    obj = tune.with_parameters(
        objective,
        experiment_dir=experiment_dir,
    )
    obj = tune.with_resources(obj, {"cpu": num_cores_per_trial})
    tuner = tune.Tuner(
        obj,
        tune_config=tune.TuneConfig(
            num_samples=num_samples,
            search_alg=search_alg,
            # max_concurrent_trials=max_concurrent_trials,
        ),
        param_space=search_space,
    )

    result = tuner.fit()
    result.get_dataframe().to_csv(experiment_dir / "results.csv", index=False)


if __name__ == "__main__":
    main()
