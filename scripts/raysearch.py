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


def run_simulator(label: str, output_dir: Path, flags: list):
    output_dir = (output_dir/label)
    output_dir.mkdir(parents=True, exist_ok=True)

    def absp(p):
        return output_dir / p

    def outp(ext):
        return f"{label}.{ext}"

    log_file = outp("log")
    csv_file = outp("csv")
    flags.extend(
        [
            f"--log_dir={output_dir.resolve()}",
            f"--log={log_file}",
            "--log_level=debug",
            f"--csv={csv_file}",
        ]
    )
    conf_file = absp(outp("conf"))
    with open(conf_file, "w") as f:
        f.write("\n".join(flags))
        f.write("\n")

    stdout, stderr = outp("stdout"), outp("stderr")
    with open(stdout, "w") as f_stdout, open(stderr, "w") as f_stderr:
        cmd = [
            "python3",
            "main.py",
            "--flagfile",
            str(conf_file),
        ]
        subprocess.Popen(cmd, stdout=f_stdout, stderr=f_stderr).wait()

    return output_dir


def run_analysis(label: str, results_dir: Path):
    output_dir = results_dir / "analysis"
    output_dir.mkdir(parents=True, exist_ok=True)

    def outp(ext):
        return (output_dir / f"{label}.{ext}").resolve()

    stdout, stderr = outp("stdout"), outp("stderr")
    with open(stdout, "w") as f_stdout, open(stderr, "w") as f_stderr:
        cmd = [
            "python3",
            "analyze.py",
            f"--csv_files={results_dir}/{label}.csv",
            f"--conf_files={results_dir}/{label}.conf",
            f"--output_dir={output_dir}",
            "--goodresource_utilization",
        ]
        subprocess.Popen(cmd, stdout=f_stdout, stderr=f_stderr).wait()

    return output_dir


def parse_analysis(result: Path):
    with open(result, "r") as f:
        data = f.read()
    eff = float(
        re.search(r"Average Good Utilization:\s+([-+]?\d*\.\d+|\d+)", data).group(1)
    )
    avg = float(re.search(r"Average Utilization:\s+([-+]?\d*\.\d+|\d+)", data).group(1))
    return {"avg": avg, "eff": eff}


def parse_slo(result: Path):
    with open(result, "r") as f:
        data = reversed(f.readlines())
    slo = None
    for line in data:
        parts = line.split(",")
        if len(parts) < 1:
            break
        if parts[1] == "LOG_STATS":
            finished = float(parts[5])
            cancelled = float(parts[6])
            missed = float(parts[7])
            slo = (finished - missed) / (finished + cancelled) * 100
            slo = float(parts[8])
            break
    return slo


def run_and_analyze(label: str, output_dir: Path, flags: list):
    sim = run_simulator(label, output_dir, flags)
    analysis = run_analysis(label, sim)
    return parse_slo(sim / f"{label}.csv"), parse_analysis(analysis / "stdout")


def run_edf(output_dir: Path, flags: list):
    output_dir = output_dir / "edf"
    flags = [
        *flags,
        "--scheduler=EDF",
        "--scheduler_runtime=0",
        "--enforce_deadlines",
        "--scheduler_plan_ahead_no_consideration_gap=1",
    ]
    return run_and_analyze("edf", output_dir, flags)


def run_dsched(output_dir: Path, flags: list):
    output_dir = output_dir / "dsched"
    flags = [
        *flags,
        "--scheduler=TetriSched",
        "--scheduler_runtime=0",
        "--enforce_deadlines",
        "--release_taskgraphs",
        "--opt_passes=CRITICAL_PATH_PASS",
        "--opt_passes=CAPACITY_CONSTRAINT_PURGE_PASS",
        "--opt_passes=DYNAMIC_DISCRETIZATION_PASS",
        "--retract_schedules",
        "--scheduler_max_occupancy_threshold=0.999",
        "--finer_discretization_at_prev_solution",
        "--scheduler_selective_rescheduling",
        "--scheduler_reconsideration_period=0.9",
        "--scheduler_time_discretization=1",
        "--scheduler_max_time_discretization=5",
        "--finer_discretization_window=5",
        "--scheduler_plan_ahead_no_consideration_gap=2",
        "--drop_skipped_tasks",
    ]
    return run_and_analyze("dsched", output_dir, flags)


def generate_search_space():
    return {
        "min_deadline_variance": 10,
        "max_deadline_variance": 25,

        "easy_invoc_weight": tune.uniform(0, 1),
        "med_invoc_weight": tune.uniform(0, 1),
        "hard_invoc_weight": tune.uniform(0, 1),

        "easy_ar_weight": tune.uniform(0, 1),
        "med_ar_weight": tune.uniform(0, 1),
        "hard_ar_weight": tune.uniform(0, 1),

        "arrival_rate": tune.uniform(0.022, 0.055),
        "invocations": 220,

        "tpch_max_executors_per_job": 75,
        "tpch_dataset_size": 250,

        "partitioning": "2,11,13,16,19,22:1,6,7,10,12,14,15,20:3,4,5,8,9,17,18,21",
    }


def objective(config, experiment_dir):
    output_dir = experiment_dir / str(train.get_context().get_trial_id())
    output_dir.mkdir(parents=True)

    base_flags = [
        "--runtime_variance=0",
        "--tpch_min_task_runtime=12",
        "--execution_mode=replay",
        "--replay_trace=tpch",
        "--tpch_query_dag_spec=profiles/workload/tpch/queries.yaml",
        "--worker_profile_path=profiles/workers/tpch_cluster.yaml",
        "--random_seed=1234",
    ]

    arrival_rate = config["arrival_rate"]
    num_invocations = config["invocations"]

    total_ar_weight = config["easy_ar_weight"] + config["med_ar_weight"] + config["hard_ar_weight"]
    easy_ar = arrival_rate * config["easy_ar_weight"] / total_ar_weight
    med_ar = arrival_rate * config["med_ar_weight"] / total_ar_weight
    hard_ar = arrival_rate * config["hard_ar_weight"] / total_ar_weight

    total_invoc_weight = config["easy_invoc_weight"] + config["med_invoc_weight"] + config["hard_invoc_weight"]
    easy_invoc = int(num_invocations * config["easy_invoc_weight"] / total_invoc_weight)
    med_invoc = int(num_invocations * config["med_invoc_weight"] / total_invoc_weight)
    hard_invoc = int(num_invocations * config["hard_invoc_weight"] / total_invoc_weight)

    if easy_invoc + med_invoc + hard_invoc < num_invocations:
        hard_invoc += num_invocations - (easy_invoc + med_invoc + hard_invoc)

    config_specific_flags = [
        f'--min_deadline_variance={config["min_deadline_variance"]}',
        f'--max_deadline_variance={config["max_deadline_variance"]}',
        "--override_release_policy=poisson",

        f'--override_poisson_arrival_rates={easy_ar},{med_ar},{hard_ar}',
        f'--override_num_invocations={easy_invoc},{med_invoc},{hard_invoc}',

        # f'--override_poisson_arrival_rate={config["override_poisson_arrival_rate"]}',
        # f'--override_num_invocation={config["override_num_invocation"]}',

        f'--tpch_dataset_size={config["tpch_dataset_size"]}',
        f'--tpch_max_executors_per_job={config["tpch_max_executors_per_job"]}',

        f'--tpch_query_partitioning={config["partitioning"]}',
    ]

    flags = [*base_flags, *config_specific_flags]

    edf_slo, edf_analysis = run_edf(output_dir, flags)
    dsched_slo, dsched_analysis = run_dsched(output_dir, flags)

    metric = (
        150 * math.log(dsched_slo / 0.8) # penalize for dsched going below 80%
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
    num_samples = 2000
    # num_cores_per_trial = 2
    # max_concurrent_trials = 4
    search_space = generate_search_space()
    exp_name = f"config-search-{date.today().isoformat()}"

    ray.init(num_cpus=14)

    experiment_dir = (Path("ray") / exp_name).resolve()
    if experiment_dir.exists():
        # clear up previous results
        shutil.rmtree(experiment_dir)
    experiment_dir.mkdir(parents=True)

    search_alg = HyperOptSearch(metric="metric", mode="max")
    obj = tune.with_parameters(
        objective,
        experiment_dir=experiment_dir,
    )
    # obj = tune.with_resources(obj, {"cpu": num_cores_per_trial})
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
