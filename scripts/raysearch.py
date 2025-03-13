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


def generate_workload(output_dir: Path, flags: list, label="workload") -> Path:
    # Not used by ray, but useful to reference during analysis
    conf_file = output_dir / f"{label}-workload-spec.conf"
    with open(conf_file, "w") as f:
        f.write("\n".join(str(flag) for flag in flags))
        f.write("\n")

    spec_file = output_dir / f"{label}.json"
    with open(spec_file, "w") as f:
        cmd = [
            "python3",
            "-m",
            "scripts.generate_workload_spec",
            *(str(flag) for flag in flags),
        ]
        subprocess.Popen(cmd, stdout=f).wait()
    return spec_file


def run_simulator(label: str, output_dir: Path, flags: list):
    output_dir.mkdir(parents=True, exist_ok=True)

    def outp(ext):
        return (output_dir / f"{label}.{ext}").resolve()

    log_file = outp("log")
    csv_file = outp("csv")
    flags.extend(
        [
            f"--log={log_file}",
            "--log_level=debug",
            f"--csv={csv_file}",
        ]
    )
    conf_file = outp("conf")
    with open(conf_file, "w") as f:
        f.write("\n".join(str(flag) for flag in flags))
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
    return parse_slo(sim / f"{label}.csv"), parse_analysis(analysis / f"{label}.stdout")


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

        "easy_ar_weight": tune.uniform(0, 1),
        "med_ar_weight": tune.uniform(0, 1),
        "hard_ar_weight": tune.uniform(0, 1),

        "arrival_rate": tune.uniform(0.022, 0.055),
        "invocations": 220,

        "tpch_max_executors_per_job": tune.choice((75, 100, 200)),
        "tpch_dataset_size": tune.choice((100, 250)),
    }


def generate_partition_string(difficulty_dict):
    """
    Generate a partitioning string from a single difficulty dictionary.

    Format: 'easy1,easy2,easy3:medium1,medium2,medium3:hard1,hard2,hard3'

    :param difficulty_dict: Dictionary with 'easy', 'medium', 'hard' keys
    :return: Formatted partition string
    """
    # Convert each difficulty list to comma-separated strings
    easy_str = ','.join(map(str, difficulty_dict['easy']))
    medium_str = ','.join(map(str, difficulty_dict['medium']))
    hard_str = ','.join(map(str, difficulty_dict['hard']))

    # Combine with : delimiter
    return f"{easy_str}:{medium_str}:{hard_str}"


query_difficulty_map = {
        (100, 75): {
            'easy': [11, 13, 14, 15, 19, 20, 22],
            'medium': [1, 2, 4, 6, 10, 12, 16, 17, 18],
            'hard': [3, 5, 7, 8, 9, 21]
        },
        (100, 100): {
            'easy': [2, 11, 13, 16, 19, 22],
            'medium': [1, 4, 6, 10, 12, 14, 15, 17, 20],
            'hard': [3, 5, 7, 8, 9, 18, 21]
        },
        (100, 200): {
            'easy': [6, 11, 13, 19, 22],
            'medium': [1, 2, 4, 10, 12, 14, 15, 16, 20],
            'hard': [3, 5, 7, 8, 9, 17, 18, 21]
        },
        (250, 75): {
            'easy': [2, 11, 13, 16, 19, 22],
            'medium': [1, 6, 7, 10, 12, 14, 15, 20],
            'hard': [3, 4, 5, 8, 9, 17, 18, 21]
        },
        (250, 100): {
            'easy': [2, 11, 13, 16, 19, 22],
            'medium': [1, 6, 10, 12, 14, 15, 20],
            'hard': [3, 4, 5, 7, 8, 9, 17, 18, 21]
        },
        (250, 200): {
            'easy': [1, 2, 6, 11, 13, 16, 22],
            'medium': [4, 7, 10, 12, 14, 15, 19, 20],
            'hard': [3, 5, 8, 9, 17, 18, 21]
        }
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
    workload_spec = generate_workload(output_dir, workload_spec_flags)

    sim_flags = [
        "--runtime_variance=0",
        "--tpch_min_task_runtime=12",
        "--execution_mode=replay",
        "--replay_trace=tpch",
        "--tpch_query_dag_spec=profiles/workload/tpch/queries.yaml",
        "--worker_profile_path=profiles/workers/tpch_cluster.yaml",
        "--random_seed=1234",
        "--slo_ramp_up_clip", 10,
        "--slo_ramp_down_clip", 10,
        "--tpch_workload_spec", workload_spec,
        "--tpch_dataset_size", config["tpch_dataset_size"],
        "--tpch_max_executors_per_job", config["tpch_max_executors_per_job"],
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
