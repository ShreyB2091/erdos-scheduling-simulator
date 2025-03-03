import itertools
import concurrent.futures
from pathlib import Path
from dataclasses import dataclass

from raysearch import run_and_analyze

import pandas as pd
import numpy as np
from tqdm import tqdm


@dataclass
class SchedSpec:
    name: str
    flags: list[str]

    def output_dir(self, base_dir: Path) -> Path:
        return base_dir / self.name
        

sched_specs = {
    spec.name: spec for spec in [
        SchedSpec(
            name="DSched",
            flags=[
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
            ],
        ),

        SchedSpec(
            name="EDF",
            flags=[
                "--scheduler=EDF",
                "--scheduler_runtime=0",
                "--enforce_deadlines",
                "--scheduler_plan_ahead_no_consideration_gap=1",
            ],
        ),
    ]
}


def partition_num(n, rats):
    s = sum(rats)
    return [n*(r/s) for r in rats]


def partition_num_int(n, rats):
    parts = partition_num(n, rats)
    parts = [int(part) for part in parts]
    s = sum(parts)
    if s < n:
        parts = [*parts[:-1], parts[-1] + n - s]
    assert(sum(parts) == n)
    return parts

def main():
    exp_dir = Path("tpch_plot")
    if not exp_dir.exists(): exp_dir.mkdir(parents=True)

    num_invocations_total = 220
    num_invocations_weights = (
        0.0004249565543,
        0.752791656,
        0.5211645858,
    )
    num_invocations = partition_num_int(num_invocations_total, num_invocations_weights)

    ar_lo, ar_hi = (0.022, 0.052)
    ar_weights = (
        0.3497239108,
        0.8929019532,
        0.6319769419,
    )
    num_interp = 10
    arrival_rates = [
        partition_num(float(ar), ar_weights)
        for ar in np.linspace(ar_lo, ar_hi, num_interp)
    ]

    configs = []
    base_flags = [
        "--runtime_variance=0",
        "--tpch_min_task_runtime=12",
        "--execution_mode=replay",
        "--replay_trace=tpch",
        "--tpch_query_dag_spec=profiles/workload/tpch/queries.yaml",
        "--worker_profile_path=profiles/workers/tpch_cluster.yaml",
        "--random_seed=1234",
        f'--min_deadline_variance=10',
        f'--max_deadline_variance=25',
        f'--tpch_dataset_size=250',
        f'--tpch_max_executors_per_job=75',
        f'--tpch_query_partitioning=2,11,13,16,19,22:1,6,7,10,12,14,15,20:3,4,5,8,9,17,18,21',
    ]
    for spec in sched_specs.values():
        output_dir = spec.output_dir(exp_dir)

        for (ar, ni) in itertools.product(arrival_rates, [num_invocations]):
            ar_s = ','.join([str(a) for a in ar])
            ni_s = ','.join([str(n) for n in ni])

            label = f'arrival_rate::{":".join([str(a) for a in ar])}'
            flags = [
                "--override_release_policy=poisson",
                f'--override_poisson_arrival_rates={ar_s}',
                f'--override_num_invocations={ni_s}',
                *base_flags,
                *spec.flags,
            ]

            configs.append({
                "sched": spec.name,
                "arrival_rate": sum(ar),
                "args": (label, output_dir, flags)
            })

    
    def task(config):
        try:
            analysis = run_and_analyze(*config["args"])
            return {
                **config,
                "slo": analysis[0],
                **analysis[1],
            }
        except:
            print(f"Failed to run {config}")
            return config

    num_workers = 20
    with concurrent.futures.ThreadPoolExecutor(max_workers=num_workers) as executor:
        results = list(tqdm(executor.map(task, configs), total=len(configs)))

    df = pd.DataFrame(results)
    df.to_csv(exp_dir / "results.csv", index=False)
    print(df)


if __name__ == "__main__":
    main()
