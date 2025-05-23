import traceback
import itertools
import concurrent.futures
from pathlib import Path
from dataclasses import dataclass

from raysearch import run_and_analyze, query_difficulty_map, generate_partition_string, generate_workload

import pandas as pd
import numpy as np
from tqdm import tqdm

from pprint import pprint


@dataclass
class SchedSpec:
    name: str
    flags: list[str]

    def output_dir(self, base_dir: Path) -> Path:
        return base_dir / self.name
        

sched_specs = {
    spec.name: spec for spec in [
        # SchedSpec(
        #     name="DSched",
        #     flags=[
        #         "--scheduler=TetriSched",
        #         "--enforce_deadlines",
        #         "--release_taskgraphs",
        #         "--opt_passes=CRITICAL_PATH_PASS",
        #         "--opt_passes=CAPACITY_CONSTRAINT_PURGE_PASS",
        #         "--opt_passes=DYNAMIC_DISCRETIZATION_PASS",
        #         "--retract_schedules",
        #         "--scheduler_max_occupancy_threshold=0.999",
        #         "--finer_discretization_at_prev_solution",
        #         "--scheduler_selective_rescheduling",
        #         "--scheduler_reconsideration_period=0.9",
        #         "--scheduler_time_discretization=1",
        #         "--scheduler_max_time_discretization=5",
        #         "--finer_discretization_window=5",
        #         "--scheduler_plan_ahead_no_consideration_gap=2",
        #         "--drop_skipped_tasks",
        #     ],
        # ),

        # SchedSpec(
        #     name="EDF",
        #     flags=[
        #         "--scheduler=EDF",
        #         "--enforce_deadlines",
        #         "--scheduler_plan_ahead_no_consideration_gap=1",
        #     ],
        # ),

        SchedSpec(
            name="Graphene_0",
            flags=[
                "--scheduler=Graphene",
                "--scheduler_time_discretization=1",
                "--retract_schedules",
                "--scheduler_plan_ahead=0",
                "--opt_passes=CRITICAL_PATH_PASS",
                "--opt_passes=CAPACITY_CONSTRAINT_PURGE_PASS",
            ],
        ),

        # SchedSpec(
        #     name="Graphene_10",
        #     flags=[
        #         "--scheduler=Graphene",
        #         "--scheduler_time_discretization=1",
        #         "--retract_schedules",
        #         "--scheduler_plan_ahead=0",
        #         "--scheduler_time_limit=10",
        #         "--opt_passes=CRITICAL_PATH_PASS",
        #         "--opt_passes=CAPACITY_CONSTRAINT_PURGE_PASS",
        #     ],
        # ),

        # SchedSpec(
        #     name="Graphene_30",
        #     flags=[
        #         "--scheduler=Graphene",
        #         "--scheduler_time_discretization=1",
        #         "--retract_schedules",
        #         "--scheduler_plan_ahead=0",
        #         "--scheduler_time_limit=30",
        #         "--opt_passes=CRITICAL_PATH_PASS",
        #         "--opt_passes=CAPACITY_CONSTRAINT_PURGE_PASS",
        #     ],
        # ),

        SchedSpec(
            name="TetriSched_0",
            flags=[
                "--scheduler=TetriSched",
                "--enforce_deadlines",
                "--scheduler_time_discretization=1",
                "--retract_schedules",
                "--scheduler_plan_ahead=0",
                "--opt_passes=CRITICAL_PATH_PASS",
                "--opt_passes=CAPACITY_CONSTRAINT_PURGE_PASS",
            ],
        ),

        # SchedSpec(
        #     name="TetriSched_1",
        #     flags=[
        #         "--scheduler=TetriSched",
        #         "--enforce_deadlines",
        #         "--scheduler_time_discretization=1",
        #         "--retract_schedules",
        #         "--scheduler_plan_ahead=1",
        #         "--opt_passes=CRITICAL_PATH_PASS",
        #         "--opt_passes=CAPACITY_CONSTRAINT_PURGE_PASS",
        #     ],
        # ),

        # SchedSpec(
        #     name="TetriSched_5",
        #     flags=[
        #         "--scheduler=TetriSched",
        #         "--enforce_deadlines",
        #         "--scheduler_time_discretization=1",
        #         "--retract_schedules",
        #         "--scheduler_plan_ahead=5",
        #         "--opt_passes=CRITICAL_PATH_PASS",
        #         "--opt_passes=CAPACITY_CONSTRAINT_PURGE_PASS",
        #     ],
        # ),

        # SchedSpec(
        #     name="TetriSched_10",
        #     flags=[
        #         "--scheduler=TetriSched",
        #         "--enforce_deadlines",
        #         "--scheduler_time_discretization=1",
        #         "--retract_schedules",
        #         "--scheduler_plan_ahead=10",
        #         "--opt_passes=CRITICAL_PATH_PASS",
        #         "--opt_passes=CAPACITY_CONSTRAINT_PURGE_PASS",
        #     ],
        # ),

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
    exp_dir = Path("tpch_sim_baselines-graphene+tetrisched")
    if not exp_dir.exists(): exp_dir.mkdir(parents=True)

    num_invocations_total = 220

    ar_lo, ar_hi = (0.01, 0.054)
    ar_weights = (
        0.1346377367,
        0.1507476164,
        0.3153456339,
    )
    num_interp = 45
    arrival_rates = [
        partition_num(float(ar), ar_weights)
        for ar in np.linspace(ar_lo, ar_hi, num_interp)
    ]

    min_task_runtime = 12
    dataset_size = 100
    max_executors_per_job = 100
    random_seed = 1234

    configs = []
    
    for spec in sched_specs.values():
        output_dir = spec.output_dir(exp_dir)
        output_dir.mkdir(parents=True, exist_ok=True)

        for ar in arrival_rates:
            ars = [str(a) for a in ar]
            label = f'arrival_rate::{":".join(ars)}'
            flags = [
                *('--arrival-rates', *ar), 
                *('--num-queries', num_invocations_total),
                *('--partitioning-scheme', generate_partition_string(query_difficulty_map[(dataset_size, max_executors_per_job)])),
                *('--dataset-size', dataset_size),
                *('--max-cores', max_executors_per_job),
                *('--deadline-variance', 10, 25),
                *('--min-task-runtime', min_task_runtime), 
                *('--tpch-query-dag-spec', 'profiles/workload/tpch/queries.yaml'),
                *('--profile-type', 'Cloudlab'),
                *('--random-seed', 1234),
                
            ]
            configs.append({
                "label": label,
                "output_dir": output_dir,
                "spec_flags": flags,
                "sched_name": spec.name,
                "sched_flags": spec.flags,
                "arrival_rate": sum(ar),
            })


    def task(config):
        try:
            label = config["label"]
            output_dir = config["output_dir"]
            spec_flags = config["spec_flags"]
            sched_name = config["sched_name"]
            sched_flags = config["sched_flags"]
            arrival_rate = config["arrival_rate"]

            spec_file = generate_workload(
                label=label,
                output_dir=output_dir,
                flags=spec_flags,
            )

            sim_flags = [
                "--scheduler_runtime=0",
                "--runtime_variance=0",
                "--execution_mode=replay",
                "--replay_trace=tpch",
                "--worker_profile_path=profiles/workers/tpch_cluster.yaml",
                f"--random_seed={random_seed}",

                # tpch flags
                f"--tpch_workload_spec={spec_file}",
                "--tpch_query_dag_spec=profiles/workload/tpch/queries.yaml",
                f"--tpch_dataset_size={dataset_size}",
                f"--tpch_min_task_runtime={min_task_runtime}",
                f"--tpch_max_executors_per_job={max_executors_per_job}",

                # scheduler flags
                *sched_flags
            ]
            slo, util = run_and_analyze(
                label=label,
                output_dir=output_dir,
                flags=sim_flags,
            )

            return {
                "config": config,
                "name": sched_name,
                "arrival_rate": arrival_rate,
                "slo": slo,
                **util,
            }
        except Exception as e:
            print(f"Failed to run {config}")
            print("Exception:", e)
            print(traceback.format_exc())
            return config

    num_workers = 75
    with concurrent.futures.ThreadPoolExecutor(max_workers=num_workers) as executor:
        results = list(tqdm(executor.map(task, configs), total=len(configs)))

    df = pd.DataFrame(results)
    df.to_csv(exp_dir / "results.csv", index=False)
    print(df)


if __name__ == "__main__":
    main()
