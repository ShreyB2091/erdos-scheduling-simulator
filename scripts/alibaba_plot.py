"""
I calculated the final results by splitting the job across three nodes; was in a time crunch :)

Here are the invocations:

(shepherd)  python3 scripts/alibaba_plot.py --arrival-rates  "0.0145,0.026" "0.01,0.0165" "0.0135,0.022" "0.01,0.012" "0.012,0.02"
(inara)     python3 scripts/alibaba_plot.py --arrival-rates  "0.016,0.026" "0.01,0.0175" "0.013,0.025" "0.01,0.014" "0.0135,0.02"
(mal)       python3 scripts/alibaba_plot.py --arrival-rates  "0.01,0.033" "0.01,0.0345" "0.01,0.0385" "0.01,0.015" "0.015,0.014" "0.014,0.025"

You can run all of the configs on a single node though.
"""


"""
Dsched opt re-runs

(shepherd)  python3 scripts/alibaba_plot.py --arrival-rates  "0.0145,0.026" "0.01,0.0165" "0.0135,0.022" "0.01,0.012" "0.012,0.02"
(inara)     python3 scripts/alibaba_plot.py --arrival-rates  "0.016,0.026" "0.01,0.0175" "0.013,0.025" "0.01,0.014" "0.0135,0.02"
(mal)       python3 scripts/alibaba_plot.py --arrival-rates  "0.01,0.033" "0.01,0.0345" "0.01,0.0385" "0.01,0.015" "0.015,0.014" "0.014,0.025"
"""
#     (0.01, 0.0345),
#     (0.01, 0.033),
#     (0.016, 0.026),
#     (0.0145, 0.026),
#     (0.014, 0.025),
#     (0.0135, 0.022),
#     (0.0135, 0.02),
#     (0.012, 0.02),
#     (0.015, 0.014),
#     (0.01, 0.0175),
#     (0.01, 0.0165),
#     (0.01, 0.015),



import argparse
import traceback
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

        SchedSpec(
            name="DSched-noopt",
            flags=[
                "--scheduler=TetriSched",
                "--enforce_deadlines",
                "--release_taskgraphs",
                # "--opt_passes=CRITICAL_PATH_PASS",
                # "--opt_passes=CAPACITY_CONSTRAINT_PURGE_PASS",
                # "--opt_passes=DYNAMIC_DISCRETIZATION_PASS",
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

    ]
}

# List of tuples (medium, hard arrival rates)
# Sorted from hardest workload to easiest
# These are the arrival rates
ARRIVAL_RATES = [
    (0.01, 0.0385),
    (0.01, 0.0345),
    (0.01, 0.033),
    (0.016, 0.026),
    (0.0145, 0.026),
    (0.014, 0.025),
    (0.013, 0.025),
    (0.0135, 0.022),
    (0.0135, 0.02),
    (0.012, 0.02),
    (0.015, 0.014),
    (0.01, 0.0175),
    (0.01, 0.0165),
    (0.01, 0.015),
    (0.01, 0.014),
    (0.01, 0.012),
]

# tuple of medium, hard num invocations
NUM_INVOCATIONS = (350, 650)

def main():
    parser = argparse.ArgumentParser(description='Process arrival rates')
    
    parser.add_argument('--arrival-rates', type=str, nargs='+',
                        help='List of arrival rate pairs (e.g. "0.01,0.012" "0.015,0.014")')

    args = parser.parse_args()
    arrival_rates = []
    if args.arrival_rates:
        for pair in args.arrival_rates:
            try:
                medium, hard = pair.split(',')
                arrival_rates.append((float(medium), float(hard)))
            except ValueError:
                print(f"Error: Invalid pair format '{pair}'. Expected format: 'medium,hard'")
                return None
    else:
        arrival_rates = ARRIVAL_RATES


    exp_dir = Path("alibaba_scheduler_runtime-dsched")
    if not exp_dir.exists(): exp_dir.mkdir(parents=True)

    # Base alibaba workload flags
    base_flags = [
        # Worker config
        "--worker_profile_path=profiles/workers/alibaba_cluster_final.yaml",
        
        # Workload config
        "--workload_profile_paths=traces/alibaba-cluster-trace-v2018/easy_dag_sukrit_10k.pkl,traces/alibaba-cluster-trace-v2018/medium_dag_sukrit_10k.pkl,traces/alibaba-cluster-trace-v2018/hard_dag_sukrit_10k.pkl",
        "--workload_profile_path_labels=easy,medium,hard",
        
        # Loader config
"--execution_mode=replay",
"--replay_trace=alibaba",
"--alibaba_loader_task_cpu_usage_random",
"--alibaba_loader_task_cpu_multiplier=1",
"--alibaba_loader_task_cpu_usage_min=120",
"--alibaba_loader_task_cpu_usage_max=1500",
"--alibaba_loader_min_critical_path_runtimes=200,500,600",
"--alibaba_loader_max_critical_path_runtimes=500,1000,1000",

"--override_release_policies=poisson,poisson,poisson",
"--randomize_start_time_max=50",
"--min_deadline=5",
"--max_deadline=500",
"--min_deadline_variances=25,50,10",
"--max_deadline_variances=50,100,25",
"--enforce_deadlines",
"--random_seed=420665456",

        # Important: scheduler_runtime=0 is not set!
    ]


    configs = []
    for ar in arrival_rates:
        for spec in sched_specs.values():
            output_dir = spec.output_dir(exp_dir)
            if not output_dir.exists(): output_dir.mkdir(parents=True)

            ar_s = [str(a) for a in ar]
            label = f'arrival_rate::{":".join(ar_s)}'
            print(label)
            flags = [
                *base_flags,
                *spec.flags,
                f"--override_num_invocations=0,{NUM_INVOCATIONS[0]},{NUM_INVOCATIONS[1]}",
                f"--override_poisson_arrival_rates=0,{ar[0]},{ar[1]}",
            ]
            configs.append({
                "label": label,
                "output_dir": output_dir,
                "sched_name": spec.name,
                "flags": flags,
                "arrival_rate": sum(ar),
            })

    def task(config):
        try:
            label = config["label"]
            output_dir = config["output_dir"]
            sched_name = config["sched_name"]
            flags = config["flags"]
            arrival_rate = config["arrival_rate"]

            result = run_and_analyze(
                label=label,
                output_dir=output_dir,
                flags=flags,
            )

            return {
                "config": config,
                "name": sched_name,
                "arrival_rate": arrival_rate,
                "slo": result["slo"],
                "avg_scheduler_runtime": result["avg_scheduler_runtime"],
                **result["analysis"],
            }
        except Exception as e:
            print(f"Failed to run {config}")
            print("Exception:", e)
            print(traceback.format_exc())
            return config

    num_workers = 5
    with concurrent.futures.ThreadPoolExecutor(max_workers=num_workers) as executor:
        results = list(tqdm(executor.map(task, configs), total=len(configs)))

    df = pd.DataFrame(results)
    df.to_csv(exp_dir / "results.csv", index=False)
    print(df)


if __name__ == "__main__":
    main()
