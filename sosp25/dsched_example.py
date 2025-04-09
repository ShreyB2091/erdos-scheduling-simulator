import os
from pathlib import Path
from pprint import pprint
from scripts.raysearch import run_simulator


def main():
    output_dir = Path(os.path.dirname(os.path.realpath(__file__))) / "output"
    if not output_dir.exists(): output_dir.mkdir(parents=True)

    base_flags = [
        # Task configs.
        "--runtime_variance=0",
        # Scheduler configs.
        "--scheduler=TetriSched",
        "--scheduler_runtime=0",
        "--enforce_deadlines",
        "--retract_schedules",
        "--release_taskgraphs",
        "--drop_skipped_tasks",
        "--scheduler_time_discretization=1",
        # Execution mode configs.
        "--execution_mode=json",
        "--workload_profile_path=./profiles/workload/dsched_sosp_example.yaml",
        "--worker_profile_path=./profiles/workers/dsched_sosp_example.yaml",
        "--scheduler_log_to_file=True",
    ]

    experiments = [
        (
            "dsched-init",
            [
                *base_flags,
            ],
        ),
        (
            "dsched-after-critical-opt",
            [
                *base_flags,
                "--opt_passes=CRITICAL_PATH_PASS",
                "--opt_passes=CAPACITY_CONSTRAINT_PURGE_PASS",
            ],
        ),
        (
            "dsched-after-dynamic-discretization",
            [
                *base_flags,
                "--opt_passes=CRITICAL_PATH_PASS",
                "--opt_passes=CAPACITY_CONSTRAINT_PURGE_PASS",
                "--opt_passes=DYNAMIC_DISCRETIZATION_PASS",
            ],
        ),
    ]

    for label, flags in experiments:
        run_simulator(label, output_dir, flags)
        print(f"done {label}")


if __name__ == "__main__":
    main()
