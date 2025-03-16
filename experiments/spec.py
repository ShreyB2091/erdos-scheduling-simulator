from dataclasses import dataclass
from pathlib import Path


def partitioning_scheme(dataset_size: int, max_cores: int) -> str:
    """Get the preferred partitioning scheme for a given config."""

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

    return ":".join(
        ",".join(map(str, partition))
        for partition in query_difficulty_map[dataset_size, max_cores].values()
    )


@dataclass
class Experiment:
    """Specification for a single experiment."""

    deadline_variance: tuple[int, int]
    ar_weights: list[float]
    num_invocations: int
    max_cores: int
    dataset_size: int
    total_arrival_rate: float

    def workload_spec_flags(self) -> list[str]:
        """Return the flags to pass to generate_workload_spec."""
        arrival_rates = self._calc_arrival_rates()
        partitioning = partitioning_scheme(self.dataset_size, self.max_cores)
        flags = [
            "--partitioning-scheme", partitioning,
            "--num-queries", self.num_invocations,
            "--arrival-rates", *arrival_rates,
            "--dataset-size", self.dataset_size,
            "--max-cores", self.max_cores,
            "--deadline-variance", *(self.deadline_variance),
            "--min-task-runtime", 12,
            "--tpch-query-dag-spec", "profiles/workload/tpch/queries.yaml",
            "--profile-type", "Cloudlab",
            "--random-seed", 1234,
        ]
        return list(map(str, flags))

    def base_sim_flags(self) -> list[str]:
        """Return a base set of flags to run the simulator with.

        Does not include workload spec or scheduler information.

        """
        flags = [
            "--runtime_variance=0",
            "--tpch_min_task_runtime=12",
            "--execution_mode=replay",
            "--replay_trace=tpch",
            "--tpch_query_dag_spec=profiles/workload/tpch/queries.yaml",
            "--worker_profile_path=profiles/workers/tpch_cluster.yaml",
            "--random_seed=1234",
            "--slo_ramp_up_clip", 10,
            "--slo_ramp_down_clip", 10,
            "--tpch_dataset_size", self.dataset_size,
            "--tpch_max_executors_per_job", self.max_cores,
        ]
        return list(map(str, flags))

    def _calc_arrival_rates(self) -> list[float]:
        """Calculate arrival rates for each partition, given a total rate."""
        total_weight = sum(self.ar_weights)
        return [
            self.total_arrival_rate * weight / total_weight
            for weight in self.ar_weights
        ]


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
        SchedSpec(
            name="FIFO",
            flags=[
                "--scheduler=FIFO",
                "--scheduler_runtime=0",
                "--enforce_deadlines",
                "--scheduler_plan_ahead_no_consideration_gap=1",
            ]
        ),
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
    ]
}
