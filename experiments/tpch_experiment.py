from dataclasses import dataclass

from .experiment_spec import Experiment


def partitioning_scheme(dataset_size: int, max_cores: int) -> str:
    """Return the preferred TPC-H partitioning scheme for a given config."""

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
class TpchExperiment(Experiment):
    """Specification for a single TPC-H experiment.

    This experiment spec always partitions queries into easy, medium,
    and hard buckets.  The assignment of each query into these buckets
    is determined by the dataset size and maximum number of cores.
    Arrival rates for each bucket are specified separately.

    """

    deadline_variance: tuple[int, int]
    ar_weights: list[float]
    num_invocations: int
    max_cores: int
    dataset_size: int
    total_arrival_rate: float

    def workload_spec_flags(self) -> list[str]:
        """Return the flags to pass to generate_workload_spec."""

        # Calculate arrival rates for each bucket.
        arrival_rates = (
            weight * self.total_arrival_rate / sum(self.ar_weights)
            for weight in self.ar_weights
        )

        partitioning = partitioning_scheme(self.dataset_size, self.max_cores)
        flags = [
            "--partitioning-scheme", partitioning,
            "--num-queries", self.num_invocations,
            "--arrival-rates", *arrival_rates,
            "--dataset-size", self.dataset_size,
            "--max-cores", self.max_cores,
            "--deadline-variance", *self.deadline_variance,
            "--min-task-runtime", 12,
            "--tpch-query-dag-spec", "profiles/workload/tpch/queries.yaml",
            "--profile-type", "Cloudlab",
            "--random-seed", 1234,
        ]
        return list(map(str, flags))

    def _base_flags(self) -> list[str]:
        flags = [
            "--runtime_variance", "0",
            "--tpch_min_task_runtime", "12",
            "--random_seed", "1234",
            "--slo_ramp_up_clip", 10,
            "--slo_ramp_down_clip", 10,
        ]
        return list(map(str, flags))

    def sim_flags(self) -> list[str]:
        return self._base_flags() + list(map(str, [
            "--execution_mode", "replay",
            "--replay_trace", "tpch",
            "--tpch_query_dag_spec", "profiles/workload/tpch/queries.yaml",
            "--worker_profile_path", "profiles/workers/tpch_cluster.yaml",
            "--tpch_dataset_size", self.dataset_size,
            "--tpch_max_executors_per_job", self.max_cores,
        ]))

    def service_flags(self) -> list[str]:
        return self._base_flags() + ["--override_worker_cpu_count"]
