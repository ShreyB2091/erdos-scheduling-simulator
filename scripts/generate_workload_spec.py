"""Generate a JSON workload specification for TPC-H queries.

The generated spec can be used by TpchWorkloadLoader and
launch_tpch_queries.py, in order to provide a single source of truth
for the workload, preventing issues where the service and simulator
generate different workloads given the same arguments.

The workload spec is a JSON object with keys:
- dataset_size: Dataset size per query in GB
- max_cores: Maximum executor cores
- queries: array of objects with keys:
  - query_number: Which query to execute
  - release_time: Time in seconds when the query is released
  - deadline: Deadline in seconds after the query is made
The queries list should be sorted by release time in ascending order.

The possible TPC-H queries are partitioned, and queries in each subset
are released according to the Poisson distribution at a given arrival
rate.
"""

import argparse
from pathlib import Path
from itertools import islice
from heapq import merge
from collections.abc import Generator
import json
import logging

from data.tpch_loader import TpchLoader
from utils import EventTime
import numpy.random


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--partitioning-scheme",
        required=True,
        type=lambda x: [list(int(q) for q in b.split(",")) for b in x.split(":")],
        help="Scheme for partitioning TPC-H queries into buckets.  Format is a "
        "colon separated list of comma separated integers.",
    )
    parser.add_argument(
        "--num-queries",
        required=True,
        type=int,
        help="The number of queries to generate in the workload.",
    )
    parser.add_argument(
        "--arrival-rates",
        required=True,
        type=float,
        nargs="+",
        help="Arrival rates for each type of query.",
    )
    parser.add_argument(
        "--dataset-size",
        required=True,
        type=int,
        choices=(50, 100, 250, 500),
        help="Dataset size per query in GB",
    )
    parser.add_argument(
        "--max-cores",
        required=True,
        type=int,
        choices=(50, 75, 100, 200),
        help="Maximum executor cores per query",
    )
    parser.add_argument(
        "--deadline-variance",
        type=float,
        nargs=2,
        default=[10, 25],
        help="Percent variance to fuzz the generated deadlines by",
    )
    parser.add_argument(
        "--min-task-runtime",
        type=int,
        default=12,
        help="Minimum runtime of a TPC-H task",
    )
    parser.add_argument(
        "--tpch-query-dag-spec",
        type=Path,
        default=Path("profiles/workload/tpch/queries.yaml"),
        help="Path to a YAML file specifying the TPC-H query DAGs",
    )
    parser.add_argument(
        "--profile-type",
        type=str,
        choices=("Decima", "Cloudlab"),
        default="Cloudlab",
        help="Type of TPC-H profile the data loader must use",
    )
    parser.add_argument(
        "--random-seed",
        type=int,
        help="Seed for the RNG",
    )
    return parser.parse_args()


# This duplicates code in JobGraph.ReleasePolicy.get_release_times,
# but is a generator instead of returning a list; it would be
# preferable to refactor that method to be a generator instead.
def poisson_release_times(
        rng: numpy.random.Generator,
        arrival_rate: float,
) -> Generator[EventTime]:
    """Generate a sequence of Poisson-distributed EventTimes.

    Arguments:
    rng -- the random number generator to use for inter-arrival times
    arrival_rate -- the average number of events per second
    """
    cur = EventTime.zero()
    while True:
        inter_arrival_time = rng.poisson(1/arrival_rate)
        cur += EventTime(int(inter_arrival_time), EventTime.Unit.US)
        yield cur


def queries(
        rng: numpy.random.Generator,
        arrival_rate: float,
        completion_times: dict[int, EventTime],
        query_numbers: list[int],
        deadline_variance: tuple[int, int],
) -> Generator[dict]:
    """Generate a sequence of Poisson-distributed query objects.

    Arguments:
    rng -- the random number generator to use for inter-arrival times
    arrival_rate -- the average number of events per second
    completion_times -- a dictionary of completion times for each query number
    query_numbers -- the possible query numbers to sample from
    deadline_variance -- the min and max factors to fuzz the deadline by
    """
    release_times = poisson_release_times(rng, arrival_rate)
    while True:
        release_time = next(release_times)
        query_number = int(rng.choice(query_numbers))
        completion_time = completion_times[query_number]
        deadline = completion_time.fuzz(deadline_variance, rng=rng)
        yield {
            "query_number": query_number,
            "release_time": release_time.time,
            "deadline": deadline.time,
        }


args = parse_args()
# For simplicity, we use a single global RNG throughout the program.
rng = numpy.random.default_rng(seed=args.random_seed)

# TpchLoader generates a lot of debug logging messages---those aren't
# necessary here.
logging.disable(logging.INFO)

loader = TpchLoader(path=args.tpch_query_dag_spec, flags=None)
# Completion times for each TPC-H task graph according to the given profile
completion_times = {
    query_number: loader.make_job_graph(
        id="",                  # doesn't really matter
        query_num=query_number,
        profile_type=args.profile_type,
        dataset_size=args.dataset_size,
        max_executors_per_job=args.max_cores,
        min_task_runtime=args.min_task_runtime,
    )[0].completion_time
    for query_number in range(1, loader.num_queries+1)
}

assert len(args.partitioning_scheme) == len(args.arrival_rates)
# A list of iterators, one per subset in the partitioning scheme,
# which return query objects of increasing release times according to
# the given arrival rates.
query_generators = [
    queries(rng, arrival_rate, completion_times, query_numbers, args.deadline_variance)
    for (query_numbers, arrival_rate) in zip(args.partitioning_scheme, args.arrival_rates)
]

print(json.dumps({
    "dataset_size": args.dataset_size,
    "max_cores": args.max_cores,
    "workload": list(islice(
        merge(*query_generators, key=lambda q: q["release_time"]),
        args.num_queries,
    )),
}))
