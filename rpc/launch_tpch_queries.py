import argparse
import os
import random
import subprocess
import sys
import time
import json
import numpy as np

from pathlib import Path

from workload import JobGraph
from utils import EventTime
from data.tpch_loader import make_release_policy
from rpc import erdos_scheduler_pb2
from rpc import erdos_scheduler_pb2_grpc

import grpc


def map_dataset_to_deadline(dataset_size):
    # 50gb => 2mins, 100gb => 6mins, 250gb => 12mins, 500gb => 24mins
    mapping = {"50": 120, "100": 360, "250": 720, "500": 1440}
    return mapping.get(dataset_size, 120)  # Default to 120s if dataset size is NA


def launch_query(query_number, deadline, dataset_size, max_cores, args):
    spark_deadline = map_dataset_to_deadline(args.dataset_size)

    cmd = [
        f"{args.spark_mirror_path.resolve()}/bin/spark-submit",
        *("--deploy-mode", "cluster"),
        *("--master", f"spark://{args.spark_master_ip}:7077"),
        *("--conf", "'spark.port.maxRetries=132'"),
        *("--conf", "'spark.eventLog.enabled=true'"),
        *("--conf", f"'spark.eventLog.dir={args.spark_eventlog_dir.resolve()}'"),
        *("--conf", "'spark.sql.adaptive.enabled=false'"),
        *("--conf", "'spark.sql.adaptive.coalescePartitions.enabled=false'"),
        *("--conf", "'spark.sql.autoBroadcastJoinThreshold=-1'"),
        *("--conf", "'spark.sql.shuffle.partitions=1'"),
        *("--conf", "'spark.sql.files.minPartitionNum=1'"),
        *("--conf", "'spark.sql.files.maxPartitionNum=1'"),
        *("--conf", f"'spark.app.deadline={spark_deadline}'"),
        *("--class", "'main.scala.TpchQuery'"),
        f"{args.tpch_spark_path.resolve()}/target/scala-2.13/spark-tpc-h-queries_2.13-1.0.jar",
        f"{query_number}",
        f"{deadline}",
        f"{dataset_size}",
        f"{max_cores}",
    ]

    # print(
    #     f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] Launching Query: {query_number}, "
    #     f"dataset: {args.dataset_size}GB, deadline: {deadline}s, maxCores: {args.max_cores}"
    # )

    try:
        cmd = " ".join(cmd)
        print("Launching:", cmd)
        p = subprocess.Popen(
            cmd,
            shell=True,
        )
        print("Query launched successfully.")
        return p
    except Exception as e:
        print(f"Error launching query: {e}")


def main():
    parser = argparse.ArgumentParser(
        description="Generate a workload of queries based on distribution type."
    )
    parser.add_argument(
        "--spark-mirror-path",
        type=Path,
        required=True,
        help="Path to spark-mirror repository",
    )
    parser.add_argument(
        "--spark-master-ip",
        type=str,
        required=True,
        help="IP address of node running Spark master",
    )
    parser.add_argument(
        "--tpch-spark-path",
        type=Path,
        required=True,
        help="Path to TPC-H Spark repository",
    )
    parser.add_argument(
        "--spark-eventlog-dir",
        default=Path(os.getcwd()) / "spark-eventlog",
        type=Path,
        help="Path to directory in which to Spark event logs will be dumped",
    )
    parser.add_argument(
        "--workload-spec",
        type=Path,
        help="JSON file specifying the workload to launch"
    )

    args = parser.parse_args()

    if not args.spark_eventlog_dir.exists():
        args.spark_eventlog_dir.mkdir(parents=True)

    os.environ["TPCH_INPUT_DATA_DIR"] = str(args.tpch_spark_path.resolve() / "dbgen")

    with open(args.workload_spec) as f:
        workload_spec = json.load(f)

    # Format of the workload spec: JSON object with keys
    # - dataset_size: Dataset size per query in GB
    # - max_cores: Maximum executor cores
    # - queries: array of objects with keys
    #   - query_number: Which query to execute
    #   - release_time: Time in seconds when the query is released
    #   - deadline: Deadline in seconds after the query is made
    # The queries list should be sorted by release times in ascending order.

    release_times = [q["release_time"] for q in workload_spec.queries]

    # Launch queries
    ps = []
    inter_arrival_times = [release_times[0].time]
    for i in range(len(release_times) - 1):
        inter_arrival_times.append(release_times[i + 1].time - release_times[i].time)
    for i, inter_arrival_time in enumerate(inter_arrival_times):
        time.sleep(inter_arrival_time)
        query = workload_spec.queries[i]
        ps.append(launch_query(
            query_number=query.query_number,
            deadline=query.deadline,
            dataset_size=workload_spec.dataset_size,
            max_cores=workload_spec.max_cores,
            args,
        ))
        print(
            f"({i+1}/{len(release_times)})",
            "Current time: ",
            time.strftime("%Y-%m-%d %H:%M:%S"),
            " launching query: ",
            query.query_number,
        )

    for p in ps:
        p.wait()

    # Wait for some time before sending the shutdown signal
    time.sleep(20)

    channel = grpc.insecure_channel("localhost:50051")
    stub = erdos_scheduler_pb2_grpc.SchedulerServiceStub(channel)
    response = stub.Shutdown(erdos_scheduler_pb2.Empty())
    channel.close()
    print("Sent shutdown signal to the service")


if __name__ == "__main__":
    main()
