#!/usr/bin/env python3
"""
Simple script to schedule LLM inference requests using TetriSched.
Uses the same flag system as main.py.
"""
import sys
import os

# Add parent directory to path to import from main codebase
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from absl import app, flags
from utils import EventTime
from schedulers import TetriSchedScheduler
from workload import (
    Job,
    Task,
    TaskGraph,
    Workload,
    Resource,
    Resources,
    ExecutionStrategy,
    ExecutionStrategies,
    WorkProfile,
)
from workers import Worker, WorkerPool, WorkerPools

FLAGS = flags.FLAGS

# Logging flags (from test_dsched.conf)
flags.DEFINE_string("log_file_name", None, "Name of the file to log the results to.", short_name="log")
flags.DEFINE_string("log_level", "debug", "Level of logging.")
flags.DEFINE_string("csv_file_name", None, "Name of the CSV file to log results to.", short_name="csv")
flags.DEFINE_string("log_dir", "./llm_server", "Directory for logs")

# Scheduler flags (from test_dsched.conf and main.py)
flags.DEFINE_integer("scheduler_runtime", 0, "Runtime for scheduler in µs")
flags.DEFINE_bool("enforce_deadlines", True, "Enforce task deadlines")
flags.DEFINE_bool("retract_schedules", True, "Allow schedule retraction")
flags.DEFINE_bool("release_taskgraphs", True, "Release entire task graphs")
flags.DEFINE_bool("drop_skipped_tasks", False, "Drop tasks that cannot be scheduled")
flags.DEFINE_integer("scheduler_time_discretization", 1, "Time discretization in µs")
flags.DEFINE_integer("scheduler_plan_ahead", -1, "Plan ahead time in µs")
flags.DEFINE_integer("scheduler_lookahead", 0, "Scheduler lookahead in µs")
flags.DEFINE_integer("scheduler_time_limit", -1, "Time limit for scheduler in seconds")
flags.DEFINE_bool("scheduler_log_to_file", False, "Log scheduler output to file")
flags.DEFINE_list("scheduler_log_times", [], "Timestamps at which to request extra logging")
flags.DEFINE_bool("scheduler_adaptive_discretization", False, "Use adaptive discretization")
flags.DEFINE_integer("scheduler_max_time_discretization", 5, "Maximum time discretization")
flags.DEFINE_float("scheduler_max_occupancy_threshold", 0.8, "Max occupancy threshold")
flags.DEFINE_bool("finer_discretization_at_prev_solution", False, "Finer discretization at previous solution")
flags.DEFINE_integer("finer_discretization_window", 5, "Finer discretization window")
flags.DEFINE_integer("scheduler_plan_ahead_no_consideration_gap", 0, "Plan ahead no consideration gap")
flags.DEFINE_multi_enum(
    "opt_passes",
    ["CRITICAL_PATH_PASS", "DYNAMIC_DISCRETIZATION_PASS"],
    ["CRITICAL_PATH_PASS", "CAPACITY_CONSTRAINT_PURGE_PASS", "DYNAMIC_DISCRETIZATION_PASS"],
    "Optimization passes to enable"
)

# Other flags
flags.DEFINE_integer("runtime_variance", 0, "Runtime variance percentage")
flags.DEFINE_integer("num_gpus", 4, "Number of GPU workers")

# LLM-specific flags
flags.DEFINE_integer("num_prefill", 3, "Number of prefill requests")
flags.DEFINE_integer("num_decode", 5, "Number of decode requests")
flags.DEFINE_integer("prefill_runtime", 20000, "Prefill runtime in µs")
flags.DEFINE_integer("prefill_deadline", 100000, "Prefill deadline in µs")
flags.DEFINE_integer("decode_runtime", 10000, "Decode runtime in µs")
flags.DEFINE_integer("decode_deadline", 500000, "Decode deadline in µs")


def create_task(name, task_type, task_graph_name, runtime_us, deadline_us):
    """Create a Task for an LLM request."""
    job = Job(
        name=f"{task_type}_job",
        profile=WorkProfile(
            name=f"{task_type}_profile",
            execution_strategies=ExecutionStrategies(
                strategies=[
                    ExecutionStrategy(
                        resources=Resources(
                            resource_vector={Resource(name="GPU", _id="any"): 1}
                        ),
                        batch_size=1,
                        runtime=EventTime(runtime_us, EventTime.Unit.US),
                    )
                ]
            ),
        ),
    )
    
    return Task(
        name=name,
        task_graph=task_graph_name,
        job=job,
        deadline=EventTime(deadline_us, EventTime.Unit.US),
        timestamp=0,
        release_time=EventTime.zero(),
    )


def main(argv):
    """Main function - schedule LLM inference requests."""
    
    print("\n" + "=" * 70)
    print(f"Scheduling {FLAGS.num_prefill} Prefill + {FLAGS.num_decode} Decode Requests")
    print("=" * 70)
    
    # Create scheduler (same as main.py lines 865-895)
    print("\nCreating TetriSched scheduler...")
    scheduler = TetriSchedScheduler(
        preemptive=False,
        runtime=EventTime(FLAGS.scheduler_runtime, EventTime.Unit.US),
        lookahead=EventTime(FLAGS.scheduler_lookahead, EventTime.Unit.US),
        enforce_deadlines=FLAGS.enforce_deadlines,
        retract_schedules=FLAGS.retract_schedules,
        release_taskgraphs=FLAGS.release_taskgraphs,
        goal="max_goodput",
        time_discretization=EventTime(FLAGS.scheduler_time_discretization, EventTime.Unit.US),
        plan_ahead=EventTime(FLAGS.scheduler_plan_ahead, EventTime.Unit.US),
        log_to_file=FLAGS.scheduler_log_to_file,
        adaptive_discretization=FLAGS.scheduler_adaptive_discretization,
        _flags=FLAGS,
        max_time_discretization=EventTime(FLAGS.scheduler_max_time_discretization, EventTime.Unit.US),
        max_occupancy_threshold=FLAGS.scheduler_max_occupancy_threshold,
        finer_discretization_at_prev_solution=FLAGS.finer_discretization_at_prev_solution,
        finer_discretization_window=EventTime(FLAGS.finer_discretization_window, EventTime.Unit.US),
        plan_ahead_no_consideration_gap=EventTime(FLAGS.scheduler_plan_ahead_no_consideration_gap, EventTime.Unit.US),
    )
    print(f"✓ Scheduler created with opt_passes: {FLAGS.opt_passes}")
    
    # Create GPU workers
    print(f"\nCreating {FLAGS.num_gpus} GPU workers...")
    workers = []
    for i in range(FLAGS.num_gpus):
        workers.append(Worker(
            name=f"GPU_{i}",
            resources=Resources(resource_vector={Resource(name="GPU"): 1}),
        ))
    worker_pools = WorkerPools([WorkerPool(name="GPU_Pool", workers=workers)])
    print(f"✓ Created {FLAGS.num_gpus} GPUs")
    
    # Create tasks for LLM requests
    print("\nCreating tasks...")
    tasks = []
    task_graph_name = "llm_batch"
    
    # Prefill tasks
    for i in range(FLAGS.num_prefill):
        task = create_task(
            name=f"prefill_{i}",
            task_type="prefill",
            task_graph_name=task_graph_name,
            runtime_us=FLAGS.prefill_runtime,
            deadline_us=FLAGS.prefill_deadline,
        )
        tasks.append(task)
    print(f"✓ Created {FLAGS.num_prefill} prefill tasks (runtime={FLAGS.prefill_runtime}µs, deadline={FLAGS.prefill_deadline}µs)")
    
    # Decode tasks
    for i in range(FLAGS.num_decode):
        task = create_task(
            name=f"decode_{i}",
            task_type="decode",
            task_graph_name=task_graph_name,
            runtime_us=FLAGS.decode_runtime,
            deadline_us=FLAGS.decode_deadline,
        )
        tasks.append(task)
    print(f"✓ Created {FLAGS.num_decode} decode tasks (runtime={FLAGS.decode_runtime}µs, deadline={FLAGS.decode_deadline}µs)")
    
    # Create TaskGraph and Workload
    task_graph = TaskGraph(name=task_graph_name, tasks={task: [] for task in tasks})
    workload = Workload.from_task_graphs(task_graphs={task_graph_name: task_graph})
    
    # Schedule!
    print("\n" + "=" * 70)
    print("Running Scheduler...")
    print("=" * 70)
    
    placements = scheduler.schedule(
        sim_time=EventTime.zero(),
        workload=workload,
        worker_pools=worker_pools,
    )
    
    # Display results
    print(f"\n✓ Scheduling completed! {len(placements)} placements")
    print("\nResults:")
    print("-" * 70)
    for placement in placements:
        print(f"  {placement.task.name:15s} | Worker: {placement.worker_pool_id:15s} | Start: {placement.placement_time.time:8d} µs")
    print("-" * 70)
    
    # Summary
    prefill_count = len([p for p in placements if "prefill" in p.task.name])
    decode_count = len([p for p in placements if "decode" in p.task.name])
    print(f"\nScheduled: {prefill_count}/{FLAGS.num_prefill} prefill, {decode_count}/{FLAGS.num_decode} decode\n")


if __name__ == "__main__":
    app.run(main)

