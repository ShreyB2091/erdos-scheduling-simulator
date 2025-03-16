from dataclasses import dataclass


@dataclass
class SchedSpec:
    name: str
    flags: list[str]


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
