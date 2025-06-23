import re
from dataclasses import dataclass
from collections import defaultdict
from functools import cached_property
from pathlib import Path


from data.csv_reader import CSVReader


class Result:
    def __init__(self, output_dir: Path):
        self.output_dir = output_dir
        self.conf_file = list(output_dir.glob('*.conf'))[0]

        # For backwards compatibility, be conservative and ignore files that start with 'tetrisched_' or 'libtetrisched_'
        self.log_file = [path for path in output_dir.glob("*.log") if not path.name.startswith("tetrisched_")][0]
        self.csv_file = [path for path in output_dir.glob("*.csv") if not path.name.startswith("libtetrisched_")][0]

        self.config = self._parse_config_file(self.conf_file)
        self.events = self._parse_csv_file(self.csv_file)
        self.solver_stats = self._parse_solver_stats(self.log_file)

        # LEGACY: for cluster utilization
        self.csv_reader = CSVReader([self.csv_file])

    @cached_property
    def num_invocations(self):
        return list(map(int, self.config['--override_num_invocations'][0].split(',')))

    @cached_property
    def slo(self):
        parts = self.last_log_stats_line()
        if parts is None:
            return 0.0
        finished = float(parts[5])
        cancelled = float(parts[6])
        missed = float(parts[7])
        slo = (finished - missed) / (finished + cancelled) * 100
        slo = float(parts[8])
        return slo * 100

    def last_log_stats_line(self):
        row = None
        for parts in reversed(self.events):
            if len(parts) < 1:
                break
            if parts[1] == "LOG_STATS":
                row = parts
                break
        return row

    @cached_property
    def cluster_utilization(self):
        tasks = self.csv_reader.get_tasks(self.csv_file)
        task_graphs = self.csv_reader.get_task_graph(self.csv_file)
        resource_utilization = {}
        simulator_end_time = 0
        for task in tasks:
            if task.task_graph not in task_graphs:
                raise ValueError(f"Graph {task.task_graph} not found in {self.csv_file}.")
            task_graph = task_graphs[task.task_graph]
            is_task_good = task_graph.was_completed and not task_graph.missed_deadline
            for placement in task.placements:
                for resource in placement.resources_used:
                    if resource.name not in resource_utilization:
                        # The format is (good, bad) utilization.
                        resource_utilization[resource.name] = defaultdict(
                            lambda: [0, 0]
                        )

                    for t in range(placement.placement_time, placement.completion_time):
                        if is_task_good:
                            resource_utilization[resource.name][t][
                                0
                            ] += resource.quantity
                        else:
                            resource_utilization[resource.name][t][
                                1
                            ] += resource.quantity
                        if t > simulator_end_time:
                            simulator_end_time = t
        result = {}
        for resource in resource_utilization.keys():
            worker_pools = self.csv_reader.get_worker_pools(self.csv_file)
            max_resource_available = 0
            for worker_pool in worker_pools:
                for wp_resource in worker_pool.resources:
                    if resource == wp_resource.name:
                        max_resource_available += wp_resource.quantity
            usage_map = []

            total_good_utilization = 0
            total_utilization = 0
            for t in range(0, simulator_end_time):
                if t in resource_utilization[resource]:
                    good_utilization = resource_utilization[resource][t][0]
                    total_good_utilization += good_utilization
                    bad_utilization = resource_utilization[resource][t][1]
                    total_utilization += good_utilization + bad_utilization
                    usage_map.append((good_utilization, bad_utilization))
                else:
                    usage_map.append((0, 0))


            avg_effective_cluster_utilization = (
                total_good_utilization / (max_resource_available * simulator_end_time)
            )

            avg_total_cluster_utilization = (
                total_utilization / (max_resource_available * simulator_end_time)
            )

            result[resource] = {
                'eff': avg_effective_cluster_utilization,
                'tot': avg_total_cluster_utilization,
                'series': usage_map,
            }
        return result


    @cached_property
    def scheduler_runtimes(self):
        times=[]
        for parts in self.events:
            if parts[1] == "SCHEDULER_FINISHED" and (int(parts[3]) != 0 or int(parts[4]) != 0):
                times.append(float(parts[-1])/1e6)
        return times

    @cached_property
    def num_constraints(self):
        return [x['num_constraints'] for x in self.solver_stats]

    @cached_property
    def solver_times(self):
        times = [x['solver_time_s'] for x in self.solver_stats if x['solver_time_s'] is not None]
        return times

    @cached_property
    def arrival_rate(self):
        value = self.config["--override_poisson_arrival_rates"][0]
        parts = list(map(float, value.split(",")))
        return parts, sum(parts)

    def _parse_config_file(self, config_file: Path):
        config = defaultdict(list)
        with open(config_file, 'r') as f:
            data = f.readlines()

        for line in data:
            parts = line.strip().split("=")
            if len(parts) == 1:
                config[parts[0]].append(True)
            elif len(parts) == 2:
                config[parts[0]].append(parts[1])
            else:
                config[parts[0]] = ",".join(parts[1:])

        return config

    def _parse_csv_file(self, csv_file: Path):
        with open(csv_file, 'r') as f:
            data = f.readlines()

        for i, line in enumerate(data):
            parts = line.split(",")
            if parts[0].isdigit():
                break

        return [row.strip().split(",") for row in data[i:]]

    def _parse_solver_stats(self, log_file: Path):
        with open(log_file, 'r', encoding='latin-1') as f:
            data = f.readlines()

        def parse_log_line(line):
            # Extract solver time from the beginning part of the log line
            solver_time_match = re.search(r'took (\d+).*s to solve', line)
            solver_time = float(solver_time_match.group(1))/1e6 if solver_time_match else None

            # Extract the detailed metrics
            metrics = {
                'numVariables': None,
                'numCachedVariables': None,
                'numUncachedVariables': None,
                'numConstraints': None
            }

            for key in metrics.keys():
                match = re.search(rf'{key}=(\d+)', line)
                metrics[key] = int(match.group(1)) if match else None

            # Create record
            record = {
                'solver_time_s': solver_time,
                'num_variables': metrics['numVariables'],
                'num_cached_variables': metrics['numCachedVariables'],
                'num_uncached_variables': metrics['numUncachedVariables'],
                'num_constraints': metrics['numConstraints']
            }
            return record

        records = []
        for line in data:
            if 'TetriSchedScheduler INFO' in line and 'SolverSolution' in line:
                record = parse_log_line(line)
                records.append(record)

        return records
