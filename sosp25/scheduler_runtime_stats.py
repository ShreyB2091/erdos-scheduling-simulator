import pandas as pd
from tabulate import tabulate
import sys

def parse_result(path):
    with open(path, "r") as f:
        data = f.readlines()
    return data

def parse_slack(data):
    slacks=[]
    for row in data:
        parts = row.split(",")
        if parts[1] == "TASK_GRAPH_RELEASE":
            release_time = float(parts[2])
            deadline = float(parts[3])
            critical_path_runtime = float(parts[-1])
            slacks.append(deadline - (release_time + critical_path_runtime))
    return slacks

def parse_scheduler_runtimes(data, label):
    times=[]
    for row in data:
        parts = row.split(",")
        if parts[1] == "SCHEDULER_FINISHED" and (int(parts[3]) != 0 or int(parts[4]) != 0):
            times.append(float(parts[-1])/1e6)
    return times

def parse_slo(data):
    data = list(reversed(data))
    last_event = data[0].split(",")
    if last_event[1].strip() != 'SIMULATOR_END':
        return None
    slo = None
    for line in data:
        parts = line.split(",")
        if len(parts) < 1:
            break
        if parts[1] == "LOG_STATS":
            finished = float(parts[5])
            cancelled = float(parts[6])
            missed = float(parts[7])
            slo = (finished - missed) / (finished + cancelled) * 100
            slo = float(parts[8])
            break
    return slo

if __name__ == "__main__":
    results = []
    for path in sys.argv[1:]:
        data = parse_result(path)
        slo = parse_slo(data)
        times = parse_scheduler_runtimes(data, path)
        slacks = parse_slack(data)
        if len(times) == 0:
            results.append((path, 0, 0, 0))
        else:
            results.append((path, slo,sum(times)/len(times), max(times), min(times)))
    results = sorted(results, key=lambda x: x[0])
    print(tabulate(results, headers=['path', 'slo', 'avg (s)', 'max (s)', 'min (s)']))
