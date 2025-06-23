import re
import pandas as pd
import sys

from pathlib import Path

def parse_log_line(line):
    # # Extract solver time from the beginning part of the log line
    # solver_time_match = re.search(r'took (\d+).*s to solve', line)
    # solver_time = float(solver_time_match.group(1))/1e6 if solver_time_match else None

    # Extract the detailed metrics
    metrics = {
        'numVariables': None,
        'numCachedVariables': None,
        'numUncachedVariables': None,
        'numConstraints': None,
        'solverTimeMicroseconds': None,
    }
    
    for key in metrics.keys():
        match = re.search(rf'{key}=(\d+)', line)
        metrics[key] = int(match.group(1)) if match else None
    
    # Create record
    record = {
        'solver_time_s': metrics['solverTimeMicroseconds']/1e6,
        'num_variables': metrics['numVariables'],
        'num_cached_variables': metrics['numCachedVariables'],
        'num_uncached_variables': metrics['numUncachedVariables'],
        'num_constraints': metrics['numConstraints']
    }
    
    return record

def process_logs(file_path=None):
    records = []
    
    # Read from stdin if no file is specified
    lines = sys.stdin if file_path is None else open(file_path, 'r', encoding='latin-1')
    
    try:
        for line in lines:
            if 'TetriSchedScheduler INFO' in line and 'SolverSolution' in line:
                record = parse_log_line(line)
                records.append(record)
    finally:
        if file_path is not None and lines != sys.stdin:
            lines.close()
    
    # Convert to pandas DataFrame
    df = pd.DataFrame(records)
    return df


def parse_label(file):
    try:
        file = Path(file)
        sched = file.parts[-3]
        arrival_rate = sum(map(float, file.parts[-2].split("::")[1].split(":")))
        return sched, arrival_rate
    except:
        return file, None


if __name__ == "__main__":
    # Example log line for testing
    example_log = """2025-04-12,04:39:41.005 TetriSchedScheduler INFO: [21] Solver returned utility of 2.0 and took 52512µs to solve. The solution result was SolverSolution<type=FEASIBLE, numVariables=12720, numCachedVariables=700, numUncachedVariables=12020, numConstraints=6470, numDeactivatedConstraints=400, numNonZeroCoefficients=31828, numSolutions=1, objectiveValue=2.000000, objectiveValueBound=2.000000, solverTimeMicroseconds=29925>."""
    
    # If command line argument is provided, use it as file path
    if len(sys.argv) > 1:
        files = sys.argv[1:]
        labels = sorted([(*parse_label(file), file) for file in files], key=lambda x: (x[0], x[1]))
        for (sched, ar, file) in labels:
            df = process_logs(file)
            df['complexity'] = df['num_uncached_variables'] + df['num_constraints']
            print(sched, ar)
            print(df.describe())
            print("---")
    else:
        # For demonstration, use the example log
        record = parse_log_line(example_log)
        df = pd.DataFrame([record])
        print(df)
    
    # Optional: Save to CSV
    # df.to_csv('solver_metrics.csv', index=False)
