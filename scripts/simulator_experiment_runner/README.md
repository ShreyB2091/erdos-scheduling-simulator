# Simulator Experiment Runner

## Overview

This script is designed to run scheduling experiments using the Erdos Scheduling Simulator. It reads a YAML configuration file, generates experiment combinations based on the provided matrix, and executes them in parallel.

## Usage

Run the script using Python:

```bash
python -m scripts.simulator_experiment_runner --config <path_to_config> --output-dir <path_to_output_dir> [--num-workers <num>] [--dry-run]
```

### Arguments

- `--config`: Path to the YAML configuration file defining the experiment setup.
- `--output-dir`: Path to the directory where results will be stored.
- `--num-workers`: Number of parallel workers to run experiments (default: 5).
- `--dry-run`: If specified, the script will print the experiment configurations without executing them.

### Example Command

```bash
python -m scripts.simulator_experiment_runner --config sample_experiment.yaml --output-dir results --num-workers 10
```

### Configuration File

The configuration file is a YAML file that defines the experiment setup. Below is an example configuration file:

```yaml
# Extracted experiment configuration from alibaba_plot.py

# Base flags that apply to all experiments
base_flags:
    # Worker config
    worker_profile_path: "profiles/workers/alibaba_cluster_final.yaml"

    # Workload config
    workload_profile_paths:
        - "traces/alibaba-cluster-trace-v2018/easy_dag_sukrit_10k.pkl"
        - "traces/alibaba-cluster-trace-v2018/medium_dag_sukrit_10k.pkl"
        - "traces/alibaba-cluster-trace-v2018/hard_dag_sukrit_10k.pkl"
    workload_profile_path_labels: ["easy", "medium", "hard"]

    # Loader config
    execution_mode: "replay"
    replay_trace: "alibaba"
    alibaba_loader_task_cpu_usage_random: true
    alibaba_loader_task_cpu_multiplier: 1
    alibaba_loader_task_cpu_usage_min: 120
    alibaba_loader_task_cpu_usage_max: 1500
    alibaba_loader_min_critical_path_runtimes: [200, 500, 600]
    alibaba_loader_max_critical_path_runtimes: [500, 1000, 1000]

    override_release_policies: ["poisson", "poisson", "poisson"]
    randomize_start_time_max: 50
    min_deadline: 5
    max_deadline: 500
    min_deadline_variances: [25, 50, 10]
    max_deadline_variances: [50, 100, 25]
    enforce_deadlines: true
    random_seed: 420665456

    # Scheduler runtime set to zero
    scheduler_runtime: 0

# Different schedulers to test
schedulers:
    - name: "DSched"
      flags:
          scheduler: "TetriSched"
          release_taskgraphs: true
          opt_passes:
              [
                  "CRITICAL_PATH_PASS",
                  "CAPACITY_CONSTRAINT_PURGE_PASS",
                  "DYNAMIC_DISCRETIZATION_PASS",
              ]
          retract_schedules: true
          scheduler_max_occupancy_threshold: 0.999
          finer_discretization_at_prev_solution: true
          scheduler_reconsideration_period: 0.9
          scheduler_time_discretization: 1
          scheduler_max_time_discretization: 5
          finer_discretization_window: 5
          scheduler_plan_ahead_no_consideration_gap: 5
          drop_skipped_tasks: true
          scheduler_time_limit: 120

# Matrix of parameters to vary across experiments
matrix:
    # Arrival rate pairs (medium, hard) - these are the key experimental parameters
    # Each inner list represents arrival rates for [easy, medium, hard] workloads
    override_poisson_arrival_rates:
        - [0, 0.01, 0.0385]

    # Number of invocations for [easy, medium, hard] workloads
    override_num_invocations:
        - [0, 1, 1]

# Naming template for experiments
naming: "{scheduler}+arrival={override_poisson_arrival_rates}+inv={override_num_invocations}"

# Additonal metadata
multi_enum_flags:
    - opt_passes
```

The only funky thing to keep in mind is multi enum flags (like `--opt-passes`). You will need to explicitly mark such flags as multi enum under the `multi_enum_flags` section.

### Output

The script generates a CSV file (`results.csv`) in the specified output directory, containing the results of all experiments. Each row corresponds to an experiment, including its configuration and results.

### Dry Run Mode

Use the `--dry-run` flag to preview the experiment configurations without executing them. This is useful for validating the setup before running the experiments.

### Error Handling

The script includes robust error handling:
- If the output directory already exists, the user is prompted to delete it or abort the operation.
- Invalid configurations or unexpected errors are logged with detailed traceback information.

### Debugging

In case of unexpected failures, the script starts a `pdb` debug session to help identify the root cause.

### Notes

- Ensure the YAML configuration file is properly formatted and includes all required fields.
- The script supports multi-enum flags, allowing complex configurations for schedulers.

For more details, refer to the script source code in `__main__.py`.