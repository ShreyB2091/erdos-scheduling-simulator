# Comprehensive Workload Profile Parameters

This document provides a complete reference for all tunable parameters in workload YAML files for the ERDOS Scheduling Simulator.

## Table of Contents
1. [File Structure](#file-structure)
2. [Profiles Section](#profiles-section)
3. [Graphs Section](#graphs-section)
4. [Release Policies](#release-policies)
5. [Job Node Parameters](#job-node-parameters)
6. [Resource Specification](#resource-specification)
7. [Command-Line Overrides](#command-line-overrides)
8. [Examples](#examples)

---

## File Structure

A workload YAML file consists of two main sections:

```yaml
profiles:
  - [array of work profiles]

graphs:
  - [array of job graphs]
```

---

## Profiles Section

Work profiles define reusable execution characteristics that can be referenced by jobs.

### WorkProfile Parameters

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `name` | string | **Yes** | N/A | Unique identifier for the profile |
| `loading_strategies` | array | No | `[]` | Strategies for loading this profile onto workers |
| `execution_strategies` | array | No | `[]` | Strategies for executing tasks with this profile |

### ExecutionStrategy / LoadingStrategy Parameters

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `runtime` | int | No | `0` | Execution/loading time in microseconds |
| `batch_size` | int | No | `1` | Number of items processed together |
| `resource_requirements` | dict | No | `{}` | Resource requirements (see [Resource Specification](#resource-specification)) |

### Example Profile

```yaml
profiles:
  - name: MyProfile
    loading_strategies:
      - runtime: 5000
        batch_size: 1
        resource_requirements:
          CPU:any: 2
    execution_strategies:
      - runtime: 1000
        batch_size: 1
        resource_requirements:
          GPU:any: 1
          Slot_1:any: 4
      - runtime: 2000
        batch_size: 1
        resource_requirements:
          CPU:any: 4
```

---

## Graphs Section

Job graphs define task DAGs and their release patterns.

### JobGraph Parameters

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `name` | string | **Yes** | N/A | Unique identifier for the job graph |
| `release_policy` | string | **Yes** | N/A | One of: `periodic`, `fixed`, `poisson`, `gamma`, `closed_loop` |
| `graph` | array | **Yes** | N/A | Array of job nodes (see [Job Node Parameters](#job-node-parameters)) |
| `start` | int | No | `0` | Start time in microseconds |
| `deadline_variance` | array | No | `[0, 0]` | `[min, max]` variance added to deadlines (microseconds) |

### Policy-Specific Parameters

Additional parameters required based on `release_policy`:

#### Periodic (`release_policy: periodic`)
| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `period` | int | **Yes** | Period between job arrivals (microseconds) |

#### Fixed (`release_policy: fixed`)
| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `period` | int | **Yes** | Period between job arrivals (microseconds) |
| `invocations` | int | **Yes** | Total number of job instances to release |

#### Poisson (`release_policy: poisson`)
| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `rate` | float | **Yes** | Arrival rate (jobs per microsecond) |
| `invocations` | int | **Yes** | Total number of job arrivals |

#### Gamma (`release_policy: gamma`)
| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `rate` | float | **Yes** | Arrival rate (jobs per microsecond) |
| `coefficient` | float | **Yes** | Gamma distribution shape parameter (coefficient of variation) |
| `invocations` | int | **Yes** | Total number of job arrivals |

#### Closed Loop (`release_policy: closed_loop`)
| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `concurrency` | int | **Yes** | Maximum number of concurrent job instances |
| `invocations` | int | **Yes** | Total number of job instances to release |

---

## Job Node Parameters

Each job node in the `graph` array can have the following parameters:

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `name` | string | **Yes** | N/A | Unique job identifier within this graph |
| `work_profile` | string | No | `null` | Reference to a profile name from the profiles section |
| `slo` | int | No | N/A | Service Level Objective (deadline) in microseconds |
| `children` | array | No | `[]` | Array of child job names (defines DAG edges) |
| `conditional` | bool | No | `false` | Whether this job is a conditional branch |
| `probability` | float | No | `1.0` | Execution probability (0.0 to 1.0) for conditional jobs |
| `terminal` | bool | No | `false` | Whether this is a terminal/sink node |

### Job Node Example

```yaml
graph:
  - name: RootTask
    work_profile: MyProfile
    slo: 5000
    children: ["ChildTask1", "ChildTask2"]
  
  - name: ChildTask1
    work_profile: FastProfile
    conditional: true
    probability: 0.7
    children: ["MergeTask"]
  
  - name: ChildTask2
    work_profile: SlowProfile
    children: ["MergeTask"]
  
  - name: MergeTask
    work_profile: FinalProfile
    terminal: true
```

---

## Resource Specification

Resources are specified as key-value pairs in the `resource_requirements` dictionary.

### Format

```
"ResourceType:ResourceID": quantity
```

- **ResourceType**: The type of resource (e.g., `CPU`, `GPU`, `Memory`, `Slot_1`, `Slot_2`)
- **ResourceID**: Specific worker ID or `any` for any worker
- **quantity**: Integer quantity required

### Examples

```yaml
resource_requirements:
  Slot_1:any: 4          # 4 units of Slot_1 on any worker
  CPU:worker1: 2         # 2 CPUs specifically on worker1
  GPU:any: 1             # 1 GPU on any worker
  Memory:any: 1024       # 1024 memory units on any worker
```

### Common Resource Types

- `Slot_N`: Generic slot resources (N = 1, 2, 3, ...)
- `CPU`: CPU cores
- `GPU`: GPU devices
- `Memory`: Memory units
- Custom types can be defined based on your worker configuration

---

## Command-Line Overrides

The following command-line flags can override YAML-specified values:

| Flag | Type | Description | Applies To |
|------|------|-------------|------------|
| `--override_poisson_arrival_rate` | float | Override arrival rate | Poisson, Gamma policies |
| `--override_gamma_coefficient` | float | Override gamma coefficient | Gamma policy |
| `--override_arrival_period` | int | Override period | Periodic, Fixed policies |
| `--override_num_invocation` | int | Override invocations count | Fixed, Poisson, Gamma, Closed Loop |
| `--override_slo` | int | Override SLO for all jobs | All jobs |
| `--unique_work_profiles` | bool | Don't deepcopy profiles per graph | All profiles |
| `--replication_factor` | int | Replicate each job graph N times | All graphs |

### Example Usage

```bash
python main.py \
  --workload_profile_path=./profiles/workload/my_workload.yaml \
  --override_arrival_period=5000 \
  --override_slo=10000 \
  --replication_factor=3
```

---

## Examples

### Example 1: Simple Periodic Workload

```yaml
profiles:
  - name: SimpleTask
    execution_strategies:
      - runtime: 1000
        batch_size: 1
        resource_requirements:
          Slot_1:any: 2

graphs:
  - name: SimpleGraph
    release_policy: periodic
    period: 10000
    graph:
      - name: Task1
        work_profile: SimpleTask
        slo: 5000
```

### Example 2: DAG with Multiple Execution Strategies

```yaml
profiles:
  - name: FlexibleTask
    execution_strategies:
      # Fast strategy
      - runtime: 500
        resource_requirements:
          GPU:any: 1
      # Slow strategy  
      - runtime: 2000
        resource_requirements:
          CPU:any: 4

graphs:
  - name: DAGGraph
    release_policy: fixed
    period: 5000
    invocations: 100
    graph:
      - name: Root
        work_profile: FlexibleTask
        children: ["Child1", "Child2"]
      - name: Child1
        work_profile: FlexibleTask
      - name: Child2
        work_profile: FlexibleTask
```

### Example 3: Conditional Branching

```yaml
profiles:
  - name: BranchTask
    execution_strategies:
      - runtime: 1000
        resource_requirements:
          Slot_1:any: 1

graphs:
  - name: ConditionalGraph
    release_policy: poisson
    rate: 0.0001
    invocations: 50
    graph:
      - name: Root
        work_profile: BranchTask
        children: ["BranchA", "BranchB"]
      
      - name: BranchA
        work_profile: BranchTask
        conditional: true
        probability: 0.6
        children: ["Merge"]
      
      - name: BranchB
        work_profile: BranchTask
        conditional: true
        probability: 0.4
        children: ["Merge"]
      
      - name: Merge
        work_profile: BranchTask
```

### Example 4: Complex Multi-Resource Profile

```yaml
profiles:
  - name: MLInference
    loading_strategies:
      - runtime: 10000  # 10ms to load model
        resource_requirements:
          Memory:any: 2048
          CPU:any: 2
    
    execution_strategies:
      # GPU inference
      - runtime: 500
        batch_size: 4
        resource_requirements:
          GPU:any: 1
          Memory:any: 512
      
      # CPU inference
      - runtime: 3000
        batch_size: 1
        resource_requirements:
          CPU:any: 8
          Memory:any: 256

graphs:
  - name: MLPipeline
    release_policy: closed_loop
    concurrency: 10
    invocations: 1000
    deadline_variance: [0, 2000]
    graph:
      - name: Preprocess
        work_profile: MLInference
        slo: 15000
        children: ["Inference"]
      
      - name: Inference
        work_profile: MLInference
        slo: 5000
        children: ["Postprocess"]
      
      - name: Postprocess
        work_profile: MLInference
        slo: 2000
```

---

## Validation Rules

1. **Profile Names**: Must be unique across all profiles
2. **Job Names**: Must be unique within a job graph
3. **Children References**: Must reference existing job names in the same graph
4. **Work Profile References**: Must reference existing profile names
5. **Probability**: Must be between 0.0 and 1.0
6. **Time Values**: Must be non-negative integers (microseconds)
7. **Resource Format**: Must follow "Type:ID" format with integer quantities

---

## Best Practices

1. **Profile Reusability**: Define profiles for common task types and reuse them
2. **Multiple Strategies**: Provide multiple execution strategies to give the scheduler flexibility
3. **Resource Specificity**: Use `:any` for flexible placement, specific IDs for affinity
4. **Deadline Variance**: Use for realistic workload modeling with variable deadlines
5. **Conditional Jobs**: Model probabilistic branches in data processing pipelines
6. **Start Times**: Stagger graph start times to avoid initial burst
7. **Batch Sizes**: Use larger batch sizes for tasks that can process multiple items efficiently

---

## Troubleshooting

### Common Errors

1. **"A name was not defined for the JobGraph"**: Add `name` field to each graph
2. **"The key 'name' was not found in the WorkProfile"**: Add `name` field to each profile
3. **"Child X was not present in the graph"**: Ensure all children references exist
4. **"Empty Workload generated"**: Check file has both `profiles` and `graphs` sections
5. **"Unsupported extension"**: Use `.yaml` or `.yml` file extension

---

## Related Files

- **Worker Configuration**: `profiles/workers/*.yaml` - Defines available workers and resources
- **Config Files**: `configs/*.conf` - Simulator configuration including scheduler parameters
- **Main Entry**: `main.py` - Accepts `--workload_profile_path` flag

---

## See Also

- [workload_loader.py](../../data/workload_loader.py) - Implementation reference
- [comprehensive_example.yaml](./comprehensive_example.yaml) - Complete example with all parameters

