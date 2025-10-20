# LLM Scheduler

One simple file to schedule LLM inference requests using TetriSched.

## Usage

```bash
# Basic usage
python llm_server/llm_scheduler.py

# With your config file flags
python llm_server/llm_scheduler.py \
  --log=./test_dsched.log \
  --csv=./test_dsched2.csv \
  --log_level=debug \
  --enforce_deadlines \
  --retract_schedules \
  --release_taskgraphs \
  --scheduler_log_to_file=True \
  --opt_passes=CRITICAL_PATH_PASS \
  --opt_passes=DYNAMIC_DISCRETIZATION_PASS

# Or use a config file directly
python llm_server/llm_scheduler.py --flagfile=configs/test_dsched.conf

# Customize LLM requests
python llm_server/llm_scheduler.py \
  --num_prefill=5 \
  --num_decode=10 \
  --prefill_runtime=30000 \
  --prefill_deadline=150000 \
  --num_gpus=8
```

## Key Flags

### LLM Request Flags
| Flag | Default | Description |
|------|---------|-------------|
| `--num_prefill` | 3 | Number of prefill requests |
| `--num_decode` | 5 | Number of decode requests |
| `--prefill_runtime` | 20000 | Prefill runtime in µs |
| `--prefill_deadline` | 100000 | Prefill deadline in µs |
| `--decode_runtime` | 10000 | Decode runtime in µs |
| `--decode_deadline` | 500000 | Decode deadline in µs |
| `--num_gpus` | 4 | Number of GPU workers |

### Scheduler Flags (from test_dsched.conf)
| Flag | Default | Description |
|------|---------|-------------|
| `--scheduler_runtime` | 0 | Scheduler runtime in µs |
| `--enforce_deadlines` | True | Enforce task deadlines |
| `--retract_schedules` | True | Allow schedule retraction |
| `--release_taskgraphs` | True | Release entire task graphs |
| `--scheduler_time_discretization` | 1 | Time discretization in µs |
| `--scheduler_log_to_file` | False | Log scheduler to file |
| `--opt_passes` | CRITICAL_PATH_PASS, DYNAMIC_DISCRETIZATION_PASS | Optimization passes |

### Logging Flags
| Flag | Default | Description |
|------|---------|-------------|
| `--log` | None | Log file path |
| `--csv` | None | CSV file path |
| `--log_level` | debug | Logging level |

## Using Config File

You can use your existing config file:

```bash
python llm_server/llm_scheduler.py \
  --flagfile=configs/test_dsched.conf \
  --num_prefill=5 \
  --num_decode=10
```

Note: Command-line flags override config file values.

