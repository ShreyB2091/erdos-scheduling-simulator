# Quick Start: Running Simple LLM Inference Workload

## Files Overview

### Workload
- **`profiles/workload/llm_inference_workload.yaml`**
  - 1 prefill (5 μs) + 5 decode steps (1 μs each)
  - 10 requests, arriving every 20 μs
  - Total latency per request: 10 μs

### Workers
- **`profiles/workers/llm_simple_workers.yaml`**
  - 4 workers, each with 1 Slot_1 resource
  - Total capacity: 4 execution slots

### Config
- **`configs/llm_simple.conf`**
  - Pre-configured settings for TetriSched

---

## Run the Workload

### Option 1: Using Config File (Easiest)

```bash
cd /home/hice1/sbansal309/scratch/erdos-scheduling-simulator

python main.py --flagfile=configs/llm_simple.conf
```

### Option 2: Direct Command Line

```bash
python main.py \
  --execution_mode=benchmark \
  --workload_profile_path=./profiles/workload/llm_inference_workload.yaml \
  --worker_profile_path=./profiles/workers/llm_simple_workers.yaml \
  --scheduler=tetrisched \
  --enforce_deadlines=True \
  --release_taskgraphs=True \
  --log_level=info
```

---

## Expected Behavior

**Workload Characteristics:**
- 10 LLM requests
- Each request: 6 tasks (1 prefill + 5 decode)
- Critical path: 10 μs per request
- Arrival period: 20 μs

**Resource Requirements:**
- Each task needs 1 Slot_1 resource
- Tasks are sequential (prefill → decode1 → decode2 → decode3 → decode4 → decode5)

**With 4 Workers:**
- Can process multiple requests in parallel
- Each worker can handle one task at a time
- Should complete all requests within deadlines

---

## Modify the Setup

### Add More Workers

Edit `profiles/workers/llm_simple_workers.yaml`:

```yaml
worker_pools:
  - name: LLMWorkerPool
    workers:
      - name: worker_5
        resources:
          Slot_1:worker_5: 1
      # Add more workers...
```

### Change Request Pattern

Edit `profiles/workload/llm_inference_workload.yaml`:

```yaml
graphs:
  - name: LLMInferenceRequest
    period: 10                # Faster arrival (10 μs instead of 20)
    invocations: 50           # More requests (50 instead of 10)
```

### Adjust Deadlines

In the workload file:

```yaml
- name: Prefill
  slo: 20                     # Relax deadline to 20 μs
```

---

## View Results

After running, check:

1. **Console Output**: Summary statistics
2. **CSV File**: `llm_simple_results.csv` - Detailed metrics
3. **Log File**: `logs/llm_simple.log` - Scheduling decisions

---

## Troubleshooting

### Issue: "No Slot_1 resources available"
**Solution:** Workers don't have Slot_1 resources. Check worker config matches workload.

### Issue: "Tasks missing deadlines"
**Solution:** 
- Increase SLO in workload
- Add more workers
- Increase arrival period

### Issue: "Scheduler takes too long"
**Solution:**
- Increase `--scheduler_time_limit=120`
- Use coarser `--time_discretization=5`

---

## Next Steps

1. **Try different schedulers:**
   ```bash
   python main.py --flagfile=configs/llm_simple.conf --scheduler=edf
   ```

2. **Scale up workers:**
   ```bash
   python main.py --flagfile=configs/llm_simple.conf \
     --worker_profile_path=profiles/workers/llm_inference_workers.yaml
   ```
   (Uses 4 GPU + 3 CPU workers from the full config)

3. **Use realistic workload:**
   ```bash
   python main.py \
     --workload_profile_path=profiles/workload/llm_inference_realistic.yaml \
     --worker_profile_path=profiles/workers/llm_simple_workers.yaml \
     --scheduler=tetrisched
   ```

---

## File Locations

```
erdos-scheduling-simulator/
├── profiles/
│   ├── workload/
│   │   └── llm_inference_workload.yaml     ← Workload definition
│   └── workers/
│       └── llm_simple_workers.yaml         ← Worker configuration
├── configs/
│   └── llm_simple.conf                     ← Run configuration
└── main.py                                  ← Entry point
```

---

## Summary

**Quickest way to run:**
```bash
python main.py --flagfile=configs/llm_simple.conf
```

That's it! 🚀

