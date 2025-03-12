from pathlib import Path
import subprocess
import re
from typing import Optional, Callable

from . import spec

def generate_workload(output_dir: Path, expt: spec.Experiment) -> Path:
    # Dump the workload spec config.
    flags = expt.workload_spec_flags()
    conf_file = output_dir / "workload.conf"
    with open(conf_file, "w") as f:
        f.write("\n".join(flags))
        f.write("\n")

    # Save the workload spec in workload.json
    spec_file = output_dir / "workload.json"
    with open(spec_file, "w") as f:
        cmd = [
            "python3",
            "-m",
            "scripts.generate_workload_spec",
            *flags,
        ]
        subprocess.Popen(cmd, stdout=f).wait()
    return spec_file


def run_simulator(
        output_dir: Path,
        workload_spec: Path,
        sched: spec.SchedSpec,
        expt: spec.Experiment,
):
    output_dir.mkdir(exist_ok=True)

    def outp(ext):
        return (output_dir / f"{sched.name}.{ext}").resolve()

    log_file = outp("log")
    csv_file = outp("csv")
    flags = expt.base_sim_flags() + sched.flags + [
        "--tpch_workload_spec", str(workload_spec),
        "--log", str(log_file),
        "--log_level", "debug",
        "--csv", str(csv_file),
    ]
    conf_file = outp("conf")
    with open(conf_file, "w") as f:
        f.write("\n".join(flags))
        f.write("\n")

    stdout, stderr = outp("stdout"), outp("stderr")
    with open(stdout, "w") as f_stdout, open(stderr, "w") as f_stderr:
        cmd = [
            "python3",
            "main.py",
            "--flagfile",
            str(conf_file),
        ]
        subprocess.Popen(cmd, stdout=f_stdout, stderr=f_stderr).wait()

    return output_dir


def run_analysis(results_dir: Path, sched: spec.SchedSpec):
    output_dir = results_dir / "analysis"
    output_dir.mkdir(parents=True, exist_ok=True)

    def outp(ext):
        return (output_dir / f"{sched.name}.{ext}").resolve()

    stdout, stderr = outp("stdout"), outp("stderr")
    with open(stdout, "w") as f_stdout, open(stderr, "w") as f_stderr:
        cmd = [
            "python3",
            "analyze.py",
            f"--csv_files={results_dir}/{sched.name}.csv",
            f"--conf_files={results_dir}/{sched.name}.conf",
            f"--output_dir={output_dir}",
            "--goodresource_utilization",
        ]
        subprocess.Popen(cmd, stdout=f_stdout, stderr=f_stderr).wait()

    return output_dir


def parse_analysis(result: Path) -> dict[str, float]:
    with open(result, "r") as f:
        data = f.read()
    eff = float(
        re.search(r"Average Good Utilization:\s+([-+]?\d*\.\d+|\d+)", data).group(1)
    )
    avg = float(re.search(r"Average Utilization:\s+([-+]?\d*\.\d+|\d+)", data).group(1))
    return {"avg": avg / 100, "eff": eff / 100}


def parse_slo(result: Path) -> float:
    with open(result) as f:
        for line in reversed(f.readlines()):
            parts = line.split(",")
            if len(parts) >= 1 and parts[1] == "LOG_STATS":
                return float(parts[8])
    assert False                # should throw an exception instead


def run_and_analyze(
        output_dir: Path,
        expt: spec.Experiment,
        sched: spec.SchedSpec,
) -> dict[str, float]:
    output_dir.mkdir(parents=True, exist_ok=True)
    workload_spec = generate_workload(output_dir, expt)
    sim = run_simulator(output_dir, workload_spec, sched, expt)
    analysis = run_analysis(sim, sched)
    return {
        "slo": parse_slo(sim / f"{sched.name}.csv"),
        **parse_analysis(analysis / f"{sched.name}.stdout"),
    }


# def make_experiment_tasks(
#         output_dir: Path,
#         expt: spec.Experiment,
# ) -> list[Callable[[], dict[str, float]]]:
#     """Generate callables that run an experiment on every scheduler."""

#     tasks = []
#     for name in spec.sched_specs:
#         def task():
#             sched = spec.sched_specs[name]
#             return run_and_analyze(output_dir / name, sched, expt)
#         tasks.append(task)
#     return tasks
