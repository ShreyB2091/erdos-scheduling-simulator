from pathlib import Path
import subprocess
import re

from .experiment_spec import Experiment
from .scheduler_spec import SchedSpec


def generate_workload(output_dir: Path, expt: Experiment) -> Path:
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
        sched: SchedSpec,
        expt: Experiment,
) -> dict[str, Path]:
    output_files = {
        ext: (output_dir / f"simulator.{ext}")
        for ext in ("log", "csv", "conf", "stdout", "stderr")
    }

    log_file = output_files["log"]
    csv_file = output_files["csv"]
    flags = expt.base_sim_flags() + sched.flags + [
        "--tpch_workload_spec", str(workload_spec),
        "--log", str(log_file),
        "--log_level", "debug",
        "--csv", str(csv_file),
    ]
    conf_file = output_files["conf"]
    with open(conf_file, "w") as f:
        f.write("\n".join(flags))
        f.write("\n")

    stdout, stderr = output_files["stdout"], output_files["stderr"]
    with open(stdout, "w") as f_stdout, open(stderr, "w") as f_stderr:
        cmd = [
            "python3",
            "main.py",
            "--flagfile",
            str(conf_file),
        ]
        subprocess.Popen(cmd, stdout=f_stdout, stderr=f_stderr).wait()

    return output_files


def analyze(output_dir: Path, sim_output: dict[str, Path]) -> dict[str, float]:
    def outp(ext):
        return (output_dir / f"analyze.{ext}")

    stdout, stderr = outp("stdout"), outp("stderr")
    with open(stdout, "w") as f_stdout, open(stderr, "w") as f_stderr:
        cmd = [
            "python3",
            "analyze.py",
            f"--csv_files={sim_output['csv']}",
            f"--conf_files={sim_output['conf']}",
            f"--output_dir={output_dir}",
            "--goodresource_utilization",
        ]
        subprocess.Popen(cmd, stdout=f_stdout, stderr=f_stderr).wait()

    with open(stdout) as f:
        data = f.read()

    def parse_output(tag):
        return float(re.search(tag + r":\s+([-+]?\d*\.\d+|\d+)", data).group(1)) / 100

    return {
        "avg": parse_output("Average Utilization"),
        "eff": parse_output("Average Good Utilization"),
    }


def parse_slo(sim_csv: Path) -> float:
    with open(sim_csv) as f:
        for line in reversed(f.readlines()):
            parts = line.split(",")
            if len(parts) >= 1 and parts[1] == "LOG_STATS":
                return float(parts[8])
    raise RuntimeError("Could not find a LOG_STATS line in the simulator CSV")


def run_simulator_experiment(
        output_dir: Path,
        expt: Experiment,
        sched: SchedSpec,
) -> dict[str, float]:
    output_dir.mkdir(parents=True, exist_ok=True)
    workload_spec = generate_workload(output_dir, expt)
    sim_output = run_simulator(output_dir, workload_spec, sched, expt)
    analysis = analyze(output_dir, sim_output)
    return {
        "slo": parse_slo(sim_output["csv"]),
        **analysis,
    }
