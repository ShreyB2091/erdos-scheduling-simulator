import os
import subprocess
import re


from pathlib import Path


def generate_workload(output_dir: Path, flags: list, label="workload") -> Path:
    # Not used by ray, but useful to reference during analysis
    conf_file = output_dir / f"{label}-workload-spec.conf"
    with open(conf_file, "w") as f:
        f.write("\n".join(str(flag) for flag in flags))
        f.write("\n")

    spec_file = output_dir / f"{label}.json"
    with open(spec_file, "w") as f:
        cmd = [
            "python3",
            "-m",
            "scripts.generate_workload_spec",
            *(str(flag) for flag in flags),
        ]
        subprocess.Popen(cmd, stdout=f).wait()
    return spec_file


def run_simulator(label: str, output_dir: Path, flags: list):
    output_dir = output_dir / label
    output_dir.mkdir(parents=True, exist_ok=True)

    def absp(p):
        return output_dir / p

    log_dir = output_dir.resolve()
    log_file = "output.log"
    csv_file = "output.csv"
    flags.extend(
        [
            f"--log_dir={log_dir}",
            f"--log={log_file}",
            "--log_level=debug",
            f"--csv={csv_file}",
        ]
    )

    conf_file = output_dir / "flags.conf"
    with open(conf_file, "w") as f:
        f.write("\n".join(str(flag) for flag in flags))
        f.write("\n")

    stdout, stderr = output_dir / "cmd.stdout", output_dir / "cmd.stderr"
    with open(stdout, "w") as f_stdout, open(stderr, "w") as f_stderr:
        cmd = [
            "python3",
            "main.py",
            "--flagfile",
            str(conf_file),
        ]
        env = os.environ.copy()

        tetrisched_dir = output_dir / "tetrisched"
        tetrisched_dir.mkdir(parents=True, exist_ok=True)
        env["TETRISCHED_LOGGING_DIR"] = str(tetrisched_dir)

        subprocess.Popen(cmd, stdout=f_stdout, stderr=f_stderr, env=env).wait()

    return output_dir


def run_analysis(label: str, results_dir: Path):
    output_dir = results_dir / "analysis"
    output_dir.mkdir(parents=True, exist_ok=True)

    stdout, stderr = output_dir / "cmd.stdout", output_dir / "cmd.stderr"
    with open(stdout, "w") as f_stdout, open(stderr, "w") as f_stderr:
        cmd = [
            "python3",
            "analyze.py",
            f"--csv_files={results_dir}/output.csv",
            f"--conf_files={results_dir}/flags.conf",
            f"--output_dir={output_dir}",
            "--goodresource_utilization",
        ]
        subprocess.Popen(cmd, stdout=f_stdout, stderr=f_stderr).wait()

    return output_dir


def parse_analysis(result: Path):
    with open(result, "r") as f:
        data = f.read()
    eff = float(
        re.search(r"Average Good Utilization:\s+([-+]?\d*\.\d+|\d+)", data).group(1)
    )
    avg = float(re.search(r"Average Utilization:\s+([-+]?\d*\.\d+|\d+)", data).group(1))
    return {"avg": avg, "eff": eff}


def parse_simulator_result(result: Path):
    with open(result, "r") as f:
        data = reversed(f.readlines())
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
    scheduler_runtimes = []
    for line in data:
        parts = line.split(",")
        if parts[1] == "SCHEDULER_FINISHED" and (
            int(parts[3]) != 0 or int(parts[4]) != 0
        ):
            scheduler_runtimes.append(float(parts[-1]))
    return {
        "slo": slo,
        "scheduler_runtimes": scheduler_runtimes,
    }


def run_and_analyze(label: str, output_dir: Path, flags: list):
    sim = run_simulator(label, output_dir, flags)
    analysis = run_analysis(label, sim)

    sim_results = parse_simulator_result(sim / "output.csv")
    avg_scheduler_runtime = 0.0
    if len(sim_results["scheduler_runtimes"]) > 0:
        avg_scheduler_runtime = sum(sim_results["scheduler_runtimes"]) / len(
            sim_results["scheduler_runtimes"]
        )
    return {
        "slo": sim_results["slo"],
        "avg_scheduler_runtime": avg_scheduler_runtime,
        "analysis": parse_analysis(analysis / "cmd.stdout"),
    }
