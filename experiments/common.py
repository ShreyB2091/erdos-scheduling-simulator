import subprocess
from typing import Callable
from pathlib import Path
import re
from dataclasses import dataclass
import logging
import time
import datetime
import shutil

from .experiment_spec import Experiment
from .scheduler_spec import SchedSpec

logger = logging.getLogger(__name__)


@dataclass
class ExpOutputs:
    csv: Path
    conf: Path
    do_analysis: bool


def generate_workload(output_dir: Path, expt: Experiment) -> Path:
    logger.info("Generating workload spec.")
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
        subprocess.run(cmd, stdout=f, check=True)
    return spec_file


def analyze(output_dir: Path, exp_outputs: ExpOutputs) -> dict[str, float]:
    logger.info("Analyzing simulator output.")
    def outp(ext):
        return (output_dir / f"analyze.{ext}")

    stdout, stderr = outp("stdout"), outp("stderr")
    with open(stdout, "w") as f_stdout, open(stderr, "w") as f_stderr:
        cmd = [
            "python3",
            "analyze.py",
            f"--csv_files={exp_outputs.csv}",
            f"--conf_files={exp_outputs.conf}",
            f"--output_dir={output_dir}",
            "--goodresource_utilization",
        ]
        subprocess.run(cmd, stdout=f_stdout, stderr=f_stderr, check=True)

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


def run_experiment(
        output_dir: Path,
        expt: Experiment,
        sched: SchedSpec,
        run: Callable[[Path, Path, Experiment, SchedSpec], ExpOutputs],
) -> dict[str, float]:
    logger.info(f"Running experiment in {output_dir}.")
    shutil.rmtree(output_dir, ignore_errors=True)
    output_dir.mkdir(parents=True)

    start_time = time.time()

    workload_spec = generate_workload(output_dir, expt)
    exp_outputs = run(output_dir, workload_spec, expt, sched)
    analysis = analyze(output_dir, exp_outputs) if exp_outputs.do_analysis else {}

    elapsed_time = datetime.timedelta(seconds=time.time() - start_time)
    logger.info(f"Experiment complete.  Time elapsed: {elapsed_time}")

    return {
        "slo": parse_slo(exp_outputs.csv),
        **analysis,
    }
