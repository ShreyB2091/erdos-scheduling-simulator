from pathlib import Path
import subprocess
import logging

from .experiment_spec import Experiment
from .scheduler_spec import SchedSpec
from .common import run_experiment, ExpOutputs

logger = logging.getLogger(__name__)


def run_simulator(
        output_dir: Path,
        workload_spec: Path,
        expt: Experiment,
        sched: SchedSpec,
) -> ExpOutputs:
    logger.info("Running simulator.")

    def outp(ext: str) -> Path:
        return output_dir / f"simulator.{ext}"

    log_file = outp("log")
    csv_file = outp("csv")
    flags = expt.sim_flags() + sched.flags + [
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
        subprocess.run(cmd, stdout=f_stdout, stderr=f_stderr, check=True)

    return ExpOutputs(csv=outp("csv"), conf=outp("conf"), do_analysis=True)


def run_simulator_experiment(
        output_dir: Path,
        expt: Experiment,
        sched: SchedSpec,
) -> dict[str, float]:
    return run_experiment(output_dir, expt, sched, run_simulator)
