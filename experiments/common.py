from __future__ import annotations

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


@dataclass
class Event:
    """An event recorded in the CSV log of an experiment run.

    Represents a single line in the CSV log, with time corresponding
    to the first field, event_type corresponding to the second field,
    and params corresponding to the remaining fields.

    In the future, this class should be unified with the simulator's
    internal event structure and be more strongly typed.

    Attributes:
        time       -- the timestamp of the event in simulator ticks
        event_type -- the type of the event
        params     -- extra information corresponding to the event

    """
    time: int
    event_type: str
    params: list[str]

    @staticmethod
    def parse_csv_line(line: str) -> Event:
        parts = line.strip().split(",")
        return Event(time=int(parts[0]), event_type=parts[1], params=parts[2:])


@dataclass
class ExpResults:
    """The metrics of a completed experiment run.

    Currently this class is a front for the results of analyze.py and
    the parsed CSV log.  In the future, Simulator.run() should return
    a list of Events directly, and we should be able to implement all
    analysis here.

    Attributes:
        slo           -- clipped SLO attainment in the range 0.0-1.0
        avg_res_util  -- average resource utilization
        good_res_util -- average resource utilization by tasks that
                         complete succesfully
        trace         -- a trace of the simulator events
    """

    slo: float
    avg_res_util: float
    good_res_util: float

    trace: list[Event]


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


def parse_slo(trace: list[Event]) -> float:
    # Get the final clipped SLO value from the last LOG_STATS line in
    # the csv.
    for event in reversed(trace):
        if event.event_type == "LOG_STATS":
            return float(event.params[6])
    raise RuntimeError("Could not find a LOG_STATS line in the simulator CSV")


def analyze(output_dir: Path, exp_outputs: ExpOutputs) -> ExpResults:
    # This method does two different things; it calls analyze.py and
    # parses the result, and it parses the CSV log.  In the future
    # we'll do all analysis on the CSV log directly.

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
        analysis_out = f.read()

    def parse_output(tag):
        match_ = re.search(tag + r":\s+([-+]?\d*\.\d+|\d+)", analysis_out)
        if match_ is None:
            raise RuntimeError(f"Tag {tag} not found in analysis output")
        return float(match_.group(1)) / 100

    with open(exp_outputs.csv) as f:
        trace = [Event.parse_csv_line(line) for line in f]

    return ExpResults(
        slo=parse_slo(trace),
        avg_res_util=parse_output("Average Utilization"),
        good_res_util=parse_output("Average Good Utilization"),
        trace=trace,
    )


def run_experiment(
        output_dir: Path,
        expt: Experiment,
        sched: SchedSpec,
        run: Callable[[Path, Path, Experiment, SchedSpec], ExpOutputs],
) -> ExpResults:
    logger.info(f"Running experiment in {output_dir}.")
    shutil.rmtree(output_dir, ignore_errors=True)
    output_dir.mkdir(parents=True)

    start_time = time.time()

    workload_spec = generate_workload(output_dir, expt)
    exp_outputs = run(output_dir, workload_spec, expt, sched)

    # TODO: Service runs don't play nicely with how analyze.py
    # reads the event log.  For now, we will leave returning their
    # results unimplemented, but when analyze.py is cleaned up
    # this check should be removed.
    if not exp_outputs.do_analysis:
        raise Exception("service runs don't support analysis")

    analysis = analyze(output_dir, exp_outputs)

    elapsed_time = datetime.timedelta(seconds=time.time() - start_time)
    logger.info(f"Experiment complete.  Time elapsed: {elapsed_time}")

    return analysis
