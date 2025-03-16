from pathlib import Path
from contextlib import contextmanager
import subprocess
import time
from functools import partial
from typing import TextIO

from .experiment_spec import Experiment
from .scheduler_spec import SchedSpec
from .common import run_experiment, ExpOutputs


@contextmanager
def run_service(conf_file: Path, stdout: TextIO, stderr: TextIO):
    service = subprocess.Popen([
        "python3", "-m", "rpc.service",
        "--flagfile", conf_file,
    ], stdout=stdout, stderr=stderr)
    try:
        yield service
    finally:
        service.terminate()


@contextmanager
def run_spark(spark_mirror: Path, properties_file: Path):
    subprocess.run([
        spark_mirror / "sbin" / "start-master.sh",
        "--host", "localhost",
        "--properties-file", properties_file,
    ])
    try:
        subprocess.run([
            spark_mirror / "sbin" / "start-worker.sh",
            "spark://localhost:7077",
            "--properties-file", properties_file,
        ])
        try:
            yield
        finally:
            subprocess.run(spark_mirror / "sbin" / "stop-worker.sh")
    finally:
        subprocess.run(spark_mirror / "sbin" / "stop-master.sh")


def run_all(
        output_dir: Path,
        workload_spec: Path,
        expt: Experiment,
        sched: SchedSpec,
        spark_mirror: Path,
        properties_file: Path,
) -> ExpOutputs:
    def outp(ext: str) -> Path:
        return output_dir / f"service.{ext}"

    log_file = outp("log")
    csv_file = outp("csv")
    flags = expt.service_flags() + sched.flags + [
        "--log", str(log_file),
        "--log_level", "debug",
        "--csv", str(csv_file),
    ]
    conf_file = outp("conf")
    with open(conf_file, "w") as f:
        f.write("\n".join(flags))
        f.write("\n")

    with open(outp("stdout"), "w") as f_stdout, open(outp("stderr"), "w") as f_stderr:
        with run_service(conf_file, f_stdout, f_stderr) as service:
            time.sleep(3)
            with run_spark(spark_mirror, properties_file):
                time.sleep(5)
                # Now, launch the queries
                subprocess.run([
                    "python3", "-u", "-m", "rpc.launch_tpch_queries",
                    "--workload-spec", workload_spec,
                    "--spark-master-ip", "localhost",
                    "--spark-mirror-path", spark_mirror,
                    "--tpch-spark-path", "rpc/tpch-spark",
                    "--spark-eventlog-dir", output_dir / "spark-eventlog",
                ])
                # Wait for the service run to end
                service.wait()
                return ExpOutputs(csv=csv_file, conf=conf_file)


def run_service_experiment(
        output_dir: Path,
        expt: Experiment,
        sched: SchedSpec,
        spark_mirror: Path,
        properties_file: Path,
) -> dict[str, float]:
    run = partial(
        run_all,
        spark_mirror=spark_mirror,
        properties_file=properties_file
    )
    return run_experiment(output_dir, expt, sched, run)
