from pathlib import Path
from contextlib import contextmanager
import subprocess
import time
from functools import partial
import os
import logging

from .experiment_spec import Experiment
from .scheduler_spec import SchedSpec
from .common import run_experiment, ExpOutputs, ExpResults

logger = logging.getLogger(__name__)


@contextmanager
def run_service(output_dir: Path, conf_file: Path):
    def outp(ext: str) -> Path:
        return output_dir / f"service.{ext}"

    logger.info("Starting spark service.")
    with open(outp("stdout"), "w") as stdout, open(outp("stderr"), "w") as stderr:
        service = subprocess.Popen([
            "python3", "-m", "rpc.service",
            "--flagfile", conf_file,
        ], stdout=stdout, stderr=stderr)

    try:
        yield service
    finally:
        if service.poll() is None:
            logger.info("Terminating spark service")
            service.terminate()


@contextmanager
def run_launcher(output_dir: Path, workload_spec: Path, spark_mirror: Path):
    def outp(ext: str) -> Path:
        return output_dir / f"launcher.{ext}"

    logger.info("Launching queries...")
    with open(outp("stdout"), "w") as stdout, open(outp("stderr"), "w") as stderr:
        launcher = subprocess.Popen([
            "python3", "-u", "-m", "rpc.launch_tpch_queries",
            "--workload-spec", workload_spec,
            "--spark-master-ip", "localhost",
            "--spark-mirror-path", spark_mirror,
            "--tpch-spark-path", "rpc/tpch-spark",
            "--spark-eventlog-dir", output_dir / "spark-eventlog",
        ], stdout=stdout, stderr=stderr)

    try:
        yield launcher
    finally:
        if launcher.poll() is None:
            logger.info("Terminating query launcher")
            launcher.terminate()


@contextmanager
def run_spark(spark_mirror: Path, properties_file: Path):
    logger.info("Starting spark master.")
    subprocess.run([
        spark_mirror / "sbin" / "start-master.sh",
        "--host", "localhost",
        "--properties-file", properties_file,
    ])
    try:
        logger.info("Starting spark worker.")
        subprocess.run([
            spark_mirror / "sbin" / "start-worker.sh",
            "spark://localhost:7077",
            "--properties-file", properties_file,
        ])
        try:
            yield
        finally:
            logger.info("Stopping spark worker.")
            subprocess.run(spark_mirror / "sbin" / "stop-worker.sh")
    finally:
        logger.info("Stopping spark master.")
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

    with run_service(output_dir, conf_file) as service:
        time.sleep(3)
        with run_spark(spark_mirror, properties_file):
            time.sleep(5)

            with run_launcher(output_dir, workload_spec, spark_mirror) as launcher:
                while True:
                    os.wait()
                    if service.poll():
                        raise RuntimeError("The service exited with an error.")
                    if launcher.poll():
                        raise RuntimeError("The query launcher exited with an error.")
                    if service.poll() is not None and launcher.poll() is not None:
                        # Both the service and launcher have completed
                        # without error.
                        break

    logger.info("Service complete.")
    return ExpOutputs(csv=csv_file, conf=conf_file, do_analysis=False)


def run_service_experiment(
        output_dir: Path,
        expt: Experiment,
        sched: SchedSpec,
        spark_mirror: Path,
        properties_file: Path,
) -> ExpResults:
    run = partial(
        run_all,
        spark_mirror=spark_mirror,
        properties_file=properties_file
    )
    return run_experiment(output_dir, expt, sched, run)
