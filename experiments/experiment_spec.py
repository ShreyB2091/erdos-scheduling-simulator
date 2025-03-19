from typing import Protocol
from abc import abstractmethod


class Experiment(Protocol):
    """A single scheduler-agnostic experiment.

    Each experiment must specify three sets of flags: one to pass to
    scripts/generate_workload_spec.py, one to pass to the simulator,
    and one to pass to the service.

    By scheduler-agnostic, we mean that the flags returned by the
    Experiment shouldn't be specific to any particular scheduler.  The
    run_simulator_experiment() and run_service_experiment() functions
    take both an Experiment and a SchedSpec, to allow a single
    experiment to be tested with many different schedulers.

    """

    @abstractmethod
    def workload_spec_flags(self) -> list[str]:
        """Return the flags to pass to generate_workload_spec."""
        raise NotImplementedError

    @abstractmethod
    def sim_flags(self) -> list[str]:
        """Return the flags to pass to the simulator."""
        raise NotImplementedError

    @abstractmethod
    def service_flags(self) -> list[str]:
        """Return the flags to pass to the service."""
        raise NotImplementedError
