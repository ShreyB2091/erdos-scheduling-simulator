import argparse
import itertools
import concurrent.futures
import shutil
import traceback


from pathlib import Path
from typing import Dict, Any, List


from scripts.raysearch import run_and_analyze


import yaml
import pandas as pd


from tqdm import tqdm


def parse_experiment_config(config_path: Path) -> Dict[str, Any]:
    """Parse and validate experiment configuration from YAML file."""
    with open(config_path, "r") as f:
        data = yaml.safe_load(f)

    fields = set(data.keys())

    # Validate fields
    required_fields = {"base_flags", "schedulers", "matrix", "naming"}
    optional_fields = {"multi_enum_flags"}
    valid_fields = required_fields | optional_fields

    # Check required fields
    missing = required_fields - fields
    if missing:
        raise ValueError(f"Missing required fields: {missing}")

    # Check for invalid fields
    invalid = fields - valid_fields
    if invalid:
        raise ValueError(f"Invalid fields: {invalid}")

    # Validate schedulers structure
    if not isinstance(data["schedulers"], list) or not data["schedulers"]:
        raise ValueError("'schedulers' must be a non-empty list")

    for scheduler in data["schedulers"]:
        if "name" not in scheduler or "flags" not in scheduler:
            raise ValueError("Each scheduler must have 'name' and 'flags' fields")
        if not isinstance(scheduler["flags"], dict):
            raise ValueError("Scheduler 'flags' must be a dictionary")

    # Validate base_flags structure
    if not isinstance(data["base_flags"], dict):
        raise ValueError("'base_flags' must be a dictionary")

    # Validate matrix structure
    if not isinstance(data["matrix"], dict):
        raise ValueError("'matrix' must be a dictionary")

    for key, values in data["matrix"].items():
        if not isinstance(values, list):
            raise ValueError(f"Matrix parameter '{key}' must be a list")
        # Values can be either a simple list or list of lists
        if values and isinstance(values[0], list):
            # Nested list - each inner list is a set of values to use together
            for inner_list in values:
                if not isinstance(inner_list, list):
                    raise ValueError(
                        f"Matrix parameter '{key}' contains mixed list types"
                    )

    return data


def generate_experiment_matrix(config: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Generate all experiment combinations from the configuration matrix."""
    experiments = []

    # Get matrix parameters and their values
    matrix_params = config["matrix"]
    param_names = list(matrix_params.keys())

    # Extract multi-enum flags, if specified
    multi_enum_flags = config.get("multi_enum_flags", [])

    # Process parameter values - handle both simple lists and nested lists
    param_value_sets = []
    for param_name in param_names:
        values = matrix_params[param_name]
        if values and isinstance(values[0], list):
            # Nested list - each inner list is a separate option
            param_value_sets.append(values)
        else:
            # Simple list - wrap each value as a single-item list for consistency
            param_value_sets.append([[v] for v in values])

    def format_flag_value(flag_value: Any, delim=",") -> str:
        if isinstance(flag_value, list):
            # Join list values with comma
            values_str = delim.join(map(str, flag_value))
            return f"{values_str}"
        else:
            return f"{flag_value}"

    def format_flag(flag_name: str, flag_value: Any) -> List[str]:
        # Boolean flags
        if (
            (isinstance(flag_value, bool) and flag_value)
            or flag_value is None
            or flag_value == ""
        ):
            return [f"--{flag_name}"]

        # Multi-enum flags, with a list values
        #
        # Consider the --opt_passes flag, specified in the YAML file as follows:
        #
        # opt_passes:
        #   - CRITICAL_PATH_OPTIMIZATION
        #   - DYNAMIC_DISCRETIZATION
        #
        # The flags must be formatted as
        #
        # ["--opt_passes=CRITICAL_PATH_OPTIMIZATION", "--opt_passes=DYNAMIC_DISCRETIZATION"]
        #
        # and not
        #
        # ["--opt_passes="CRITICAL_PATH_OPTIMIZATION,DYNAMIC_DISCRETIZATION"]
        #
        # because it --opt_passes is a multi-enum flag
        elif flag_name in multi_enum_flags and isinstance(flag_value, list):
            return [f"--{flag_name}={format_flag_value(v)}" for v in flag_value]

        # All other variants
        else:
            return [f"--{flag_name}={format_flag_value(flag_value)}"]

    # Generate all combinations of parameter sets
    for param_set_combination in itertools.product(*param_value_sets):
        # param_set_combination is a tuple of lists, e.g. ([0.01, 0.02, 0.05], [200, 500])

        # Generate experiment for each scheduler with this parameter set combination
        for scheduler in config["schedulers"]:
            experiment = {
                "name": None,  # Will be set after we have the param dict
                "flags": [],
            }

            # Add base flags
            for flag_name, flag_value in config["base_flags"].items():
                experiment["flags"].extend(format_flag(flag_name, flag_value))

            # Add scheduler-specific flags
            for flag_name, flag_value in scheduler["flags"].items():
                experiment["flags"].extend(format_flag(flag_name, flag_value))

            # Add matrix parameter flags
            param_dict = {}
            for param_name, param_value in zip(param_names, param_set_combination):
                experiment["flags"].extend(format_flag(param_name, param_value))
                param_dict[param_name] = format_flag_value(param_value, delim="~")

            # Set experiment name using the param_dict
            name = config["naming"].format(scheduler=scheduler["name"], **param_dict)
            if "," in name:
                raise ValueError(
                    f"Invalid experiment name '{name}'. Must not contain ','"
                )
            experiment["name"] = name

            experiments.append(experiment)

    return experiments


def run_job(experiment: Dict[str, Any], output_dir: Path):
    try:
        result = run_and_analyze(
            label=experiment["name"],
            output_dir=output_dir,
            flags=experiment["flags"],
        )
        return {
            "experiment": experiment,
            "result": result,
        }
    except Exception as e:
        print(f"Failed to run {experiment}")
        print("Exception:", e)
        print(traceback.format_exc())
        return {
            "experiment": experiment,
            "error": {
                "traceback": traceback.format_exc(),
            },
        }


class InvalidOutputDirectoryException(Exception):
    pass


def prepare_output_directory(output_dir: Path):
    if output_dir.exists():
        while True:
            user_input = (
                input(
                    f"'{output_dir.resolve()}' already exists. Do you want to delete it? (y/n): "
                )
                .lower()
                .strip()
            )

            if user_input == "y":
                shutil.rmtree(output_dir)
                print(f"Directory '{output_dir}' has been deleted.")
                break
            elif user_input == "n":
                raise InvalidOutputDirectoryException(
                    f"Output directory already exists"
                )
            else:
                print("Please enter 'y' for yes or 'n' for no.")

    output_dir.mkdir(parents=True)


def run_experiment(
    experiment: List[Dict[str, Any]], output_dir: Path, num_workers: int
):
    def task(job):
        return run_job(job, output_dir)

    prepare_output_directory(output_dir)

    with concurrent.futures.ThreadPoolExecutor(max_workers=num_workers) as executor:
        results = list(tqdm(executor.map(task, experiment), total=len(experiment)))

    df = pd.json_normalize(results)
    with open(output_dir / "results.csv", "w") as f:
        df.to_csv(f, index=False)

    print(df)

    return results


def main():
    parser = argparse.ArgumentParser("Helper script to spawn simulator experiments")
    parser.add_argument(
        "--config",
        type=str,
        required=True,
        help="Path to experiment configuration YAML file",
    )
    parser.add_argument(
        "--num-workers",
        type=int,
        default=5,
        help="Number of parallel workers to run experiments.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Dry run mode that prints experiment configurations that will be spawned.",
    )
    parser.add_argument("--output-dir", type=Path, help="Path to output directory")

    args = parser.parse_args()
    if not args.dry_run:
        if not args.config or not args.output_dir:
            parser.error(
                "--config and --output-dir are required when not using --dry-run"
            )

    config = parse_experiment_config(Path(args.config))
    experiment = generate_experiment_matrix(config)

    if args.dry_run:
        print("DRY RUN - Printing experiment configurations without running them")
        print(f"Config: {args.config}")
        print(f"Total jobs to run: {len(experiment)}\n")

        for i, job in enumerate(experiment, 1):
            print(f"Job {i}: {job['name']}")
            print(f"  Flags:")
            for flag in job["flags"]:
                print(f"    {flag}")
            print()

        return

    print(
        f"Running experiment config '{args.config}' ({len(experiment)} jobs with {args.num_workers} workers)."
    )
    print(f"Dumping output to '{args.output_dir.resolve()}'")

    try:
        run_experiment(experiment, args.output_dir, args.num_workers)
        print(
            f"Successfully ran experiment. Results are available at '{args.output_dir.resolve()}'"
        )
    except InvalidOutputDirectoryException as e:
        print(f"Failed to run experiment. Reason: {e}")
    except Exception as e:
        print(f"Unexpectedly failed to run experiment. Reason: {e}")
        print("Starting a pdb debug session to help root cause")
        breakpoint()


if __name__ == "__main__":
    main()
