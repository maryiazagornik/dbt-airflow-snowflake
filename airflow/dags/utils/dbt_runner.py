import os
import subprocess
import sys
from typing import List

from utils.get_creds import get_snowflake_config
from utils.constants import DBT_ROOT_PATH
from utils.dbt_logger import log


def run_dbt(args: List[str]):
    secrets = get_snowflake_config()

    dbt_env = os.environ.copy()
    dbt_env.update(secrets)

    local_bin = os.path.expanduser("~/.local/bin")
    current_path = dbt_env.get("PATH", "")
    if local_bin not in current_path:
        dbt_env["PATH"] = f"{local_bin}:{current_path}"

    cmd = ["dbt"] + args

    log.info(f"Running dbt command: {' '.join(cmd)}")

    try:
        subprocess.run(cmd, cwd=DBT_ROOT_PATH, env=dbt_env, check=True, text=True)
        log.success(f"dbt {' '.join(args)} completed successfully!")
    except subprocess.CalledProcessError as e:
        log.error(f"dbt failed with return code {e.returncode}")
        sys.exit(e.returncode)
    except Exception as e:
        log.critical(f"Unexpected error running dbt: {e}")
        sys.exit(1)


if __name__ == "__main__":
    arguments = sys.argv[1:]
    if not arguments:
        log.error("No dbt arguments provided. Example: python dbt_runner.py run")
        sys.exit(1)

    run_dbt(arguments)
