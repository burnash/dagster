# this script can be used to pack and upload a python virtualenv to an s3 bucket
# requires `uv` and `tar`

import argparse
import os
import subprocess
import sys
import tempfile
from pathlib import Path

SCRIPT_DIR = Path(__file__).parent
REQUIREMENTS_TXT = SCRIPT_DIR / "requirements.txt"
DAGSTER_DIR = Path(*SCRIPT_DIR.parts[: SCRIPT_DIR.parts.index("examples")])

DAGSTER_PIPES_DIR = DAGSTER_DIR / "python_modules/dagster-pipes"

parser = argparse.ArgumentParser(description="Upload a python virtualenv to an s3 path")
parser.add_argument(
    "--python", type=str, help="python version to use", default="3.10.8"
)
parser.add_argument(
    "--requirements",
    type=str,
    help="path to the requirements.txt file",
    default=str(REQUIREMENTS_TXT),
)
parser.add_argument("--s3-path", type=str, help="s3 path to copy to", required=True)


def main():
    args = parser.parse_args()

    with tempfile.TemporaryDirectory() as temp_dir:
        os.chdir(temp_dir)
        subprocess.run(
            f"uv python install --python-preference only-managed {args.python}",
            shell=True,
            check=True,
        )
        subprocess.run(
            f"uv venv --seed --relocatable --python-preference only-managed --python {args.python}",
            shell=True,
            check=True,
        )
        os.environ["VIRTUAL_ENV"] = str(Path(temp_dir) / ".venv")
        subprocess.run("source ./.venv/bin/activate", shell=True, check=True)
        subprocess.run(
            f"uv pip install --link-mode clone {DAGSTER_PIPES_DIR} ",
            shell=True,
            check=True,
        )
        subprocess.run(
            "tar -czf pyspark_venv.tar.gz -C .venv .",
            shell=True,
            check=True,
        )
        subprocess.run(
            f"aws s3 cp {temp_dir}/pyspark_venv.tar.gz {args.s3_path}",
            shell=True,
            check=True,
        )


if __name__ == "__main__":
    main()
