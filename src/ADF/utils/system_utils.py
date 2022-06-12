import os
import logging
import subprocess

from typing import Callable, Optional

from zipfile import ZipFile


def zip_files(
    zip_hanlder: ZipFile,
    path: str,
    exclude: Callable[[str], bool] = lambda x: False,
    prefix: Optional[str] = None,
    verbose: bool = False,
) -> None:
    if os.path.isdir(path):
        for root, dirs, files in os.walk(path):
            for file in files:
                cur_path = os.path.join(root, file)
                rel_path = os.path.relpath(cur_path, os.path.join(path, ""))
                if not exclude(rel_path):
                    if verbose:
                        logging.info(f"Writing {cur_path}...")
                    zip_hanlder.write(
                        cur_path,
                        f"{prefix or ''}{rel_path}",
                    )
                elif verbose:
                    logging.info(f"Excluding '{rel_path}'...")
    elif os.path.isfile(path):
        zip_hanlder.write(path, os.path.basename(path))
    else:
        raise ValueError(
            f"Path '{path}' cannot be zipped as it is neither a directory nor a file."
        )


def run_command(cmd: str, **run_kwargs) -> subprocess.CompletedProcess:
    logging.info(f"RUNNING '{cmd}' with options '{run_kwargs}'")
    run_kwargs.setdefault("shell", True)
    result = subprocess.run(
        cmd, **run_kwargs, stdout=subprocess.PIPE, stderr=subprocess.PIPE
    )
    if result.returncode:
        logging.error(f"CMD : {cmd}")
        logging.error(f"OPTIONS : {run_kwargs}")
        logging.error(f"STDOUT :\n{result.stdout.decode('utf-8')}")
        logging.error(f"STDERR :\n{result.stderr.decode('utf-8')}")
        raise RuntimeError(f"Failed to run command '{cmd}'")
    return result
