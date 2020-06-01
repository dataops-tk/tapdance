"""tapdance.paths - Helper functions for dealing with paths."""

import json
import os
from pathlib import Path

from logless import get_logger
from typing import Optional
import uio

logging = get_logger("tapdance")

SINGER_PLUGINS_INDEX = os.environ.get("SINGER_PLUGINS_INDEX", "./singer_index.yml")
VENV_ROOT = "/venv"
INSTALL_ROOT = "/usr/bin"
_ROOT_DIR = "/projects/my-project"
# _ROOT_DIR = "."

ENV_PIPELINE_VERSION_NUMBER = "PIPELINE_VERSION_NUMBER"

ENV_TAP_SECRETS_DIR = "TAP_SECRETS_DIR"
ENV_TAP_SCRATCH_DIR = "TAP_SCRATCH_DIR"
ENV_TAP_CONFIG_DIR = "TAP_CONFIG_DIR"
ENV_TAP_STATE_FILE = "TAP_STATE_FILE"


def get_config_file(plugin_name: str, config_dir: str = None, required: bool = True):
    """
    Return a path to the configuration file which also contains secrets.

     - If file is blank or does not exist at the default secrets path, a new file will be created.
     - If any environment variables exist in the form of TAP_MY_TAP_my_setting, a new file
    will be created which contains these settings.
     - If the default file exists and environment variables also exist, the temp file will
    contain the default file values along with the environment variable overrides.
    """
    secrets_path = os.path.abspath(config_dir or get_secrets_dir())
    default_path = f"{secrets_path}/{plugin_name}-config.json"
    tmp_path = f"{secrets_path}/tmp/{plugin_name}-config.json"
    use_tmp_file = False
    if uio.file_exists(default_path):
        json_text = uio.get_text_file_contents(default_path)
        conf_dict = json.loads(json_text)
    elif required:
        raise FileExistsError(default_path)
    else:
        conf_dict = {}
        use_tmp_file = True
    for k, v in os.environ.items():
        prefix = f"{plugin_name.replace('-', '_').upper()}_"
        if k.startswith(prefix):
            setting_name = k.split(prefix)[1]
            conf_dict[setting_name] = v
            use_tmp_file = True
    if use_tmp_file:
        uio.create_folder(str(Path(tmp_path).parent))
        uio.create_text_file(tmp_path, json.dumps(conf_dict))
        if not uio.file_exists(tmp_path):
            raise FileExistsError(tmp_path)
        return tmp_path
    return default_path


def get_pipeline_version_number():
    return os.environ.get(ENV_PIPELINE_VERSION_NUMBER, "1")


def get_state_file_path(required: bool = True) -> Optional[str]:
    """Return a path to the state file or None if no state file path is configured.

    Returns
    -------
    str
        The state file path.
    """
    result = os.environ.get(ENV_TAP_STATE_FILE, None)
    if not result:
        logging.warning(
            f"Could not locate env var '{ENV_TAP_STATE_FILE}'. "
            f"State may not be maintained."
        )
    return result


def get_taps_dir(override: str = None) -> str:
    """Get a path to a local copy of the taps metadata directory.

    Parameters
    ----------
    override : str, optional
        Overrides the source directory. This can be any supported cloud location.

    Returns
    -------
    str
        Returns a local path. If the default or override path is a cloud directory, the
        return value will be a local copy of the remote path.
    """
    taps_dir = override or os.environ.get(ENV_TAP_CONFIG_DIR, ".")
    return uio.make_local(taps_dir)  # if remote path provided, download locally


def get_plan_file(tap_name: str, taps_dir: str = None, required: bool = True) -> str:
    """Get path to plan file.

    Parameters
    ----------
    tap_name : str
        The name of the tap without the tap- prefix.
    taps_dir : str, optional
        The taps metadata directory, by default None

    Returns
    -------
    str
        The path to the file.
    """
    result = os.path.join(get_taps_dir(taps_dir), f"{tap_name}.plan.yml")
    if required and not uio.file_exists(result):
        raise FileExistsError(result)
    return result


def get_root_dir():
    # return _ROOT_DIR
    return "."


def get_secrets_dir():
    result = os.environ.get(ENV_TAP_SECRETS_DIR, f"{get_root_dir()}/.secrets")
    uio.create_folder(result)
    return result


def get_scratch_dir():
    result = os.environ.get(ENV_TAP_SCRATCH_DIR, f"{get_root_dir()}/.output")
    uio.create_folder(result)
    return result


def get_catalog_output_dir(tap_name):
    result = f"{get_scratch_dir()}/taps/{tap_name}-catalog"
    uio.create_folder(result)
    return result


def get_rules_file(taps_dir: str, tap_name: str, required: bool = True):
    result = os.path.join(get_taps_dir(taps_dir), f"{tap_name}.rules.txt")
    if required and not uio.file_exists(result):
        raise FileExistsError(result)
    return result
