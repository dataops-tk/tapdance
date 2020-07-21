"""tapdance.paths - Helper functions for dealing with paths."""

import json
import os
from pathlib import Path

from logless import get_logger, logged
from typing import Optional
import uio

logging = get_logger("tapdance")

# These plugins will attempt to scrape and pass along AWs Credentials from the local environment.
S3_TARGET_IDS = ["S3-CSV", "REDSHIFT", "SNOWFLAKE"]

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


@logged(
    "getting '{plugin_name}' config file using: config_dir={config_dir}, "
    "config_file={config_file}, required={required}",
    log_fn=logging.debug,
)
def get_config_file(
    plugin_name: str,
    config_dir: str = None,
    config_file: str = None,
    required: bool = True,
) -> str:
    """
    Return a path to the configuration file which also contains secrets.

     - If file is blank or does not exist at the default secrets path, a new file will be created.
     - If any environment variables exist in the form of TAP_MY_TAP_my_setting, a new file
    will be created which contains these settings.
     - If the default file exists and environment variables also exist, the temp file will
    contain the default file values along with the environment variable overrides.
    """
    secrets_path = os.path.abspath(config_dir or get_secrets_dir())
    config_file = config_file or f"{secrets_path}/{plugin_name}-config.json"
    tmp_path = f"{secrets_path}/tmp/{plugin_name}-config.json"
    use_tmp_file = False
    if uio.file_exists(config_file):
        conf_dict = json.loads(uio.get_text_file_contents(config_file))
    elif required:
        raise FileExistsError(config_file)
    else:
        logging.info(f"No {plugin_name} config file exists. A file will be created...")
        conf_dict = {}
        use_tmp_file = True

    # Parse settings and secrets from environment variables
    for k, v in os.environ.items():
        prefix = f"{plugin_name.replace('-', '_').upper()}_"
        if k.startswith(prefix) and not k.endswith("_EXE"):
            logging.debug(f"Parsing env variable '{k}' for '{plugin_name}'...")
            setting_name = k.split(prefix)[1]
            # Ensure truthinesss and falseness are maintained
            if str(v).lower() == "false":
                conf_dict[setting_name] = False
            elif str(v).lower() == "true":
                conf_dict[setting_name] = True
            elif str(v) == "0":
                conf_dict[setting_name] = 0
            else:
                conf_dict[setting_name] = v
            use_tmp_file = True
    if "-".join(plugin_name.split("-")[1:]).upper() in S3_TARGET_IDS:
        conf_dict = _inject_s3_config_creds(plugin_name, conf_dict)
        use_tmp_file = True

    if use_tmp_file:
        logging.info(f"Writing temporary config file to '{tmp_path}'...")
        uio.create_folder(str(Path(tmp_path).parent))
        uio.create_text_file(tmp_path, json.dumps(conf_dict))
        config_file = tmp_path
    if not uio.file_exists(config_file):
        raise FileExistsError(config_file)
    return config_file


def get_pipeline_version_number():
    return os.environ.get(ENV_PIPELINE_VERSION_NUMBER, "1")


def get_exe(tap_or_target_id: str) -> str:
    """Gets the exe for the tap or target.

    Parameters
    ----------
    tap_or_target_id : str
        The tap or target ID, including the tap-/target- prefix.

    Returns
    -------
    str
        The exe to use.
    """
    env_var = f"{tap_or_target_id.upper().replace('-', '_')}_EXE"
    if env_var in os.environ:
        result = os.environ[env_var]
        logging.info(f"Found exe '{result}' for '{tap_or_target_id}' in env variable.")
    else:
        result = tap_or_target_id
        logging.info(f"Defaulting to exe='{tap_or_target_id}' from tap name.")
    return result


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


def _inject_s3_config_creds(
    plugin_name: str, config_defaults: dict,
):
    if (
        "aws_access_key_id" in config_defaults
        and "aws_secret_access_key" in config_defaults
    ):
        logging.info("AWS creds captured from '{plugin_name}' config")
        return config_defaults
    logging.info("Scanning for AWS creds for '{plugin_name}' config...")
    new_config = config_defaults.copy()
    (
        aws_access_key_id,
        aws_secret_access_key,
        aws_session_token,
    ) = uio.parse_aws_creds()
    if aws_access_key_id and aws_secret_access_key:
        logging.info(
            f"Passing 'aws_access_key_id' (...{aws_access_key_id[-4:]}) and "
            f"'aws_secret_access_key' ({aws_secret_access_key[:4]}...) "
            f"credentials to '{plugin_name}'"
        )
        new_config["aws_access_key_id"] = aws_access_key_id
        new_config["aws_secret_access_key"] = aws_secret_access_key
    else:
        logging.warning(
            f"Could not find 'aws_access_key_id' and 'aws_secret_access_key' "
            f"credentials for '{plugin_name}'"
        )
    if aws_session_token:
        logging.info(
            f"Passing 'aws_session_token' ('{aws_session_token[:4]}...') "
            f"to '{plugin_name}'"
        )
        new_config["aws_session_token"] = aws_session_token
    return new_config


def get_single_table_target_config_file(
    target_name, target_config_file, *, tap_name, table_name, pipeline_version_num,
):
    config_defaults = json.loads(uio.get_text_file_contents(target_config_file))
    new_config = replace_placeholders(
        config_defaults, tap_name, table_name, pipeline_version_num
    )
    new_file_path = target_config_file.replace(".json", f"-{table_name}.json")
    uio.create_text_file(new_file_path, json.dumps(new_config))
    return new_file_path


def replace_placeholders(config_dict, tap_name, table_name, pipeline_version_num):
    new_config = config_dict.copy()
    for setting_name in new_config.keys():
        if isinstance(new_config[setting_name], str):
            for param, replacement_value in {
                "tap": tap_name,
                "table": table_name,
                "version": pipeline_version_num,
            }.items():
                search_key = "{" + f"{param}" + "}"
                if search_key in new_config[setting_name]:
                    logging.info(
                        f"Modifying '{setting_name}' setting value, "
                        f"replacing '{search_key}' placeholder with '{replacement_value}'."
                    )
                    new_config[setting_name] = new_config[setting_name].replace(
                        search_key, replacement_value
                    )
    return new_config
