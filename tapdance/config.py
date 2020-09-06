"""tapdance.config - Helper functions for dealing with paths."""

import json
import os
from pathlib import Path

from logless import get_logger, logged
from typing import Optional, Dict, Any, Tuple
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


def validate_replication_strategy(replication_strategy):
    if replication_strategy not in ["FULL_TABLE", "INCREMENTAL", "LOG_BASED"]:
        raise ValueError(
            f"Replication strategy {replication_strategy} not supported. Expected: "
            "FULL_TABLE, INCREMENTAL, or LOG_BASED"
        )


def get_plugin_settings_from_env(plugin_name: str, meta_args: bool) -> Dict[str, Any]:
    """Get all the settings from env vars which match the plugin prefix.

    Parameters
    ----------
    plugin_name : str
        The name of the plugin, including the 'tap-' or 'target-' prefix
    meta_args : bool
        True to pull _uppercase_ settings only, which are reserved for the orchestrator;
        otherwise pull only _lowercase_ settings. By default false

    Returns
    -------
    Dict[str, Any]
        A dictionary of setting values.
    """
    conf_dict: Dict[str, Any] = {}
    for k, v in os.environ.items():
        prefix = f"{plugin_name.replace('-', '_').upper()}_"
        if k.startswith(prefix):
            logging.debug(f"Parsing env variable '{k}' for '{plugin_name}'...")
            setting_name = k.split(prefix)[1]
            case_match = meta_args == (setting_name.upper() == setting_name)
            if case_match:
                # Ensure truthinesss and falseness are maintained
                if str(v).lower() == "false":
                    conf_dict[setting_name] = False
                elif str(v).lower() == "true":
                    conf_dict[setting_name] = True
                elif str(v) == "0":
                    conf_dict[setting_name] = 0
                else:
                    conf_dict[setting_name] = v
    return conf_dict


def print_version():
    """Print the tapdance version number."""
    try:
        from importlib import metadata
    except ImportError:
        # Running on pre-3.8 Python; use importlib-metadata package
        import importlib_metadata as metadata
    try:
        version = metadata.version("tapdance")
    except metadata.PackageNotFoundError:
        version = "[could not be detected]"
    print(f"tapdance version {version}")


@logged(
    "getting '{plugin_name}' config file using: config_dir={config_dir}, "
    "config_file={config_file}",
    log_fn=logging.debug,
)
def get_or_create_config(
    plugin_name: str,
    taps_dir: str = None,
    config_dir: str = None,
    config_file: str = None,
) -> Tuple[str, Dict[str, Any]]:
    """
    Return a path to the configuration file and a dictionary of settings values.

     - If file is blank or does not exist at the default secrets path, a new file will
       be created.
     - If any environment variables exist in the form of TAP_MY_TAP_my_setting, a new
       file will be created which contains these settings.
     - If the default file exists and environment variables also exist, the temp file
       will contain the default file values along with the environment variable
       overrides.
     - Unless `config_file=False` (by boolean or case-insensitive string comparison),
       the config file must exist at the path provided or (if 'None' is passed) at the
       default location.
    """
    use_tmp_file = False
    secrets_path = os.path.abspath(config_dir or get_secrets_dir(taps_dir))
    tmp_path = f"{secrets_path}/tmp/{plugin_name}-config.json"

    orchestrator_env_vars = get_plugin_settings_from_env(plugin_name, meta_args=True)
    config_file = config_file or orchestrator_env_vars.get("CONFIG_FILE", None)
    if (config_file is not None) and str(config_file).lower() == "false":
        logging.info(f"Skipping check for '{plugin_name}' config (`config_file=False`)")
        use_tmp_file = True
        config_file = tmp_path
        conf_dict = {}  # Start with empty config
        orchestrator_settings = orchestrator_env_vars
    else:
        config_file = config_file or f"{secrets_path}/{plugin_name}-config.json"
        if not uio.file_exists(config_file):
            raise FileExistsError(
                f"Could not find '{plugin_name}' config at expected path: {config_file}"
            )
        logging.info(f"Reading '{plugin_name}' config from {config_file}...")
        conf_dict = json.loads(uio.get_text_file_contents(config_file))
        orchestrator_settings = {
            k: v
            for k, v in conf_dict.items()
            if k.upper() == k  # Uppercase settings only
        }
        orchestrator_settings.update(orchestrator_env_vars)

    # Parse settings and secrets from environment variables
    conf_dict.update(get_plugin_settings_from_env(plugin_name, meta_args=False))
    if conf_dict:
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
    return config_file, orchestrator_settings


def get_pipeline_version_number() -> str:
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


def get_secrets_dir(taps_dir: Optional[str]):
    result = os.environ.get(ENV_TAP_SECRETS_DIR, f"{get_taps_dir(taps_dir)}/.secrets")
    uio.create_folder(result)
    return result


def get_scratch_dir(taps_dir: Optional[str]):
    result = os.environ.get(ENV_TAP_SCRATCH_DIR, f"{get_taps_dir(taps_dir)}/.output")
    uio.create_folder(result)
    return result


def get_catalog_output_dir(tap_name: str, taps_dir: str) -> str:
    result = f"{get_scratch_dir(taps_dir)}/taps/{tap_name}-catalog"
    uio.create_folder(result)
    return result


def get_raw_catalog_file(catalog_dir: str, tap_name: str):
    custom_catalog_path = f"{catalog_dir}/{tap_name}-catalog-custom.json"
    catalog_file = f"{catalog_dir}/{tap_name}-catalog-raw.json"
    if uio.file_exists(custom_catalog_path):
        logging.info(f"Using custom catalog file: {custom_catalog_path}")
        catalog_file = custom_catalog_path
    elif uio.file_exists(catalog_file):
        if uio.get_text_file_contents(catalog_file).strip() == "":
            logging.info(f"Cleaning up empty catalog file: {catalog_file}")
            uio.delete_file(catalog_file)
    return catalog_file.replace("\\", "/")


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
        logging.info(f"AWS creds captured from '{plugin_name}' config")
        return config_defaults
    logging.info(f"Scanning for AWS creds for '{plugin_name}' config...")
    new_config = config_defaults.copy()
    (
        aws_access_key_id,
        aws_secret_access_key,
        aws_session_token,
    ) = uio.parse_aws_creds()
    if aws_access_key_id and aws_secret_access_key:
        logging.info(
            f"Passing 'aws_access_key_id' (...{aws_access_key_id[-6:]}) and "
            f"'aws_secret_access_key' ({aws_secret_access_key[:6]}...) "
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
            f"Passing 'aws_session_token' ({aws_session_token[:6]}...) "
            f"to '{plugin_name}'"
        )
        new_config["aws_session_token"] = aws_session_token
    return new_config


def get_single_table_target_config_file(
    target_name: str,
    target_config_file: str,
    *,
    tap_name: str,
    table_name: str,
    pipeline_version_num: str,
) -> str:
    config_defaults = json.loads(uio.get_text_file_contents(target_config_file))
    new_config = replace_placeholders(
        config_defaults, tap_name, table_name, pipeline_version_num
    )
    new_file_path = target_config_file.replace(".json", f"-{table_name}.json")
    uio.create_text_file(new_file_path, json.dumps(new_config))
    return new_file_path


def replace_placeholders(
    config_dict: Dict[str, Any],
    tap_name: str,
    table_name: str,
    pipeline_version_num: str,
) -> Dict[str, Any]:
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
                    logging.debug(
                        f"Modifying '{setting_name}' setting value, "
                        f"replacing '{search_key}' placeholder with '{replacement_value}'."
                    )
                    new_config[setting_name] = new_config[setting_name].replace(
                        search_key, replacement_value
                    )
    return new_config
