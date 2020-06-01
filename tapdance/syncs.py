"""tapdance.syncs - Module containing the sync() function and sync helper functions."""

import json
import os

import uio
import runnow
from logless import get_logger, logged

from tapdance import config, docker, plans

# These plugins will attempt to scrape and pass along AWs Credentials from the local environment.
S3_TARGET_IDS = ["S3-CSV", "REDSHIFT", "SNOWFLAKE"]

logging = get_logger("tapdance")


@logged("syncing '{table_name or 'all tables'}' from '{tap_name}' to '{target_name}'")
def sync(
    tap_name: str,
    target_name: str = "csv",
    table_name: str = "*",
    taps_dir: str = None,
    *,
    rescan: bool = False,
    dockerized: bool = None,
    config_dir: str = None,
    config_file: str = None,
    catalog_dir: str = None,
    target_config_file: str = None,
    state_file: str = None,
):
    """
    Synchronize data from tap to target.

    Arguments:
        tap_name {str} -- The name/alias of the source tap, without the `tap-` prefix.

    Keyword Arguments:
        target_name {str} -- The name/alias of the target, without the `tap-` prefix.
        (default: {"csv"})
        table_name {str} -- The name of the table to sync or "*" to sync all.
        (default: {"*"})
        rescan {bool} -- Optional. True to force a rescan and replace existing metadata.
        (default: False)
        dockerized {bool} -- Optional. True or False to force whether the command is run
        dockerized. If omitted, the best option will be selected automatically.
        taps_dir {str} -- Optional. The directory containing the rules file. (Default=cwd)
        (`{tap-name}.rules.txt`).
        config_dir {str} -- Optional. The default location of config, catalog and other
        potentially sensitive information. (Recommended to be excluded from source control.)
        (Default="${taps_dir}/.secrets")
        config_file {str} -- Optional. The location of the JSON config file which contains
        config for the specified tap. (Default=f"${config_dir}/${plugin_name}-config.json")

        catalog_dir {str} -- Optional. The output directory to be used for saving catalog
        files. If not provided, a path will be generated automatically within `.output` or
        a path specified by the `TAP_SCRATCH_DIR` environment variable.
        target_config_file {str} -- Optional. The location of the JSON config file which contains
        config for the specified target. (Default=f"${config_dir}/${plugin_name}-config.json")
        state_file {str} -- Optional. The path to a state file. If not provided, a state
        file path will be generated automatically within `catalog_dir`.
    """
    if (dockerized is None) and (uio.is_windows() or uio.is_mac()):
        dockerized = True
        logging.info(
            "The 'dockerized' argument is not set when running either Windows or OSX."
            "Attempting to run sync from inside docker."
        )
    if dockerized:
        args = ["plan", tap_name, target_name]
        for var in [
            "table_name",
            "rescan",
            "taps_dir",
            "config_dir",
            "config_file",
            "catalog_dir",
            "target_config_file",
            "state_file",
        ]:
            if var in locals() and locals()[var]:
                args.append(f"--{var}={locals()[var]}")
        docker.rerun_dockerized(tap_name, args=args)
        return
    taps_dir = config.get_taps_dir(taps_dir)
    rules_file = config.get_rules_file(taps_dir, tap_name)
    config_file = config_file or config.get_config_file(f"tap-{tap_name}", config_dir)
    target_config_file = target_config_file or config.get_config_file(
        f"target-{target_name}", config_dir
    )
    catalog_dir = catalog_dir or config.get_catalog_output_dir(tap_name)
    full_catalog_file = f"{catalog_dir}/{tap_name}-catalog-selected.json"
    if rescan or rules_file or not uio.file_exists(full_catalog_file):
        # Create or update `*-catalog-selected.json` and `plan-*.yml` files
        # using the `{tap-name}.rules.txt` rules file
        plans.plan(
            tap_name,
            taps_dir=taps_dir,
            config_file=config_file,
            config_dir=catalog_dir,
            rescan=rescan,
        )
    if table_name is None or table_name == "*":
        list_of_tables = sorted(
            plans._get_catalog_tables_dict(full_catalog_file).keys()
        )
    else:
        list_of_tables = [table_name]

    for table in list_of_tables:
        # Call each tap independently so that table state files are kept separate:
        tmp_catalog_file = f"{catalog_dir}/{tap_name}-{table}-catalog.json"
        table_state_file = (
            state_file
            or config.get_state_file_path()
            or f"{catalog_dir}/{table}-state.json"
        )
        plans._create_single_table_catalog(
            tap_name=tap_name,
            table_name=table,
            full_catalog_file=full_catalog_file,
            output_file=tmp_catalog_file,
        )
        _sync_one_table(
            tap_name=tap_name,
            target_name=target_name,
            table_name=table,
            config_file=config_file,
            target_config_file=target_config_file,
            table_catalog_file=tmp_catalog_file,
            table_state_file=table_state_file,
        )


def _sync_one_table(
    tap_name: str,
    table_name: str,
    config_file: str,
    target_name: str,
    target_config_file: str,
    table_catalog_file: str,
    table_state_file: str,
):
    tap_cmd = f"tap-{tap_name} --config {config_file} --catalog {table_catalog_file}"
    pipeline_version_num = config.get_pipeline_version_number()
    table_state_file = _replace_placeholders(
        {"table_state_file": table_state_file},
        tap_name,
        table_name,
        pipeline_version_num,
    )["table_state_file"]

    if uio.file_exists(table_state_file):
        if uio.is_local(table_state_file):
            local_state_file = table_state_file
        else:
            local_state_file = os.path.join(
                uio.get_scratch_dir(), os.path.basename(table_state_file)
            )
            uio.download_file(table_state_file, local_state_file)
        tap_cmd += f" --state {local_state_file}"
    else:
        local_state_file = os.path.join(
            uio.get_temp_dir(), f"{tap_name}-{table_name}-state.json"
        )
    tmp_target_config = _get_customized_target_config_file(
        target_name,
        target_config_file,
        tap_name=tap_name,
        table_name=table_name,
        pipeline_version_num=pipeline_version_num,
    )
    sync_cmd = (
        f"{tap_cmd} | target-{target_name} --config {tmp_target_config} "
        f">> {local_state_file}"
    )
    runnow.run(sync_cmd)
    # TODO: decide whether trimming to only the final line is necessary
    # tail -1 state.json > state.json.tmp && mv state.json.tmp state.json
    if not uio.file_exists(local_state_file):
        logging.warning(
            f"State file does not exist at path '{local_state_file}'. Skipping upload. "
            f"This can be caused by having no data, or no new data, in the source table."
        )
    elif local_state_file != table_state_file:
        uio.upload_file(local_state_file, table_state_file)


def _get_customized_target_config_file(
    target_name, target_config_file, *, tap_name, table_name, pipeline_version_num,
):
    config_defaults = json.loads(uio.get_text_file_contents(target_config_file))
    new_config = config_defaults.copy()
    if target_name.upper() in S3_TARGET_IDS:
        if (
            "aws_access_key_id" in config_defaults
            and "aws_secret_access_key" in config_defaults
        ):
            logging.info("AWS creds captured from 'target-{target_name}' config")
        else:
            logging.info("Scanning for AWS creds for target 'target-{target_name}'...")
            (
                aws_access_key_id,
                aws_secret_access_key,
                aws_session_token,
            ) = uio.parse_aws_creds()
            if aws_access_key_id and aws_secret_access_key:
                logging.info(
                    f"Passing 'aws_access_key_id' and 'aws_secret_access_key' "
                    f"credentials to 'target-{target_name}'"
                )
                new_config["aws_access_key_id"] = aws_access_key_id
                new_config["aws_secret_access_key"] = aws_secret_access_key
            else:
                logging.warning(
                    f"Could not find 'aws_access_key_id' and 'aws_secret_access_key' "
                    f"credentials for 'target-{target_name}'"
                )
            if aws_session_token:
                logging.info(f"Passing 'aws_session_token' to 'target-{target_name}'")
                new_config["aws_session_token"] = aws_session_token
    new_config = _replace_placeholders(
        new_config, tap_name, table_name, pipeline_version_num
    )
    new_file_path = target_config_file.replace(".json", f"-{table_name}.json")
    uio.create_text_file(new_file_path, json.dumps(new_config))
    return new_file_path


def _replace_placeholders(config_dict, tap_name, table_name, pipeline_version_num):
    new_config = config_dict.copy()
    for setting_name in new_config.keys():
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
