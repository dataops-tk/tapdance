#!bin/env python3

import json
import os
from pathlib import Path
import re
import sys
from typing import List

import uio
import runnow
from logless import logged, logged_block, get_logger
import yaml

BASE_DOCKER_REPO = "dataopstk/tapdance"
SINGER_PLUGINS_INDEX = os.environ.get("SINGER_PLUGINS_INDEX", "./singer_index.yml")
VENV_ROOT = "/venv"
INSTALL_ROOT = "/usr/bin"
_ROOT_DIR = "/projects/my-project"
# _ROOT_DIR = "."

# These plugins will attempt to scrape and pass along AWs Credentials from the local environment.
S3_TARGET_IDS = ["S3-CSV", "REDSHIFT"]

ENV_TAP_SECRETS_DIR = "TAP_SECRETS_DIR"
ENV_TAP_SCRATCH_DIR = "TAP_SCRATCH_DIR"
ENV_TAP_CONFIG_DIR = "TAP_CONFIG_DIR"
ENV_TAP_STATE_FILE = "TAP_STATE_FILE"
ENV_PIPELINE_VERSION_NUMBER = "PIPELINE_VERSION_NUMBER"


logging = get_logger("tapdance")

try:
    import dock_r
except Exception as ex:
    dock_r = None  # type: ignore
    logging.warning(f"Docker libraries were not able to be loaded ({ex}).")


def _get_root_dir():
    # return _ROOT_DIR
    return "."


def _get_secrets_dir():
    result = os.environ.get(ENV_TAP_SECRETS_DIR, f"{_get_root_dir()}/.secrets")
    uio.create_folder(result)
    return result


def _get_scratch_dir():
    result = os.environ.get(ENV_TAP_SCRATCH_DIR, f"{_get_root_dir()}/.output")
    uio.create_folder(result)
    return result


def _get_taps_dir(override=None):
    taps_dir = override or os.environ.get(ENV_TAP_CONFIG_DIR, ".")
    return uio.make_local(taps_dir)  # if remote path provided, download locally


def _get_state_file_path():
    result = os.environ.get(ENV_TAP_STATE_FILE, None)
    if not result:
        logging.warning(
            f"Could not locate env var '{ENV_TAP_STATE_FILE}'. "
            f"State may not be maintained."
        )
    return result


def _get_pipeline_version_number():
    return os.environ.get(ENV_PIPELINE_VERSION_NUMBER, "1")


def _get_catalog_output_dir(tap_name):
    result = f"{_get_scratch_dir()}/taps/{tap_name}-catalog"
    uio.create_folder(result)
    return result


def _get_plan_file(tap_name, taps_dir=None):
    return os.path.join(_get_taps_dir(taps_dir), f"data-plan-{tap_name}.yml")


def _get_rules_file(taps_dir=None):
    return os.path.join(_get_taps_dir(taps_dir), f"data.select")


def _get_catalog_tables_dict(catalog_file):
    catalog_full = json.loads(Path(catalog_file).read_text())
    table_objects = {s["stream"]: s for s in catalog_full["streams"]}
    return table_objects


def _get_catalog_table_columns(table_object):
    return table_object["schema"]["properties"].keys()


def _get_config_file(plugin_name, config_dir=None):
    """
    Return a path to the configuration file which also contains secrets.

     - If file is blank or does not exist at the default secrets path, a new file will be created.
     - If any environment variables exist in the form of TAP_MY_TAP_my_setting, a new file
    will be created which contains these settings.
     - If the default file exists and environment variables also exist, the temp file will
    contain the default file values along with the environment variable overrides.
    """
    secrets_path = config_dir or _get_secrets_dir()
    default_path = f"{secrets_path}/{plugin_name}-config.json"
    tmp_path = f"{secrets_path}/tmp/{plugin_name}-config.json"
    use_tmp_file = False
    if uio.file_exists(default_path):
        json_text = uio.get_text_file_contents(default_path)
        conf_dict = json.loads(json_text)
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
        return tmp_path
    return default_path


@logged(
    "installing '{plugin_name}' as '{alias or plugin_name}' "
    "using 'pip3 install {source or plugin_name}'"
)
def install(plugin_name: str, source: str = None, alias: str = None):
    """
    Install the requested plugin to the local machine.

    Arguments:
        plugin_name {str} -- The name of the plugin to install, including the tap- or
        target- prefix.

    Keyword Arguments:
        source {str} -- Optional. Overrides the pip installation source.
        alias {str} -- Optional. Overrides the name (alias) of the plugin.

    Raises:
        RuntimeError: [description]
    """
    source = source or plugin_name
    alias = alias or plugin_name

    venv_dir = os.path.join(VENV_ROOT, alias)
    install_path = os.path.join(INSTALL_ROOT, alias)
    if uio.file_exists(install_path):
        response = input(
            f"The file '{install_path}' already exists. "
            f"Are you sure you want to replace this file? [y/n]"
        )
        if not response.lower() in ["y", "yes"]:
            raise RuntimeError(f"File already exists '{install_path}'.")
        uio.delete_file(install_path)
    runnow.run(f"python3 -m venv {venv_dir}")
    runnow.run(f"{os.path.join(venv_dir ,'bin', 'pip3')} install {source}")
    runnow.run(f"ln -s {venv_dir}/bin/{plugin_name} {install_path}")


@logged("Updating plan file for 'tap-{tap_name}'")
def plan(
    tap_name: str,
    *,
    rescan: bool = None,
    taps_dir: str = None,
    config_dir: str = None,
    config_file: str = None,
):
    """
    Perform all actions necessary to prepare (plan) for a tap execution.

     1. Scan (discover) the source system metadata (if catalog missing or `rescan=True`)
     2. Apply filter rules from `rules_file` and create human-readable `plan.yml` file to
        describe planned inclusions/exclusions.
     2. Create a new `catalog-selected.json` file which applies the plan file and which
        can be used by the tap to run data extractions.

    Arguments:
        tap_name {str} -- [description]

    Keyword-Only Arguments:
        rescan {bool} -- Optional. True to force a rescan and replace existing metadata.
        (default: False)
        taps_dir {str} -- Optional. The directory containing the rules file. (Default=cwd)
        (`data.select`).
        config_dir {str} -- Optional. The default location of config, catalog and other
        potentially sensitive information. (Recommended to be excluded from source control.)
        (Default="${taps_dir}/.secrets")
        config_file {str} -- Optional. The location of the JSON config file which contains
        config for the specified plugin. (Default=f"${config_dir}/${plugin_name}-config.json")
    """

    if uio.is_windows() or uio.is_mac():
        _rerun_dockerized(tap_name)
        return
    taps_dir = _get_taps_dir(taps_dir)
    config_file = config_file or _get_config_file(
        f"tap-{tap_name}", config_dir=config_dir
    )
    catalog_dir = _get_catalog_output_dir(tap_name)
    catalog_file = f"{catalog_dir}/{tap_name}-catalog-raw.json"
    selected_catalog_file = f"{catalog_dir}/{tap_name}-catalog-selected.json"
    plan_file = _get_plan_file(tap_name, taps_dir)
    if (
        uio.file_exists(catalog_file)
        and uio.get_text_file_contents(catalog_file).strip() == ""
    ):
        logging.info(f"Cleaning up empty catalog file: {catalog_file}")
        uio.delete_file(catalog_file)
    if rescan or not uio.file_exists(catalog_file):
        _discover(tap_name, config_file, catalog_dir)
    rules_file = _get_rules_file(taps_dir)
    select_rules = [
        line.split("#")[0].rstrip()
        for line in uio.get_text_file_contents(rules_file).splitlines()
        if line.split("#")[0].rstrip()
    ]
    matches = {}
    excluded_tables = []
    for table_name, table_object in _get_catalog_tables_dict(catalog_file).items():
        table_match_text = f"{tap_name}.{table_name}"
        if _table_match_check(table_match_text, select_rules):
            matches[table_name] = {}
            for col_object in _get_catalog_table_columns(table_object):
                col_name = col_object
                col_match_text = f"{tap_name}.{table_name}.{col_name}"
                matches[table_name][col_name] = _col_match_check(
                    col_match_text, select_rules
                )
        else:
            excluded_tables.append(table_name)
    sorted_tables = sorted(matches.keys())
    file_text = ""
    file_text += f"selected_tables:\n"
    for table in sorted_tables:
        included_cols = [col for col, selected in matches[table].items() if selected]
        ignored_cols = [col for col, selected in matches[table].items() if not selected]
        file_text += f"{'  ' * 1}{table}:\n"
        file_text += f"{'  ' * 2}selected_columns:\n"
        for col in included_cols:
            file_text += f"{'  ' * 2}- {col}\n"
        if ignored_cols:
            file_text += f"{'  ' * 2}ignored_columns:\n"
            for col in ignored_cols:
                file_text += f"{'  ' * 2}- {col}\n"
    if excluded_tables:
        file_text += f"ignored_tables:\n"
        for table in sorted(excluded_tables):
            file_text += f"{'  ' * 1}- {table}\n"
    uio.create_text_file(plan_file, file_text)
    _create_selected_catalog(
        tap_name,
        plan_file=plan_file,
        full_catalog_file=catalog_file,
        output_file=selected_catalog_file,
    )


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
    exclude_tables: List[str] = None,
):
    """
    Synchronize data from tap to target.

    Arguments:
        tap_name {str} -- The name/alias of the source tap, without the `tap-` prefix.

    Keyword Arguments:
        target_name {str} -- The name/alias of the target, without the `tap-` prefix.
        (default: {"csv"})
        table_name {str} -- The name of the table to sync. To sync multiple tables, specify
        a comma-separated list of tables surrounded by queare brackets (e.g. "[tb1,tbl2]"),
        or use "*" to sync all table.
        (default: {"*"})
        rescan {bool} -- Optional. True to force a rescan and replace existing metadata.
        (default: False)
        dockerized {bool} -- Optional. True or False to force whether the command is run
        dockerized. If omitted, the best option will be selected automatically.
        taps_dir {str} -- Optional. The directory containing the rules file. (Default=cwd)
        (`data.select`).
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
        exclude_table_names {List(str)} -- Optional. A list of tables to exclude. Ignored
        if table_name arg is not "*".
    """
    if dockerized is None:
        if uio.is_windows() or uio.is_mac():
            logging.info(
                "The 'dockerized' argument is not set when running either Windows or OSX."
                "Attempting to run sync from inside docker."
            )
            _rerun_dockerized(tap_name, target_name)
            return
    taps_dir = _get_taps_dir(taps_dir)
    rules_file = _get_rules_file(taps_dir)
    config_file = config_file or _get_config_file(f"tap-{tap_name}", config_dir)
    target_config_file = target_config_file or _get_config_file(
        f"target-{target_name}", config_dir
    )
    catalog_dir = catalog_dir or _get_catalog_output_dir(tap_name)
    full_catalog_file = f"{catalog_dir}/{tap_name}-catalog-selected.json"
    if rescan or rules_file or not uio.file_exists(full_catalog_file):
        # Create or update `*-catalog-selected.json` and `plan-*.yml` files
        # using the `data.select` rules file
        plan(
            tap_name,
            taps_dir=taps_dir,
            config_file=config_file,
            config_dir=catalog_dir,
            rescan=rescan,
        )
    if isinstance(table_name, list):
        list_of_tables = table_name
    elif table_name is None or table_name == "*":
        list_of_tables = sorted(_get_catalog_tables_dict(full_catalog_file).keys())
    elif table_name[0] == "[":
        # Remove square brackets and split the result on commas
        list_of_tables = table_name.replace("[", "").replace("]", "").split(",")
    else:
        list_of_tables = [table_name]
    if exclude_tables:
        logging.info(f"Table(s) to exclude from sync: {', '.join(exclude_tables)}")
        list_of_tables = [t for t in list_of_tables if t not in exclude_tables]

    logging.info(f"Table(s) to sync: {', '.join(list_of_tables)}")
    for table in list_of_tables:
        # Call each tap independently so that table state files are kept separate:
        tmp_catalog_file = f"{catalog_dir}/{tap_name}-{table}-catalog.json"
        table_state_file = (
            state_file or _get_state_file_path() or f"{catalog_dir}/{table}-state.json"
        )
        _create_single_table_catalog(
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


def build_all_images(push: bool = False, pre: bool = False, ignore_cache: bool = False):
    """
    Build all images.

    :param push: Push images after building
    :param pre: Create and publish pre-release builds
    :param ignore_cache: True to build images without cached image layers. (default: {False})
    """
    _build_all_standalone(push=push, pre=pre, ignore_cache=ignore_cache)
    _build_all_composite(push=push, pre=pre, ignore_cache=ignore_cache)


def build_image(
    tap_or_plugin_alias: str,
    target_alias: str = None,
    push: bool = False,
    pre: bool = False,
    ignore_cache: bool = False,
):
    """
    Build a single image. If tap and target are both provided, any required upstream images
    will be built as well.

    Arguments:
        tap_or_plugin_alias {str} -- The name of the tap (without the `tap-` prefix).

    Keyword Arguments:
        target_alias {str} -- Optional. The name of the target (without the `target-` prefix).
        push {bool} -- True to push images to image repository after build. (default: {False})
        pre {bool} -- True to use and create prelease versions. (default: {False})
        ignore_cache {bool} -- True to build images without cached image layers. (default: {False})
    """
    name, source, alias = _get_plugin_info(f"tap-{tap_or_plugin_alias}")
    _build_plugin_image(
        name, source=source, alias=alias, push=push, pre=pre, ignore_cache=ignore_cache
    )
    if target_alias:
        name, source, alias = _get_plugin_info(f"target-{target_alias}")
        _build_plugin_image(
            name,
            source=source,
            alias=alias,
            push=push,
            pre=pre,
            ignore_cache=ignore_cache,
        )
        _build_composite_image(
            tap_alias=tap_or_plugin_alias,
            target_alias=target_alias,
            push=push,
            pre=pre,
            ignore_cache=ignore_cache,
        )


@logged("running discovery on '{tap_name}'")
def _discover(tap_name: str, config_file: str = None, catalog_dir: str = None):
    if uio.is_windows() or uio.is_mac():
        _rerun_dockerized(tap_name)
        return
    config_file = config_file or _get_config_file(f"tap-{tap_name}")
    catalog_dir = catalog_dir or _get_catalog_output_dir(tap_name)
    catalog_file = f"{catalog_dir}/{tap_name}-catalog-raw.json"
    uio.create_folder(catalog_dir)
    runnow.run(f"tap-{tap_name} --config {config_file} --discover > {catalog_file}")


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
    pipeline_version_num = _get_pipeline_version_number()
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


def _table_match_check(match_text: str, select_rules: list):
    selected = False
    for rule in select_rules:
        result = _check_table_rule(match_text, rule)
        if result is True:
            selected = True
        elif result is False:
            selected = False
    return selected


def _col_match_check(match_text: str, select_rules: list):
    selected = False
    for rule in select_rules:
        result = _check_column_rule(match_text, rule)
        if result is True:
            selected = True
        elif result is False:
            selected = False
    return selected


def _is_match(value, pattern):
    if not pattern:
        return None
    if value.lower() == pattern.lower():
        return True
    if pattern == "*":
        return True
    re_pattern = None
    if "/" in pattern:
        if pattern[0] == "/" and pattern[-1] == "/":
            re_pattern = pattern[1:-1]
            re_pattern = f"\\b{re_pattern}\\b"
            # logging.info(f"Found regex pattern: {pattern}")
    elif "*" in pattern:
        # logging.info(f"Found wildcard pattern: {pattern}")
        re_pattern = pattern.replace("*", ".*")
    if re_pattern:
        # logging.info(f"Checking regex pattern: {re_pattern}")
        result = re.search(re_pattern.lower(), value.lower())
        if result:
            # logging.info(f"Matched regex pattern '{re_pattern}' on '{value}'")
            return True
    return False


def _check_column_rule(match_text: str, rule_text: str):
    """Check rule. Returns True to include, False to exclude, or None if not a match."""
    if rule_text[0] == "!":
        match_result = False  # Exclude if matched
        rule_text = rule_text[1:]
    else:
        match_result = True  # Include if matched
    rule_text = rule_text.replace("**.", "*.*.")
    tap_match, table_match, column_match = (
        rule_text.split(".")[0],
        rule_text.split(".")[1],
        ".".join(rule_text.split(".")[2:]),
    )
    if not _is_match(match_text.split(".")[0], tap_match):
        return None
    if not _is_match(match_text.split(".")[1], table_match):
        return None
    if not _is_match(match_text.split(".")[2], column_match):
        return None
    # logging.info(
    #     f"Column '{match_text}' matched column filter '{column_match}' in '{rule_text}'"
    # )
    return match_result


def _check_table_rule(match_text: str, rule_text: str):
    """Check rule. Returns True to include, False to exclude, or None if not a match."""
    if rule_text[0] == "!":
        match_result = False  # Exclude if matched
        rule_text = rule_text[1:]
    else:
        match_result = True  # Include if matched
    rule_text = rule_text.replace("**.", "*.*.")
    if "*.*." in rule_text:
        return None  # Global column rules do not apply to tables
    if match_result is True and rule_text[-2:] == ".*":
        rule_text = rule_text[:-2]
    if len(rule_text.split(".")) > 2:
        return None  # Column rules do not apply to tables
    tap_name, table_name = (
        match_text.split(".")[0],
        ".".join(match_text.split(".")[1:]),
    )
    tap_rule, table_rule = (rule_text.split(".")[0], ".".join(rule_text.split(".")[1:]))
    if not _is_match(tap_name, tap_rule):
        return None
    if not _is_match(table_name, table_rule):
        return None
    # Table '{match_text}' matched table filter '{table_rule}' in '{rule_text}'"
    return match_result


@logged(
    "selecting catalog metadata "
    "from '{tap_name}' source catalog file: {full_catalog_file}"
)
def _create_selected_catalog(
    tap_name, plan_file=None, full_catalog_file=None, output_file=None
):
    catalog_dir = _get_catalog_output_dir(tap_name)
    source_catalog_path = full_catalog_file or os.path.join(
        catalog_dir, "catalog-raw.json"
    )
    output_file = output_file or os.path.join(catalog_dir, f"selected-catalog.json")
    catalog_full = json.loads(Path(source_catalog_path).read_text())
    plan_file = plan_file or _get_plan_file(tap_name)
    plan = yaml.safe_load(uio.get_text_file_contents(plan_file))
    included_table_objects = []
    for tbl in catalog_full["streams"]:
        stream_name = tbl["stream"]
        if stream_name in plan["selected_tables"].keys():
            _select_table(tbl)
            for col_name in _get_catalog_table_columns(tbl):
                col_selected = (
                    col_name in plan["selected_tables"][stream_name]["selected_columns"]
                )
                _select_table_column(tbl, col_name, col_selected)
            included_table_objects.append(tbl)
    catalog_new = {"streams": included_table_objects}
    with open(output_file, "w") as f:
        json.dump(catalog_new, f, indent=2)


def _select_table(tbl: dict):
    for metadata in tbl["metadata"]:
        if len(metadata["breadcrumb"]) == 0:
            metadata["metadata"]["selected"] = True


def _select_table_column(tbl: dict, col_name: str, selected: bool):
    for metadata in tbl["metadata"]:
        if (
            len(metadata["breadcrumb"]) >= 2
            and metadata["breadcrumb"][0] == "properties"
            and metadata["breadcrumb"][1] == col_name
        ):
            metadata["metadata"]["selected"] = selected
            return
    tbl["metadata"].append(
        {"breadcrumb": ["properties", col_name], "metadata": {"selected": selected}}
    )
    return


@logged(
    "selecting '{table_name}' catalog metadata "
    "from '{tap_name}' source catalog file: {full_catalog_file}"
)
def _create_single_table_catalog(
    tap_name, table_name, full_catalog_file=None, output_file=None
):
    catalog_dir = _get_catalog_output_dir(tap_name)
    source_catalog_path = full_catalog_file or os.path.join(
        catalog_dir, "catalog-selected.json"
    )
    output_file = output_file or os.path.join(catalog_dir, f"{table_name}-catalog.json")
    included_table_objects = []
    catalog_full = json.loads(Path(source_catalog_path).read_text())
    for tbl in catalog_full["streams"]:
        stream_name = tbl["stream"]
        if stream_name == table_name:
            for metadata in tbl["metadata"]:
                if len(metadata["breadcrumb"]) == 0:
                    metadata["metadata"]["selected"] = True
            included_table_objects.append(tbl)
    catalog_new = {"streams": included_table_objects}
    with open(output_file, "w") as f:
        json.dump(catalog_new, f, indent=2)


def _get_docker_tap_image(tap_alias, target_alias=None):
    if tap_alias.startswith("tap-"):
        tap_alias = tap_alias.replace("tap-", "")
    if not target_alias:
        return f"{BASE_DOCKER_REPO}:tap-{tap_alias}"
    if target_alias.startswith("target-"):
        target_alias = target_alias.replace("target-", "")
    return f"{BASE_DOCKER_REPO}:{tap_alias}-to-{target_alias}"


def _rerun_dockerized(tap_alias, target_alias=None):
    cmd = f"tapdance {' '.join(sys.argv[1:])}"
    env = {
        k: v
        for k, v in os.environ.items()
        if (k.startswith("TAP_") or k.startswith("TARGET_"))
    }
    image_name = _get_docker_tap_image(tap_alias, target_alias)
    try:
        dock_r.pull(image_name)
    except Exception as ex:
        logging.warning(f"Could not pull docker image '{image_name}'. {ex}")
    with logged_block(f"running dockerized command '{cmd}' on image '{image_name}'"):

        def _build_docker_run(image, command, environment, working_dir, volumes):
            e_str = " ".join([f"-e {k}={v}" for k, v in environment.items()])
            w_str = f"-w {working_dir}" if working_dir else ""
            v_str = " ".join([f"-v {x}:{y}" for x, y in volumes.items()])
            docker_run_cmd = f"docker run {e_str} {v_str} {w_str} {image} {command}"
            return docker_run_cmd

        volumes = {os.path.abspath("."): "/projects/my-project"}
        # DEV_DEBUG = False  # Used to test while developing, override python lib path
        # if DEV_DEBUG:
        #     container_lib = "/usr/local/lib/python3.8/site-packages/tapdance"
        #     host_lib = "C:\\Files\\Source\\tapdance\\tapdance"
        #     volumes[host_lib] = container_lib
        docker_run_cmd = _build_docker_run(
            image=image_name,
            command=cmd,
            environment=env,
            working_dir="/projects/my-project",
            volumes=volumes,
        )
        runnow.run(docker_run_cmd)
    return True


def _get_plugins_list(plugins_index=None):
    plugins_index = plugins_index or SINGER_PLUGINS_INDEX
    if not uio.file_exists(plugins_index):
        raise RuntimeError(
            f"No file found at '{plugins_index}'."
            "Please set SINGER_PLUGINS_INDEX and try again."
        )
    yml_doc = yaml.safe_load(uio.get_text_file_contents(plugins_index))
    taps = yml_doc["singer-taps"]
    list_of_tuples = []
    taps = yml_doc["singer-taps"]
    targets = yml_doc["singer-targets"]
    plugins = taps + targets
    for plugin in plugins:
        list_of_tuples.append(
            (plugin["name"], plugin.get("source", None), plugin.get("alias", None))
        )
    return list_of_tuples


def _build_all_standalone(
    source_image=None, plugins_index=None, push=False, pre=False, ignore_cache=False
):
    plugins = _get_plugins_list(plugins_index)
    created_images = []
    for name, source, alias in plugins:
        created_images.append(
            _build_plugin_image(
                name,
                source=source,
                alias=alias,
                source_image=source_image,
                push=push,
                pre=pre,
                ignore_cache=ignore_cache,
            )
        )
    return created_images


def _get_plugin_info(plugin_id, plugins_index=None):
    plugins = _get_plugins_list(plugins_index)
    for name, source, alias in plugins:
        if (alias or name) == plugin_id:
            return (name, source, alias)
    raise ValueError(f"Could not file a plugin called '{plugin_id}'")


def _build_all_composite(
    source_image=None, plugins_index=None, push=False, pre=False, ignore_cache=False
):
    plugins = _get_plugins_list(plugins_index)
    created_images = []
    for tap_name, tap_source, tap_alias in plugins:
        tap_alias = tap_alias or tap_name
        for target_name, target_source, target_alias in plugins:
            target_alias = target_alias or target_name
            if tap_alias.startswith("tap-") and target_alias.startswith("target-"):
                created_images.append(
                    _build_composite_image(
                        tap_alias,
                        target_alias,
                        push=push,
                        pre=pre,
                        ignore_cache=ignore_cache,
                    )
                )
    return created_images


def _build_plugin_image(
    plugin_name,
    source,
    alias,
    source_image=None,
    push=False,
    pre=False,
    ignore_cache=False,
):
    source = source or plugin_name
    alias = alias or plugin_name
    image_name = f"{BASE_DOCKER_REPO}:{alias}"
    build_cmd = f"docker build"
    if ignore_cache:
        build_cmd += " --no-cache"
    if source_image:
        build_cmd += f" --build-arg source_image={source_image}"
    if pre:
        build_cmd += f" --build-arg prerelease=true"
        image_name += "--pre"
    build_cmd += (
        f" --build-arg PLUGIN_NAME={plugin_name}"
        f" --build-arg PLUGIN_SOURCE={source}"
        f" --build-arg PLUGIN_ALIAS={alias}"
        f" -t {image_name}"
        f" -f singer-plugin.Dockerfile"
        f" ."
    )
    runnow.run(build_cmd)
    if push:
        _push(image_name)
    return image_name


def _build_composite_image(
    tap_alias, target_alias, push=False, pre=False, ignore_cache=False
):
    if tap_alias.startswith("tap-"):
        tap_alias = tap_alias.replace("tap-", "", 1)
    if target_alias.startswith("target-"):
        target_alias = target_alias.replace("target-", "", 1)
    image_name = f"{BASE_DOCKER_REPO}:{tap_alias}-to-{target_alias}"
    build_cmd = f"docker build"
    if ignore_cache:
        build_cmd += " --no-cache"
    if pre:
        build_cmd += " --build-arg source_image_suffix=--pre"
        image_name += "--pre"
    build_cmd += (
        f" --build-arg tap_alias={tap_alias}"
        f" --build-arg target_alias={target_alias}"
        f" -t {image_name}"
        f" -f tap-to-target.Dockerfile"
        f" ."
    )
    runnow.run(build_cmd)
    if push:
        _push(image_name)
    return image_name


def _push(image_name):
    runnow.run(f"docker push {image_name}")
