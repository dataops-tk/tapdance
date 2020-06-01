"""tapdance.plans - Defines plan() function and discovery helper functions."""

import json
import os
from pathlib import Path
import re
from typing import Dict, List, Tuple
import yaml

import uio
from logless import logged, get_logger
import runnow

# from tapdance.paths import ENV_TAP_STATE_FILE, ENV_TAP_CONFIG_DIR
from tapdance import docker, config

logging = get_logger("tapdance")

USE_2PART_RULES = True


@logged("running discovery on '{tap_name}'")
def _discover(tap_name: str, config_file: str = None, catalog_dir: str = None):
    config_file = config_file or config.get_config_file(f"tap-{tap_name}")
    catalog_dir = catalog_dir or config.get_catalog_output_dir(tap_name)
    catalog_file = f"{catalog_dir}/{tap_name}-catalog-raw.json"
    uio.create_folder(catalog_dir)
    runnow.run(f"tap-{tap_name} --config {config_file} --discover > {catalog_file}")


def _check_rules(
    catalog_file: str, rules_file: List[str]
) -> Tuple[Dict[str, Dict[str, str]], List[str]]:
    # Check rules_file to fill `matches`
    select_rules = [
        line.split("#")[0].rstrip()
        for line in uio.get_text_file_contents(rules_file).splitlines()
        if line.split("#")[0].rstrip()
    ]
    matches: Dict[str, dict] = {}
    excluded_table_list = []
    for table_name, table_object in _get_catalog_tables_dict(catalog_file).items():
        table_match_text = f"{table_name}"
        if _table_match_check(table_match_text, select_rules):
            matches[table_name] = {}
            for col_object in _get_catalog_table_columns(table_object):
                col_name = col_object
                col_match_text = f"{table_name}.{col_name}"
                matches[table_name][col_name] = _col_match_check(
                    col_match_text, select_rules
                )
        else:
            excluded_table_list.append(table_name)
    return matches, excluded_table_list


@logged("Updating plan file for 'tap-{tap_name}'")
def plan(
    tap_name: str,
    *,
    rescan: bool = None,
    taps_dir: str = None,
    config_dir: str = None,
    config_file: str = None,
    dockerized: bool = None,
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
        (`{tap-name}.rules.txt`).
        config_dir {str} -- Optional. The default location of config, catalog and other
        potentially sensitive information. (Recommended to be excluded from source control.)
        (Default="${taps_dir}/.secrets")
        config_file {str} -- Optional. The location of the JSON config file which contains
        config for the specified plugin. (Default=f"${config_dir}/${plugin_name}-config.json")
        dockerized {bool} -- Optional. If specified, will override the default behavior for
        the local platform.
    """
    if (dockerized is None) and (uio.is_windows() or uio.is_mac()):
        dockerized = True
        logging.info(
            "The 'dockerized' argument is not set when running either Windows or OSX."
            "Attempting to run sync from inside docker."
        )
    if dockerized:
        args = ["plan", tap_name]
        for var in [
            "rescan",
            "taps_dir",
            "config_dir",
            "config_file",
        ]:
            if var in locals() and locals()[var]:
                args.append(f"--{var}={locals()[var]}")
        docker.rerun_dockerized(tap_name, args=args)
        return

    # Initialize paths
    taps_dir = config.get_taps_dir(taps_dir)
    config_file = config_file or config.get_config_file(
        f"tap-{tap_name}", config_dir=config_dir
    )
    catalog_dir = config.get_catalog_output_dir(tap_name)
    catalog_file = f"{catalog_dir}/{tap_name}-catalog-raw.json"
    if not uio.file_exists(config_file):
        raise FileNotFoundError(config_file)
    if not uio.file_exists(catalog_file):
        raise FileNotFoundError(catalog_file)
    selected_catalog_file = f"{catalog_dir}/{tap_name}-catalog-selected.json"
    plan_file = config.get_plan_file(tap_name, taps_dir)
    if (
        uio.file_exists(catalog_file)
        and uio.get_text_file_contents(catalog_file).strip() == ""
    ):
        logging.info(f"Cleaning up empty catalog file: {catalog_file}")
        uio.delete_file(catalog_file)

    if rescan or not uio.file_exists(catalog_file):
        # Run discover, if needed, to get catalog.json (raw)
        _discover(tap_name, config_file, catalog_dir)

    rules_file = config.get_rules_file(taps_dir, tap_name)
    matches, excluded_tables = _check_rules(
        catalog_file=catalog_file, rules_file=rules_file
    )

    file_text = _make_plan_file_text(matches, excluded_tables)
    uio.create_text_file(plan_file, file_text)

    _create_selected_catalog(
        tap_name,
        plan_file=plan_file,
        full_catalog_file=catalog_file,
        output_file=selected_catalog_file,
    )


def _make_plan_file_text(
    matches: Dict[str, Dict[str, str]], excluded_tables_list: List[str]
) -> str:
    sorted_tables = sorted(matches.keys())

    file_text = ""
    file_text += "selected_tables:\n"
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
    if excluded_tables_list:
        file_text += "ignored_tables:\n"
        for table in sorted(excluded_tables_list):
            file_text += f"{'  ' * 1}- {table}\n"
    return file_text


def _get_catalog_table_columns(table_object):
    return table_object["schema"]["properties"].keys()


def _get_catalog_tables_dict(catalog_file: str) -> dict:
    catalog_full = json.loads(Path(catalog_file).read_text())
    table_objects = {s["stream"]: s for s in catalog_full["streams"]}
    return table_objects


@logged(
    "selecting catalog metadata "
    "from '{tap_name}' source catalog file: {full_catalog_file}"
)
def _create_selected_catalog(
    tap_name, plan_file=None, full_catalog_file=None, output_file=None
):
    catalog_dir = config.get_catalog_output_dir(tap_name)
    source_catalog_path = full_catalog_file or os.path.join(
        catalog_dir, "catalog-raw.json"
    )
    output_file = output_file or os.path.join(catalog_dir, "selected-catalog.json")
    catalog_full = json.loads(Path(source_catalog_path).read_text())
    plan_file = plan_file or config.get_plan_file(tap_name)
    plan = yaml.safe_load(uio.get_text_file_contents(plan_file))
    if ("selected_tables" not in plan) or (plan["selected_tables"] is None):
        raise ValueError(f"No selected tables found in plan file '{plan_file}'.")
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
    catalog_dir = config.get_catalog_output_dir(tap_name)
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
    table_match = rule_text.split(".")[0]
    column_match = ".".join(rule_text.split(".")[1:])
    if not _is_match(match_text.split(".")[0], table_match):
        # Non-matching table part; skip!
        return None
    if not _is_match(match_text.split(".")[1], column_match):
        # Non-matching column part; skip!
        return None
    return match_result


def _check_table_rule(match_text: str, rule_text: str):
    """Check rule. Returns True to include, False to exclude, or None if not a match."""
    if rule_text[0] == "!":
        match_result = False  # Exclude if matched
        rule_text = rule_text[1:]
    else:
        match_result = True  # Include if matched
    if USE_2PART_RULES:
        if match_result is True and rule_text[-2:] == ".*":
            # Ignore the "all columns" spec for table selection
            rule_text = rule_text[:-2]
        if rule_text[:2] == "*.":
            # Global column rules do not apply to tables
            return None
    else:
        rule_text = rule_text.replace("**.", "*.*.")
        if "*.*." in rule_text:
            # Global column rules do not apply to tables
            return None
    if len(rule_text.split(".")) > 1:
        # Column rules do not affect table inclusion
        return None
    if USE_2PART_RULES:
        table_name = match_text.split(".")[0]
        table_rule = rule_text.split(".")[0]
    else:  # Using 3-part rules, including tap name as first part
        tap_name = match_text.split(".")[0]
        tap_rule = rule_text.split(".")[0]
        table_name = ".".join(match_text.split(".")[1:])
        table_rule = ".".join(rule_text.split(".")[1:])
        if not _is_match(tap_name, tap_rule):
            return None
    if not _is_match(table_name, table_rule):
        return None
    # Table '{match_text}' matched table filter '{table_rule}' in '{rule_text}'"
    return match_result
