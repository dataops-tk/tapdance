"""tapdance.plans - Defines plan() function and discovery helper functions."""

import json
import os
from pathlib import Path
import re
from typing import Dict, List, Tuple, Optional, Union
import yaml

import uio
from logless import logged, get_logger
import runnow

# from tapdance.paths import ENV_TAP_STATE_FILE, ENV_TAP_CONFIG_DIR
from tapdance import docker, config

logging = get_logger("tapdance")

USE_2PART_RULES = True
SKIP_SENSELESS_VALIDATORS = (
    True
    # Ignore senseless schema validation rules, e.g. 'multipleOf', etc.
    # These only fail when our source is internally incoherent, which alone
    # is almost never sufficient cause for failing the data extraction.
)
SMALL_TABLE_THRESHOLD = 100000  # Warn if >= 10,000 rows and no replication key


def _is_valid_json(json_text: str):
    try:
        _ = json.loads(json_text)
    except ValueError:
        return False
    return True


@logged("running discovery on '{tap_name}'")
def _discover(
    tap_name: str,
    *,
    config_file: str,
    catalog_dir: str,
    dockerized: bool,
    tap_exe: str,
) -> None:
    catalog_file = config.get_raw_catalog_file(catalog_dir, tap_name)
    uio.create_folder(catalog_dir)
    img = f"{docker.BASE_DOCKER_REPO}:{tap_exe}"
    if dockerized:
        cdw = os.getcwd().replace("\\", "/")
        config_file = os.path.relpath(config_file).replace("\\", "/")
        config_file = f"/home/local/{config_file}"
        _, _ = runnow.run(f"docker pull {img}")
        _, output_text = runnow.run(
            f"docker run --rm -it "
            f"-v {cdw}:/home/local "
            f"{img} --config {config_file} --discover",
            echo=False,
            capture_stderr=False,
        )
        if not _is_valid_json(output_text):
            raise RuntimeError(f"Could not parse json file from output:\n{output_text}")
        uio.create_text_file(catalog_file, output_text)
    else:
        runnow.run(f"{tap_exe} --config {config_file} --discover > {catalog_file}")


def _check_rules(
    catalog_file: str, rules_file: List[str]
) -> Tuple[Dict[str, Dict[str, bool]], List[str]]:
    """Evaluate rules against the contents of a catalog file.

    Parameters
    ----------
    catalog_file : str
        Path to a catalog file.
    rules_file : List[str]
        Path to a rules file.

    Returns
    -------
    Tuple[Dict[str, Dict[str, bool]], List[str]]
        - Dictionary of tables names to dictionary of column names having values of True
          (selected) or False (ignored)
        - List of excluded tables
    """
    select_rules = [
        line.split("#")[0].rstrip()
        for line in uio.get_text_file_contents(rules_file).splitlines()
        if line.split("#")[0].rstrip()
    ]
    declared_tables = set(
        [
            rule.split(".")[0].rstrip()
            for rule in select_rules
            if rule.split(".")[0].rstrip() and ("*" not in rule.split(".")[0].rstrip())
        ]
    )
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
    for required_table in declared_tables:
        if required_table not in matches.keys():
            logging.warning(
                f"The table {required_table} was declared in the rules file "
                "but could not be found in the catalog."
            )
    for match, match_cols in matches.items():
        if not match_cols:
            logging.warning(
                f"The table {match} was declared in the rules file "
                "but did not match with any columns in the catalog."
            )
    return matches, excluded_table_list


def _validate_keys(table_object: dict, key_type: str):
    _ = _get_table_key_cols(key_type, table_object, warn_if_missing=True)


def _get_table_key_cols(
    key_type: str, table_object: dict, warn_if_missing: bool, log_fn=None
):
    result = []
    metadata_object = _get_stream_metadata_object(table_object)
    row_count: Optional[int] = metadata_object.get("row-count", None)
    row_count_desc = f" (est. {row_count:,} rows)" if row_count else ""
    view_warning = f"(is-view=TRUE)" if metadata_object.get("is-view", None) else ""
    if key_type == "replication-key":
        if not log_fn:
            if row_count and row_count < SMALL_TABLE_THRESHOLD:
                log_fn = logging.debug
            else:
                log_fn = logging.warning
        if "valid-replication-keys" in metadata_object:
            result = metadata_object["valid-replication-keys"]
    elif key_type == "primary-key":
        log_fn = log_fn or logging.warning
        if "table-key-properties" in metadata_object:
            result = metadata_object["table-key-properties"]
    else:
        raise ValueError("Expected key_type of 'primary-key' or 'replication-key'")
    if not result and warn_if_missing:
        table_name = table_object.get("stream", "(unknown stream)")
        log_fn(
            " ".join(
                [
                    f"Could not locate '{key_type}' for '{table_name}'",
                    row_count_desc,
                    view_warning,
                ]
            )
        )
    return result


def _set_catalog_file_keys(table_object: dict, table_plan: dict):
    metadata = _get_stream_metadata_object(table_object)
    table_name = _get_stream_name(table_object)
    if table_plan.get("primary_key"):
        if table_plan.get("primary_key") != metadata.get("table-key-properties", []):
            logging.info(
                f"Overriding primary key columns for '{table_name}': "
                + str(table_plan["primary_key"])
                + f" (was "
                + (
                    str(metadata.get("table-key-properties"))
                    if metadata.get("table-key-properties")
                    else "blank"
                )
                + ")"
            )
            metadata["table-key-properties"] = table_plan["primary_key"]
    if table_plan.get("replication_key"):
        if table_plan.get("replication_key") != metadata.get("valid-replication-keys"):
            logging.info(
                f"Overriding replication key columns for '{table_name}': "
                + str(table_plan["replication_key"])
                + f" (was "
                + (
                    str(metadata.get("valid-replication-keys"))
                    if metadata.get("valid-replication-keys")
                    else "blank"
                )
                + ")"
            )
            metadata["valid-replication-keys"] = table_plan["replication_key"]
            metadata["replication-key"] = table_plan["replication_key"][0]


def _get_catalog_file_keys(
    key_type: str,
    matches: Dict[str, Dict[str, bool]],
    catalog_file: str,
    warn_if_missing: bool = False,
) -> Dict[str, List[str]]:
    """Return a dictionary of stream names to the list of keys of `key_type`.

    Parameters
    ----------
    key_type : str
        Either 'primary-key' or 'replication-key'
    matches : Dict[str, Dict[str, str]]
        Matches, used in filtering
    catalog_file : str
        Path to catalog file
    warn_if_missing : bool
        Path to catalog file

    Returns
    -------
    Dict[str, List[str]]
        Mapping of table names to each table's list of keys
    """
    result: Dict[str, List[str]] = {}
    for table_name, table_object in sorted(
        _get_catalog_tables_dict(catalog_file).items()
    ):
        if table_name not in matches.keys():
            continue
        result[table_name] = _get_table_key_cols(
            key_type, table_object, warn_if_missing=warn_if_missing
        )
    return result


def _get_rules_file_keys(
    key_type: str, matches: Dict[str, Dict[str, bool]], rules_file: str,
) -> Dict[str, List[str]]:
    result: Dict[str, List[str]] = {}
    if key_type not in ["primary-key", "replication-key"]:
        raise ValueError(
            f"Unexpected key type '{key_type}'. "
            "Expected: 'replication-key' or 'primary-key'"
        )
    # Check rules_file to fill `matches`
    plan_file_lines = uio.get_text_file_contents(rules_file).splitlines()
    key_overrides = [
        line.split("->")[0].rstrip()
        for line in plan_file_lines
        if "->" in line and line.split("->")[1].lstrip().rstrip() == key_type
    ]
    for key_spec in key_overrides:
        if len(key_spec.split(".")) != 2 or "*" in key_spec:
            raise ValueError(
                f"Expected '{key_type}' indicator with exact two-part key, separated "
                f"by '.'.  Found '{key_spec}'"
            )
        table_name, key_col_name = key_spec.split(".")
        if table_name not in matches:
            raise ValueError(f"Could not locate table '{table_name}' in selected list.")
        if key_col_name not in matches[table_name]:
            raise ValueError(f"Key column '{key_spec}' is not in column list.")
        elif not matches[table_name][key_col_name]:
            raise ValueError(f"Key column '{key_spec}' is not a selected column.")
        result[table_name] = [key_col_name]
    return result


def get_table_list(
    table_filter: Optional[Union[str, List[str]]],
    exclude_tables: Optional[Union[str, List[str]]],
    catalog_file: str,
) -> List[str]:
    """Return a list of tables to be selected.

    Parameters
    ----------
    table_filter : Optional[Union[str, List[str]]]
        A list of tables, a string representing a list, a single table name, or "*"
        for all tables.
    exclude_tables : Optional[Union[str, List[str]]]
        A list of tables or a string representing a list of tables.
    catalog_file : str
        The path to a catalog file.

    Returns
    -------
    List[str]
        A list of table names selected.
    """
    if isinstance(table_filter, list):
        list_of_tables = table_filter
    elif table_filter is None or table_filter == "*":
        list_of_tables = sorted(_get_catalog_tables_dict(catalog_file).keys())
    elif table_filter[0] == "[":
        # Remove square brackets and split the result on commas
        list_of_tables = table_filter.replace("[", "").replace("]", "").split(",")
    else:
        list_of_tables = [table_filter]
    if exclude_tables:
        if exclude_tables[0] == "[":
            # Remove square brackets and split the result on commas
            exclude_tables = (
                str(exclude_tables).replace("[", "").replace("]", "").split(",")
            )
        logging.info(f"Table(s) to exclude from sync: {', '.join(exclude_tables)}")
        list_of_tables = [t for t in list_of_tables if t not in exclude_tables]
    return list_of_tables


def plan(
    tap_name: str,
    *,
    dockerized: bool = None,
    rescan: bool = None,
    tap_exe: str = None,
    taps_dir: str = None,
    config_dir: str = None,
    config_file: str = None,
    replication_strategy: str = None,
) -> None:
    """Perform all actions necessary to prepare (plan) for a tap execution.

     1. Scan (discover) the source system metadata (if catalog missing or `rescan=True`)
     2. Apply filter rules from `rules_file` and create human-readable `plan.yml` file to
        describe planned inclusions/exclusions.
     3. Add primary-key and replication-key to the catalog.json file if specified in the
        rules file.
     4. Create a new `catalog-selected.json` file which applies the plan file and which
        can be used by the tap to run data extractions.

    Parameters:
    -----------
    tap_name : {str}
        The name of the tap without the 'tap-' prefix.
    dockerized : {bool}
        If specified, will override the default behavior for the local platform.
    tap_exe : {str}
        Specifies the tap executable, if different from `tap-{tap_name}`.
    rescan : {bool}
        True to force a rescan and replace existing metadata.
    taps_dir: {str}
        The directory containing the rules file.
        (Default=cwd)
    config_dir: {str}
        The default location of config, catalog and other potentially sensitive
        information. (Recommended to be excluded from source control.)
        (Default="${taps_dir}/.secrets")
    config_file : {str}
        The location of the JSON config file which contains config for the specified
        plugin. (Default=f"${config_dir}/${plugin_name}-config.json")
    replication_strategy : {str}
        One of "FULL_TABLE", "INCREMENTAL", or "LOG_BASED"; by default "INCREMENTAL" or
        a value is set in the TAP_{TAPNAME}_REPLICATION_STRATEGY environment variable.

    Raises
    ------
    ValueError
        Raised if an argument value is not within expected domain.
    FileExistsError
        Raised if files do not exist in default locations, or if paths provided do not
        point to valid files.
    """
    config.print_version()

    taps_dir = config.get_taps_dir(taps_dir)
    config_file, tap_settings = config.get_or_create_config(
        f"tap-{tap_name}",
        taps_dir=taps_dir,
        config_dir=config_dir,
        config_file=config_file,
    )
    tap_exe = tap_exe or tap_settings.get("EXE", f"tap-{tap_name}")
    replication_strategy = replication_strategy or tap_settings.get(
        "REPLICATION_STRATEGY", "INCREMENTAL"
    )
    config.validate_replication_strategy(replication_strategy)

    # TODO: Resolve bug in Windows STDERR inclusion when emitting catalog json from
    #       docker run
    # if (dockerized is None) and (uio.is_windows() or uio.is_mac()):
    #     dockerized = True
    #     logging.info(
    #         "The 'dockerized' argument is not set when running either Windows or OSX. "
    #         "Defaulting to dockerized=True..."
    #     )

    catalog_dir = config.get_catalog_output_dir(tap_name, taps_dir)
    raw_catalog_file = config.get_raw_catalog_file(catalog_dir, tap_name)
    selected_catalog_file = f"{catalog_dir}/{tap_name}-catalog-selected.json"
    plan_file = config.get_plan_file(tap_name, taps_dir, required=False)

    if rescan or not uio.file_exists(raw_catalog_file):
        # Run discover, if needed, to get catalog.json (raw)
        _discover(
            tap_name,
            config_file=config_file,
            catalog_dir=catalog_dir,
            dockerized=dockerized,
            tap_exe=tap_exe,
        )

    rules_file = config.get_rules_file(taps_dir, tap_name)
    matches, excluded_tables = _check_rules(
        catalog_file=raw_catalog_file, rules_file=rules_file
    )
    primary_keys = _get_catalog_file_keys(
        "primary-key", matches=matches, catalog_file=raw_catalog_file
    )
    primary_keys.update(
        _get_rules_file_keys("primary-key", matches=matches, rules_file=rules_file)
    )
    replication_keys = _get_catalog_file_keys(
        "replication-key", matches=matches, catalog_file=raw_catalog_file,
    )
    replication_keys.update(
        _get_rules_file_keys("replication-key", matches=matches, rules_file=rules_file,)
    )
    file_text = _make_plan_file_text(
        matches, primary_keys, replication_keys, excluded_tables
    )
    uio.create_text_file(plan_file, file_text)
    _create_selected_catalog(
        tap_name,
        plan_file=plan_file,
        raw_catalog_file=raw_catalog_file,
        output_file=selected_catalog_file,
        replication_strategy=replication_strategy,
        skip_senseless_validators=SKIP_SENSELESS_VALIDATORS,
    )
    _validate_selected_catalog(tap_name, selected_catalog_file=selected_catalog_file)


def _make_plan_file_text(
    matches: Dict[str, Dict[str, bool]],
    primary_keys: Dict[str, List[str]],
    replication_keys: Dict[str, List[str]],
    excluded_tables_list: List[str],
) -> str:
    sorted_tables = sorted(matches.keys())

    file_text = ""
    file_text += "selected_tables:\n"
    for table in sorted_tables:
        primary_key_cols: List[str] = primary_keys[table]
        replication_key_cols: List[str] = replication_keys[table]
        included_cols = [
            col
            for col, selected in matches[table].items()
            if selected
            and col not in primary_key_cols
            and col not in replication_key_cols
        ]
        ignored_cols = [col for col, selected in matches[table].items() if not selected]
        file_text += f"{' ' * 2}{table}:\n"
        file_text += f"{' ' * 4}primary_key:\n"
        for col in primary_key_cols:
            file_text += f"{' ' * 6}- {col}\n"
        file_text += f"{' ' * 4}replication_key:\n"
        for col in replication_key_cols:
            file_text += f"{' ' * 6}- {col}\n"
        file_text += f"{' ' * 4}selected_columns:\n"
        for col in included_cols:
            file_text += f"{' ' * 6}- {col}\n"
        if ignored_cols:
            file_text += f"{' ' * 4}ignored_columns:\n"
            for col in ignored_cols:
                file_text += f"{' ' * 6}- {col}\n"
    if excluded_tables_list:
        file_text += "ignored_tables:\n"
        for table in sorted(excluded_tables_list):
            file_text += f"{' ' * 2}- {table}\n"
    return file_text


def _get_catalog_table_columns(table_object: dict) -> List[str]:
    col_names: List[str] = []
    if "properties" not in table_object["schema"]:
        if table_object["schema"].get("type", "") == "object":
            logging.warning(
                "No properties defined, treating record as single json_raw column."
            )
            col_names = ["json_raw"]
    else:
        col_names = table_object["schema"]["properties"].keys()
    if not col_names:
        raise ValueError(
            f"Could not detect table columns from catalog table: {table_object}"
        )
    return col_names


def _get_catalog_tables_dict(catalog_file: str) -> dict:
    catalog_full = json.loads(Path(catalog_file).read_text())
    table_objects = {_get_stream_name(s): s for s in catalog_full["streams"]}
    return table_objects


def _get_stream_name(table_object: dict) -> str:
    return table_object["stream"]


def _get_table_name(table_object: dict) -> str:
    return table_object.get(
        "table_name", table_object.get("stream", table_object["tap_stream_id"])
    )


@logged(
    "selecting catalog metadata "
    "from '{tap_name}' source catalog file: {raw_catalog_file}"
)
def _create_selected_catalog(
    tap_name: str,
    plan_file: str,
    raw_catalog_file: str,
    output_file: str,
    replication_strategy: str,
    skip_senseless_validators: bool,
) -> None:
    taps_dir = config.get_taps_dir()
    catalog_dir = config.get_catalog_output_dir(tap_name, taps_dir)
    output_file = output_file or os.path.join(catalog_dir, "selected-catalog.json")
    catalog_full = json.loads(Path(raw_catalog_file).read_text())
    plan_file = plan_file or config.get_plan_file(tap_name)
    plan = yaml.safe_load(uio.get_text_file_contents(plan_file))
    if ("selected_tables" not in plan) or (plan["selected_tables"] is None):
        raise ValueError(f"No selected tables found in plan file '{plan_file}'.")
    included_table_objects = []
    for tbl in sorted(catalog_full["streams"], key=lambda x: _get_stream_name(x)):
        stream_name = _get_stream_name(tbl)
        if stream_name in plan["selected_tables"].keys():
            _set_catalog_file_keys(tbl, plan["selected_tables"][stream_name])
            _select_table(tbl, replication_strategy=replication_strategy)
            for col_name in _get_catalog_table_columns(tbl):
                col_selected = col_name in (
                    (plan["selected_tables"][stream_name]["selected_columns"] or [])
                    + (plan["selected_tables"][stream_name]["replication_key"] or [])
                    + (plan["selected_tables"][stream_name]["primary_key"] or [])
                )
                _select_table_column(tbl, col_name, col_selected)
            if skip_senseless_validators:
                _remove_senseless_validators(tbl)
            included_table_objects.append(tbl)
    catalog_new = {"streams": included_table_objects}
    with open(output_file, "w") as f:
        json.dump(catalog_new, f, indent=2)


@logged(
    "validating selected catalog metadata "
    "from '{tap_name}' selected catalog file: {selected_catalog_file}"
)
def _validate_selected_catalog(tap_name: str, selected_catalog_file: str,) -> None:
    selected_catalog = json.loads(Path(selected_catalog_file).read_text())
    for tbl in sorted(selected_catalog["streams"], key=lambda x: _get_stream_name(x)):
        _validate_keys(tbl, key_type="primary-key")
    for tbl in sorted(selected_catalog["streams"], key=lambda x: _get_stream_name(x)):
        _validate_keys(tbl, key_type="replication-key")


def _select_table(tbl: dict, replication_strategy: str):
    stream_metadata = _get_stream_metadata_object(tbl)
    stream_metadata["selected"] = True
    if (
        "replication-method" not in stream_metadata
        and "forced-replication-method" not in stream_metadata
    ):
        replication_keys = stream_metadata.get("valid-replication-keys", [])
        if replication_strategy in ["LOG_BASED", "FULL_TABLE"]:
            replication_method = replication_strategy
            basis = f"{replication_strategy} strategy"
        elif replication_keys:
            replication_method = "INCREMENTAL"
            basis = (
                f"{replication_strategy} strategy and "
                f"{replication_keys} replication keys"
            )
        else:
            replication_method = "FULL_TABLE"
            basis = f"{replication_strategy} strategy and no valid replication keys"
        logging.debug(
            f"Defaulting to {replication_method} replication based on {basis} "
            f"for '{_get_stream_name(tbl)}'."
        )
        stream_metadata["replication-method"] = replication_method


def _remove_senseless_validators(tbl: dict) -> None:
    if "properties" not in tbl["schema"]:
        return
    for col, props in tbl["schema"]["properties"].items():
        for senseless in [
            "multipleOf",
            "minimum",
            "maximum",
            "exclusiveMinimum",
            "exclusiveMaximum",
            "maxLength",
        ]:
            if senseless in props:
                props.pop(senseless)


def _select_table_column(tbl: dict, col_name: str, selected: bool) -> None:
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


def _get_stream_metadata_object(stream_object: dict):
    for metadata in stream_object["metadata"]:
        if len(metadata["breadcrumb"]) == 0:
            return metadata["metadata"]
    return None


@logged(
    "selecting '{table_name}' catalog metadata "
    "from '{tap_name}' source catalog file: {full_catalog_file}"
)
def _create_single_table_catalog(
    tap_name: str,
    table_name: str,
    full_catalog_file: str = None,
    output_file: str = None,
) -> None:
    taps_dir = config.get_taps_dir()
    catalog_dir = config.get_catalog_output_dir(tap_name, taps_dir)
    source_catalog_path = full_catalog_file or os.path.join(
        catalog_dir, "catalog-selected.json"
    )
    output_file = output_file or os.path.join(catalog_dir, f"{table_name}-catalog.json")
    included_table_objects = []
    catalog_full = json.loads(Path(source_catalog_path).read_text())
    for tbl in catalog_full["streams"]:
        stream_name = _get_stream_name(tbl)
        if stream_name == table_name:
            _get_stream_metadata_object(tbl)["selected"] = True
            included_table_objects.append(tbl)
    catalog_new = {"streams": included_table_objects}
    with open(output_file, "w") as f:
        json.dump(catalog_new, f, indent=2)


def _table_match_check(match_text: str, select_rules: list) -> bool:
    selected = False
    for rule in select_rules:
        result = _check_table_rule(match_text, rule)
        if result is True:
            selected = True
        elif result is False:
            selected = False
    return selected


def _col_match_check(match_text: str, select_rules: list) -> bool:
    selected = False
    for rule in select_rules:
        result = _check_column_rule(match_text, rule)
        if result is True:
            selected = True
        elif result is False:
            selected = False
    return selected


def _is_match(value: str, pattern: str) -> Optional[bool]:
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


def _check_column_rule(match_text: str, rule_text: str) -> Optional[bool]:
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


def _check_table_rule(match_text: str, rule_text: str) -> Optional[bool]:
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
