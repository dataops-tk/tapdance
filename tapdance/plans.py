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
    taps_dir: str,
    *,
    config_file: str,
    catalog_dir: str,
    dockerized: bool,
    tap_exe: str,
) -> None:
    catalog_file = config.get_raw_catalog_file(
        taps_dir, catalog_dir, tap_name, allow_custom=False
    )
    uio.create_folder(catalog_dir)
    img = f"{docker.BASE_DOCKER_REPO}:{tap_exe}"
    hide_cmd = False
    if dockerized:
        cdw = os.getcwd().replace("\\", "/")
        tap_config = json.loads(uio.get_text_file_contents(config_file))
        tap_docker_args = ""
        # TODO: Replace with logic to parse from AWS_SHARED_CREDENTIALS_FILE env var:
        for k in ["aws_access_key_id", "aws_secret_access_key", "aws_session_token"]:
            if k in tap_config:
                key = f"TAP_{tap_name}_{k}".replace("-", "_").upper()
                os.environ[key] = tap_config[k]
                tap_docker_args += f' -e {k.upper()}="{tap_config[k]}"'
                hide_cmd = True
        _, _ = runnow.run(f"docker pull {img}")
        _, output_text = runnow.run(
            f"docker run --rm -i "
            f"-v {cdw}:/home/local {tap_docker_args} "
            f"{img} --config {config.dockerize_cli_args(config_file)} --discover",
            echo=False,
            capture_stderr=False,
            hide=hide_cmd,
        )
        if not _is_valid_json(output_text):
            raise RuntimeError(f"Could not parse json file from output:\n{output_text}")
        uio.create_text_file(catalog_file, output_text)
    else:
        runnow.run(
            f"{tap_exe} --config {config_file} --discover > {catalog_file}",
            hide=hide_cmd,
        )


@logged("running custom schema inference on a dry run of '{tap_name}'")
def _infer_schema(
    tap_name: str,
    taps_dir: str,
    raw_catalog_file: str,
    selected_catalog_file: str,
    *,
    config_file: str,
    catalog_dir: str,
    dockerized: bool,
    tap_exe: str,
) -> str:
    custom_catalog = json.loads(uio.get_text_file_contents(raw_catalog_file))
    tmp_folder = f"{catalog_dir}/tmp"
    tmp_outfile = f"{catalog_dir}/tmp/sync-dryrun.jsonl"
    uio.create_folder(catalog_dir)
    uio.create_folder(tmp_folder)
    logging.info(f"Cleaning up old files in tmp folder '{tmp_folder}'...")
    for file in uio.list_files(tmp_folder):
        if any(
            [
                file.endswith(x)
                for x in ["-config.json", "-dryrun.jsonl", "-table.inferred.json"]
            ]
        ):
            uio.delete_file(file)
    img = f"{docker.BASE_DOCKER_REPO}:{tap_exe}"
    hide_cmd = False
    if dockerized:
        cdw = os.getcwd().replace("\\", "/")
        tap_config = json.loads(uio.get_text_file_contents(config_file))
        tap_docker_args = ""
        # TODO: Replace with logic to parse from AWS_SHARED_CREDENTIALS_FILE env var:
        for k in ["aws_access_key_id", "aws_secret_access_key", "aws_session_token"]:
            if k in tap_config:
                key = f"TAP_{tap_name}_{k}".replace("-", "_").upper()
                os.environ[key] = tap_config[k]
                tap_docker_args += f' -e {k.upper()}="{tap_config[k]}"'
                hide_cmd = True
        _, _ = runnow.run(f"docker pull {img}")
        _, jsonl_out = runnow.run(
            f"docker run --rm -i "
            f"-v {cdw}:/home/local {tap_docker_args} "
            f"{img} "
            f"--config {config.dockerize_cli_args(config_file)}"
            f"--catalog {selected_catalog_file}",
            hide=hide_cmd,
            echo=False,
            capture_stderr=False,
        )
    else:
        _, jsonl_out = runnow.run(
            f"{tap_exe} "
            f"--config {config_file} "
            f"--catalog {selected_catalog_file}",
            hide=hide_cmd,
            echo=False,
            capture_stderr=False,
        )
    uio.create_text_file(tmp_outfile, jsonl_out)
    _, jsonl_out = runnow.run(
        f"cat {tmp_outfile} | singer-infer-schema --out-dir {tmp_folder}",
    )
    for file in uio.list_files(tmp_folder):
        if file.endswith(".inferred.json"):
            logging.info(f"Parsing inferred schema from '{file}'...")
            inferred_schema = json.loads(uio.get_text_file_contents(file))
            stream_name = file.split("/")[-1].split(".")[0]
            stream = (
                [x for x in custom_catalog["streams"] if x["stream"] == stream_name]
                or [None]
            )[0]
            if not stream:
                raise ValueError(
                    f"Failed to append inferred schema for stream name '{stream_name}'."
                    f" Stream not present in catalog file {selected_catalog_file}."
                )
            stream["schema"] = inferred_schema
    custom_catalog_file = config.get_custom_catalog_file(taps_dir, tap_name)
    uio.create_text_file(custom_catalog_file, json.dumps(custom_catalog, indent=2))
    return custom_catalog_file


def _check_rules(
    tap_name: str,
    catalog_file: str,
    rules_file: str,
    replication_strategy: str,
    plan_file_out: str,
    selected_catalog_file_out: str,
    log_dir: Optional[str],
) -> None:
    """
    Create plan file and selected catalog file from provided rules and raw catalog.

    Parameters
    ----------
    catalog_file : str
        Path to a catalog file.
    rules_file : str
        Path to a rules file.
    plan_file_out : str
        Path to save the plan file.
    selected_catalog_file_out : str
        Path to save the selected catalog file.
    """
    select_rules = [
        line.split("#")[0].rstrip()
        for line in uio.get_text_file_contents(rules_file).splitlines()
        if line.split("#")[0].rstrip()
    ]
    matches: Dict[str, dict] = {}
    excluded_table_stream_ids: Dict[str, List[str]] = {}
    matched_stream_ids: Dict[str, str] = {}
    for stream_id, table_object in _get_catalog_tables_dict(catalog_file).items():
        table_name = _get_stream_name(table_object)
        if _table_match_check(
            table_name=table_name, stream_id=stream_id, select_rules=select_rules,
        ):
            if table_name in matched_stream_ids:
                raise RuntimeError(
                    f"Table name '{table_name}' matched multiple stream IDs: "
                    f'"{matched_stream_ids[table_name]}" and "{stream_id}". '
                    "This is most often caused by tables with the same name under "
                    "different source database schemas. Please qualify or disqualify "
                    "specific stream name patterns by using double-quoted stream IDs "
                    "in your rules file instead of or in addition to bare table names."
                )
            matched_stream_ids[table_name] = stream_id
            matches[table_name] = {}
            for col_object in _get_catalog_table_columns(table_object):
                col_name = col_object
                matches[table_name][col_name] = _col_match_check(
                    table_name, stream_id, col_name, select_rules
                )
        else:
            if table_name in excluded_table_stream_ids:
                excluded_table_stream_ids[table_name].append(stream_id)
            else:
                excluded_table_stream_ids[table_name] = [stream_id]
    all_matches_lower = [m.lower() for m in matches.keys()] + [
        f'"{m.lower()}"' for m in matched_stream_ids.values()
    ]
    declared_tables = set(
        [
            rule.split(".")[0].rstrip().lstrip("!")
            for rule in select_rules
            if rule.split(".")[0].rstrip() and ("*" not in rule.split(".")[0])
        ]
    )
    for required_table in declared_tables:
        if required_table.lower() not in all_matches_lower:
            logging.warning(
                f"The table '{required_table}' was declared in the rules file "
                "but could not be found in the catalog."
            )
    for match, match_cols in matches.items():
        if not match_cols:
            logging.warning(
                f"The table '{match}' was declared in the rules file "
                "but did not match with any columns in the catalog."
            )
    primary_keys, replication_keys = _get_table_keys(
        matches, matched_stream_ids, catalog_file, rules_file
    )
    file_text = _make_plan_file_text(
        matches,
        primary_keys,
        replication_keys,
        matched_stream_ids,
        excluded_table_stream_ids,
    )
    logging.info(f"Updating plan file: {plan_file_out}")
    uio.create_text_file(plan_file_out, file_text)
    config.push_logs(log_dir, [rules_file, plan_file_out])
    _create_selected_catalog(
        tap_name,
        plan_file=plan_file_out,
        raw_catalog_file=catalog_file,
        output_file=selected_catalog_file_out,
        replication_strategy=replication_strategy,
        skip_senseless_validators=SKIP_SENSELESS_VALIDATORS,
    )
    config.push_logs(log_dir, [selected_catalog_file_out])
    # return matches, excluded_table_list


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
    matched_stream_ids: Dict[str, str],
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
    for stream_id, table_object in sorted(
        _get_catalog_tables_dict(catalog_file).items()
    ):
        table_name = _get_stream_name(table_object)
        if table_name not in matches.keys():
            continue
        if stream_id != matched_stream_ids[table_name]:
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


def _get_table_keys(
    matches: Dict[str, Dict[str, bool]],
    matched_stream_ids: Dict[str, str],
    catalog_file: str,
    rules_file: str,
) -> Tuple[Dict[str, List[str]], Dict[str, List[str]]]:
    primary_keys: Dict[str, List[str]] = {}
    replication_keys: Dict[str, List[str]] = {}
    stream_name_lookup = {v: k for k, v in matched_stream_ids.items()}
    for key_type, collection in [
        ("primary-key", primary_keys),
        ("replication-key", replication_keys),
    ]:
        collection.update(
            _get_catalog_file_keys(
                key_type,
                matches=matches,
                matched_stream_ids=matched_stream_ids,
                catalog_file=catalog_file,
            )
        )
        key_overrides = _get_rules_file_keys(
            key_type, matches=matches, rules_file=rules_file
        )
        for table, override in key_overrides.items():
            if table.startswith('"'):
                table = stream_name_lookup[table.lstrip('"').rstrip('"')]
            collection[table] = override
    return primary_keys, replication_keys


@logged(
    "getting table list using: filter={table_filter}, exclude={exclude_tables}, "
    "and catalog={catalog_file}",
    success_msg=None,
)
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
        list_of_tables = sorted(
            [
                _get_stream_name(x)
                for x in _get_catalog_tables_dict(catalog_file).values()
            ]
        )
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
    infer_custom_schema: bool = None,
    tap_exe: str = None,
    taps_dir: str = None,
    config_dir: str = None,
    config_file: str = None,
    log_dir: str = None,
    replication_strategy: str = None,
) -> None:
    """Perform all actions necessary to prepare (plan) for a tap execution.

    1. Capture raw catalog schema using discover (if needed or if --rescan).
    2. If it exists, use the 'custom' catalog file in place of the 'raw' catalog.
    3. Create the plan file and 'selected' version of the raw schema using
       `*.rules.txt`.
        - Add primary-key and replication-key to the catalog.json file if specified in the
          rules file.
    4. If infer_custom_schema=true:
        - Use the 'selected' catalog to execute a dry run for 'infer_custom_schema'.
        - Create or update the 'custom' catalog file using inferred schema.
        - Rebuild the plan file and rebuild the 'selected' catalog.

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
    infer_custom_schema : {bool}
        True to infer schema by performing a dry run data sync.
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
    log_dir : {str}
        Optional. The location to publish logs and other artifacts. If omitted, no
        extra publishing will be performed.
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
    catalog_dir = config.get_tap_output_dir(tap_name, taps_dir)
    log_dir = config.get_log_dir(log_dir)
    raw_catalog_file = config.get_raw_catalog_file(
        taps_dir, catalog_dir, tap_name, allow_custom=True
    )
    selected_catalog_file = f"{catalog_dir}/{tap_name}-catalog-selected.json"
    plan_file = config.get_plan_file(tap_name, taps_dir, required=False)
    if rescan or not uio.file_exists(raw_catalog_file):
        # Run discover, if needed, to get catalog.json (raw)
        _discover(
            tap_name,
            taps_dir,
            config_file=config_file,
            catalog_dir=catalog_dir,
            dockerized=dockerized,
            tap_exe=tap_exe,
        )
    config.push_logs(log_dir, [raw_catalog_file])
    logging.info(f"Using catalog file for initial plan: {raw_catalog_file}")
    rules_file = config.get_rules_file(taps_dir, tap_name)
    _check_rules(
        tap_name=tap_name,
        catalog_file=raw_catalog_file,
        rules_file=rules_file,
        plan_file_out=plan_file,
        selected_catalog_file_out=selected_catalog_file,
        replication_strategy=replication_strategy,
        log_dir=log_dir,
    )
    if infer_custom_schema:
        custom_catalog_file = _infer_schema(
            tap_name,
            taps_dir,
            raw_catalog_file=raw_catalog_file,
            selected_catalog_file=selected_catalog_file,
            config_file=config_file,
            catalog_dir=catalog_dir,
            dockerized=dockerized,
            tap_exe=tap_exe,
        )
        config.push_logs(log_dir, [custom_catalog_file])
        _check_rules(
            tap_name=tap_name,
            catalog_file=custom_catalog_file,
            rules_file=rules_file,
            plan_file_out=plan_file,
            selected_catalog_file_out=selected_catalog_file,
            replication_strategy=replication_strategy,
            log_dir=log_dir,
        )
    _validate_selected_catalog(tap_name, selected_catalog_file=selected_catalog_file)


def _make_plan_file_text(
    matches: Dict[str, Dict[str, bool]],
    primary_keys: Dict[str, List[str]],
    replication_keys: Dict[str, List[str]],
    table_stream_ids: Dict[str, str],
    excluded_table_stream_ids: Dict[str, List[str]],
) -> str:
    sorted_tables = sorted(matches.keys())
    file_text = ""
    file_text += "selected_tables:\n"
    for table in sorted_tables:
        stream_id = table_stream_ids[table]
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
        if table != stream_id:
            file_text += f"{' ' * 4}stream_id: {stream_id}\n"
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
    if excluded_table_stream_ids:
        file_text += "ignored_tables:\n"
        for table, stream_ids in sorted(excluded_table_stream_ids.items()):
            if len(stream_ids) < 2 and table not in matches.keys():
                file_text += f"{' ' * 2}- {table}\n"
            else:
                for id in stream_ids:
                    file_text += f'{" " * 2}- {table} ("{id}")\n'
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
    """Return a dictionary of streams by their unique stream ID."""
    catalog_full = json.loads(Path(catalog_file).read_text())
    table_objects = {_get_stream_id(s): s for s in catalog_full["streams"]}
    return table_objects


def _get_stream_name(table_object: dict) -> str:
    return table_object["stream"]


def _get_stream_id(table_object: dict) -> str:
    return table_object.get("tap_stream_id", _get_stream_name(table_object))


def _get_table_name(table_object: dict) -> str:
    return table_object.get(
        "table_name", table_object.get("stream", table_object["tap_stream_id"])
    )


@logged(
    "selecting catalog metadata "
    "from '{tap_name}' source catalog file: {raw_catalog_file}",
    success_msg=False,
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
    catalog_dir = config.get_tap_output_dir(tap_name, taps_dir)
    output_file = output_file or os.path.join(catalog_dir, "selected-catalog.json")
    catalog_full = json.loads(Path(raw_catalog_file).read_text())
    plan_file = plan_file or config.get_plan_file(tap_name)
    plan = yaml.safe_load(uio.get_text_file_contents(plan_file))
    if ("selected_tables" not in plan) or (plan["selected_tables"] is None):
        raise ValueError(f"No selected tables found in plan file '{plan_file}'.")
    included_table_objects = []
    for tbl in sorted(catalog_full["streams"], key=lambda x: _get_stream_name(x)):
        stream_name = _get_stream_name(tbl)
        stream_id = _get_stream_id(tbl)
        if stream_name in plan["selected_tables"].keys() and stream_id == plan[
            "selected_tables"
        ][stream_name].get("stream_id", stream_name):
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
    "from '{tap_name}' selected catalog file: {selected_catalog_file}",
    success_msg=False,
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
            if isinstance(props, dict) and senseless in props:
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
    catalog_dir = config.get_tap_output_dir(tap_name, taps_dir)
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


def _table_match_check(table_name: str, stream_id: str, select_rules: list) -> bool:
    selected = False
    for rule in select_rules:
        if rule.lstrip("!").startswith('"'):
            result = _check_table_rule(stream_id, rule)
        else:
            result = _check_table_rule(table_name, rule)
        if result is True:
            selected = True
        elif result is False:
            selected = False
    return selected


def _col_match_check(
    table_name: str, stream_id: str, col_name: str, select_rules: list
) -> bool:
    selected = False
    for rule in select_rules:
        if rule.lstrip("!").startswith('"'):
            result = _check_column_rule(f"{stream_id}.{col_name}", rule)
        else:
            result = _check_column_rule(f"{table_name}.{col_name}", rule)
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
    table_rule = rule_text.split(".")[0].lstrip('"').rstrip('"')
    column_match = ".".join(rule_text.split(".")[1:])
    if not _is_match(match_text.split(".")[0], table_rule):
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
        table_rule = rule_text.split(".")[0].lstrip('"').rstrip('"')
    else:  # Using 3-part rules, including tap name as first part
        tap_name = match_text.split(".")[0]
        tap_rule = rule_text.split(".")[0]
        table_name = ".".join(match_text.split(".")[1:])
        table_rule = ".".join(rule_text.split(".")[1:])
        if not _is_match(tap_name, tap_rule):
            return None
    # logging.info(f"Checking for '{table_name}'...")
    if not _is_match(table_name, table_rule):
        return None
    # Table '{match_text}' matched table filter '{table_rule}' in '{rule_text}'"
    return match_result
