"""tapdance.syncs - Module containing the sync() function and sync helper functions."""

import os
from typing import List, Optional

import uio
import runnow
from logless import get_logger, logged
from tapdance import config, docker, plans, states

logging = get_logger("tapdance")


def sync(
    tap_name: str,
    target_name: str = "csv",
    table_name: str = "*",
    taps_dir: Optional[str] = None,
    *,
    dockerized: Optional[bool] = None,
    rescan: bool = False,
    tap_exe: Optional[str] = None,
    target_exe: Optional[str] = None,
    config_dir: Optional[str] = None,
    config_file: Optional[str] = None,
    catalog_dir: Optional[str] = None,
    target_config_file: Optional[str] = None,
    state_file: Optional[str] = None,
    exclude_tables: Optional[List[str]] = None,
    replication_strategy: Optional[str] = None,
) -> None:
    """
    Synchronize data from tap to target.

    Parameters:
    ----------
    tap_name : {str}
        The name/alias of the source tap, without the `tap-` prefix.
    target_name : {str}
        The name/alias of the target, without the `tap-` prefix.
        (Default="csv")
    table_name : {str}
        The name of the table to sync. To sync multiple tables, specify
        a comma-separated list of tables surrounded by queare brackets (e.g. "[tb1,tbl2]"),
        or use "*" to sync all table.
        (Default="*")
    dockerized : {bool}
        True or False to force whether the command is run
        dockerized. If omitted, the best option will be selected automatically.
    rescan : {bool}
        True to force a rescan and replace existing metadata.
    tap_exe : {str}
        Overrides the tap executable, if different from `tap-{tap_name}`.
    target_exe : {str}
        Overrides the target executable, if different from `target-{tap_name}`.
    taps_dir : {str}
        The directory containing the rules file. (Default=cwd)
    config_dir : {str}
        The default location of config, catalog and other
        potentially sensitive information. (Recommended to be excluded from source control.)
        (Default="${taps_dir}/.secrets")
    config_file : {str}
        The location of the JSON config file which
        contains config for the specified tap or 'False' to only pull settings from
        environment variables. Default path is f"${config_dir}/${plugin_name}-config.json".
    catalog_dir : {str}
        The output directory to be used for saving catalog
        files. If not provided, a path will be generated automatically within `.output` or
        a path specified by the `TAP_SCRATCH_DIR` environment variable.
    target_config_file : {str}
        The location of the JSON config file which
        contains config for the specified target or 'False' to only pull settings from
        environment variables. Default path is f"${config_dir}/${plugin_name}-config.json".
    state_file : {str}
        The path to a state file. If not provided, a state
        file path will be generated automatically within `catalog_dir`.
    exclude_tables: {List(str)}
        A list of tables to exclude. Ignored
        if table_name arg is not "*".
    replication_strategy : {str}
        One of "FULL_TABLE", "INCREMENTAL", or "LOG_BASED"; by default "INCREMENTAL" or
        a value is set in the TAP_{TAPNAME}_REPLICATION_STRATEGY environment variable.
    """
    config.print_version()

    taps_dir = config.get_taps_dir(taps_dir)
    config_file, tap_settings = config.get_or_create_config(
        f"tap-{tap_name}",
        taps_dir=taps_dir,
        config_dir=config_dir,
        config_file=config_file,
    )
    target_config_file, target_settings = config.get_or_create_config(
        f"target-{target_name}",
        taps_dir=taps_dir,
        config_dir=config_dir,
        config_file=target_config_file,
    )
    tap_exe = tap_exe or tap_settings.get("EXE", f"tap-{tap_name}")
    target_exe = target_exe or target_settings.get("EXE", f"target-{target_name}")
    replication_strategy = replication_strategy or tap_settings.get(
        "REPLICATION_STRATEGY", "INCREMENTAL"
    )
    config.validate_replication_strategy(replication_strategy)

    table_name = table_name or tap_settings.get("TABLE_NAME", None)
    exclude_tables = exclude_tables or tap_settings.get("EXCLUDE_TABLES", None)
    rules_file = config.get_rules_file(taps_dir, tap_name)

    # TODO: Resolve bug in Windows STDERR inclusion when emitting catalog json from
    #       docker run
    # if dockerized is None:
    #     if uio.is_windows() or uio.is_mac():
    #         dockerized = True
    #         logging.info(
    #             "The 'dockerized' argument is not set when running either Windows or OSX..."
    #             "Defaulting to dockerized=True"
    #         )

    catalog_dir = catalog_dir or config.get_catalog_output_dir(tap_name, taps_dir)
    full_catalog_file = f"{catalog_dir}/{tap_name}-catalog-selected.json"
    if rescan or rules_file or not uio.file_exists(full_catalog_file):
        plans.plan(
            tap_name,
            dockerized=dockerized,
            rescan=rescan,
            tap_exe=tap_exe,
            taps_dir=taps_dir,
            config_file=config_file,
            config_dir=catalog_dir,
        )
    list_of_tables = plans.get_table_list(
        table_filter=table_name,
        exclude_tables=exclude_tables,
        catalog_file=full_catalog_file,
    )
    logging.info(f"Table(s) to sync: {', '.join(list_of_tables)}")
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
            dockerized=dockerized,
            tap_exe=tap_exe,
            target_exe=target_exe,
        )


def _dockerize_path(localpath: str, container_volume_root="/home/local") -> str:
    result = os.path.relpath(localpath).replace("\\", "/")
    result = f"{container_volume_root}/{result}"
    return result


def _dockerize_cli_args(arg_str: str, container_volume_root="/home/local") -> str:
    args = arg_str.split(" ")
    newargs: List[str] = []
    for arg in args:
        if uio.file_exists(arg):
            newargs.append(_dockerize_path(arg, container_volume_root))
        elif "=" in arg:
            left, right = arg.split("=")[0], "=".join(arg.split("=")[1:])
            if uio.file_exists(right):
                newargs.append(
                    f"{left}={_dockerize_path(right, container_volume_root)}"
                )
        else:
            newargs.append(arg)
    return " ".join(newargs)


@logged("running sync of table '{table_name}' from '{tap_name}'")
def _sync_one_table(
    tap_name: str,
    table_name: str,
    config_file: str,
    target_name: str,
    target_config_file: str,
    table_catalog_file: str,
    table_state_file: str,
    dockerized: bool,
    tap_exe: str,
    target_exe: str,
) -> None:
    if not tap_exe:
        tap_exe = f"tap-{tap_name}"
    pipeline_version_num = config.get_pipeline_version_number()
    table_state_file = config.replace_placeholders(
        {"table_state_file": table_state_file},
        tap_name,
        table_name,
        pipeline_version_num,
    )["table_state_file"]
    tap_args = f"--config {config_file} --catalog {table_catalog_file}"
    if uio.file_exists(table_state_file):
        local_state_file_in = os.path.join(
            uio.get_temp_dir(), f"{tap_name}-{table_name}-state.json"
        )
        if not uio.get_text_file_contents(table_state_file):
            logging.warning(f"Ignoring blank state file from '{table_state_file}'.")
        else:
            states.make_aggregate_state_file(table_state_file, local_state_file_in)
            tap_args += f" --state {local_state_file_in}"
        local_state_file_out = (
            f"{'.'.join(local_state_file_in.split('.')[:-1])}-new.json"
        )
    else:
        local_state_file_out = os.path.join(
            uio.get_temp_dir(), f"{tap_name}-{table_name}-state-new.json"
        )

    tmp_target_config = config.get_single_table_target_config_file(
        target_name,
        target_config_file,
        tap_name=tap_name,
        table_name=table_name,
        pipeline_version_num=pipeline_version_num,
    )
    target_args = f"--config {tmp_target_config}"
    if dockerized:
        cdw = os.getcwd().replace("\\", "/")
        tap_image_name = docker._get_docker_tap_image(tap_exe)
        target_image_name = docker._get_docker_tap_image(target_exe=target_exe)
        sync_cmd = (
            f"docker run --rm -it -v {cdw}:/home/local {tap_image_name} "
            f"{_dockerize_cli_args(tap_args)} "
            "| "
            f"docker run --rm -it -v {cdw}:/home/local {target_image_name} "
            f"{_dockerize_cli_args(target_args)} "
            ">> "
            f"{local_state_file_out}"
        )
    else:
        sync_cmd = (
            f"{tap_exe} "
            f"{tap_args} "
            "| "
            f"{target_exe} "
            f"{target_args} "
            "> "
            f"{local_state_file_out}"
        )
    runnow.run(sync_cmd)
    if not uio.file_exists(local_state_file_out):
        logging.warning(
            f"State file does not exist at path '{local_state_file_out}'. Skipping upload. "
            f"This can be caused by having no data, or no new data, in the source table."
        )
    else:
        uio.upload_file(local_state_file_out, table_state_file)
