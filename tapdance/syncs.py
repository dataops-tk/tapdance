"""tapdance.syncs - Module containing the sync() function and sync helper functions."""

import os
from typing import List

import uio
import runnow
from logless import get_logger, logged

from tapdance import config, docker, plans, states

logging = get_logger("tapdance")


@logged("syncing '{table_name or 'all tables'}' from '{tap_name}' to '{target_name}'")
def sync(
    tap_name: str,
    target_name: str = "csv",
    table_name: str = "*",
    taps_dir: str = None,
    *,
    tap_exe: str = None,
    target_exe: str = None,
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
        (`{tap-name}.rules.txt`).
        config_dir {str} -- Optional. The default location of config, catalog and other
        potentially sensitive information. (Recommended to be excluded from source control.)
        (Default="${taps_dir}/.secrets")
        config_file {str} -- Optional. The location of the JSON config file which
        contains config for the specified tap or 'False' to only pull settings from
        environment variables. Default path is f"${config_dir}/${plugin_name}-config.json".

        catalog_dir {str} -- Optional. The output directory to be used for saving catalog
        files. If not provided, a path will be generated automatically within `.output` or
        a path specified by the `TAP_SCRATCH_DIR` environment variable.
        target_config_file {str} -- Optional. The location of the JSON config file which
        contains config for the specified target or 'False' to only pull settings from
        environment variables. Default path is f"${config_dir}/${plugin_name}-config.json".
        state_file {str} -- Optional. The path to a state file. If not provided, a state
        file path will be generated automatically within `catalog_dir`.
        exclude_table_names {List(str)} -- Optional. A list of tables to exclude. Ignored
        if table_name arg is not "*".
    """
    tap_exe = tap_exe or config.get_exe(f"tap-{tap_name}")
    target_exe = target_exe or config.get_exe(f"target-{target_name}")
    if dockerized is None:
        if uio.is_windows() or uio.is_mac():
            dockerized = True
            logging.info(
                "The 'dockerized' argument is not set when running either Windows or OSX..."
                "Defaulting to dockerized=True"
            )
        else:
            dockerized = False
    # Deprecated in favor of partial dockerization:
    # if dockerized:
    #     args = ["plan", tap_name, target_name]
    #     for var in [
    #         "table_name",
    #         "rescan",
    #         "taps_dir",
    #         "config_dir",
    #         "config_file",
    #         "catalog_dir",
    #         "target_config_file",
    #         "state_file",
    #     ]:
    #         if var in locals() and locals()[var]:
    #             args.append(f"--{var}={locals()[var]}")
    #     docker.rerun_dockerized(tap_name, args=args)
    #     return
    taps_dir = config.get_taps_dir(taps_dir)
    rules_file = config.get_rules_file(taps_dir, tap_name)
    config_required = True
    target_config_required = True
    logging.info(
        "Attempting to configure sync using config_file={config_file} and "
        "target_config_file={target_config_file}."
    )
    if (config_file is not None) and str(config_file).lower() == "false":
        logging.info("Skipping check for tap config (--config_file=False)")
        config_file = None
        config_required = False
    if (target_config_file is not None) and str(target_config_file).lower() == "false":
        logging.info("Skipping check for target config (--target_config_file=False)")
        target_config_file = None
        target_config_required = False
    config_file = str(
        config.get_config_file(
            f"tap-{tap_name}",
            config_dir=config_dir,
            config_file=config_file,
            required=config_required,
        )
    )
    target_config_file = str(
        config.get_config_file(
            f"target-{target_name}",
            config_dir=config_dir,
            config_file=target_config_file,
            required=target_config_required,
        )
    )
    if not uio.file_exists(config_file):
        raise FileExistsError(config_file)
    if not uio.file_exists(target_config_file):
        raise FileExistsError(target_config_file)
    catalog_dir = catalog_dir or config.get_catalog_output_dir(tap_name)
    full_catalog_file = f"{catalog_dir}/{tap_name}-catalog-selected.json"
    if rescan or rules_file or not uio.file_exists(full_catalog_file):
        # Create or update `*-catalog-selected.json` and `*-plan.yml` files
        # using the `{tap-name}.rules.txt` rules file
        plans.plan(
            tap_name,
            taps_dir=taps_dir,
            config_file=config_file,
            config_dir=catalog_dir,
            rescan=rescan,
            tap_exe=tap_exe,
        )
    if isinstance(table_name, list):
        list_of_tables = table_name
    elif table_name is None or table_name == "*":
        list_of_tables = sorted(
            plans._get_catalog_tables_dict(full_catalog_file).keys()
        )
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


def _dockerize_path(localpath, container_volume_root="/home/local"):
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
):
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
        if not uio.get_text_file_contents(table_state_file):
            logging.warning(f"Ignoring blank state file from '{table_state_file}'.")
        else:
            local_state_file_in = os.path.join(
                uio.get_temp_dir(), f"{tap_name}-{table_name}-state.json"
            )
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
