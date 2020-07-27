"""Generates a profile data file.

Intended to be run in response to warnings in plan generation.
"""

import datetime
from logless import logged, get_logger
import os
from typing import List

import pandas as pd
from pandas.api.types import is_numeric_dtype, is_string_dtype
import numpy as np
import uio

from tapdance import config, plans, syncs

DATE_NOW = datetime.datetime.now()

logging = get_logger("tapdance")


@logged("profiling '{table_name or 'all tables'}' from '{tap_name}'")
def profile(
    tap_name: str,
    table_name: str = "*",
    taps_dir: str = None,
    *,
    tap_exe: str = None,
    config_file: str = None,
    exclude_tables: List[str] = None,
    dockerized: bool = None,
):
    if dockerized is None:
        if uio.is_windows() or uio.is_mac():
            dockerized = True
            logging.info(
                "The 'dockerized' argument is not set when running either Windows or OSX. "
                "Defaulting to dockerized=True..."
            )
        else:
            dockerized = False
    taps_dir = config.get_taps_dir(taps_dir)
    # rules_file = config.get_rules_file(taps_dir, tap_name)
    catalog_dir = config.get_catalog_output_dir(tap_name, taps_dir)
    selected_catalog_file = f"{catalog_dir}/{tap_name}-catalog-selected.json"
    config_file = str(
        config.get_config_file(
            f"tap-{tap_name}",
            taps_dir=taps_dir,
            config_file=config_file,
            config_dir=f"{taps_dir}/.secrets",
            required=True,
        )
    )
    target_config_file = str(
        config.get_config_file(
            f"target-csv",
            taps_dir=taps_dir,
            config_file=None,
            config_dir=f"{taps_dir}/.secrets",
            required=False,
        )
    )
    if not uio.file_exists(config_file):
        raise FileExistsError(config_file)
    list_of_tables = plans.get_table_list(
        table_filter=table_name,
        exclude_tables=exclude_tables,
        catalog_file=selected_catalog_file,
    )
    for table in list_of_tables:
        tmp_catalog_file = f"{catalog_dir}/{tap_name}-{table}-catalog.json"
        plans._create_single_table_catalog(
            tap_name=tap_name,
            table_name=table,
            full_catalog_file=selected_catalog_file,
            output_file=tmp_catalog_file,
        )
        syncs._sync_one_table(
            tap_name=tap_name,
            target_name="csv",
            table_name=table,
            config_file=config_file,
            table_catalog_file=tmp_catalog_file,
            dockerized=dockerized,
            tap_exe=tap_exe or f"tap-{tap_name}",
            table_state_file=f"{catalog_dir}/{table}-state.json",
            target_config_file=target_config_file,
            target_exe="target-csv",
        )
        profile_csv(
            tap_name=tap_name,
            table_name=table,
            csv=f"./{table}.csv",
            config_file=config_file,
            table_catalog_file=tmp_catalog_file,
        )


def profile_csv(
    tap_name: str,
    table_name: str,
    csv: str,
    taps_dir: str = None,
    *,
    tap_exe: str = None,
    config_file: str = None,
    exclude_tables: List[str] = None,
    dockerized: bool = None,
):
    df = _load_df_from_file(csv)
    pdf = _create_profile_dataframe(df)
    pdf.show()


def _load_df_from_file(filepath: str) -> pd.DataFrame:
    tmpdir = uio.get_temp_folder()
    tmpfile = f"{tmpdir}/{os.path.basename(filepath)}"
    if not uio.is_local(filepath):
        uio.download_file(filepath, tmpfile)
    else:
        tmpfile = filepath
    df = pd.read_csv(tmpfile, thousands=",", float_precision=2)
    if tmpfile != filepath:
        uio.delete_file(tmpfile)
    return df


def _create_profile_dataframe(df: pd.DataFrame):
    # Number of rows of the ProfileDF will be the count of columns in the raw date `df`
    num_rows = len(df.columns)

    # Constructing the data_qlt_df dataframe and pre-assigning and columns
    data_qlt_df = pd.DataFrame(
        index=np.arrange(0, num_rows),
        columns=[
            "column_name",
            "col_data_type",
            "col_memory",
            "non_null_values",
            "unique_values_count",
            "column_dtype",
        ],
    )
    mem_used_dtypes = pd.DataFrame(df.memory_usage(deep=True) / 1024 ** 2)

    # Add rows to the data_qlt_df dataframe
    for ind, cols in enumerate(df.columns):
        data_qlt_df.loc[ind] = [
            cols,
            df[cols].dtype,
            mem_used_dtypes["memory"][ind],
            df[cols].count(),
            df[cols].nunique(),
            cols + "~" + str(df[cols].dtype),
        ]
