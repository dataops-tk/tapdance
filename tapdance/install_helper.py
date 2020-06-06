"""tapdance.install_helper - functions to help install plugins."""

# TODO: Deprecate this module in favor of using pipx.

import os

import uio
import runnow
from logless import logged, get_logger

from tapdance import config


logging = get_logger("tapdance")


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

    venv_dir = os.path.join(config.VENV_ROOT, alias)
    install_path = os.path.join(config.INSTALL_ROOT, alias)
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
