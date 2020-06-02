"""tapdance.cli - Defines the command-line interface (CLI) for tapdance."""

import fire

from tapdance.docker import build_all_images, build_image
from tapdance.install_helper import install
from tapdance.plans import plan
from tapdance.syncs import sync


def main():
    """Run the tapdance CLI."""
    fire.Fire(
        {
            "install": install,
            "plan": plan,
            "sync": sync,
            "build_all_images": build_all_images,
            "build_image": build_image,
            "version": print_version,
        }
    )


def print_version():
    """Print the tapdance version number."""
    try:
        from importlib import metadata
    except ImportError:
        # Running on pre-3.8 Python; use importlib-metadata package
        import importlib_metadata as metadata

    version = metadata.version("tapdance")
    print(f"tapdance version {version}")


if __name__ == "__main__":
    main()
