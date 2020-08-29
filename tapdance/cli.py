"""tapdance.cli - Defines the command-line interface (CLI) for tapdance."""

import fire
from logless import logged

from tapdance.config import print_version
from tapdance.docker import build_all_images, build_image
from tapdance.install_helper import install
from tapdance.plans import plan
from tapdance.syncs import sync


@logged("tapdance execution")
def main():
    """Run the tapdance CLI."""
    fire.Fire(
        {
            "plan": plan,
            "sync": sync,
            "install": install,
            "build_all_images": build_all_images,
            "build_image": build_image,
            "--version": print_version,
        }
    )


if __name__ == "__main__":
    main()
