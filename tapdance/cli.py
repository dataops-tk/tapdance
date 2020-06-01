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
        }
    )


if __name__ == "__main__":
    main()
