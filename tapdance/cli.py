import fire

from .taputils import install, plan, sync, build_all_images, build_image


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
