import fire

from .taputils import install, discover, plan, sync, build_image, build_all_images


def main():
    """Run the tapdance CLI."""
    fire.Fire(
        {
            "install": install,
            "plan": plan,
            "sync": sync,
            "build_image": build_image,
            "build_all_images": build_all_images,
        }
    )


if __name__ == "__main__":
    main()
