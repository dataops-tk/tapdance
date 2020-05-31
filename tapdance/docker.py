import os
import sys

# import dock_r
from logless import get_logger, logged_block
import runnow
import uio
import yaml

from tapdance.config import SINGER_PLUGINS_INDEX

logging = get_logger("tapdance")

try:
    import dock_r
except Exception as ex:
    dock_r = None  # type: ignore
    logging.warning(f"Docker libraries were not able to be loaded ({ex}).")


BASE_DOCKER_REPO = "dataopstk/tapdance"


def rerun_dockerized(tap_alias, target_alias=None):
    cmd = f"tapdance {' '.join(sys.argv[1:])}"
    env = {
        k: v
        for k, v in os.environ.items()
        if (k.startswith("TAP_") or k.startswith("TARGET_"))
    }
    image_name = get_docker_tap_image(tap_alias, target_alias)
    try:
        dock_r.pull(image_name)
    except Exception as ex:
        logging.warning(f"Could not pull docker image '{image_name}'. {ex}")
    with logged_block(f"running dockerized command '{cmd}' on image '{image_name}'"):

        def _build_docker_run(image, command, environment, working_dir, volumes):
            e_str = " ".join([f"-e {k}={v}" for k, v in environment.items()])
            w_str = f"-w {working_dir}" if working_dir else ""
            v_str = " ".join([f"-v {x}:{y}" for x, y in volumes.items()])
            docker_run_cmd = f"docker run {e_str} {v_str} {w_str} {image} {command}"
            return docker_run_cmd

        volumes = {os.path.abspath("."): "/projects/my-project"}
        # DEV_DEBUG = False  # Used to test while developing, override python lib path
        # if DEV_DEBUG:
        #     container_lib = "/usr/local/lib/python3.8/site-packages/tapdance"
        #     host_lib = "C:\\Files\\Source\\tapdance\\tapdance"
        #     volumes[host_lib] = container_lib
        docker_run_cmd = _build_docker_run(
            image=image_name,
            command=cmd,
            environment=env,
            working_dir="/projects/my-project",
            volumes=volumes,
        )
        runnow.run(docker_run_cmd)
    return True


def get_docker_tap_image(tap_alias, target_alias=None):
    if tap_alias.startswith("tap-"):
        tap_alias = tap_alias.replace("tap-", "")
    if not target_alias:
        return f"{BASE_DOCKER_REPO}:tap-{tap_alias}"
    if target_alias.startswith("target-"):
        target_alias = target_alias.replace("target-", "")
    return f"{BASE_DOCKER_REPO}:{tap_alias}-to-{target_alias}"


def get_plugins_list(plugins_index=None):
    plugins_index = plugins_index or SINGER_PLUGINS_INDEX
    if not uio.file_exists(plugins_index):
        raise RuntimeError(
            f"No file found at '{plugins_index}'."
            "Please set SINGER_PLUGINS_INDEX and try again."
        )
    yml_doc = yaml.safe_load(uio.get_text_file_contents(plugins_index))
    taps = yml_doc["singer-taps"]
    list_of_tuples = []
    taps = yml_doc["singer-taps"]
    targets = yml_doc["singer-targets"]
    plugins = taps + targets
    for plugin in plugins:
        list_of_tuples.append(
            (plugin["name"], plugin.get("source", None), plugin.get("alias", None))
        )
    return list_of_tuples


def build_all_standalone(
    source_image=None, plugins_index=None, push=False, pre=False, ignore_cache=False
):
    plugins = get_plugins_list(plugins_index)
    created_images = []
    for name, source, alias in plugins:
        created_images.append(
            build_plugin_image(
                name,
                source=source,
                alias=alias,
                source_image=source_image,
                push=push,
                pre=pre,
                ignore_cache=ignore_cache,
            )
        )
    return created_images


def get_plugin_info(plugin_id, plugins_index=None):
    plugins = get_plugins_list(plugins_index)
    for name, source, alias in plugins:
        if (alias or name) == plugin_id:
            return (name, source, alias)
    raise ValueError(f"Could not file a plugin called '{plugin_id}'")


def build_all_composite(
    source_image=None, plugins_index=None, push=False, pre=False, ignore_cache=False
):
    plugins = get_plugins_list(plugins_index)
    created_images = []
    for tap_name, tap_source, tap_alias in plugins:
        tap_alias = tap_alias or tap_name
        for target_name, target_source, target_alias in plugins:
            target_alias = target_alias or target_name
            if tap_alias.startswith("tap-") and target_alias.startswith("target-"):
                created_images.append(
                    build_composite_image(
                        tap_alias,
                        target_alias,
                        push=push,
                        pre=pre,
                        ignore_cache=ignore_cache,
                    )
                )
    return created_images


def build_plugin_image(
    plugin_name,
    source,
    alias,
    source_image=None,
    push=False,
    pre=False,
    ignore_cache=False,
):
    source = source or plugin_name
    alias = alias or plugin_name
    image_name = f"{BASE_DOCKER_REPO}:{alias}"
    build_cmd = f"docker build"
    if ignore_cache:
        build_cmd += " --no-cache"
    if source_image:
        build_cmd += f" --build-arg source_image={source_image}"
    if pre:
        build_cmd += f" --build-arg prerelease=true"
        image_name += "--pre"
    build_cmd += (
        f" --build-arg PLUGIN_NAME={plugin_name}"
        f" --build-arg PLUGIN_SOURCE={source}"
        f" --build-arg PLUGIN_ALIAS={alias}"
        f" -t {image_name}"
        f" -f singer-plugin.Dockerfile"
        f" ."
    )
    runnow.run(build_cmd)
    if push:
        push(image_name)
    return image_name


def build_composite_image(
    tap_alias, target_alias, push=False, pre=False, ignore_cache=False
):
    if tap_alias.startswith("tap-"):
        tap_alias = tap_alias.replace("tap-", "", 1)
    if target_alias.startswith("target-"):
        target_alias = target_alias.replace("target-", "", 1)
    image_name = f"{BASE_DOCKER_REPO}:{tap_alias}-to-{target_alias}"
    build_cmd = f"docker build"
    if ignore_cache:
        build_cmd += " --no-cache"
    if pre:
        build_cmd += " --build-arg source_image_suffix=--pre"
        image_name += "--pre"
    build_cmd += (
        f" --build-arg tap_alias={tap_alias}"
        f" --build-arg target_alias={target_alias}"
        f" -t {image_name}"
        f" -f tap-to-target.Dockerfile"
        f" ."
    )
    runnow.run(build_cmd)
    if push:
        push(image_name)
    return image_name


def push(image_name):
    runnow.run(f"docker push {image_name}")


def build_image(
    tap_or_plugin_alias: str,
    target_alias: str = None,
    push: bool = False,
    pre: bool = False,
    ignore_cache: bool = False,
):
    """Build a single image.

    If tap and target are both provided, any required upstream images will be built as well.

    Arguments:
        tap_or_plugin_alias {str} -- The name of the tap (without the `tap-` prefix).

    Keyword Arguments:
        target_alias {str} -- Optional. The name of the target (without the `target-` prefix).
        push {bool} -- True to push images to image repository after build. (default: {False})
        pre {bool} -- True to use and create prelease versions. (default: {False})
        ignore_cache {bool} -- True to build images without cached image layers. (default: {False})
    """
    name, source, alias = get_plugin_info(f"tap-{tap_or_plugin_alias}")
    build_plugin_image(
        name, source=source, alias=alias, push=push, pre=pre, ignore_cache=ignore_cache
    )
    if target_alias:
        name, source, alias = get_plugin_info(f"target-{target_alias}")
        build_plugin_image(
            name,
            source=source,
            alias=alias,
            push=push,
            pre=pre,
            ignore_cache=ignore_cache,
        )
        build_composite_image(
            tap_alias=tap_or_plugin_alias,
            target_alias=target_alias,
            push=push,
            pre=pre,
            ignore_cache=ignore_cache,
        )


def build_all_images(push: bool = False, pre: bool = False, ignore_cache: bool = False):
    """
    Build all images.

    :param push: Push images after building
    :param pre: Create and publish pre-release builds
    :param ignore_cache: True to build images without cached image layers. (default: {False})
    """
    build_all_standalone(push=push, pre=pre, ignore_cache=ignore_cache)
    build_all_composite(push=push, pre=pre, ignore_cache=ignore_cache)
