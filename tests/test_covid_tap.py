"""Test using covid-19 tap."""

import os

from tapdance import plan, sync

PROJECT_ROOT = "."


def test_covid_plan():
    """Test covid plan."""
    for env_var in ["GITHUB_TOKEN"]:
        if env_var in os.environ:
            os.environ["TAP_COVID_19_api_token"] = os.environ[env_var]
            break
    plan(
        "covid-19",
        taps_dir=f"{PROJECT_ROOT}/samples/taps/covid-19",
        config_dir=f"{PROJECT_ROOT}/samples/taps/covid-19/.secrets",
        dockerized=False,
    )


def test_covid_sync():
    """Test covid sync."""
    for env_var in ["GITHUB_TOKEN"]:
        if env_var in os.environ:
            os.environ["TAP_COVID_19_api_token"] = os.environ[env_var]
            break
    sync(
        "covid-19",
        taps_dir=f"{PROJECT_ROOT}/samples/taps/covid-19",
        config_dir=f"{PROJECT_ROOT}/samples/taps/covid-19/.secrets",
    )


if __name__ == "__main__":
    test_covid_plan()
    test_covid_sync()
