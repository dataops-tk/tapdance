"""Test Version Prints"""

from tapdance import cli


def test_print_version():
    """Test the ability to print a version string"""
    cli.print_version()


if __name__ == "__main__":
    test_print_version()
