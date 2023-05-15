"""tapdance.states - Defines state aggregation and state handling helper functions."""

import json

from logless import get_logger
import uio
import backoff
from botocore.exceptions import ConnectionClosedError

logging = get_logger("tapdance")


def _is_valid_json(json_text: str) -> bool:
    try:
        _ = json.loads(json_text)
    except ValueError:
        return False
    return True


def make_aggregate_state_file(raw_json_lines_file: str, output_json_file: str) -> None:
    """
    Create a valid json state file from one or more json lines ('jsonl' format).

    Parameters
    ----------
    raw_json_lines_file : str
        Path to a jsonl (json lines) file containing one or more json documents to
        aggregate.
    output_json_file : str
        Path to use when saving the aggregated json file.
    """
    try:
        download_state_file(
            output_json_file,
            get_aggregate_state(download_raw_state_file_contents(raw_json_lines_file))
        )
    except ValueError as ex:
        raise ValueError(
            f"State file from '{raw_json_lines_file}' is not valid JSON or JSONL. "
            f"Please either delete or fix the file and then retry. {ex}"
        )

def log_backoff_attempt(details):
   logging.warning(f'Error detected communicating with AWS downloading state file, triggering backoff: {details.get("tries")}')

@backoff.on_exception(
    backoff.expo,
    ConnectionClosedError,
    max_tries=3,
    on_backoff=log_backoff_attempt
)
def download_raw_state_file_contents(raw_json_lines_file):
    return uio.get_text_file_contents(raw_json_lines_file)

@backoff.on_exception(
    backoff.expo,
    ConnectionClosedError,
    max_tries=3,
    on_backoff=log_backoff_attempt
)
def download_state_file(output_json_file,aggregate_state):
        uio.create_text_file(
            output_json_file,
            aggregate_state)


def get_aggregate_state(raw_json_lines_text: str) -> str:
    """
    Return a valid aggregated json state string from one or json lines ('jsonl' format).

    Parameters
    ----------
    raw_json_lines_text : str
        String jsonl (json lines) with one or more json documents to aggregate.
    """
    if raw_json_lines_text == "":
        raise ValueError("Cannot aggregate json state - text is empty string.")
    if _is_valid_json(raw_json_lines_text):
        return raw_json_lines_text
    elif _is_valid_json(raw_json_lines_text.splitlines()[-1]):
        logging.warning(
            "State file contains multiple states. Using final line of JSON state: "
            + raw_json_lines_text.replace("\n", "\\n")
        )
        return raw_json_lines_text.splitlines()[-1]
    elif len(raw_json_lines_text.splitlines()) >= 2 and _is_valid_json(
        raw_json_lines_text.splitlines()[-2]
    ):
        logging.warning(
            "State file contains multiple states. "
            "Using 2nd-to-last line of JSON state: "
            + raw_json_lines_text.replace("\n", "\\n")
        )
        return raw_json_lines_text.splitlines()[-2]
    else:
        raise ValueError(
            "State is not valid JSON: " + raw_json_lines_text.replace("\n", "\\n")
        )
