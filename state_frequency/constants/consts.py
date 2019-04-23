"""Create Project Constants."""

from state_frequency.constants.config_reader import ConfigReader
from state_frequency.utils.exceptions import StateFileError


class Constants:
    """Define Constants based on Config File."""
    config_reader = ConfigReader()

    try:
        config_file = "state_frequency/config.yaml"
    except:
        raise StateFileError

    config_reader.set_config_path(config_file)

    # SPARK ETL PROCESSING
    status = config_reader.get_config_from_file("hdfs_paths")
    _INPUT_PATH = status.get("INPUT_PATH")
    _RESULT_PATH = status.get("RESULT_PATH")