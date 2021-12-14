import typing as tp
import logging
import yaml
try:
    from importlib.resources import files
except ImportError:
    # Try backported to PY<37 `importlib_resources`.
    from importlib_resources import files
import config as cg


def read_config() -> tp.Dict[str, str]:
    try:
        content = files(cg).joinpath("config.yaml").read_text()
        config = yaml.load(content, Loader=yaml.FullLoader)
        logging.debug(f"Configuration dict: {content}")
        return config
    except OSError as e:
        logging.error("Error in configuration file!")
        logging.error(e)
        exit()
