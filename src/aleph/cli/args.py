import argparse
import logging

from aleph import __version__


def parse_args(args):
    """Parse command line parameters

    Args:
      args ([str]): command line parameters as list of strings

    Returns:
      :obj:`argparse.Namespace`: command line parameters namespace
    """
    parser = argparse.ArgumentParser(prog="aleph", description="Aleph Network Node")
    parser.add_argument(
        "--version", action="version", version="pyaleph {ver}".format(ver=__version__)
    )
    parser.add_argument("-c", "--config", action="store", dest="config_file")
    parser.add_argument(
        "-p", "--port", action="store", type=int, dest="port", required=False
    )
    parser.add_argument(
        "--bind", "-b", action="store", type=str, dest="host", required=False
    )
    parser.add_argument("--debug", action="store_true", dest="debug", default=False)
    parser.add_argument(
        "--no-commit", action="store_true", dest="no_commit", default=False
    )
    parser.add_argument("--no-jobs", action="store_true", dest="no_jobs", default=False)
    parser.add_argument(
        "-v",
        "--verbose",
        dest="loglevel",
        help="set loglevel to INFO",
        action="store_const",
        const=logging.INFO,
        default=logging.WARNING,
    )
    parser.add_argument(
        "-vv",
        "--very-verbose",
        dest="loglevel",
        help="set loglevel to DEBUG",
        action="store_const",
        const=logging.DEBUG,
    )
    parser.add_argument(
        "-k",
        "--key-dir",
        dest="key_dir",
        help="Path to the keys directory. Only used in combination with --gen-keys.",
        action="store",
        type=str,
        default="keys",
    )
    parser.add_argument(
        "--disable-sentry",
        dest="sentry_disabled",
        help="Disable Sentry error tracking",
        action="store_true",
        default=False,
    )
    return parser.parse_args(args)
