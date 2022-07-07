"""
Implements the main command line interface of the E3 data crawler
"""

import logging

logger = logging.getLogger(__name__)


def main(argv=None, prog=None):
    """
    Parses the commandline arguments, reads the configuration and starts the main program flow

    :param argv:
    :param prog:
    """

    logging.basicConfig(format="%(asctime)s %(name)s %(levelname)s: %(message)s", level=logging.DEBUG)

    logger.debug("Startup E3 Data Crawler")

    pass  # TODO: Implement the main program flow
