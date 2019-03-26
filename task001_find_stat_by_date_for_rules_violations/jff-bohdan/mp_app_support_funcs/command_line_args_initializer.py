import argparse

PERIODIC_PRINT_INTERVAL = 10000
DEFAULT_IP_ADDRESSES_MATCHER_REG_EXP = "192\.168\..*"
DEFAULT_PERIODIC_PRINTS = 0


def parse_command_line():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--input",
        metavar="FILE",
        help="input file path"
    )

    parser.add_argument(
        "--output",
        metavar="FILE",
        help="output file path"
    )

    parser.add_argument(
        "--ip_address_mask",
        action="store",
        help="RegExp for IP addresses validation",
        default=DEFAULT_IP_ADDRESSES_MATCHER_REG_EXP
    )

    parser.add_argument(
        "--delimiter",
        action="store",
        help="CSV delimiter",
        default=","
    )

    parser.add_argument(
        "--quote_char",
        action="store",
        help="CSV quote char",
        default="\""
    )

    parser.add_argument(
        "--periodic_interval",
        action="store",
        help="Interval between periodical status updates to console",
        default=DEFAULT_PERIODIC_PRINTS,
        type=int
    )

    parser.add_argument(
        "--output_limit",
        action="store",
        help="Limit output to specified count of rows",
        type=int
    )

    return parser.parse_args()
