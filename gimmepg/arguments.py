import argparse


def create_argument_parser():
    parser = argparse.ArgumentParser(
        description="Batch insert directly into Postgres compatible databases."
    )
    parser.add_argument(
        "resource_name",
        metavar="RESOURCE NAME",
        help="the name of the resource to create",
        nargs="?",
    )

    parser.add_argument(
        "-l",
        "--list",
        help="list all resources and exit",
        dest="list_all_resources",
        action="store_true",
    )

    parser.add_argument(
        "-rd",
        "--resource_directory",
        metavar="PATH/TO/RESOURCE/DIRECTORY",
        dest="resource_path",
        help="the directory with JSON resource definitions",
        required=True,
    )

    parser.add_argument(
        "-dd",
        "--database_directory",
        metavar="PATH/TO/DATABASE/DIRECTORY",
        dest="database_path",
        help="the directory with database configuration JSON files",
        required=True,
    )

    parser.add_argument(
        "-sq",
        "--show_all_queries",
        dest="show_all_queries",
        help="outputs all the queries being made",
        action="store_true",
    )

    parser.add_argument(
        "-n",
        "--num",
        "--number",
        type=int,
        metavar="NUMBER",
        dest="resource_count",
        default=1,
        help="the number of resources to insert",
    )

    parser.add_argument(
        "-b",
        "--batch_size",
        type=int,
        metavar="NUMBER",
        dest="batch_size",
        default=100,
        help="the maximum number of elements being inserted in one transaction",
    )

    parser.add_argument(
        "-c",
        "--connections",
        type=int,
        metavar="NUMBER",
        dest="connections",
        default=1,
        help="the number of connections open to the database",
    )

    return parser
