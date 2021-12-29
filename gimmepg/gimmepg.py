import logging
import math

import asyncio
import yaml

from arguments import create_argument_parser
from worker import create_worker
from resources import Resources


class GimmePG:
    def __init__(self, resource_path):
        self.workers = []
        self.resources = Resources(resource_path)

    async def _init(self, connections, database_path):
        databases = self.load_databases(database_path)
        group_task = asyncio.gather(
            *[create_worker(databases, self.resources) for _ in range(connections)]
        )
        try:
            await asyncio.wait_for(group_task, 3)
        except asyncio.TimeoutError:
            logging.error("Timed out connecting to the databases...")
        try:
            self.workers = await group_task
        except asyncio.CancelledError:
            logging.error(
                "Failed to create {connections} connections to each database..."
            )

    @staticmethod
    def load_databases(database_path):
        with open(database_path, "r") as stream:
            return yaml.safe_load(stream)["databases"]

    async def create_resources(
        self, resource_name, count, batch_size, show_all_queries
    ):
        number_per_group = math.floor(count / len(self.workers))
        remainder = count % len(self.workers)
        group_task = asyncio.gather(
            *[
                worker.create_resources(
                    resource_name,
                    number_per_group + (int(i == 0) * remainder),
                    batch_size,
                    show_all_queries,
                )
                for i, worker in enumerate(self.workers)
            ]
        )
        await group_task


async def create_gimme_pg(connections, database_path, resource_path):
    gimme_pg = GimmePG(resource_path)
    await gimme_pg._init(connections, database_path)
    return gimme_pg


async def main(
    batch_size=100,
    connections=1,
    database_path="test",
    list_all_resources=False,
    resource_count=1,
    resource_name=None,
    resource_path="test",
    show_all_queries=False,
):
    logging.debug("Starting GimmePG")
    gimme_pg = await create_gimme_pg(connections, database_path, resource_path)
    await gimme_pg.create_resources(
        resource_name=resource_name,
        count=resource_count,
        batch_size=batch_size,
        show_all_queries=show_all_queries,
    )


if __name__ == "__main__":
    parser = create_argument_parser()
    args = parser.parse_args()
    asyncio.run(main(**vars(args)))
