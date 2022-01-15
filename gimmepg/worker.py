from copy import deepcopy
import functools
import logging
import random
import uuid
from string import Formatter
from datetime import datetime

import asyncpg

from query_builder import build_query


def flatten(t):
    return [item for sublist in t for item in sublist]


def convert_type(value, type):
    if type == "text":
        return value
    if type == "timestamptz":
        # Use fromisoformat instead of datetime.strptime(value, "%Y-%m-%d %H:%M:%S.%f").
        # It is much faster
        return datetime.fromisoformat(value)
    if type == "bigint":
        return int(value)


def flatten_object(o, count):
    if isinstance(o, list):
        return o
    if not isinstance(o, dict):
        raise Exception("unexpected type")
    results = [{} for _ in range(count)]
    for k, v in o.items():
        objects = flatten_object(v, count)
        for i, object in enumerate(objects):
            results[i][k] = object
    return results


async def create_worker(databases, resources):
    w = Worker(resources)
    await w._init(databases)
    return w


class Worker:
    def __init__(self, resources):
        self.connections = {}
        self.resources = resources

    async def _init(self, databases):
        self.connections = {}
        for database_name, kwargs in databases.items():
            self.connections[database_name] = await asyncpg.connect(**kwargs)

    def with_new_transactions(func):
        @functools.wraps(func)
        async def wrapper(self, *args, **kwargs):
            transactions = {
                db_name: connection.transaction()
                for db_name, connection in self.connections.items()
            }
            logging.debug("Opening transactions")
            for tx in transactions.values():
                await tx.start()
            try:
                return_val = await func(self, *args, **kwargs)
            except Exception as e:
                logging.warning(
                    f"Rolling back transaction due to error while running: {e}"
                )
                for tx in transactions.values():
                    await tx.rollback()
                raise
            else:
                # TODO this may fail on commit. I need to work out what to do with the other transactions
                for tx in transactions.values():
                    await tx.commit()
                return return_val

        return wrapper

    async def execute_operations(
        self, resource_name, count, show_all_queries, constants, variables
    ):
        operations = self.resources.get_operations(resource_name)
        operation_results = {}
        for operation in operations:
            logging.debug(f"Running operation {operation['name']}")
            operation_results[operation["name"]] = await self.execute_operation(
                operation=operation,
                count=count,
                show_query=show_all_queries,
                constants=constants,
                variables=variables,
                operation_results=operation_results,
            )
        return operation_results

    async def execute_operation(
        self, operation, count, show_query, constants, variables, operation_results
    ):
        query = build_query(
            table_name=operation["table"],
            data_column_names=tuple(operation["data"].keys()),
            operation_type=operation["operation"],
            where_column_names=tuple(operation.get("where", [])),
            count=count
        )
        query_parameters = await self._derive_query_parameters(
            data=operation["data"],
            count=count,
            show_all_queries=show_query,
            constants=constants,
            variables=variables,
            operation_results=operation_results,
        )
        where_parameters = {}
        if operation["operation"] == "update":
            where_params = operation.get("where", None)
            if not where_params:
                raise Exception("where conditions required")
            where_parameters = await self._derive_query_parameters(
                data=where_params,
                count=count,
                show_all_queries=show_query,
                constants=constants,
                variables=variables,
                operation_results=operation_results,
            )

        all_parameters = list(query_parameters.values()) + list(
            where_parameters.values()
        )

        sorted_parameters = flatten(zip(*all_parameters))
        logging.debug(f"{query=}")
        logging.debug(f"{sorted_parameters=}")
        results = await self.connections[operation["database"]].fetch(
            query, *sorted_parameters
        )
        converted_results = {}
        for row in results:
            for column in row.keys():
                existing_column_results = converted_results.get(column, [])
                existing_column_results.append(row[column])
                converted_results[column] = existing_column_results
        return converted_results

    async def derive_constants(self, resource_name, show_all_queries):
        return await self._derive_query_parameters(
            self.resources.get_constants(resource_name),
            1,
            show_all_queries,
            constants={},
            variables={},
            operation_results={},
        )

    async def derive_variables(self, resource_name, count, show_all_queries, constants):
        return await self._derive_query_parameters(
            self.resources.get_variables(resource_name),
            count,
            show_all_queries,
            constants=constants,
            variables={},
            operation_results={},
        )

    async def create_resources(
        self, resource_name, count, batch_size, show_all_queries
    ):
        resources_left = count
        constants = await self.derive_constants(resource_name, show_all_queries)
        while resources_left:
            current_batch_size = (
                resources_left if resources_left < batch_size else batch_size
            )
            resources_left = resources_left - current_batch_size
            # TODO maybe this can be moved so it awaits after dispatching everything?
            await self._create_resources(
                resource_name=resource_name,
                count=current_batch_size,
                show_all_queries=show_all_queries,
                constants=constants,
            )

    @with_new_transactions
    async def _create_resources(
        self, resource_name, count, show_all_queries, constants
    ):
        variables = await self.derive_variables(
            resource_name=resource_name,
            count=count,
            show_all_queries=show_all_queries,
            constants=constants,
        )
        return await self.execute_operations(
            resource_name=resource_name,
            count=count,
            show_all_queries=show_all_queries,
            constants=constants,
            variables=variables,
        )

    async def _derive_single_column_query_parameters(
        self,
        parameter_name,
        count,
        show_all_queries,
        constants,
        variables,
        operation_results,
    ):
        if parameter_name == "uuid":
            # using custom method for uuids as it is much faster than str(uuid.uuid4()) and
            # should be good enough for what is needed
            return [
                uuid.UUID(int=random.getrandbits(128), version=4) for _ in range(count)
            ]
        elif parameter_name == "now":
            return [datetime.utcnow()] * count
        elif parameter_name.startswith("resource"):
            resource_name = parameter_name.split("[")[1][:-1]
            return {
                resource_name: await self._create_resources(
                    resource_name,
                    count,
                    show_all_queries=show_all_queries,
                    constants=constants,
                )
            }
        elif parameter_name.startswith("constant"):
            return constants
        elif parameter_name.startswith("variable"):
            return variables
        else:
            resource_name = parameter_name.split("[")[0]
            return operation_results[resource_name]

    async def _derive_query_parameters(
        self, data, count, show_all_queries, constants, variables, operation_results
    ):
        formatted_data = {}
        for column_name, column_value in data.items():
            value = column_value["value"]
            parameters = [
                parameter
                for _, parameter, _, _ in Formatter().parse(value)
                if parameter
            ]
            values = {}
            for parameter in parameters:
                new_request = parameter.split("[")[0]
                values[new_request] = await self._derive_single_column_query_parameters(
                    parameter,
                    count,
                    show_all_queries,
                    constants,
                    variables,
                    operation_results,
                )
            formatted_values = []
            all_values = flatten_object(values, count)
            for i in range(count):
                formatted_values.append(
                    convert_type(
                        value.format(**all_values[i]), column_value.get("type", "text")
                    )
                )

            formatted_data[column_name] = formatted_values
        return formatted_data
