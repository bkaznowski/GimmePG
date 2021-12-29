import functools
import logging
import uuid
from string import Formatter
from datetime import datetime

import asyncpg

from query_builder import build_query


def flatten(t):
    return [item for sublist in t for item in sublist]


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
        for database in databases:
            dsn = database["dsn"]
            self.connections[database["name"]] = await asyncpg.connect(dsn)

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
        query = build_query(operation, count)
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

    @with_new_transactions
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
            await self._create_resources(
                resource_name=resource_name,
                count=current_batch_size,
                show_all_queries=show_all_queries,
                constants=constants,
            )

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
            return [str(uuid.uuid4()) for _ in range(count)]
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
            for i in range(count):
                all_values = flatten_object(values, i)
                formatted_values.append(value.format(**all_values))
            formatted_data[column_name] = convert_types(
                formatted_values, column_value.get("type", "text")
            )
        return formatted_data


def convert_types(values, type):
    if type == "text":
        return values
    if type == "timestamptz":
        return [datetime.strptime(value, "%Y-%m-%d %H:%M:%S.%f") for value in values]
    if type == "bigint":
        return [int(value) for value in values]


def flatten_object(o, element):
    if isinstance(o, list):
        return o[element]
    if not isinstance(o, dict):
        raise Exception("unexpected type")
    return {k: flatten_object(v, element) for k, v in o.items()}
