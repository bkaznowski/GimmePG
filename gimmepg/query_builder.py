import functools


@functools.lru_cache
def build_query(
    table_name, data_column_names, operation_type, where_column_names, count
):
    all_data_params = []
    vals_length = len(data_column_names) + len(where_column_names)
    for i in range(0, count * vals_length, vals_length):
        data_params = ",".join([f"${j+1+i}" for j in range(vals_length)])
        data_params = f"({data_params})"
        all_data_params.append(data_params)
    data_params = ",".join(all_data_params)
    query = ""
    if operation_type == "insert":
        query = (
            f"INSERT INTO {table_name} ( "
            f"{','.join(data_column_names)} "
            f") VALUES  "
            f"{data_params} "
            f" RETURNING *; "
        )
    elif operation_type == "update":
        column_sets = []
        for column_name in data_column_names:
            column_sets.append(f"{column_name} = vals.{column_name}")
        if not where_column_names:
            raise Exception("where conditions required")

        where_conditions = []
        for column_name in where_column_names:
            where_conditions.append(f"t.{column_name} = vals.{column_name}")

        vals_column_names = list(data_column_names) + list(where_column_names)

        query = (
            f"UPDATE {table_name} AS t "
            f"SET {','.join(column_sets)} "
            f"FROM (VALUES {data_params}) as vals({','.join(vals_column_names)}) "
            f"WHERE {' AND '.join(where_conditions)} "
            f"RETURNING *; "
        )

    return query
