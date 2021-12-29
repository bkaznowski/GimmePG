def build_query(operation, count):
    all_data_params = []
    vals_length = len(operation["data"]) + len(operation.get("where", []))
    for i in range(0, count*vals_length, vals_length):
        data_params = ",".join([f"${j+1+i}" for j in range (vals_length)])
        data_params = f"({data_params})"
        all_data_params.append(data_params)
    data_params = ",".join(all_data_params)
    query = ""
    if operation["operation"] == "insert":
        query = (
            f"INSERT INTO {operation['table']} ( "
            f"{','.join(operation['data'].keys())} "
            f") VALUES  "
            f"{data_params} "
            f" RETURNING *; "
        )
    if operation["operation"] == "update":
        column_sets = []
        for column_name in operation['data'].keys():
            column_sets.append(f"{column_name} = vals.{column_name}")
        where_params = operation.get("where", None)
        if not where_params:
            raise Exception("where conditions required")

        where_conditions = []
        for column_name in where_params.keys():
            where_conditions.append(f"t.{column_name} = vals.{column_name}")

        vals_column_names = list(operation['data'].keys()) + list(where_params.keys())

        query = (
            f"UPDATE {operation['table']} AS t "
            f"SET {','.join(column_sets)} "
            f"FROM (VALUES {data_params}) as vals({','.join(vals_column_names)}) "
            f"WHERE {' AND '.join(where_conditions)} "
            f"RETURNING *; "
        )

    return query