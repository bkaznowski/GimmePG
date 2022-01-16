# GimmePG
GimmePG is a tool that allows you to quickly populate a Postgres compatible database with data. Its intended use is for populating brand new test environments with lots of pseudo-random but valid data.

**WARNING: Do not allow unverified data into this tool. The way the queries are built means it can be used to perform SQL injection. Ideally, only use this for internal testing purposes with no exposure to the outside world.** 

# How to use
## General usage
GimmePG works by defining *resources*. *Resources* can be thought of as a logical grouping of related operations. For example, say your system has users and groups of users. Each user must be assigned to a user group. You may want to define a user group as a resource and also user as a resource. User can then refer to the user group operations when it is being created.

Each resource must have a name. This is used so other resources can refer to the resource and also so you can tell GimmePG which resource you wish to create.
Additionally, each resource consists of three main parts:
- constants - these are executed first and are only executed once per operation. For example, using the idea of users and user groups, you may define a constant called `user_group_id` which resolves to the ID from creating a user group. Then, when you want to create 5000 users it will first create a user group and then use the same user group for all 5000 users.
- variables - these are executed after the constants. What sets variables apart is that they are executed once per resource. For example, say you want to store a randomly generated UUID as a variable and refer to it in future operations. You can define it as `user_id` and then refer to it in operations with `{variable[user_id]}`.
- operations - this is a list of operations that will get executed on the databases. These are executed in order and after constants and variables.

### Operations
The operations field is a list with items following this example format
```yaml
  - name: "users_update_status"
    database: "users"
    operation: "update"
    table: "users"
    data:
      update_id:
        value: "{user_statuses_insert[id]}"
        type: text
    where:
      id:
        value: "{variable[user_id]}"
        type: text
```
The fields here are:
- name - this is used to name the operation so it can be refered to in future operations.
- database - this is the name of the database the operations must be executed on.
- operation - this can be one of `insert` or `update`. This describes if the operation being performed is an insert or an update.
- table - this is the name of the table (+ schema) that an operations gets executed on.
- data - this is a map describing the values of the columns. The keys are the column names and the values are a map with keys `value` and `type`. `value` is the value that will be insert into the database and `type` describes the data type to convert the value to. Currently, you can provide `text`, `bigint`, `timestamptz` for the value.
- where - this can only be provided for updates. This describes the condition required for the update. The condition must be unique, so it only ever has the chance of updating a single row.

Operations and variables get executed in batches, which increases performance.

## Using GimmePG as a standalone binary
You can use GimmePG as a standalone binary.

## Using GimmePG as a library
You can use GimmePG as a binary for more flexibility. This allows you to use the data output from it for other steps in your testing pipeline.

```python
import asyncio

from gimmepg import create_gimme_pg

user_group_yaml = """variables:
    user_group_id:
      value: "{uuid}"
operations:
    - name: "user_group_insert"
      database: "user_groups"
      operation: "insert"
      table: "user_groups"
      data:
        id:
          value: "{variable[user_group_id]}"
          type: text
        create_timestamp:
          value: "{now}"
          type: timestamptz
"""

async def main():
    databases = {
        "users": {
            # These will be passed directly to the asyncpg connect method as kwargs
            "dsn": "postgres://postgres:mysecretpassword@localhost:5432"
        },
        "user_details": {
            # These will be passed directly to the asyncpg connect method as kwargs
            "dsn": "postgres://postgres:mysecretpassword@localhost:5432"
        },
    }

    resource_yamls = {
        "user_group": user_group_yaml
    }

    gimme_pg = await create_gimme_pg(5, databases=databases, resource_yamls=resource_yamls)
    await gimme_pg.create_resources(
        resource_name="user_group",
        count=1000,
        batch_size=10,
    )


if __name__ == "__main__":
    parser = create_argument_parser()
    args = parser.parse_args()
    asyncio.run(main(**vars(args)))
```

# Further improvements and known issues:
1. Add tests
2. Add validation and make it easier to use
3. Allow for injection of custom types
4. Constants run once per worker
5. Update response cannot be used reliably
6. Return created data for future usage