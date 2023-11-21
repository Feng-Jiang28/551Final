import os
import cmd
import csv
from collections import defaultdict


def ensure_db_directory():
    if not os.path.exists('dbs'):
        os.makedirs('dbs')


def create_table(table_name, headers):
    ensure_db_directory()
    file_name = f"dbs/{table_name}.csv"
    if not os.path.exists(file_name):
        with open(file_name, 'w') as file:
            file.write(','.join(headers) + '\n')
            print(f"Table {table_name} created.")
    else:
        print(f"Table {table_name} already exists.")


# insert into employees values 1,John,30
def insert_data(file_name, data):
    with open(f"dbs/{file_name}.csv", 'a') as file:
        file.write(f"{data}\n")
        print(f"Data inserted into {file_name}.")


def is_condition_met(value, operator, condition_value):
    if operator == '=':
        return value == condition_value
    elif operator == '>':
        return value > condition_value
    elif operator == '<':
        return value < condition_value
    elif operator == '>=':
        return value >= condition_value
    elif operator == '<=':
        return value <= condition_value
    else:
        raise ValueError(f"Unknown operator: {operator}")


# query_data("employees.csv", [0 1 2], [2 >= 25])
def query_data(file_name, column_names, condition=None):
    with open(f"dbs/{file_name}", 'r') as file:
        header = next(file).strip().split(',')
        column_indices = [header.index(name) for name in column_names]

        for line in file:
            values = line.strip().split(',')
            if condition:
                condition_column_name, operator, condition_value = condition
                condition_column_index = header.index(condition_column_name)  # Convert to index
                if not is_condition_met(float(values[condition_column_index]), operator, float(condition_value)):
                    continue
            selected_values = [values[i] for i in column_indices]
            print(','.join(selected_values))


def delete_data(file_name, condition):
    print(condition)
    updated_lines = []
    with open(f"dbs/{file_name}.csv", 'r') as file:
        header = next(file).strip().split(',')  # Save the header
        condition_column_index = header.index(condition[0])  # Convert condition column name to index

        for line in file:
            values = line.strip().split(',')
            operator, condition_value = condition[1], condition[2]
            if is_condition_met(float(values[condition_column_index]), operator, float(condition_value)):
                continue  # Skip this line (row) as it meets the delete condition
            updated_lines.append(line)

    with open(f"dbs/{file_name}.csv", 'w') as file:
        file.write(','.join(header) + '\n')  # Write the header back
        for line in updated_lines:
            file.write(line)


# Lazy Reading for Querying Data
def query_data_lazy(file_name, column_names, condition=None):
    with open(f"dbs/{file_name}", 'r') as file:
        header = file.readline().strip().split(',')
        column_indices = [header.index(name) for name in column_names]

        for line in file:
            values = line.strip().split(',')
            if condition:
                condition_column_name, operator, condition_value = condition
                condition_column_index = header.index(condition_column_name)
                if not is_condition_met(float(values[condition_column_index]), operator, float(condition_value)):
                    continue
            selected_values = [values[i] for i in column_indices]
            yield ','.join(selected_values)


buffer_limit = 100
data_buffer = []


# Buffered Writing for Inserting Data
def insert_data_buffered(file_name, data):
    data_buffer.append(data)
    if len(data_buffer) >= buffer_limit:
        flush_buffer(file_name)


def flush_buffer(file_name):
    with open(f"dbs/{file_name}.csv", 'a') as file:
        for data in data_buffer:
            file.write(f"{data}\n")
    data_buffer.clear()


def update_data(file_name, condition, updates):
    updated_lines = []
    with open(f"dbs/{file_name}.csv", 'r') as file:
        header = next(file).strip().split(',')  # Save the header
        condition_column_index = header.index(condition[0])  # Convert condition column name to index

        # Convert update column names to indices
        update_indices = [(header.index(col_name), new_val) for col_name, new_val in updates]

        for line in file:
            values = line.strip().split(',')
            operator, condition_value = condition[1], condition[2]
            if is_condition_met(float(values[condition_column_index]), operator, float(condition_value)):
                for col_idx, new_val in update_indices:
                    values[col_idx] = new_val
            updated_lines.append(','.join(values))

    with open(f"dbs/{file_name}.csv", 'w') as file:
        file.write(','.join(header) + '\n')  # Write the header back
        for line in updated_lines:
            file.write(line + '\n')


def order_data(file_name, column_name, order='asc'):
    with open(f"dbs/{file_name}.csv", 'r') as file:
        header = next(file).strip().split(',')  # Read the header
        column_index = header.index(column_name)  # Find index of the column name
        print(column_index)
        # Processing the data lines
        data = [line.strip().split(',') for line in file.readlines()]

        # Sorting the data based on the specified column
        data.sort(key=lambda row: float(row[column_index]) if row[column_index].replace('.', '', 1).isdigit() else row[
            column_index],
                  reverse=(order.lower() == 'desc'))

        # Print the sorted data
        print(','.join(header))  # Print the header
        for row in data:
            print(','.join(row))  # Print the sorted data rows


def aggregate_data(file_name, column_name, agg_function):
    values = []
    with open(f"dbs/{file_name}", 'r') as file:
        header = next(file).strip().split(',')  # Read the header
        column_index = header.index(column_name)  # Find index of the column name

        for line in file:
            row = line.strip().split(',')
            values.append(float(row[column_index]))

    if agg_function == 'sum':
        return sum(values)
    elif agg_function == 'count':
        return len(values)
    elif agg_function == 'max':
        return max(values)
    elif agg_function == 'min':
        return min(values)
    elif agg_function == 'avg':
        return sum(values) / len(values) if values else 0


def join_tables(file_name1, file_name2, join_column_name, selected_columns=None):
    with open(f"dbs/{file_name1}", 'r') as file1, open(f"dbs/{file_name2}", 'r') as file2:
        header1 = next(file1).strip().split(',')
        header2 = next(file2).strip().split(',')
        join_column_index1 = header1.index(join_column_name)
        join_column_index2 = header2.index(join_column_name)  # Assuming the same column name in both tables

        data1 = [line.strip().split(',') for line in file1.readlines()]
        data2 = [line.strip().split(',') for line in file2.readlines()]

        dict2 = {row[join_column_index2]: row for row in data2}

        joined_data = []
        for row1 in data1:
            key = row1[join_column_index1]
            if key in dict2:
                combined_row = row1 + dict2[key]

                if selected_columns:
                    # Find indices for selected columns
                    combined_indices = []
                    for col in selected_columns:
                        table_name, col_name = col.split('.')
                        if table_name == file_name1.split('.')[0]:
                            combined_indices.append(header1.index(col_name))
                        else:
                            combined_indices.append(len(header1) + header2.index(col_name))
                    combined_row = [combined_row[i] for i in combined_indices]

                joined_data.append(combined_row)

        for row in joined_data:
            print(','.join(row))


def group_data(file_name, group_by_column, print_columns):
    grouped_data = defaultdict(list)
    with open(f"{file_name}", mode='r') as file:
        reader = csv.DictReader(file)
        for row in reader:
            group_key = row[group_by_column]
            grouped_data[group_key].append({col: row[col] for col in print_columns})

    # Print grouped data
    # for group, rows in grouped_data.items():
    #     print(f"Group: {group}")
    #     for row in rows:
    #         print(', '.join(f"{key}: {value}" for key, value in row.items()))
    #     print()  # Empty line for better separation between groups

    return grouped_data


def complex_query_execute(file_name1, file_name2, join_column_name, condition, selected_columns, order_column,
                          order='asc'):
    # Join, filter, and order the data from two CSV files
    with open(file_name1, mode='r') as file1, open(file_name2, mode='r') as file2:
        reader1 = csv.DictReader(file1)
        reader2 = csv.DictReader(file2)
        data1 = [row for row in reader1]
        data2 = {row[join_column_name]: row for row in reader2}

        # Joining data
        joined_data = []
        for row1 in data1:
            key = row1[join_column_name]
            if key in data2:
                joined_row = {**row1, **data2[key]}
                joined_data.append(joined_row)

        # Filtering data based on the condition
        condition_column, operator, condition_value = condition
        filtered_data = [row for row in joined_data if
                         float(row[condition_column]) > float(condition_value)]  # Assuming '>'

        # Sorting the data
        ascending = order.lower() == 'asc'
        filtered_data.sort(key=lambda x: float(x[order_column]), reverse=not ascending)

        # Selecting and printing specified columns
        for row in filtered_data:
            selected_row = [row[col] for col in selected_columns]
            print(', '.join(selected_row))


def parse_and_execute(query):
    commands = query.split(' ')
    # new table employees
    if commands[0].lower() == 'new' and commands[1].lower() == 'table':
        try:
            table_name = commands[2]
            headers = commands[3:]
            create_table(table_name, headers)
        except Exception as e:
            print(f"Error processing complex query: {e}")
    # insert into employees values 1 John 30
    elif commands[0].lower() == 'put':
        try:
            insert_data(commands[1], ','.join(commands[2:]))
        except Exception as e:
            print(f"Error processing complex query: {e}")

    # delete from employees where age = 30
    elif commands[0].lower() == 'remove':
        try:
            table_name = commands[2]
            condition_parts = query.split(' where ')[1].split(' ')
            condition = (condition_parts[0], condition_parts[1], condition_parts[2])
            delete_data(f"{table_name}", condition)
            print(f"Data deleted from {table_name}.")
        except Exception as e:
            print(f"Error processing complex query: {e}")

    # update employees set 1=John, 2=35 where col 0 = 1
    elif commands[0].lower() == 'renew':
        try:
            table_name = commands[1]
            set_clause = query.split(' put ')[1].split(' where ')[0]
            where_clause = query.split(' where ')[1]

            # Parsing the SET clause
            updates = [(part.split('=')[0], part.split('=')[1]) for part in set_clause.split(',')]

            # Parsing the WHERE clause
            condition_parts = where_clause.split(' ')
            condition_column_name = condition_parts[0]
            operator = condition_parts[1]
            condition_value = condition_parts[2]
            condition = (condition_column_name, operator, condition_value)

            # Call update_data
            update_data(f"{table_name}", condition, updates)
            print(f"Data updated in {table_name}.")
        except Exception as e:
            print(f"Error processing complex query: {e}")

    elif commands[0].lower() == 'give':
        try:
            query_parts = query.split(' where ')
            # give name,age from table employees where age >= 25

            column_names = query_parts[0].split(' ')[1].split(',')
            table_name = query_parts[0].split(' ')[-1]

            condition = None

            # If there is a where condition.
            if 'where' in query:
                condition_parts = query_parts[1].split(' ')
                condition_column_name = condition_parts[0]
                operator = condition_parts[1]
                condition_value = condition_parts[2]
                condition = (condition_column_name, operator, condition_value)

            query_data(f"{table_name}.csv", column_names, condition)
        except Exception as e:
            print(f"Error processing complex query: {e}")

    # aggregate sum|max|min|avg|count from employees on age
    elif commands[0].lower() == 'aggregate':
        try:
            agg_function = commands[1].lower()
            table_name = commands[3]
            column_name = commands[5]  # Extract the column name instead of index

            result = aggregate_data(f"{table_name}.csv", column_name, agg_function)
            print(f"Aggregate {agg_function}: {result}")
        except Exception as e:
            print(f"Error processing complex query: {e}")

    # order employees by id asc
    elif commands[0].lower() == 'order':
        try:
            table_name = commands[1]
            column_index = commands[3]
            order = commands[4] if len(commands) > 4 else 'asc'
            order_data(f"{table_name}", column_index, order)
        except Exception as e:
            print(f"Error processing complex query: {e}")

    # join employees and salaries id and print employees.name
    elif commands[0].lower() == 'join':
        try:
            file_name1 = commands[1]
            file_name2 = commands[3]
            join_column_name = commands[4]
            selected_columns = None
            if 'print' in query:
                print_columns = query.split('print')[1].strip().split(',')
                selected_columns = [col.strip() for col in print_columns]  # List of 'table.column' strings
            join_tables(f"{file_name1}.csv", f"{file_name2}.csv", join_column_name, selected_columns)
        except Exception as e:
            print(f"Error processing complex query: {e}")

    elif commands[0].lower() == 'group':
        try:
            group_by_column = commands[1]
            table_name_index = commands.index('from') + 1
            table_name = commands[table_name_index]
            print_columns_index = commands.index('print') + 1
            print_columns = commands[print_columns_index:]

            grouped_data = group_data(f"dbs/{table_name}.csv", group_by_column, print_columns)

            for key, rows in grouped_data.items():
                print(f"Group: {key}")
                for row in rows:
                    print(', '.join([f"{col}: {row[col]}" for col in row]))
        except Exception as e:
            print(f"Error processing complex query: {e}")

    # complex company_employee_details.company,company_employee_details.department,company_employees_salaries.salary from table company_employee_details join company_employees_salaries where years_in_company > 5 order by salary asc
    elif commands[0].lower() == "complex":
        try:
            parts = query.split(' ', 4)  # Splitting only first four words to handle complex queries
            selected_columns = parts[2].split(',')
            table1, join_op, table2 = parts[4].split(' join ')
            join_column = join_op.split(' ')[-1]
            where_clause = ' '.join(parts[4].split(' where ')[1:])

            # Check if 'order by' exists in the clause
            if ' order by ' in where_clause:
                condition_str, order_clause = where_clause.split(' order by ')
                order_column, order_direction = order_clause.split(' ')
            else:
                condition_str = where_clause
                order_column, order_direction = None, None

            # Debug prints
            print("Condition String:", condition_str)
            print("Order Column:", order_column)
            print("Order Direction:", order_direction)

            # Correcting file paths
            file1 = f"dbs/{table1}.csv"
            file2 = f"dbs/{table2}.csv"

            # Parsing condition string
            condition_column, operator, condition_value = condition_str.split(' ')
            condition = (condition_column.strip(), operator.strip(), condition_value.strip())

            complex_query_execute(file1, file2, join_column, condition, selected_columns, order_column, order_direction)
        except ValueError as e:
            print(f"Error processing complex query: {e}")
    else:
        print("Invalid query")


class FileDBCLI(cmd.Cmd):
    prompt = 'FileDB > '

    def do_exit(self, inp):
        '''Exit the application.'''
        print("Exiting...")
        return True

    def default(self, inp):
        if inp.lower() == 'exit':
            return self.do_exit(inp)
        parse_and_execute(inp)


if __name__ == '__main__':
    FileDBCLI().cmdloop()
