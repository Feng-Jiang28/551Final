import os
import cmd


def ensure_db_directory():
    if not os.path.exists('dbs'):
        os.makedirs('dbs')

def create_table(table_name):
    ensure_db_directory()
    file_name = f"dbs/{table_name}.csv"
    if not os.path.exists(file_name):
        with open(file_name, 'w') as file:
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
def query_data(file_name, column_indices, condition=None):
    #print(file_name)
    #print(column_indices)
    #print(condition)
    with open(f"dbs/{file_name}", 'r') as file:
        for line in file:
            values = line.strip().split(',')
            if condition:
                column_index, operator, condition_value = condition
                if not is_condition_met(float(values[column_index]), operator, float(condition_value)):
                    continue
            selected_values = [values[i] for i in column_indices]
            print(','.join(selected_values))

def delete_data(file_name, condition):
    updated_lines = []
    with open(f"dbs/{file_name}.csv", 'r') as file:
        for line in file:
            values = line.strip().split(',')
            column_index, operator, condition_value = condition
            if is_condition_met(float(values[column_index]), operator, float(condition_value)):
                continue  # Skip this line (row) as it meets the delete condition
            updated_lines.append(line)

        with open(f"dbs/{file_name}.csv", 'w') as file:
            for line in updated_lines:
                file.write(line)  # Write back all lines except those to be deleted

def update_data(file_name, condition, updates):
    updated_lines = []
    with open(f"dbs/{file_name}.csv", 'r') as file:
        for line in file:
            values = line.strip().split(',')
            column_index, operator, condition_value = condition
            if is_condition_met(float(values[column_index]), operator, float(condition_value)):
                for col_idx, new_val in updates:
                    values[col_idx] = new_val
            updated_lines.append(','.join(values))

    with open(f"dbs/{file_name}.csv", 'w') as file:
        for line in updated_lines:
            file.write(line + '\n')
def order_data(file_name, column_index, order='asc'):
    with open(f"dbs/{file_name}.csv", 'r') as file:
        # Read all lines from the file
        lines = file.readlines()

    # Assuming the first line is the header
    header = lines[0].strip()

    # Processing the data lines
    data = [line.strip().split(',') for line in lines[1:]]

    # Sorting the data based on the specified column
    # The lambda function handles the data type for sorting (assumes float or int)
    data.sort(key=lambda row: float(row[column_index]) if row[column_index].replace('.','',1).isdigit() else row[column_index],
              reverse=(order.lower() == 'desc'))

    # Print the sorted data
    print(header)  # Print the header
    for row in data:
        print(','.join(row))  # Print the sorted data rows


def aggregate_data(file_name, column_index, agg_function):
    values = []
    with open(f"dbs/{file_name}", 'r') as file:
        for line in file:
            row = line.strip().split(',')
            values.append(float(row[column_index]))
    # print(values)
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

def join_tables(file_name1, file_name2, join_column_index1, join_column_index2, selected_columns=None):
    with open(f"dbs/{file_name1}.csv", 'r') as file1, open(f"dbs/{file_name2}.csv", 'r') as file2:
        # Read and split lines from both files
        data1 = [line.strip().split(',') for line in file1.readlines()]
        data2 = [line.strip().split(',') for line in file2.readlines()]

    # Create a dictionary for the second table for faster lookups
    dict2 = {row[join_column_index2]: row for row in data2}

    # Perform the join
    joined_data = []
    for row1 in data1:
        key = row1[join_column_index1]
        if key in dict2:
            combined_row = row1 + dict2[key]
            if selected_columns:
                combined_row = [combined_row[i] for i in selected_columns]
            joined_data.append(combined_row)

    # Print the joined data
    for row in joined_data:
        print(','.join(row))


def parse_and_execute(query):
    commands = query.split(' ')
    # new table employees
    if commands[0].lower() == 'new' and commands[1].lower() == 'table':
        create_table(commands[2])
    # insert into employees values 1 John 30
    elif commands[0].lower() == 'insert':
        insert_data(commands[2], ','.join(commands[4:]))
    # give 0,1,2 from table employees where col 2 >= 25

    # delete from employees where col 2 < 30
    elif commands[0].lower() == 'delete':
        table_name = commands[2]
        condition_parts = query.split(' where ')[1].split(' ')
        condition = (int(condition_parts[1]), condition_parts[2], condition_parts[3])
        delete_data(f"{table_name}", condition)
        print(f"Data deleted from {table_name}.")

    # update employees set 1=John, 2=35 where col 0 = 1
    elif commands[0].lower() == 'update':
        table_name = commands[1]
        set_clause = query.split(' set ')[1].split(' where ')[0]
        where_clause = query.split(' where ')[1]

        # Parsing the SET clause
        updates = []
        for update_part in set_clause.split(','):
            col_idx, new_val = update_part.split('=')
            updates.append((int(col_idx), new_val))

        # Parsing the WHERE clause
        condition_parts = where_clause.split(' ')
        condition = (int(condition_parts[1]), condition_parts[2], condition_parts[3])

        # Call update_data
        update_data(f"{table_name}", condition, updates)
        print(f"Data updated in {table_name}.")

    elif commands[0].lower() == 'give':
        query_parts = query.split(' where ')
        # give 0,1,2 from table employees, col 2 >= 25
        #  [0 1 2]
        column_indices = [int(idx) for idx in query_parts[0].split(' ')[1].split(',')]

        # "employees"
        table_name = query_parts[0].split(' ')[-1]

        # if there is a where condition.
        if 'where' in query:
            # [col 2 >= 25]
            condition_parts = query_parts[1].split(' ')
            # 2 >= 25
            condition = (int(condition_parts[1]), condition_parts[2], condition_parts[3])
            query_data(f"{table_name}.csv", column_indices, condition)
        else:
            query_data(f"{table_name}.csv", column_indices)
    # aggregate sum|max|min|avg|count from employees on col 2
    elif commands[0].lower() == 'aggregate':
        agg_function = commands[1].lower()
        table_name = commands[3]
        column_index = int(commands[6])

        result = aggregate_data(f"{table_name}.csv", column_index, agg_function)
        print(f"Aggregate {agg_function}: {result}")
        # order employees by 2 asc
    # order employees by 2 asc
    elif commands[0].lower() == 'order':
        table_name = commands[1]
        column_index = int(commands[3])
        order = commands[4] if len(commands) > 4 else 'asc'
        order_data(f"{table_name}", column_index, order)

    # join employees and salaries on 0 and 1 print 0,1,2
    elif commands[0].lower() == 'join':
        file_name1 = commands[1]
        file_name2 = commands[2]
        join_column_index1 = int(commands[4])
        join_column_index2 = int(commands[5])
        selected_columns = [int(idx) for idx in commands[7:]] if 'print' in query else None
        join_tables(f"{file_name1}.csv", f"{file_name2}.csv", join_column_index1, join_column_index2, selected_columns)
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
