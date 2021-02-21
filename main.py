import csv
import sys
import itertools
import re

schema = {}

def __init():
	# prepares the schema by reading the metadata
    metadata = open("files/metadata.txt","r")
    table = None
    columns_cnt = -1
    for line in metadata.readlines():
        line = line.strip()
        if line == "<begin_table>":
            table = None
        elif line == "<end_table>":
            columns_cnt = -1
        elif columns_cnt == -1:
            table = line.lower()
            schema[table] = []
            columns_cnt = 0
        else:
            schema[table].append(line.lower())
            columns_cnt += 1

def break_query(query):
    query = query.strip()
    if query[-1] != ';':
        print("Syntax Error : A Semicolon is expected to succed the query")
        exit(0)
    query = query[:len(query) - 1]
    query = query.lower()
    query = query.split()
    if len(query) == 0:
        print("Syntax Error : Empty Query received, Usage : python main.py ''select ... from ...''")
        exit(0)
    elif query[0] != "select":
        print("Syntax Error : Query should begin with ''select'', Usage : python main.py ''select ... from ...''")
        exit(0)  
    
    index_distinct = 0
    index_select = 0
    select_usage_cnt = 0
    index_from = 0
    index_where = 0
    index_groupby = 0
    index_orderby = 0

    for index,keyword in enumerate(query):
        if keyword == "distinct":
            if index_distinct:
                print("Version Error : The current version of engine only handles a specific distinct request, i.e., once after select")
                exit(0)
            index_distinct = index 

    if index_distinct:
        query = query[:index_distinct] + query[index_distinct + 1:]

    for index,keyword in enumerate(query):       
        if keyword == "select":
            if select_usage_cnt == 1:
                print("Syntax Error : There can be a single select request only")
                exit(0)
            index_select = index
            select_usage_cnt = 1

        if keyword == "from":
            if index_from:
                print("Syntax Error : There can be a single from request only")
                exit(0)
            index_from = index

        if keyword == "where":
            if index_where:
                print("Syntax Error : There can be a single where request only")
                exit(0)
            index_where = index

        if keyword == "group":
            if index_groupby:
                print("Syntax Error : There can be a single group by request only")
                exit(0)
            if query[index+1] != "by" :
                print("Syntax Error : group should be succeeded by 'by'")
                exit(0)
            index_groupby = index

        if keyword == "order":
            if index_orderby:
                print("Syntax Error : There can be a single order by request only")
                exit(0)
            if query[index+1] != "by" :
                print("Syntax Error : order should be succeeded by 'by'")
                exit(0)
            index_orderby = index

    if select_usage_cnt == 0 or index_from == 0:
        print("Syntax Error : There should be a single select and single for")
        exit(0)

    list_columns = query[index_select+1 : index_from]
    list_tables = []
    list_conditions = []
    list_groupby = []
    list_orderby = []

    if index_where == 0:
        if index_groupby == 0:
            if index_orderby == 0:
                list_tables += query[index_from+1:]
            else:
                if index_orderby < index_from:
                    print("Syntax Error : order by should be later to from in the query")
                    exit(0)
                list_tables += query[index_from+1 : index_orderby]
                list_orderby += query[index_orderby+2:]
        else:
            if index_groupby < index_from:
                print("Syntax Error : group by should be later to from in the query")
                exit(0)
            if index_orderby == 0:
                list_tables += query[index_from+1 : index_groupby]
                list_groupby += query[index_groupby+2:]
            else:
                if index_orderby < index_groupby:
                    print("Syntax Error : order by should be later to group by in the query")
                    exit(0)
                list_tables += query[index_from+1 : index_groupby]
                list_groupby += query[index_groupby+2 : index_orderby]
                list_orderby += query[index_orderby+2:]
    else:
        if index_where < index_from:
            print("Syntax Error : where should be later to from in the query")
            exit(0)
        if index_groupby == 0:
            if index_orderby == 0:
                list_tables += query[index_from+1 : index_where]
                list_conditions += query[index_where+1:]
            else:
                if index_orderby < index_where:
                    print("Syntax Error : order by should be later to where in the query")
                    exit(0)
                list_tables += query[index_from+1 : index_where]
                list_conditions += query[index_where+1 : index_orderby]
                list_orderby += query[index_orderby+2:]
        else:
            if index_groupby < index_where:
                print("Syntax Error : group by should be later to where in the query")
                exit(0)
            if index_orderby == 0:
                list_tables += query[index_from+1 : index_where]
                list_conditions += query[index_where+1 : index_groupby]
                list_groupby += query[index_groupby+2:]
            else:
                if index_orderby < index_groupby:
                    print("Syntax Error : order by should be later to group by in the query")
                    exit(0)
                list_tables += query[index_from+1 : index_where]
                list_conditions += query[index_where+1 : index_groupby]
                list_groupby += query[index_groupby+2 : index_orderby]
                list_orderby += query[index_orderby+2:]
    # print(list_tables, list_columns, list_conditions, list_groupby, list_orderby, index_distinct)
    if select_usage_cnt == 1 and list_columns == []:
        print("Syntax Error : After select, columns absent")
        exit(0)
    if index_from > 0 and list_tables == []:
        print("Syntax Error : After from, tables absent")
        exit(0)
    if index_where > 0 and list_conditions == []:
        print("Syntax Error : After where, conditions absent")
        exit(0)
    if index_groupby > 0 and list_groupby == []:
        print("Syntax Error : After group by, columns absent")
        exit(0)
    if index_orderby > 0 and list_orderby == []:
        print("Syntax Error : After order by, columns absent")
        exit(0)
    return list_tables, list_columns, list_conditions, list_groupby, list_orderby, index_distinct

def table_parser(list_tables):
    queried_tables = []
    for table in list_tables:
        if table in schema.keys():
            queried_tables.append(table)
        else:
            print("Metadata doesn't specify any table with name '{}' ".format(table))
            exit(0)
    return queried_tables

def coloumn_parser(list_tables, list_columns):
    queried_columns = []
    for column in list_columns:
        regcheck = re.match("(.+)\((.+)\)", column)
        if regcheck:
            aggregation, column = regcheck.groups()
        else:
            aggregation = None
        if column == "*":
            if regcheck:
                print("Syntax Error : cannot use any aggregrate on *")
                exit(0)
            else:
                for table in list_tables:
                    for attribute in schema[table]:
                        queried_columns.append(([table],[attribute],aggregation))
        else:
            column_usage_cnt = 0
            table_matched = list_tables[0]
            for table in list_tables:
                if column in schema[table]:
                    if column_usage_cnt == 1:
                        print("Syntax Error : Non unique column names across tables, not accepted")
                        exit(0)
                    table_matched = table
                    column_usage_cnt = 1
            if column_usage_cnt:
                queried_columns.append(([table_matched],[column],aggregation))
            else:
                print("Syntax Error : Column name not found in any table")
                exit(0)
    
    return queried_columns

def condition_parser(list_tables, list_conditions):
    queried_conditions = []
    conditional_operations = None
    if len(list_conditions) == 0:
        return queried_conditions, conditional_operations
    if "or" in list_conditions:
        list_conditions, conditional_operations = list_conditions.split(" or "), "or"
    elif "and" in list_conditions:
        list_conditions, conditional_operations = list_conditions.split(" and "), "and"
    else:
        list_conditions = [list_conditions]

    for condition in list_conditions:
        COMPARATORS = ["<=", ">=", "<", ">", "="]
        comparator_usage_cnt = 0
        comparator = ''
        for option in COMPARATORS:
            if option in condition:
                # if comparator_usage_cnt == 1 :
                #     print("Syntax Error : Only single comparator allowed in a condition")
                #     exit(0)
                comparator = option
                comparator_usage_cnt = 1
                break

        if comparator_usage_cnt == 0 :
            print("Syntax Error : A comparator is necessary in a condition")
            exit(0)

        LHS,RHS = condition.split(comparator)
        expressions = [LHS.strip(), RHS.strip()]
        condition_tokens = [comparator]

        for expr in expressions:
            if (expr[0] == '-' and expr[1:].isdigit()) or expr.isdigit():
                condition_tokens.append(["INT_LITERAL",expr])
            else:
                table_matched = ""
                match_cnt = 0
                for table in list_tables:
                    if expr in schema[table]:
                        if match_cnt:
                            print("Syntax Error : Match for column name identifier in conditions found across multiple tables")
                            exit(0)
                        table_matched = table
                        match_cnt = 1
                if match_cnt == 0:
                    print("Syntax Error : Could not recognise identifier in conditions")
                    exit(0)
                condition_tokens.append([table_matched, expr])

        queried_conditions.append(condition_tokens)
    return queried_conditions,conditional_operations

def group_by_parser(list_tables,list_columns,list_groupby):
    table_for_groupby = None
    column_for_groupby = None
    if len(list_groupby) == 0:
        return table_for_groupby, column_for_groupby
    
    column_for_groupby = list_groupby[0]
    column_usage_cnt = 0
    
    for table in list_tables:
        if column_for_groupby in schema[table]:
            if column_usage_cnt == 1:
                print("Syntax Error : Non-unique columns found in group by request")
                exit(0)
            table_for_groupby = table
            column_usage_cnt = 1

    if column_usage_cnt == 0:
        print("Syntax Error : Could not find the column in queried tables in group by request")
        exit(0)

    # if column_for_groupby not in list_columns:
    #     print("Syntax Error : Could not find the column in queried columns in group by request")
    #     exit(0)
    
    return table_for_groupby,column_for_groupby

def execute(queried_tables,queried_columns,required_columns,queried_conditions,conditional_operations,table_for_groupby,aggregated_columns,projection_columns,column_for_orderby,index_distinct):
    # finding all the relevant columns of the joined table and mapping each column to an index
    join_index_map = {}
    distinct_column_count = 0
    summarised_tables = [[] for table in queried_tables]

    for table_index, table in enumerate(queried_tables):
        filename = "files/" + table + ".csv"
        with open(filename) as file_pointer:
            all_columns_of_table = list(csv.reader(file_pointer, delimiter=','))
            typecasted_table = [[] for row in all_columns_of_table]
            for row_index,row in enumerate(all_columns_of_table):
                for column_index,value in enumerate(row):
                    typecasted_table[row_index].append(int(value))
        all_columns_of_table = typecasted_table

        for row_index,row in enumerate(all_columns_of_table):
            required_data = []
            for column in required_columns[table]:
                index_in_table = schema[table].index(column)
                required_data.append(row[index_in_table])
            summarised_tables[table_index].append(required_data)   

        local_column_map = {}
        for index,column in enumerate(required_columns[table]):
            local_column_map[column] = distinct_column_count
            distinct_column_count += 1
        join_index_map[table] = local_column_map

    # join of all the queried tables
    joined_table = []
    for row in itertools.product(*summarised_tables):
        join_row = []
        for cell in row:
            for value in cell:
                join_row.append(value)
        joined_table.append(join_row)

    output_table = []

    # where handling
    where_intermediate_table = joined_table
    if len(queried_conditions) != 0:
        where_intermediate_table = []
        truth_table = []
        for row in joined_table:
            truth_row = []
            for condition in queried_conditions:
                truth_row.append(True)
            truth_table.append(truth_row)

        iterator = 0
        for condition_index, condition in enumerate(queried_conditions):
            comparable_columns = []
            for table, column in condition[1:]:
                column_index = 0
                if table != "INT_LITERAL":
                    column_index = join_index_map[table][column]
                column_values = []
                for row in joined_table:
                    if table == "INT_LITERAL":
                        column_values.append([int(column)])
                    else:
                        column_values.append([row[column_index]])
                comparable_columns.append(column_values)
        
            for i,row in enumerate(joined_table):
                if condition[0] == "<":
                    if comparable_columns[0][i] >= comparable_columns[1][i]:
                        truth_table[i][iterator] = False
                if condition[0] == "<=":
                    if comparable_columns[0][i] > comparable_columns[1][i]:
                        truth_table[i][iterator] = False
                if condition[0] == ">":
                    if comparable_columns[0][i] <= comparable_columns[1][i]:
                        truth_table[i][iterator] = False
                if condition[0] == ">=":
                    if comparable_columns[0][i] < comparable_columns[1][i]:
                        truth_table[i][iterator] = False
                if condition[0] == "=":
                    if comparable_columns[0][i] != comparable_columns[1][i]:
                        truth_table[i][iterator] = False
            iterator += 1

        for i,row in enumerate(joined_table):
            if conditional_operations == "and":
                if truth_table[i][0] and truth_table[i][1]:
                    where_intermediate_table.append(row)
            elif conditional_operations == "or":
                if truth_table[i][0] or truth_table[i][1]:
                    where_intermediate_table.append(row)
            else:
                if truth_table[i][0]:
                    where_intermediate_table.append(row)

    # group_by handling
    group_by_check = 0
    group_by_intermediate_table = where_intermediate_table
    if table_for_groupby:
        group_by_check = 1
        group_by_intermediate_table = []
        projection_column_index = -1
        found = {}
        for table,columns in required_columns.items():
            for column in columns:
                if column == projection_columns:
                    projection_column_index = join_index_map[table][column]
                    break
        if projection_column_index == -1:
            print("Syntax Error : the column to group by not found in joined table")
            exit(0)
        for row_index,row in enumerate(where_intermediate_table):
            value = row[projection_column_index]
            if value in found.keys():
                found[value].append(row_index)
            else:
                found[value] = []
                found[value].append(row_index)

        for key,row_ids_with_key in found.items():
            prepare_row = []
            for table,column,aggregation in queried_columns:
                if column[0] == projection_columns:
                    prepare_row.append(key)
                    continue
                # note that rest of the columns should have aggregate functions
                if aggregation == None:
                    print("Syntax Error : The queried columns apart from the projected column should have aggregation")
                    exit(0)
                column_index = join_index_map[table[0]][column[0]]
                to_be_aggregated = []
                aggregated_value = None
                for row_index in row_ids_with_key:
                    to_be_aggregated.append(where_intermediate_table[row_index][column_index])
                if aggregation == "sum":
                    aggregated_value = sum(to_be_aggregated)
                if aggregation == "average":
                    aggregated_value = sum(to_be_aggregated)/len(to_be_aggregated)
                if aggregation == "max":
                    aggregated_value = max(to_be_aggregated)
                if aggregation == "min":
                    aggregated_value = min(to_be_aggregated)
                if aggregation == "count":
                    aggregated_value = len(to_be_aggregated)
                if aggregated_value == None:
                    print("Syntax Error : Unidentified aggregate function")
                    exit(0)
                prepare_row.append(aggregated_value)
            group_by_intermediate_table.append(prepare_row)

    # all-aggregate handling
    aggregate_intermediate_table = group_by_intermediate_table
    aggregate_usage_count = 0
    for table,column,aggregation in queried_columns:
        if aggregation != None:
            aggregate_usage_count += 1
    if group_by_check == 0 and aggregate_usage_count > 0:
        if aggregate_usage_count < len(queried_columns):
            if table_for_groupby == None:
                print("Syntax Error : aggregate used on only a subset of columns, table non-derivable")
                exit(0)
        else:
            aggregate_intermediate_table = []
            prepare_row = []
            for table,column,aggregation in queried_columns:
                column_index = join_index_map[table[0]][column[0]]
                to_be_aggregated = []
                aggregated_value = None
                for row in where_intermediate_table:
                    to_be_aggregated.append(row[column_index])
                if aggregation == "sum":
                    aggregated_value = sum(to_be_aggregated)
                if aggregation == "average":
                    aggregated_value = sum(to_be_aggregated)/len(to_be_aggregated)
                if aggregation == "max":
                    aggregated_value = max(to_be_aggregated)
                if aggregation == "min":
                    aggregated_value = min(to_be_aggregated)
                if aggregation == "count":
                    aggregated_value = len(to_be_aggregated)
                if aggregated_value == None:
                    print("Syntax Error : Unidentified aggregate function")
                    exit(0)
                prepare_row.append(aggregated_value) 
            aggregate_intermediate_table.append(prepare_row)

    # select handling
    select_intermediate_table = aggregate_intermediate_table
    if group_by_check == 0 and aggregate_usage_count == 0:
        select_intermediate_table = []
        for row in where_intermediate_table:
            prepare_row = []
            for table,column,aggregation in queried_columns:
                column_index = join_index_map[table[0]][column[0]]
                prepare_row.append(row[column_index])
            select_intermediate_table.append(prepare_row)

    # distinct handling  
    distinct_intermediate_table = select_intermediate_table
    if index_distinct > 0:
        distinct_intermediate_table = []
        for row in select_intermediate_table:
            if row not in distinct_intermediate_table:
                distinct_intermediate_table.append(row)

    # orderby handling
    orderby_intermediate_table = distinct_intermediate_table
    if column_for_orderby:
        orderby_column_index = -1
        # for table,columns in required_columns.items():
        #     for column in columns :
        #         if column == column_for_orderby[0]:
        #             orderby_column_index = join_index_map[table][column]
        #             break
        for index,(table,column,aggregation) in enumerate(queried_columns):
            if column[0] == column_for_orderby[0]:
                orderby_column_index = index
                break
        if orderby_column_index == -1:
            print("Syntax Error : the column to order by not found in joined table")
            exit(0)
        if len(column_for_orderby) == 1 or column_for_orderby[1] == "asc":
            orderby_intermediate_table = sorted(orderby_intermediate_table,key = lambda table:table[orderby_column_index])
        elif column_for_orderby[1] == "desc":
            orderby_intermediate_table = sorted(orderby_intermediate_table,key = lambda table:table[orderby_column_index],reverse=True)
        else:
            print("Syntax Error : Sorting option do not match ASC/DESC")
            exit(0)

    output_table = orderby_intermediate_table

    # printing the query response
    column_count = len(queried_columns)
    iterator = 0
    for table,column,aggregation in queried_columns:
        iterator += 1
        if aggregation == None:
            if iterator < column_count:
                print(table[0]+'.'+column[0],end=",")
            else:
                print(table[0]+'.'+column[0])
        else:
            if iterator < column_count:
                print(aggregation+'('+table[0]+'.'+column[0]+')',end=",")
            else:
                print(aggregation+'('+table[0]+'.'+column[0]+')')

    for row in output_table:
        iterator = 0
        for value in row:
            iterator += 1
            if iterator < column_count:
                print(value,end=",")
            else:
                print(value)

def main():
    
    if len(sys.argv) != 2:
    	print("Invalid query format, Usage : python main.py ''select ... from ...''")
    	exit(0)

    # call to load the schema
    __init()
    # initial query processing
    parsables = break_query(sys.argv[1])
    
    list_tables = "".join(parsables[0]).split(',')
    list_columns = "".join(parsables[1]).split(',')
    list_conditions = " ".join(parsables[2])
    list_groupby = parsables[3]
    list_orderby = parsables[4]
    index_distinct = parsables[5]

    queried_tables = table_parser(list_tables)
    queried_columns = coloumn_parser(list_tables, list_columns)
    required_columns = {table : [] for table in queried_tables}
    for table in queried_tables:
        for attribute in schema[table]:
            required_columns[table].append(attribute)
    
    queried_conditions, conditional_operations = condition_parser(list_tables, list_conditions)
    table_for_groupby,projection_columns = group_by_parser(list_tables,list_columns,list_groupby)

    column_for_orderby = []
    if len(list_orderby) == 0:
        column_for_orderby = None
    else:
        column_for_orderby = list_orderby
        if len(column_for_orderby) > 2:
            print("Syntax Error : Order by arguements error, Usage : ''... order by <column> ASC/DESC''")
            exit(0)
        # if column_for_orderby[0] not in list_columns:
        #     print("Syntax Error : Order by column do not match any queried column")
        #     exit(0)

    aggregated_columns = []
    for column in list_columns:
        regcheck = re.match("(.+)\((.+)\)", column)
        if regcheck:
            aggregration, column = regcheck.groups()
            aggregated_columns.append(column)

    # if len(list_groupby) != 0 and len(aggregated_columns) == 0:
    #     print("Syntax Error : No aggregation found for the group by clause")
    #     exit(0)

    # print(queried_tables)
    # print(queried_columns,required_columns)
    # print(queried_conditions,conditional_operations)
    # print(table_for_groupby,aggregated_columns,projection_columns)
    # print(column_for_orderby)
    execute(queried_tables,queried_columns,required_columns,queried_conditions,conditional_operations,table_for_groupby,aggregated_columns,projection_columns,column_for_orderby,index_distinct)
main()
