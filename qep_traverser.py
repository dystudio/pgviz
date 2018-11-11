import json
import sqlparse
import sql_finder
import re
from db_connection import get_execution_plan, connect_to_db
from pprint import pprint
from db_credentials import DATABASE_NAME, DATABASE_USER, DATABASE_PASSWORD

def traverseJSON(qepJSON, query):

    # assign JSON to be modified to a new JSON variable
    modifiedJSON = qepJSON

    # Declare node types
    options = {
        "Seq Scan": sql_finder.process_seq_scan,
        "Index Scan": sql_finder.process_ind_scan,
        "Nested Loop": sql_finder.process_nested_loop,
        "Bitmap Index Scan": sql_finder.process_ind_scan,
        "Bitmap Heap Scan": sql_finder.process_bitmap_heap_scan,
        "Merge Join": sql_finder.process_merge_join,
        "Aggregate": sql_finder.process_aggregate,
        "Hash Join": sql_finder.process_hash_join,
        "Sort": sql_finder.process_sort,
        "Index Only Scan": sql_finder.process_index_only_scan,
        "Hash": sql_finder.process_hash,
        "Gather": sql_finder.process_gather,
        "Unique": sql_finder.process_unique,
        "Limit": sql_finder.process_limit,
        "Subquery Scan": sql_finder.process_subquery_scan,
    }

    # Terminal node
    if 'Plans' not in qepJSON.keys():

        print("--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------")

        if qepJSON['Node Type'] in options.keys():
            # Process node
            modifiedJSON = options[qepJSON['Node Type']](qepJSON, query)

        # For debugging purposes
        if 'Relation Name' in qepJSON.keys():
            if 'Filter' in qepJSON.keys():
                print("Performed " + qepJSON['Node Type'] + " on " 
                + qepJSON['Relation Name'] + " with filter: " + qepJSON['Filter'] + ".")
            else:
                print("Performed " + qepJSON['Node Type'] + " on " 
                + qepJSON['Relation Name'] + ".")
        else:
            print("Performed " + qepJSON['Node Type'] + ".")

        return

    # Recursive part
    for subplan_data in qepJSON['Plans']:
        modifiedJSON = traverseJSON(subplan_data, query)

    # Process current node
    print("--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------")
    if qepJSON['Node Type'] in options.keys():
        # Process node
        modifiedJSON = options[qepJSON['Node Type']](qepJSON, query)
    
    # Traverse through current node
    if 'Relation Name' in qepJSON.keys():
        if 'Filter' in qepJSON.keys():
            print("Performed " + qepJSON['Node Type'] + " on " 
            + qepJSON['Relation Name'] + " with filter: " + qepJSON['Filter'] + ".")
        else:
            print("Performed " + qepJSON['Node Type'] + " on " 
            + qepJSON['Relation Name'] + ".")
    else:
        print("Performed " + qepJSON['Node Type'] + ".")

    return modifiedJSON

def connect_query(query):
    # Connect to database
    conn = connect_to_db(DATABASE_NAME, DATABASE_USER, DATABASE_PASSWORD)
    cur = conn.cursor()

    # Get QEP from query
    final_query = get_execution_plan(query)
    cur.execute(final_query)
    data = cur.fetchall()

    # Clean query
    query = re.sub(' +', ' ', query.replace("\n", " ").replace("\t", ""))

    plan_data = data[0]['Plan']

    # Modified JSON will be put here
    resultJSON = traverseJSON(plan_data, query)

    # Enclose in one plan
    dictJSON = dict()
    dictJSON["Plan"] = resultJSON

    # Enclose in list
    finalJSON = list()
    finalJSON.append(dictJSON)

    return finalJSON