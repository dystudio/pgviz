import json
import re

from utils import sql_finder


def traverse_json(qep_json, query):
    # assign JSON to be modified to a new JSON variable
    modified_json = qep_json

    # Declare node types
    options = {
        'Seq Scan': sql_finder.process_seq_scan,
        'Index Scan': sql_finder.process_ind_scan,
        'Nested Loop': sql_finder.process_nested_loop,
        'Bitmap Index Scan': sql_finder.process_ind_scan,
        'Bitmap Heap Scan': sql_finder.process_bitmap_heap_scan,
        'Merge Join': sql_finder.process_merge_join,
        'Aggregate': sql_finder.process_aggregate,
        'Hash Join': sql_finder.process_hash_join,
        'Sort': sql_finder.process_sort,
        'Index Only Scan': sql_finder.process_index_only_scan,
        'Hash': sql_finder.process_hash,
        'Gather': sql_finder.process_gather,
        'Unique': sql_finder.process_unique,
        'Limit': sql_finder.process_limit,
        'Subquery Scan': sql_finder.process_subquery_scan,
    }

    # Terminal node
    if 'Plans' not in qep_json.keys():
        print("--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------")

        if qep_json['Node Type'] in options.keys():
            # Process node
            modified_json = options[qep_json['Node Type']](qep_json, query)

        # For debugging purposes
        if 'Relation Name' in qep_json.keys():
            if 'Filter' in qep_json.keys():
                print("Performed " + qep_json['Node Type'] + " on " 
                + qep_json['Relation Name'] + " with filter: " + qep_json['Filter'] + ".")
            else:
                print("Performed " + qep_json['Node Type'] + " on " 
                + qep_json['Relation Name'] + ".")
        else:
            print("Performed " + qep_json['Node Type'] + ".")

        return modified_json

    # Recursive part
    for subplan_data in qep_json['Plans']:
        modified_json = traverse_json(subplan_data, query)

    # Process current node
    print("--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------")
    if qep_json['Node Type'] in options.keys():
        # Process node
        modified_json = options[qep_json['Node Type']](qep_json, query)
    
    # Traverse through current node
    if 'Relation Name' in qep_json.keys():
        if 'Filter' in qep_json.keys():
            print("Performed " + qep_json['Node Type'] + " on " 
            + qep_json['Relation Name'] + " with filter: " + qep_json['Filter'] + ".")
        else:
            print("Performed " + qep_json['Node Type'] + " on " 
            + qep_json['Relation Name'] + ".")
    else:
        print("Performed " + qep_json['Node Type'] + ".")

    return modified_json


def connect_query(json_dir, sql_dir, output_json_dir):
    with open(json_dir) as f:
        data = json.load(f)

    with open(sql_dir) as g:
        query = g.read()
        g.close()

        # Clean query
        query = re.sub(' +', ' ', query.replace("\n", " ").replace("\t", ""))

    plan_data = data[0]['Plan']

    # modified JSON will be put here
    result_json = traverse_json(plan_data, query)
    
    # Encapsulate again in a dictionary and in a list
    final_json = [{"Plan": result_json}]

    # write to output JSON file
    with open(output_json_dir, 'w') as outfile:
        json.dump(final_json, outfile, indent=2)


def main():
    # Set input and output directory of postGreSQL JSON file
    json_dir = 'testjson.json'
    output_json_dir = 'final_json.json'

    # Set directory of SQL file
    sql_dir = 'SQLTestQuery.sql'

    connect_query(json_dir, sql_dir, output_json_dir)


if __name__ == '__main__':
    main()