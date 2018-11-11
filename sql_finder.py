import json
import re
from itertools import islice

# Find index of nth time a value is found in a list.
def nth_index(iterable, value, n):
    matches = (idx for idx, val in enumerate(iterable) if val == value)
    return next(islice(matches, n-1, n), None)

# Find the start index of substring in SQL string
def find_str(s, char):
    index = 0

    if char in s:
        c = char[0]
        for ch in s:
            if ch == c:
                if s[index:index+len(char)] == char:
                    return index

            index += 1

    return -1

# Cleanup or resolve the filter condition
def cleanup_cond(filter):

    # Replace all type declarations in filter
    filtered_result = filter.replace("::text", "").replace("::numeric", "").replace("::double precision", "").replace("::timestamp without time zone", "")

    # Remove all parentheses
    while ('(' in filtered_result) or (')' in filtered_result):
        filtered_result = re.sub(r'\((.*?)\)', r'\1', filtered_result)

    # Restore parentheses in aggregate functions
    for aggr in ['avg', 'count', 'min', 'max', 'sum', 'div', 'mul', 'date_part']:
        if aggr in filtered_result:
            filtered_result = filtered_result.replace(aggr, aggr.upper() + '(')
        
            encountered_par = False

            new_filtered_result = ""
            par_no = 0

            for index in range(len(filtered_result)):
                
                if (filtered_result[index] == '('):
                    encountered_par = True
                
                if encountered_par:
                    if (filtered_result[index] == ' '):
                        if (filtered_result[index - 1] == ',' or filtered_result[index - 1] == filtered_result[index - 1].upper()):
                            continue
                        encountered_par = False

                        if new_filtered_result is "":
                            new_filtered_result = filtered_result[:index] + ')' + filtered_result[index:]
                        else:
                            new_filtered_result = new_filtered_result[:index + par_no] + ')' + new_filtered_result[index + par_no:]
                        
                        par_no += 1

                    if index is len(filtered_result) - 1:

                        encountered_par = False

                        if new_filtered_result is "":
                            new_filtered_result = filtered_result + ')'
                        else:
                            new_filtered_result = new_filtered_result + ')'
                        
                        par_no += 1

            filtered_result = new_filtered_result[:]

    filtered_result = filtered_result.replace('~~', 'LIKE')
    filtered_result = filtered_result.replace('~', 'SIMILAR TO')
    filtered_result = re.sub(r'\'(\d+)\'', r'\1', filtered_result)

    # Remove regular expressions
    if 'SIMILAR TO' in filtered_result:
        filtered_result = re.sub(r'\'.*\'', '', filtered_result)

    # Convert alias table names to relation names
    filtered_result = re.sub(r'(.*)\_\d+(.*)', r'\1\2', filtered_result)

    return filtered_result

# Process limit node
def process_limit(qepJSON, query):
    print("Processing limit")

    sqlfragments = list()

    if "Plan Rows" in qepJSON.keys():
        sqlfragments.append("LIMIT " + str(qepJSON["Plan Rows"]))

    start_index, end_index = search_in_sql(sqlfragments, query)

    if start_index is not -1:
        qepJSON["start_index"] = start_index
        qepJSON["end_index"] = end_index

    return qepJSON

# Process subquery scan node
def process_subquery_scan(qepJSON, query):
    print("Processing subquery scan")

    sqlfragments = list()

    if "Alias" in qepJSON.keys():
        sqlfragments.append("AS " + qepJSON["Alias"])
        sqlfragments.append(qepJSON["Alias"])

    # Find matching SQL
    start_index, end_index = search_in_sql(sqlfragments, query)

    if start_index is not -1:
        qepJSON["start_index"] = start_index
        qepJSON["end_index"] = end_index

    return qepJSON

# Process sequential scan node
def process_seq_scan(qepJSON, query):
    print("Processing seq scan")

    sqlfragments = list()

    if "Filter" in qepJSON.keys():
        filter_cond = qepJSON["Filter"]
        filter_cond = cleanup_cond(filter_cond)

        sqlfragments.append(filter_cond)
        
        sqlfragments = subquery_block_add(sqlfragments, filter_cond)

        # Account for all subqueries
        for subquery_result in re.findall("\$\d+", filter_cond):
            filter_words = filter_cond.split()
            
            for filter_word in filter_words:
                if filter_word == subquery_result:
                    if filter_words.index(filter_word) > 1:
                        sqlfragments.append("WHERE " + filter_words[filter_words.index(filter_word) - 2] + " " + filter_words[filter_words.index(filter_word) - 1] + " (")
                        sqlfragments.append(filter_words[filter_words.index(filter_word) - 2] + " " + filter_words[filter_words.index(filter_word) - 1] + " (")
    
    if "Relation Name" in qepJSON.keys():
        if "Filter" in qepJSON.keys():
            sqlfragments = resolve_relation(sqlfragments, qepJSON)

        relation_name = qepJSON["Relation Name"]
        sqlfragments.append("FROM " + relation_name)
        sqlfragments.append(relation_name)

    # Find matching SQL
    start_index, end_index = search_in_sql(sqlfragments, query)

    if start_index is not -1:
        qepJSON["start_index"] = start_index
        qepJSON["end_index"] = end_index

    return qepJSON

def process_ind_scan(qepJSON, query):
    print("Processing index scan")

    sqlfragments = list()

    if "Index Cond" in qepJSON.keys():
        filter_cond = qepJSON["Index Cond"]
        filter_cond = cleanup_cond(filter_cond)

        sqlfragments.append(filter_cond)

        filter_words = filter_cond.split()

        for filter_word in filter_words:
            if '=' in filter_word:
                if filter_words.index('=') > 0:
                    filter_words[filter_words.index('=') - 1], filter_words[filter_words.index('=') + 1] = filter_words[filter_words.index('=') + 1], filter_words[filter_words.index('=') - 1]

        new_filter_cond = ' '.join(filter_words)

        sqlfragments.insert(0, new_filter_cond)

        sqlfragments = subquery_block_add(sqlfragments, filter_cond)

        # Account for all subqueries
        for subquery_result in re.findall("\$\d+", filter_cond):
            filter_words = filter_cond.split()
            
            for filter_word in filter_words:
                if filter_word == subquery_result:
                    if filter_words.index(filter_word) > 1:
                        sqlfragments.append("WHERE " + filter_words[filter_words.index(filter_word) - 2] + " " + filter_words[filter_words.index(filter_word) - 1] + " (")
                        sqlfragments.append(filter_words[filter_words.index(filter_word) - 2] + " " + filter_words[filter_words.index(filter_word) - 1] + " (")
    
    if "Filter" in qepJSON.keys():
        filter_cond = qepJSON["Filter"]
        filter_cond = cleanup_cond(filter_cond)

        sqlfragments.append(filter_cond)

        filter_words = filter_cond.split()

        for filter_word in filter_words:
            if '=' == filter_word:
                if filter_words.index(filter_word) > 0:
                    filter_words[filter_words.index(filter_word) - 1], filter_words[filter_words.index(filter_word) + 1] = filter_words[filter_words.index(filter_word) + 1], filter_words[filter_words.index(filter_word) - 1]

        new_filter_cond = ' '.join(filter_words)

        sqlfragments.append(new_filter_cond)

        sqlfragments = subquery_block_add(sqlfragments, filter_cond)

        # Account for all subqueries
        for subquery_result in re.findall("\$\d+", filter_cond):
            filter_words = filter_cond.split()
            
            for filter_word in filter_words:
                if filter_word == subquery_result:
                    if filter_words.index(filter_word) > 1:
                        sqlfragments.append("WHERE " + filter_words[filter_words.index(filter_word) - 2] + " " + filter_words[filter_words.index(filter_word) - 1] + " (")
                        sqlfragments.append(filter_words[filter_words.index(filter_word) - 2] + " " + filter_words[filter_words.index(filter_word) - 1] + " (")

    if "Relation Name" in qepJSON.keys():
        if "Index Cond" in qepJSON.keys() or "Filter" in qepJSON.keys():
            sqlfragments = resolve_relation(sqlfragments, qepJSON)
        relation_name = qepJSON["Relation Name"]
        sqlfragments.append("FROM " + relation_name)

    # Find matching SQL
    start_index, end_index = search_in_sql(sqlfragments, query)

    if start_index is not -1:
        qepJSON["start_index"] = start_index
        qepJSON["end_index"] = end_index

    return qepJSON

def process_bitmap_heap_scan(qepJSON, query):
    print("Processing bitmap heap scan")

    sqlfragments = list()

    if "Recheck Cond" in qepJSON.keys():
        filter_cond = qepJSON["Recheck Cond"]
        filter_cond = cleanup_cond(filter_cond)

        sqlfragments.append(filter_cond)

        filter_words = filter_cond.split()

        for filter_word in filter_words:
            if '=' == filter_word:
                if filter_words.index(filter_word) > 0:
                    filter_words[filter_words.index(filter_word) - 1], filter_words[filter_words.index(filter_word) + 1] = filter_words[filter_words.index(filter_word) + 1], filter_words[filter_words.index(filter_word) - 1]

        new_filter_cond = ' '.join(filter_words)

        sqlfragments.append(new_filter_cond)

        sqlfragments = subquery_block_add(sqlfragments, filter_cond)

        # Account for all subqueries
        for subquery_result in re.findall("\$\d+", filter_cond):
            filter_words = filter_cond.split()
            
            for filter_word in filter_words:
                if filter_word == subquery_result:
                    if filter_words.index(filter_word) > 1:
                        sqlfragments.append("WHERE " + filter_words[filter_words.index(filter_word) - 2] + " " + filter_words[filter_words.index(filter_word) - 1] + " (")
                        sqlfragments.append(filter_words[filter_words.index(filter_word) - 2] + " " + filter_words[filter_words.index(filter_word) - 1] + " (")
    
    if "Filter" in qepJSON.keys():
        filter_cond = qepJSON["Filter"]
        filter_cond = cleanup_cond(filter_cond)

        sqlfragments.append(filter_cond)

        sqlfragments = subquery_block_add(sqlfragments, filter_cond)

        filter_words = filter_cond.split()

        for filter_word in filter_words:
            if '=' == filter_word:
                if filter_words.index(filter_word) > 0:
                    filter_words[filter_words.index(filter_word) - 1], filter_words[filter_words.index(filter_word) + 1] = filter_words[filter_words.index(filter_word) + 1], filter_words[filter_words.index(filter_word) - 1]

        new_filter_cond = ' '.join(filter_words)

        sqlfragments.append(new_filter_cond)

        sqlfragments = subquery_block_add(sqlfragments, filter_cond)

        # Account for all subqueries
        for subquery_result in re.findall("\$\d+", filter_cond):
            filter_words = filter_cond.split()
            
            for filter_word in filter_words:
                if filter_word == subquery_result:
                    if filter_words.index(filter_word) > 1:
                        sqlfragments.append("WHERE " + filter_words[filter_words.index(filter_word) - 2] + " " + filter_words[filter_words.index(filter_word) - 1] + " (")
                        sqlfragments.append(filter_words[filter_words.index(filter_word) - 2] + " " + filter_words[filter_words.index(filter_word) - 1] + " (")

    if "Relation Name" in qepJSON.keys():
        if "Recheck Cond" in qepJSON.keys():
            sqlfragments = resolve_relation(sqlfragments, qepJSON)
        relation_name = qepJSON["Relation Name"]
        sqlfragments.append("FROM " + relation_name)

    # Find matching SQL
    start_index, end_index = search_in_sql(sqlfragments, query)

    if start_index is not -1:
        qepJSON["start_index"] = start_index
        qepJSON["end_index"] = end_index

    return qepJSON

# Process gather merge
def process_index_only_scan(qepJSON, query):
    print("Processing index only scan")

    sqlfragments = list()

    if "Index Cond" in qepJSON.keys():
        filter_cond = qepJSON["Index Cond"]
        filter_cond = cleanup_cond(filter_cond)

        sqlfragments.append(filter_cond)

        filter_words = filter_cond.split()

        for filter_word in filter_words:
            if '=' in filter_word:
                if filter_words.index('=') > 0:
                    filter_words[filter_words.index('=') - 1], filter_words[filter_words.index('=') + 1] = filter_words[filter_words.index('=') + 1], filter_words[filter_words.index('=') - 1]

        new_filter_cond = ' '.join(filter_words)

        sqlfragments.insert(0, new_filter_cond)

        sqlfragments = subquery_block_add(sqlfragments, filter_cond)

        # Account for all subqueries
        for subquery_result in re.findall("\$\d+", filter_cond):
            filter_words = filter_cond.split()
            
            for filter_word in filter_words:
                if filter_word == subquery_result:
                    if filter_words.index(filter_word) > 1:
                        sqlfragments.append("WHERE " + filter_words[filter_words.index(filter_word) - 2] + " " + filter_words[filter_words.index(filter_word) - 1] + " (")
                        sqlfragments.append(filter_words[filter_words.index(filter_word) - 2] + " " + filter_words[filter_words.index(filter_word) - 1] + " (")
    
    if "Filter" in qepJSON.keys():
        filter_cond = qepJSON["Filter"]
        filter_cond = cleanup_cond(filter_cond)

        sqlfragments.append(filter_cond)

        filter_words = filter_cond.split()

        for filter_word in filter_words:
            if '=' == filter_word:
                if filter_words.index(filter_word) > 0:
                    filter_words[filter_words.index(filter_word) - 1], filter_words[filter_words.index(filter_word) + 1] = filter_words[filter_words.index(filter_word) + 1], filter_words[filter_words.index(filter_word) - 1]

        new_filter_cond = ' '.join(filter_words)

        sqlfragments.append(new_filter_cond)

        sqlfragments = subquery_block_add(sqlfragments, filter_cond)

        # Account for all subqueries
        for subquery_result in re.findall("\$\d+", filter_cond):
            filter_words = filter_cond.split()
            
            for filter_word in filter_words:
                if filter_word == subquery_result:
                    if filter_words.index(filter_word) > 1:
                        sqlfragments.append("WHERE " + filter_words[filter_words.index(filter_word) - 2] + " " + filter_words[filter_words.index(filter_word) - 1] + " (")
                        sqlfragments.append(filter_words[filter_words.index(filter_word) - 2] + " " + filter_words[filter_words.index(filter_word) - 1] + " (")

    if "Relation Name" in qepJSON.keys():
        if "Index Cond" in qepJSON.keys() or "Filter" in qepJSON.keys():
            sqlfragments = resolve_relation(sqlfragments, qepJSON)
        relation_name = qepJSON["Relation Name"]
        sqlfragments.append("FROM " + relation_name)

    # Find matching SQL
    start_index, end_index = search_in_sql(sqlfragments, query)

    if start_index is not -1:
        qepJSON["start_index"] = start_index
        qepJSON["end_index"] = end_index

    return qepJSON

# Process hash node
def process_hash(qepJSON, query):
    print("Processing hash")

    sqlfragments = list()

    if "Plans" in qepJSON.keys():
        for plan in qepJSON["Plans"]:
            if "Index Cond" in plan.keys():
                filter_cond = qepJSON["Index Cond"]
                filter_cond = cleanup_cond(filter_cond)

                sqlfragments.append(filter_cond)
                sqlfragments.append(plan["Index Cond"][1:len(plan["Index Cond"]) - 1])

                filter_words = filter_cond.split()

                for filter_word in filter_words:
                    if '=' in filter_word:
                        if filter_words.index('=') > 0:
                            filter_words[filter_words.index('=') - 1], filter_words[filter_words.index('=') + 1] = filter_words[filter_words.index('=') + 1], filter_words[filter_words.index('=') - 1]

                new_filter_cond = ' '.join(filter_words)

                sqlfragments.insert(0, new_filter_cond)

                sqlfragments = subquery_block_add(sqlfragments, filter_cond)

                # Account for all subqueries
                for subquery_result in re.findall("\$\d+", filter_cond):
                    filter_words = filter_cond.split()
                    
                    for filter_word in filter_words:
                        if filter_word == subquery_result:
                            if filter_words.index(filter_word) > 1:
                                sqlfragments.append("WHERE " + filter_words[filter_words.index(filter_word) - 2] + " " + filter_words[filter_words.index(filter_word) - 1] + " (")
                                sqlfragments.append(filter_words[filter_words.index(filter_word) - 2] + " " + filter_words[filter_words.index(filter_word) - 1] + " (")
            
            if "Filter" in plan.keys():
                filter_cond = plan["Filter"]
                filter_cond = cleanup_cond(filter_cond)

                sqlfragments.append(filter_cond)
                sqlfragments.append(plan["Filter"][1:len(plan["Filter"]) - 1])

                filter_words = filter_cond.split()

                for filter_word in filter_words:
                    if '=' == filter_word:
                        if filter_words.index(filter_word) > 0:
                            filter_words[filter_words.index(filter_word) - 1], filter_words[filter_words.index(filter_word) + 1] = filter_words[filter_words.index(filter_word) + 1], filter_words[filter_words.index(filter_word) - 1]

                new_filter_cond = ' '.join(filter_words)

                sqlfragments.append(new_filter_cond)

                sqlfragments = subquery_block_add(sqlfragments, filter_cond)

                # Account for all subqueries
                for subquery_result in re.findall("\$\d+", filter_cond):
                    filter_words = filter_cond.split()
                    
                    for filter_word in filter_words:
                        if filter_word == subquery_result:
                            if filter_words.index(filter_word) > 1:
                                sqlfragments.append("WHERE " + filter_words[filter_words.index(filter_word) - 2] + " " + filter_words[filter_words.index(filter_word) - 1] + " (")
                                sqlfragments.append(filter_words[filter_words.index(filter_word) - 2] + " " + filter_words[filter_words.index(filter_word) - 1] + " (")

            if "Relation Name" in plan.keys():
                if "Index Cond" in plan.keys() or "Filter" in plan.keys():
                    sqlfragments = resolve_relation(sqlfragments, plan)
                relation_name = plan["Relation Name"]
                sqlfragments.append("FROM " + relation_name)

    # Find matching SQL
    start_index, end_index = search_in_sql(sqlfragments, query)

    if start_index is not -1:
        qepJSON["start_index"] = start_index
        qepJSON["end_index"] = end_index

    return qepJSON

# Process gather node
def process_gather(qepJSON, query):
    print("Processing gather")

    sqlfragments = list()

    if "Plans" in qepJSON.keys():
        for plan in qepJSON["Plans"]:
            if "Index Cond" in plan.keys():
                filter_cond = qepJSON["Index Cond"]
                filter_cond = cleanup_cond(filter_cond)

                sqlfragments.append(filter_cond)

                filter_words = filter_cond.split()

                for filter_word in filter_words:
                    if '=' in filter_word:
                        if filter_words.index('=') > 0:
                            filter_words[filter_words.index('=') - 1], filter_words[filter_words.index('=') + 1] = filter_words[filter_words.index('=') + 1], filter_words[filter_words.index('=') - 1]

                new_filter_cond = ' '.join(filter_words)

                sqlfragments.insert(0, new_filter_cond)

                sqlfragments = subquery_block_add(sqlfragments, filter_cond)

                # Account for all subqueries
                for subquery_result in re.findall("\$\d+", filter_cond):
                    filter_words = filter_cond.split()
                    
                    for filter_word in filter_words:
                        if filter_word == subquery_result:
                            if filter_words.index(filter_word) > 1:
                                sqlfragments.append("WHERE " + filter_words[filter_words.index(filter_word) - 2] + " " + filter_words[filter_words.index(filter_word) - 1] + " (")
                                sqlfragments.append(filter_words[filter_words.index(filter_word) - 2] + " " + filter_words[filter_words.index(filter_word) - 1] + " (")
            
            if "Filter" in plan.keys():
                filter_cond = plan["Filter"]
                filter_cond = cleanup_cond(filter_cond)

                sqlfragments.append(filter_cond)

                filter_words = filter_cond.split()

                for filter_word in filter_words:
                    if '=' == filter_word:
                        if filter_words.index(filter_word) > 0:
                            filter_words[filter_words.index(filter_word) - 1], filter_words[filter_words.index(filter_word) + 1] = filter_words[filter_words.index(filter_word) + 1], filter_words[filter_words.index(filter_word) - 1]

                new_filter_cond = ' '.join(filter_words)

                sqlfragments.append(new_filter_cond)

                sqlfragments = subquery_block_add(sqlfragments, filter_cond)

                # Account for all subqueries
                for subquery_result in re.findall("\$\d+", filter_cond):
                    filter_words = filter_cond.split()
                    
                    for filter_word in filter_words:
                        if filter_word == subquery_result:
                            if filter_words.index(filter_word) > 1:
                                sqlfragments.append("WHERE " + filter_words[filter_words.index(filter_word) - 2] + " " + filter_words[filter_words.index(filter_word) - 1] + " (")
                                sqlfragments.append(filter_words[filter_words.index(filter_word) - 2] + " " + filter_words[filter_words.index(filter_word) - 1] + " (")

            if "Relation Name" in plan.keys():
                if "Index Cond" in plan.keys() or "Filter" in plan.keys():
                    sqlfragments = resolve_relation(sqlfragments, plan)
                relation_name = plan["Relation Name"]
                sqlfragments.append("FROM " + relation_name)

    # Find matching SQL
    start_index, end_index = search_in_sql(sqlfragments, query)

    if start_index is not -1:
        qepJSON["start_index"] = start_index
        qepJSON["end_index"] = end_index

    return qepJSON

# Process unique node
def process_unique(qepJSON, query):
    print("Processing unique")

    sqlfragments = list()

    if "Plans" in qepJSON.keys():
        for plan in qepJSON["Plans"]:
            if "Index Cond" in plan.keys():
                filter_cond = qepJSON["Index Cond"]
                filter_cond = cleanup_cond(filter_cond)

                sqlfragments.append(filter_cond)
                sqlfragments.append(plan["Index Cond"][1:len(plan["Index Cond"]) - 1])

                filter_words = filter_cond.split()

                for filter_word in filter_words:
                    if '=' in filter_word:
                        if filter_words.index('=') > 0:
                            filter_words[filter_words.index('=') - 1], filter_words[filter_words.index('=') + 1] = filter_words[filter_words.index('=') + 1], filter_words[filter_words.index('=') - 1]

                new_filter_cond = ' '.join(filter_words)

                sqlfragments.insert(0, new_filter_cond)

                sqlfragments = subquery_block_add(sqlfragments, filter_cond)

                # Account for all subqueries
                for subquery_result in re.findall("\$\d+", filter_cond):
                    filter_words = filter_cond.split()
                    
                    for filter_word in filter_words:
                        if filter_word == subquery_result:
                            if filter_words.index(filter_word) > 1:
                                sqlfragments.append("WHERE " + filter_words[filter_words.index(filter_word) - 2] + " " + filter_words[filter_words.index(filter_word) - 1] + " (")
                                sqlfragments.append(filter_words[filter_words.index(filter_word) - 2] + " " + filter_words[filter_words.index(filter_word) - 1] + " (")
            
            if "Filter" in plan.keys():
                filter_cond = plan["Filter"]
                filter_cond = cleanup_cond(filter_cond)

                sqlfragments.append(filter_cond)
                sqlfragments.append(plan["Filter"][1:len(plan["Filter"]) - 1])

                filter_words = filter_cond.split()

                for filter_word in filter_words:
                    if '=' == filter_word:
                        if filter_words.index(filter_word) > 0:
                            filter_words[filter_words.index(filter_word) - 1], filter_words[filter_words.index(filter_word) + 1] = filter_words[filter_words.index(filter_word) + 1], filter_words[filter_words.index(filter_word) - 1]

                new_filter_cond = ' '.join(filter_words)

                sqlfragments.append(new_filter_cond)

                sqlfragments = subquery_block_add(sqlfragments, filter_cond)

                # Account for all subqueries
                for subquery_result in re.findall("\$\d+", filter_cond):
                    filter_words = filter_cond.split()
                    
                    for filter_word in filter_words:
                        if filter_word == subquery_result:
                            if filter_words.index(filter_word) > 1:
                                sqlfragments.append("WHERE " + filter_words[filter_words.index(filter_word) - 2] + " " + filter_words[filter_words.index(filter_word) - 1] + " (")
                                sqlfragments.append(filter_words[filter_words.index(filter_word) - 2] + " " + filter_words[filter_words.index(filter_word) - 1] + " (")

            if "Group Key" in plan.keys():
                for key in plan["Group Key"]:
                    sqlfragments.append("DISTINCT " + key)
                    sqlfragments.append(key)

            if "Sort Key" in plan.keys():
                for key in plan["Sort Key"]:
                    sqlfragments.append("DISTINCT " + key)
                    sqlfragments.append(key)

            if "Relation Name" in plan.keys():
                if "Index Cond" in plan.keys() or "Filter" in plan.keys():
                    sqlfragments = resolve_relation(sqlfragments, plan)
                relation_name = plan["Relation Name"]
                sqlfragments.append("FROM " + relation_name)

    '''sqlfragments_temp = reversed(sqlfragments.copy())

    if sqlfragments_temp is not None:
        for sqlfragment in sqlfragments_temp:
            sqlfragments.insert(0, re.sub(r'(.*)\_\d+(.*)', r'\1\2', sqlfragment))
            sqlfragments.pop()'''

    # Find matching SQL
    start_index, end_index = search_in_sql(sqlfragments, query)

    if start_index is not -1:
        qepJSON["start_index"] = start_index
        qepJSON["end_index"] = end_index

    return qepJSON

# Process sort node
def process_sort(qepJSON, query):
    print("Processing sort")

    sqlfragments = list()

    # Check if sort key in list of keys
    if "Sort Key" in qepJSON.keys():
        for sort_key in qepJSON["Sort Key"]:
            sqlfragments.append("ORDER BY " + cleanup_cond(sort_key))
            sqlfragments.append(cleanup_cond(sort_key))

    '''sqlfragments_temp = reversed(sqlfragments.copy())

    if sqlfragments_temp is not None:
        for sqlfragment in sqlfragments_temp:
            sqlfragments.insert(0, re.sub(r'(.*)\_\d+(.*)', r'\1\2', sqlfragment))
            sqlfragments.pop()'''
    
    start_index, end_index = search_in_sql(sqlfragments, query)

    if start_index is not -1:
        qepJSON["start_index"] = start_index
        qepJSON["end_index"] = end_index

    return qepJSON

# Process nested loop
def process_nested_loop(qepJSON, query):
    print("Processing nested loop")

    sqlfragments = list()

    if "Join Filter" in qepJSON.keys():
        filter_cond = qepJSON["Join Filter"]
        filter_cond = cleanup_cond(filter_cond)

        sqlfragments.append(filter_cond)

        filter_words = filter_cond.split()

        for filter_word in filter_words:
            if '=' == filter_word:
                if filter_words.index(filter_word) > 0:
                    filter_words[filter_words.index(filter_word) - 1], filter_words[filter_words.index(filter_word) + 1] = filter_words[filter_words.index(filter_word) + 1], filter_words[filter_words.index(filter_word) - 1]

        new_filter_cond = ' '.join(filter_words)

        sqlfragments.append(new_filter_cond)

        sqlfragments = subquery_block_add(sqlfragments, filter_cond)
    
    if "Relation Name" in qepJSON.keys():
        if "Join Filter" in qepJSON.keys():
            sqlfragments = resolve_relation(sqlfragments, qepJSON)
        relation_name = qepJSON["Relation Name"]
        sqlfragments.append("FROM " + relation_name)

    if "Plans" in qepJSON.keys():
        all_names_present = True

        for plan in qepJSON["Plans"]:
            if "Relation Name" not in plan.keys():
                all_names_present = False
        
        if all_names_present:
            attributes = list()
            
            for plan in qepJSON["Plans"]:
                attributes.append(plan["Relation Name"])

            sqlfragment = ', '.join(attributes)
            sqlfragments.append(sqlfragment)

        sqlfragments_temp = sqlfragments.copy()

        for sqlfragment in sqlfragments_temp:
            for plan in qepJSON["Plans"]:
                if "Alias" in plan.keys():
                    sqlfragments.append(sqlfragment.replace(plan["Alias"], plan["Relation Name"]))

    # Find matching SQL
    start_index, end_index = search_in_sql(sqlfragments, query)

    if start_index is not -1:
        qepJSON["start_index"] = start_index
        qepJSON["end_index"] = end_index

    return qepJSON

# Process merge join
def process_merge_join(qepJSON, query):
    print("Processing merge join")

    sqlfragments = list()

    if "Merge Cond" in qepJSON.keys():
        filter_cond = qepJSON["Merge Cond"]
        filter_cond = cleanup_cond(filter_cond)

        sqlfragments.append(filter_cond)

        filter_words = filter_cond.split()

        for filter_word in filter_words:
            if '=' == filter_word:
                if filter_words.index(filter_word) > 0:
                    filter_words[filter_words.index(filter_word) - 1], filter_words[filter_words.index(filter_word) + 1] = filter_words[filter_words.index(filter_word) + 1], filter_words[filter_words.index(filter_word) - 1]

        new_filter_cond = ' '.join(filter_words)

        sqlfragments.append(new_filter_cond)

        sqlfragments = subquery_block_add(sqlfragments, filter_cond)
    
    if "Relation Name" in qepJSON.keys():
        if "Join Filter" in qepJSON.keys():
            sqlfragments = resolve_relation(sqlfragments, qepJSON)
        relation_name = qepJSON["Relation Name"]
        sqlfragments.append("FROM " + relation_name)

    if "Plans" in qepJSON.keys():
        all_names_present = True

        for plan in qepJSON["Plans"]:
            if "Relation Name" not in plan.keys():
                all_names_present = False
        
        if all_names_present:
            attributes = list()
            
            for plan in qepJSON["Plans"]:
                attributes.append(plan["Relation Name"])

            sqlfragment = ', '.join(attributes)
            sqlfragments.append(sqlfragment)

        sqlfragments_temp = sqlfragments.copy()

        for sqlfragment in sqlfragments_temp:
            for plan in qepJSON["Plans"]:
                if "Alias" in plan.keys():
                    sqlfragments.append(sqlfragment.replace(plan["Alias"], plan["Relation Name"]))

    # Find matching SQL
    start_index, end_index = search_in_sql(sqlfragments, query)

    if start_index is not -1:
        qepJSON["start_index"] = start_index
        qepJSON["end_index"] = end_index

    return qepJSON

# Process aggregate node
def process_aggregate(qepJSON, query):
    print("Processing aggregate")

    sqlfragments = list()

    if "Filter" in qepJSON.keys():
        group_filter = qepJSON["Filter"]
        group_filter = group_filter[1:len(group_filter)-1]
        print(group_filter)
        sqlfragments.append(group_filter)

    if "Group Key" in qepJSON.keys():
        group_key = qepJSON["Group Key"]
        
        for key in group_key:
            sqlfragments.append("GROUP BY " + cleanup_cond(key))
            sqlfragments.append(cleanup_cond(key))
    
    # Find matching SQL
    start_index, end_index = search_in_sql(sqlfragments, query)

    if start_index is not -1:
        qepJSON["start_index"] = start_index
        qepJSON["end_index"] = end_index

    return qepJSON

# Process hash join
def process_hash_join(qepJSON, query):
    print("Processing hash join")

    sqlfragments = list()

    if "Hash Cond" in qepJSON.keys():
        filter_cond = qepJSON["Hash Cond"]
        filter_cond = cleanup_cond(filter_cond)

        sqlfragments.append(filter_cond)

        filter_words = filter_cond.split()

        for filter_word in filter_words:
            if '=' == filter_word:
                if filter_words.index(filter_word) > 0:
                    filter_words[filter_words.index(filter_word) - 1], filter_words[filter_words.index(filter_word) + 1] = filter_words[filter_words.index(filter_word) + 1], filter_words[filter_words.index(filter_word) - 1]

        new_filter_cond = ' '.join(filter_words)

        sqlfragments.append(new_filter_cond)

        sqlfragments = subquery_block_add(sqlfragments, filter_cond)
    
    if "Relation Name" in qepJSON.keys():
        if "Join Filter" in qepJSON.keys():
            sqlfragments = resolve_relation(sqlfragments, qepJSON)
        relation_name = qepJSON["Relation Name"]
        sqlfragments.append("FROM " + relation_name)

    if "Plans" in qepJSON.keys():
        all_names_present = True

        for plan in qepJSON["Plans"]:
            if "Relation Name" not in plan.keys():
                all_names_present = False
        
        if all_names_present:
            attributes = list()
            
            for plan in qepJSON["Plans"]:
                attributes.append(plan["Relation Name"])

            sqlfragment = ', '.join(attributes)
            sqlfragments.append(sqlfragment)

        sqlfragments_temp = sqlfragments.copy()

        for sqlfragment in sqlfragments_temp:
            for plan in qepJSON["Plans"]:
                if "Alias" in plan.keys():
                    sqlfragments.append(sqlfragment.replace(plan["Alias"], plan["Relation Name"]))

    '''sqlfragments_temp = reversed(sqlfragments.copy())

    if sqlfragments_temp is not None:
        for sqlfragment in sqlfragments_temp:
            sqlfragments.insert(0, re.sub(r'(.*)\_\d+(.*)', r'\1\2', sqlfragment))
            sqlfragments.pop()'''

    # Find matching SQL
    start_index, end_index = search_in_sql(sqlfragments, query)

    if start_index is not -1:
        qepJSON["start_index"] = start_index
        qepJSON["end_index"] = end_index

    return qepJSON

# Search for corresponding SQL based on SQL fragments.
# Function stops once a match is found
def search_in_sql(sqlfragments, query):

    print("\nSQL Fragments: " + str(sqlfragments) + "\n")
    # print("\n" + query + "\n")

    # search for matching SQL
    for sqlfragment in sqlfragments:
        start_index = find_str(query.lower(), sqlfragment.lower())

        if start_index is not -1:
            end_index = start_index + len(sqlfragment)
            print("Start index is " + str(start_index) + " and end index is " + str(end_index))
            print("Matching SQL is: " + query[start_index : end_index] + "\n")
            break

    if start_index is -1:
        end_index = -1
        print("Start index is " + str(start_index) + " and end index is " + str(end_index))

    return start_index, end_index

# Append relation name to front of attribute in JSON
def resolve_relation(sqlfragments, qepJSON):
    sqlfragments_temp = reversed(sqlfragments.copy())

    for sqlfragment in sqlfragments_temp:
        sqlwords = sqlfragment.split()

        for operator in ['=', '!=', '<', '>', '<>', '>=', '<=', '!<', '!>', 'IS', 'NOT', 'IN', 'LIKE']:

            n = 0

            # Try to append to attributes
            for sqlword in sqlwords:
                if operator == sqlword:

                    n += 1

                    if nth_index(sqlwords, operator, n) > 0 and nth_index(sqlwords, operator, n) < len(sqlwords) - 1:
                        if '.' not in sqlwords[nth_index(sqlwords, operator, n) - 1]:
                            sqlwords[nth_index(sqlwords, operator, n) - 1] = qepJSON["Relation Name"] + "." + sqlwords[nth_index(sqlwords, operator, n) - 1]
                        if '.' not in sqlwords[nth_index(sqlwords, operator, n) + 1] and sqlwords[nth_index(sqlwords, operator, n) + 1].isidentifier():
                            sqlwords[nth_index(sqlwords, operator, n) + 1] = qepJSON["Relation Name"] + "." + sqlwords[nth_index(sqlwords, operator, n) + 1]

        sqlfragment_new = ' '.join(sqlwords)

        # print(sqlfragment_new)

        sqlfragments.insert(0, sqlfragment_new)

    for sqlfragment in sqlfragments:
        if "Alias" in qepJSON.keys():
            sqlfragment = sqlfragment.replace(qepJSON["Alias"], qepJSON["Relation Name"])
    
    return sqlfragments

# Taking into account IN and NOT IN tokens
def subquery_block_add(sqlfragments, filter_cond):

    # Parse filter condition to words
    filter_words = filter_cond.split()

    for operator in ['=', '!=', '<>']:

        # Try to append to attributes
        for filter_word in filter_words:
            if operator == filter_word:
                if filter_words.index(filter_word) > 0:
                    if (operator == '='):
                        sqlfragments.insert(0, "WHERE " + filter_words[filter_words.index(filter_word) - 1] + " IN (")
                        sqlfragments.insert(1, filter_words[filter_words.index(filter_word) - 1] + " IN (")
                    elif (operator == '!='):
                        sqlfragments.insert(0, "WHERE " + filter_words[filter_words.index(filter_word) - 1] + " NOT IN (")
                        sqlfragments.insert(1, filter_words[filter_words.index(filter_word) - 1] + " NOT IN (")

    return sqlfragments