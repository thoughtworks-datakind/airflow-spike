import json

def validate_config(data_columns, config_path):
    with open(config_path) as file :
        data = json.load(file)
        columns = traverse_json(data["columns"], "name")
        are_columns_valid = check_difference_between_two_lists(columns, data_columns, "Columns not present in data")

        rules = traverse_json(data["columns"], "rules")
        flatmap_rules = [item for sublist in rules for item in sublist]
        are_rules_valid = check_difference_between_two_lists(flatmap_rules, globals().keys(),"Following rules are not defined")

    return are_columns_valid and are_rules_valid

def check_difference_between_two_lists(subset, superset, message):
    diff = [x for x in subset if x not in superset]
    diff_is_empty = len(diff) == 0
    if not diff_is_empty:
        print(message)
        print(diff)
    return diff_is_empty

def traverse_json(input, key):
    list_columns = []
    for entry in input:
        list_columns.append(entry[key])
    return list_columns

def validate_mandatory(input):
    return input == None

def validate_numeric(input):
    return False
