'''
So this script will have function that will take in 2 arguments
1. the dataset to be cleansed
2. the configuration file that will be configured in json

for now in the configuration file we will determine that the columns stated in the configuration file will NOT contain any null values and print out OK

if there are any null values in any of the stated columns, it will print out null values detected
'''

import pandas as pd
import json
import os

def validate_clean_data(dataset, config_path):

    validation_result = 0

    data = pd.read_csv(dataset, low_memory=False)
    if validate_config(data.columns, config_path):
        with open(config_path) as file:
            config = json.load(file)
            for col in config['columns']:
                col_name = col['name']
                rules = col['rules']
                for rule in rules:
                    #print('Validating column ' + col_name)
                    result = data[col_name].apply(lambda x:globals()[rule](x)).sum()
                    #print('Result is %d' % result)
                    validation_result = validation_result + result
    else:
        print('Invalid configuration')
    
    return validation_result

def validate_config(column_names, config_path):
    with open(config_path) as file :
        data = json.load(file)
        for col in data['columns']:
            if col['name'] not in column_names:
                print('Column ' + col['name'] + ' present in config but not present in input file')
                return False
            else:
                for rule in col['rules']:
                    if rule not in globals() :
                        print('Rule ' + rule + ' present in config file but not found in script')
                        return False
    return True

def validate_mandatory(input):
    return input == None

def validate_numeric(input):
    return False