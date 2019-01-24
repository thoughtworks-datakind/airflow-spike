import pandas as pd

def get_column_names(input_file_path):
    dataset = pd.read_csv(input_file_path)
    return dataset.columns