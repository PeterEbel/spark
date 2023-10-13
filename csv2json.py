import csv
import json 
import pandas as pd

csv_file = '/home/peter/Projects/spark/data/customers.csv'
json_file = '/home/peter/Projects/spark/data/customers.json'

csv_file = pd.DataFrame(pd.read_csv(csv_file, sep = "|", header = 0, index_col = False))
csv_file.to_json(json_file, orient = "records", date_format = "epoch", double_precision = 10, force_ascii = False, date_unit = "ms", default_handler = None)
