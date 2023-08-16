import csv
import os

hashmap = {}
codes = {}
absolute_path = os.path.dirname(__file__)

def get_care_providers_as_csv():
    return load_csv_care_providers_as_object(absolute_path + "/care_providers.csv")

def load_csv_care_providers_as_object(file_path: str):
    result = []
    with open(file_path, "r", errors='ignore') as stream:
        reader = csv.reader(stream)
        header = next(reader)  # Skip header
        for line in reader:
            if (line[5], line[29]) in hashmap.keys():
                hashmap[(line[5], line[29])] += 1
            else:
                hashmap[(line[5], line[29])] = 1

            result.append({key: value for key, value in zip(header, line)})
    return result

def get_codes_hashmap():
    data_as_csv = get_care_providers_as_csv()
    codes = {}
    for data in data_as_csv:
        if data['KrajCode'] in codes:
            codes[data['KrajCode']].add(data['OkresCode'])
        else:
            codes[data['KrajCode']] = {data['OkresCode']}
    return codes