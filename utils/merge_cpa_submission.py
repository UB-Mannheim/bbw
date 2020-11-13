#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import pandas as pd
import csv
from tqdm import tqdm

# Load data and check statistics
target = pd.read_csv("../CPA_Round2_Targets.csv", dtype=object,
                     names=['tableID', 'headColumnID', 'tailColumnID'])

# order is important as the first value will beat the later ones!
files = [
    "../tables_output/bbw_r2_cpa_s4-9_sub_missings-and-searx",
    "../tables_output/bbw_r2_cpa_s4-9_sub_openrefine",
    "../tables_output/bbw_r2_cpa_s3_sub.csv"
]
calculated = [pd.read_csv(file, dtype=object, names=['tableID', 'col0', 'colx', 'property']) for file in files]

targlist = []
for target in tqdm(target.values):
    [tableid, col0, colx] = target
    found_value = None
    # for each file see if there is an result for this target and take the first one
    for result in calculated:
        result_df = result[(result.tableID == tableid) & (result.colx == colx)]
        result_values = result_df.property.to_list()
        if len(result_values) == 0:
            continue
        if len(result_values) > 1:
            print("ERROR: there are more than 1 outputted values for", tableid, colx)
            continue
        # after this point we have exactly one value
        result_value = result_values[0]
        if found_value:
            if result_value != found_value:
                print("DIFF in", tableid, colx, found_value, "(wins),", result_value, "(other)")
            continue
        else:
            found_value = result_value
    if found_value:
        targlist.append([tableid, 0, colx, found_value])


merged = pd.DataFrame(targlist)
merged.to_csv('merged.csv', index=False, header=False, quoting=csv.QUOTE_ALL)
