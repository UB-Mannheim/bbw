#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import pandas as pd
import csv
from tqdm import tqdm

# Load data and check statistics
submitted = pd.read_csv("../tables_output/bbw_r2_cpa_s4-9_sub.csv", dtype=object,
                        names=['tableID', 'col0', 'colx', 'property'])
target = pd.read_csv("../CPA_Round2_Targets.csv", dtype=object,
                     names=['tableID', 'headColumnID', 'tailColumnID'])

# Some simple statistics
print('\n# Table statistics')
total_matched = len(submitted.tableID.to_list())
total_targets = len(target.tableID.to_list())
print(total_matched, "of the total", total_targets, "target columns are matched.")
print("Thus we still have", total_targets - total_matched, "columns in the tables which are not matched at all!\n")
print("==> (Internal) Recall =", total_matched / total_targets)

analysis = []
complete_missings = []
seen_missings = []

# Unmatched columns from the target file
for target in tqdm(target.values):  #
    [tableid, col0, colx] = target
    # case 1: complete table is in the submission missing
    submitted_this = submitted[submitted.tableID == tableid]
    if len(submitted_this) == 0:
        if tableid not in seen_missings:
            filecsv = pd.read_csv('../tables/' + tableid + '.csv', dtype=str)
            (rows, cols) = filecsv.shape
            for row in range(0, rows):
                complete_missings.append([filecsv.iloc[row, 0], tableid])
            seen_missings.append(tableid)
    # case 2: only some columns of this table are missing in the submission
    else:
        submitted_this = submitted[(submitted.tableID == tableid) & (submitted.colx == colx)]
        nsubmitted = len(submitted_this)
        if nsubmitted > 1:
            print("ERROR: There are several submitted values for the same (table, column) pair", tableid, colx)
        if nsubmitted == 0:
            filecsv = pd.read_csv('../tables/' + tableid + '.csv', dtype=str)
            (rows, cols) = filecsv.shape
            for row in range(0, rows):
                analysis.append(["missing", filecsv.iloc[row, int(colx)], filecsv.iloc[row, 0], tableid, int(colx)])

print(len(seen_missings))

submitted.property = submitted.property \
    .str.replace('http://www.wikidata.org/prop/direct/', '') \
    .str.replace('http://www.wikidata.org/prop/direct-normalized/', '') \
    .str.replace('http://www.wikidata.org/prop/reference/value-normalized/', '') \
    .str.replace('http://www.wikidata.org/prop/reference/value/', '') \
    .str.replace('http://www.wikidata.org/prop/reference/', '') \
    .str.replace('http://www.wikidata.org/prop/statement/', '') \
    .str.replace('http://www.wikidata.org/prop/', '')
# only the last two versions occur in current submissions

all_properties = submitted.property.to_list()
# only unique elements and sort by the number after the P...
all_properties = sorted(list(set(all_properties)), key=lambda x: int(x[1:]))

# Here you can choose which properties you want to analyse
properties = all_properties
# properties = ["P2044"]

for prop in properties:
    found_matches = submitted[submitted.property == prop]
    print("# Found", len(found_matches.values), "instances for", prop)
    for match in found_matches.values:
        [tableid, col0, colx, p] = match
        colx = int(colx)
        if colx == 0:
            print("ERROR: 0 is not expected here", tableid, colx)
        filecsv = pd.read_csv('../tables/' + tableid + '.csv', dtype=str)
        (rows, cols) = filecsv.shape
        if colx >= cols:
            print("ERROR: columnid out of range", tableid, colx)
        for row in range(0, rows):
            analysis.append([prop, filecsv.iloc[row, colx], filecsv.iloc[row, 0], tableid, colx])

# Save to csv-files
analysis_df = pd.DataFrame(analysis)
analysis_df.to_csv('cpa_analysis.csv', index=False, quoting=csv.QUOTE_ALL,
                   header=["property", "value", "subject", "tableid", "columnid"])

if len(complete_missings) > 0:
    missings_df = pd.DataFrame(complete_missings)
    missings_df.to_csv('cpa_missings.csv', index=False, header=["value col0", "tableid"], quoting=csv.QUOTE_ALL)
