#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from bbw.bbw import preprocessing, contextual_matching, postprocessing
import pandas as pd
import csv
import argparse
from tqdm import tqdm
import time
import os
import sys

# Specify CLI
parser = argparse.ArgumentParser()
parser.add_argument('--amount', nargs='?', type=int, help='The amount of files that are considered. By default the script goes over all files but it is possible to only consider a certain amount of them.')
parser.add_argument('--offset', nargs='?', type=int, help='The offset will make it possible to ignore the first files and start with some offset. By default no offset is set.')
args = parser.parse_args()

# Path to the folders with target tables and input tables
path = ''

# Define the round in SemTab2020 and the number for your submission
nround = str(2)
nsubmission = str(42)

try:
    # Load the target data
    target_cpa = pd.read_csv(path+f"target/CPA_Round{nround}_Targets.csv", names=['file', 'column0', 'column'], dtype=object)
    target_cta = pd.read_csv(path+f"target/CTA_Round{nround}_Targets.csv", names=['file', 'column'], dtype=object)
    target_cea = pd.read_csv(path+f"target/CEA_Round{nround}_Targets.csv", names=['file', 'row', 'column'], dtype=object)

    # Create a list of filenames for matching in CPA, CEA and CTA tasks
    filelist = target_cpa.file.to_list() + target_cea.file.to_list() + target_cta.file.to_list()
    filelist = sorted(list(set(filelist)))
    if args.amount is None:
        args.amount = len(filelist)
    if args.offset is None:
        args.offset = 0
    filelist = filelist[args.offset:args.offset + args.amount]

    if __name__ == "__main__":
        print(args)
        cpa, cea, nomatch = [], [], []
        # Annotate files from filelist
        for ind, filename in enumerate(tqdm(filelist)):
            filecsv = pd.read_csv(path+f'tables_round{nround}/'+filename+'.csv', dtype=str, header=None)
            filecsv = preprocessing(filecsv)
            [cpa, cea, nomatch] = contextual_matching(filecsv, filename, cpa, cea, nomatch, 
                                                      step3=False, step4=False, step5=True, step6=True)
        # Postprocess cpa and cea lists and return the ready-for-submission dataframes
        [cpa_sub, cea_sub, cta_sub] = postprocessing(cpa, cea, filelist, 
                                                     target_cpa, target_cea, target_cta)
        # Save CP-, CE- and CT-Annotations to csv-files
        now = time.time() # It separates the outputs of parallel runs in different folders
        os.mkdir(f'r{nround}_s{nsubmission}_'+str(now))
        cpa_sub.to_csv(f'r{nround}_s{nsubmission}_{now}/bbw_r{nround}_s{nsubmission}_cpa_sub.csv', index=False, header=False, quoting=csv.QUOTE_ALL)
        cea_sub.to_csv(f'r{nround}_s{nsubmission}_{now}/bbw_r{nround}_s{nsubmission}_cea_sub.csv', index=False, header=False, quoting=csv.QUOTE_ALL)
        cta_sub.to_csv(f'r{nround}_s{nsubmission}_{now}/bbw_r{nround}_s{nsubmission}_cta_sub.csv', index=False, header=False, quoting=csv.QUOTE_ALL)

except FileNotFoundError as e:
    print(e)
    sys.exit(1)
