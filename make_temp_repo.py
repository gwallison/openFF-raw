# -*- coding: utf-8 -*-
"""
Created on Sat Oct 30 12:57:54 2021

@author: Gary

Used to create a repo-like set of data and that has not been completely
curated.  
Provided in the repo are: 
    - data table pickles (for recreating analysis sets)
    - copies of the translation tables used to created the database
"""

###############################  Used to make repository accessible ####################
import sys
sys.path.insert(0,'c:/MyDocs/OpenFF/src/openFF-build/')
###################################################################################

import os, shutil
import pandas as pd
import core.Analysis_set as ana_set
import datetime
import zipfile
import build_common
outdir = build_common.get_pickle_dir()
sources = build_common.get_data_dir()
trans_dir = build_common.get_transformed_dir()
tempfolder = './tmp/'

repo_name = 'testData'
repo_dir = build_common.get_repo_dir() + repo_name
pklsource = 'testData_pickles'

descriptive_notes = f""" This is NOT a full OpenFF data repository. This is
used to test data that have not yet been full curated.
Created {datetime.date.today()}
"""

boilerplate = """This directory contains a BETA data set generated by the Open-FF
project.
"""

print(f'Starting creation of new TEMP Data Repo set: {repo_name}')
# create new directory
try:
    os.mkdir(repo_dir)
except:
    print(f'\nDirectory <{repo_dir}> not created;  already created?')

# create and store README
with open(repo_dir+'/README.txt','w') as f:
    f.write(descriptive_notes+'\n')
    f.write(boilerplate)  # see below for the text


# copy pickles
pickledir = repo_dir+'/pickles'
try:
    os.mkdir(pickledir)
except:
    print(f'\nDirectory <{pickledir}> not created;  already created?')
flst = os.listdir(outdir+pklsource)
for fn in flst:
    if fn[-4:]=='.pkl':
        if not (fn[-7:]=='_df.pkl'):  # ignore pickled analysis sets
            shutil.copyfile(outdir+pklsource+'/'+fn, pickledir+'/'+fn)
            print(f'copied {fn}')
        
# copy curation files
files = ['carrier_list_auto.csv','carrier_list_curated.csv',
         'carrier_list_prob.csv','CAS_curated.csv',
         'casing_curated.csv','company_xlate.csv','ST_api_without_pdf.csv',
         'ING_curated.csv','CAS_synonyms.csv','CAS_ref_and_names.csv',
         'tripwire_summary.csv','upload_dates.csv']

cdir = 'curation_files/'
os.mkdir(cdir) # made in the cwd.
with zipfile.ZipFile(repo_dir+'/curation_files.zip','w') as z:
    for fn in files:
        print(f'  - zipping {fn}')
        shutil.copy(trans_dir+fn,cdir)
        z.write(cdir+fn,compress_type=zipfile.ZIP_DEFLATED)    
shutil.rmtree(cdir)         


print(f'Repo creation completed: {repo_dir}')