
# -*- coding: utf-8 -*-
"""
Created on Sun Nov 24 14:43:54 2019

@author: Gary

This script is used to download a new raw set, save it if the 
day of the week is in the list, and look for new events.
It will also record how many records are in each new event, and runs
tripwire (now called detect_changes).

This script runs independently of the main build_database set.  It is designed 
to run autonomously, can be executed from a crontab command.

Occasionally the download won't work, probably because of something on the
FF end, or slow network.  Currently this script just exits with an exception and does not
retry.

Dec 2021:  The 2019-2020 version is copied to the new directory structure and 
  modified to work there.
  
"""
###############################  Used to make build accessible ####################
import sys
sys.path.insert(0,'c:/MyDocs/OpenFF/src/openFF-build')
import build_data_set as bds

#import builder_tasks.Location_cleanup as loc_clean

###################################################################################


#import core.trip_wire as twire
import pandas as pd
import requests
from hashlib import sha256
#import subprocess
#import shutil
from datetime import datetime
import detect_changes 


force_archive = False # use sparingly, only when not doing routine checks.
do_download =  True   # if False, will run routines without downloading first,
                      # and will depend on existing test_data.zip files.
do_tripwire = True


today = datetime.today()
if today.weekday() in [5]: # Monday= 0, Sunday = 6
    archive_file=True
else:
    archive_file=False
if force_archive:
    archive_file=True

# define    
data_dir = 'c:/MyDocs/OpenFF/data/' 
sources = data_dir+'bulk_data/'
archive = data_dir+'archive/'
datefn= data_dir+'transformed/upload_dates.csv'
skyfn = 'sky_truth_final'
afile = archive+f'ff_archive_{today.strftime("%Y-%m-%d")}.zip'
currfn = 'testData'
lastfn = 'testData_last'
# outdir = './out/'
tempfilefn = './tmp/tempdownload.zip'

st = datetime.now() # start timer

def files_are_same():
    with open(tempfilefn,'rb') as f:
        newsig = sha256(f.read()).hexdigest()
    with open(sources+currfn+'.zip','rb') as f:
        lastsig = sha256(f.read()).hexdigest()
    return newsig==lastsig

def process_file():
    ## Now process file
    print('Working on data set')
    outdf = pd.read_csv(datefn)
    uklst = outdf.UploadKey.unique()
    
    t = bds.run_build(bulk_fn=currfn,mode='PRODUCTION',make_output_files=False,
                      startfile=0,endfile=None,do_abbrev=False,
                      data_source = 'bulk',  # ONLY run this with bulk data
                      construct_from_scratch=True,#inc_skyTruth=True,
                      do_end_tests=False)
    df = t.tables['chemrecs']
    
    
    #loc_clean.clean_location(t.tables['disclosures'])
    
    
    ndf = df[~df.UploadKey.isin(uklst)].copy() # just the new ones
    
    gb = ndf.groupby('UploadKey',as_index=False)['bgCAS'].count()
    gb['date_added'] = today.strftime("%Y-%m-%d")
    gb.rename({'bgCAS':'num_records'}, inplace=True,axis=1)
    
    outdf = pd.concat([outdf,gb],sort=True)
    outdf.to_csv(datefn,index=False)
    
    if len(gb)>0:
        from make_temp_repo import build_test_repo
        build_test_repo()
        
    if do_tripwire:
        detect_changes.runTripWire(currfn+'.zip',lastfn+'.zip')
        
    last_report = outdf[outdf.weekly_report.notna()].weekly_report.max()
    not_reported = outdf.weekly_report.isna().sum()
    
    print(f'\nNumber of events just added: {len(gb)}')
    print(f'  -- number since last report ({last_report}): {not_reported}')
    
# get and save files
if do_download:
    url = 'http://fracfocusdata.org/digitaldownload/fracfocuscsv.zip'
    print(f'Downloading data from {url}')
    r = requests.get(url, allow_redirects=True,timeout=20.0)
    open(tempfilefn, 'wb').write(r.content)
    
    nochange = files_are_same()
    
    if not nochange:
        if do_tripwire:
            detect_changes.backup_testData(infn=currfn+'.zip',
                                  outfn=lastfn+'.zip',
                                  sources=sources)
    
        open(sources+currfn+'.zip', 'wb').write(r.content)  # overwrites currfn file.
    if archive_file: open(afile, 'wb').write(r.content)
    
    if not nochange:
        process_file()
    else:
        print('\nNo changes detected between current and last download based on signature')
else:
    process_file()
    
endit = datetime.now()
print(f'\nWhole process completed in {endit-st}')

