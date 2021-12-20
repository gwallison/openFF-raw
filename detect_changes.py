# -*- coding: utf-8 -*-
"""

Created on Thu May 21 09:55:29 2020
@author: Gary

The aim of these functions is to detect changes to existing disclosures
and to document that they have been silently changed.  This is a scaled-down
version of trip_wire and doesn't keep track of what details are changed. (I've found
that I rarely use that function and even when I do, it is complicated and confusing).
Instead, a list of disclosures with silent changes is kept (including when a
disclosure is dropped from FF).  


"""

###############################  Used to make build accessible ####################
import sys
sys.path.insert(0,'c:/MyDocs/OpenFF/src/openFF-build')
###################################################################################

import pandas as pd
#import Find_silent_change as fsc
import core.Bulk_data_reader as rff
import shutil
import datetime

now = datetime.datetime.now()
today = datetime.datetime.today()

data_dir = 'c:/MyDocs/OpenFF/data/'
transdir = data_dir+'/transformed/'
tempdir = './tmp/'
sources = data_dir+'bulk_data/'
tw_fn = transdir+'tripwire_summary.csv'



metacols = ['APINumber','UploadKey','Latitude','TotalBaseWaterVolume',
            'CountyName','CountyNumber','FederalWell','IndianWell',
            'JobEndDate','JobStartDate','Latitude','Longitude',
            'OperatorName','StateName','StateNumber','TVD',
            'TotalBaseNonWaterVolume','WellName']

def backup_testData(infn='testData.zip', outfn='testData_last.zip',
                    sources=sources):
    shutil.copyfile(sources+infn,sources+outfn) 



def getDfForCompare_basic(fn,sources=sources):
    fn = sources+fn
    raw_df = rff.Read_FF(zname=fn,sources=data_dir,
                         outdir=tempdir).import_raw_as_str() # (na_filter=False)
    return raw_df

def getNormalizedStrLst(df,withhash=False):
    work = df.sort_values(['IngredientKey'])
    work = work.reset_index(drop=True)
    if withhash:
        work['rhash'] = pd.util.hash_pandas_object(work).astype('int64')
    str_tmp = work.to_csv().encode('utf-8')
    return str_tmp.splitlines(keepends=True)
 
def compareFrameAsStrings(df1,df2):
    lst1 = getNormalizedStrLst(df1,withhash=True)
    lst2 = getNormalizedStrLst(df2,withhash=True)
    if len(lst1)!=len(lst2):
        return True
    for i in range(len(lst1)):
        if lst1[i]!=lst2[i]:
            return True
    return False
    
def showDifference(uploadlst,olddf, df):
    outstr = ''
    cols_affected = set()
    for uk in uploadlst:
        if compareFrameAsStrings(olddf[olddf.UploadKey==uk],
                                      df[df.UploadKey==uk]):
            outstr += f'  Differences in {olddf.APINumber.iloc[0]} :: {uk} \n'
            outstr += '---------------------------------------\n'

            conc = pd.merge(olddf[olddf.UploadKey==uk],df[df.UploadKey==uk],on='IngredientKey',how='outer',
                            indicator=True)
            cols = df.columns.tolist()
            cols.remove('IngredientKey')
            for col in cols:
                x = col+'_x'
                y = col+'_y'
                conc['comp'] = conc[x]==conc[y]
                if conc.comp.sum()<len(conc):
                    cols_affected.add(col)
                    if col in metacols:
                        outstr += f'{conc[~conc.comp][[x,y]].iloc[0]}\n'
                        #outstr += f'{col}, sum = {conc.comp.sum()}\n'
                        
                    else:                        
                        outstr += f'{conc[~conc.comp][[x,y]]}\n'
                        #outstr += f'{col}, sum = {conc.comp.sum()}\n'
                    outstr += '---------------------------------------\n'
    l = list(cols_affected)
    sumtxt = ''
    for c in l:
        sumtxt += c+'; '
    return outstr, sumtxt

def get_blank_record(cols,meta):
    rec = {}
    for m in meta:
        rec[m] = False
    for col in cols:
        if not col in meta:
            rec[col] = 0
    return rec

def compileBasicDifference(olddf,newdf,outfn='unknown',skipdiffoutput=False):

    logtxt = '\n*************  New Disclosures Detected **************\n'
    gbnew = newdf.groupby(['APINumber','UploadKey'],as_index=False)['CASNumber'].count().reset_index(drop=True)
#    gbold = olddf.groupby(['APINumber','UploadKey'],as_index=False)['CASNumber'].count().reset_index(drop=True)
    gbold = olddf.groupby(['APINumber','UploadKey'],as_index=False)[['OperatorName','JobEndDate']].first().reset_index(drop=True)
    gbnew = gbnew.drop('CASNumber',axis=1)
    #gbold = gbold.drop('CASNumber',axis=1)
    mg = pd.merge(gbold,gbnew,on=['APINumber','UploadKey'],
                  how='outer',indicator=True)


    outdf = pd.DataFrame({'APINumber':[],
                          'UploadKey':[],
                          'new_date':[],
                          'type_of_diff':[],
                          'fields_changed':[],
                          'OperatorName':[],
                          'orig_date':[]})

    ### Dropped disclosures        
    logtxt += '\n*************  Old Disclosures Dropped **************\n'
    print(' Searching for dropped disclosures')
    dropped = []
    #print(mg.columns)
    for row in mg[mg._merge=='left_only'].itertuples(index=False):
        rec = pd.DataFrame({'APINumber':row.APINumber,
                            'UploadKey':row.UploadKey,
                            'new_date':outfn, # date of the new archive
                            'type_of_diff':'disclosure removed',
                            'fields_changed':'',
                            'OperatorName':row.OperatorName,
                            'orig_date':row.JobEndDate},index=[0])
        outdf = pd.concat([outdf,rec],ignore_index=True,sort=True)
        # save the disclosures
        dropped.append((row.APINumber,row.UploadKey))
    

    ### Changed disclosures
    logtxt += '\n*************  Disclosures Changed **************\n'   
    print(' Searching for changed disclosures')
    cols = newdf.columns.tolist()
    cols.remove('IngredientKey')
    # now drop raw_filenum so it doesn't show in all shifted disclosures
    olddf = olddf.drop('raw_filename',axis=1)    
    newdf = newdf.drop('raw_filename',axis=1)    

    
    print('   -- merge old and new for finding differences')
    # remove records where API/upK are not in both
    olddf = pd.merge(olddf,mg[~(mg._merge=='left_only')][['APINumber','UploadKey']],
                              on=['APINumber','UploadKey'],
                     how='inner')
    newdf = pd.merge(newdf,mg[~(mg._merge=='right_only')][['APINumber','UploadKey']],
                              on=['APINumber','UploadKey'],
                     how='inner')

    conc = pd.merge(olddf,newdf,how='outer',indicator=True)
    apis = conc[conc._merge!='both'].APINumber.unique().tolist()
    print(f'\nTRIPWIRE: number affected APIs: {len(apis)}')
    if not skipdiffoutput:
        for api in apis:
            logtxt += 50*'*'+f'\n            << {api} >>\n'+50*'*'+'\n\n'
            t = conc[conc.APINumber==api].copy()
            #print(t.columns)
            upk = t.UploadKey.iloc[0]
            operator = t.OperatorName.iloc[0]
            orig_date = t.JobEndDate.iloc[0]
            #print(f'{api}, {upk}, {operator},{orig_date}')
            logtxt += f'Shared IngredientKeys: {len(t[t._merge=="both"])}\n'
            logtxt += f'             Old only: {len(t[t._merge=="left_only"])}\n'
            logtxt += f'             New only: {len(t[t._merge=="right_only"])}\n\n'
            #make the summary text, and don't compare raw_filename
            told = olddf[olddf.APINumber==api].copy()
            tnew = newdf[newdf.APINumber==api].copy()
            lst = told.UploadKey.unique().tolist()
            sumtxt,dftxt = showDifference(lst,told,tnew)
            logtxt += sumtxt
            rec = pd.DataFrame({'APINumber':api,
                                #'UploadKey':' -- ',
                                'UploadKey':upk,
                                'new_date':outfn, # date of the new archive
                                'type_of_diff':'changes within disclosure',
                                'fields_changed':dftxt,
                                'OperatorName':operator,
                                'orig_date':orig_date},index=[0])
            #print(rec)
            outdf = pd.concat([outdf,rec],ignore_index=True,sort=True)
    return outdf, logtxt
    
    
def runTripWire(newfn,oldfn,sources=sources,usedate='today'):
    print("Fetching raw string verison of today's data set for tripwire")
    df = getDfForCompare_basic(newfn,sources)
    print("Fetching raw string verison of previous data set for tripwire")
    olddf = getDfForCompare_basic(oldfn,sources)

    # logtxt is for human readable report
    if usedate == 'today':
        outfn = now.strftime("%Y-%m-%d")
    else:
        outfn = usedate
    logtxt = f'Tripline log created: {now}\n'
    logtxt += f'Input archives: older: {oldfn} (= x, left) \n'
    logtxt += f'Input archives: newer: {newfn} (= y, right)\n\n'
    
    #logtxt +=  'Records in each raw file:\n'
    oldcnts = olddf.value_counts(['raw_filename']).to_frame('counts').reset_index()
    newcnts = df.value_counts(['raw_filename']).to_frame('counts').reset_index()

    allcnts = pd.merge(oldcnts,newcnts,on='raw_filename',how='right').sort_values('raw_filename')
    logtxt += 'Raw input files that have different record numbers:\n'
    logtxt += f'{allcnts[allcnts.counts_x!=allcnts.counts_y].to_string()}\n\n'

    ### First look for any differences between old and new and record pointer
    outdf,comptxt = compileBasicDifference(olddf,df,outfn,skipdiffoutput=False)
    outdf = outdf.reset_index()
    outdf = outdf[['APINumber','UploadKey','new_date',
                   'type_of_diff','fields_changed',
                   'OperatorName','orig_date']]
    outdf.to_csv(tempdir+outfn+'.csv')
        
    with open(tempdir+outfn+'.txt','w') as f:
        f.write(logtxt+comptxt)
    
    # save to master file
    if len(outdf)>0:
        twsum = pd.read_csv(tw_fn,quotechar="$",encoding='utf-8',
                            dtype={'APINumber':'str',
                                   'new_date':'str'})
        pd.concat([twsum,outdf],sort=True).to_csv(tw_fn,quotechar='$',
                                                  encoding='utf-8',
                                                  index=False)

if __name__ == '__main__':
    runTripWire('testData.zip','testData_last.zip')
   