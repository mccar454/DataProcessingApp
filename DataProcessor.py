# -*- coding: utf-8 -*-
"""
Created on Tue Dec 28 14:54:50 2021

@author: Benjamin McCarthy
"""
#This script was created to handle PFAS Lab data. This script is intended to showcase programming tools used, all 
#Identifying information has been removed where applicable.


#First, we need to import the packages that will be used in this script

import pandas,os, tkinter as tk, tkinter.simpledialog
from tkinter import filedialog
# from fuzzywuzzy import process
# from fuzzywuzzy import fuzz

#%% 
''' This will be the Working directory for the script. Any time it needs to be updated, just drop the 
Files in the Analytical folder and the script will automaticall finbd what it's looking for '''
root = tk.Tk()
root.withdraw()

file_path = filedialog.askdirectory()
Month = tk.simpledialog.askstring("Month", 'Please enter full month name')
Year = tk.simpledialog.askstring("year", 'Please enter full Year')

SaveName = r'Save Location'+'\ProcessedSamplingEvent'+str(Year)+str(Month)+'.xlsx'
print(file_path)



#%% Here we need to pull the sample name and date information, store in subfolders. the Files themselves have 
# and extension that ends in "LabSMP.txt" and we can use that to combine all similar files
LastFile = True
for root, dirs, files in os.walk(file_path):
    for file in files:
        if(file.endswith("LabSMP.txt")):
            dfSampleDate = pandas.read_csv(os.path.join(root,file),sep='\t')              
            dfSampleDate.columns = dfSampleDate.columns.str.replace('#','')
            print(os.path.join(root,file))
        
 #we can do the same with the actual results data, and once combined we will be able to say when these were 
#sampled, which allows us to work more smoothly with the data. In this case, we're looking for "LabRES.txt"

        if(file.endswith("LabRES.txt")):
            dfResult = pandas.read_csv(os.path.join(root,file),sep='\t')
            dfResult.columns = dfResult.columns.str.replace('#','')
            print(os.path.join(root,file))
    if any("RES" in file for file in files) and any("SMP" in file for file in files):
        if LastFile == True:
            dfMerge = pandas.merge(dfResult,dfSampleDate,how='left', on ='sys_sample_code')
            LastFile = False
        else:
            dfNew = pandas.merge(dfResult,dfSampleDate,how='left', on ='sys_sample_code')
            dfMerge = dfMerge.append(dfNew)
            
        print('MERGED!')               
#read in the analytes and match them to lab results
dfAnalyte = pandas.read_csv(r'List of Analytes.csv',sep='\t')  
dfResult = pandas.merge(dfMerge,dfAnalyte,on ='cas_rn')
print('Analytes Sorted!')


S = ['Desired well names']


#Once we have our results and in the order that we would like them to be, we need to organize the data. 
dfResult['SAMPLE_DATE'] = pandas.to_datetime(dfResult['SAMPLE_DATE']).dt.date

#Now we split our data into separate tables, which can be stored as individual sheets. 
dfGW = dfResult.copy()
keepcols = ['Well Names to Keep']
droplist = [row for row in dfGW.sys_sample_code  if row not in keepcols]
dfGW = dfGW[~dfGW['sys_sample_code'].isin(droplist)]

#Surface Water Table
dfSW = dfResult.copy()
keepcols = ['Surface points to keep']
droplist = [row for row in dfSW.sys_sample_code  if row not in keepcols]
dfSW = dfSW[~dfSW['sys_sample_code'].isin(droplist)]

#Storm Water Table
dfStorm = dfResult.copy()
keepcols = ['Storm points to keep']
droplist = [row for row in dfStorm.sys_sample_code  if row not in keepcols]
dfStorm = dfStorm[~dfStorm['sys_sample_code'].isin(droplist)]

dfUnassigned = dfResult.copy()
dfUnassigned = dfUnassigned[~dfUnassigned['sys_sample_code'].isin(S)]


print('Initializing Formatting Function')
def VISTA_Format(df):
    #for the sake of comPatibility in functions, we will change around some column dtypes 
    df['detect_flag'] = df['detect_flag'].astype(str)
    df['lab_qualifiers'] = df['lab_qualifiers'].fillna('')
    df['lab_qualifiers'] = df['lab_qualifiers'].astype(str)
    df['quantatation_limit'] = df['quantatation_limit'].astype(str)
    df['result_value'] = df['result_value'].astype(str)

    #We want any df['Result'] cell that has a Y for detection needs the result plus the lab qualifier, and detection limit plus U if not
    df['Result'] = df.apply(lambda x: x['result_value']+str(' ')+x['lab_qualifiers'] if x['detect_flag'] == "Y" else x['quantatation_limit']+str(' ')+x['lab_qualifiers'], axis=1)


    #Our last step is to pivot the whole table, we need the analytes as the indexes and the wells/dates on the columns
    test = df.pivot_table(index={'Sorting','chemical_name','cas_rn'},columns=['sys_sample_code','SAMPLE_DATE'],values=['Result'],aggfunc ='first')

    #lets try to sort the wells so that similar names are together alphabetically
    test = test.reset_index()
    test = test. sort_values(by=['Sorting'],ignore_index=True)
    #test = test.drop(['Sorting'],axis=1)
    
    return test 
#Run the Function on all of our separate tables
dfProcessed = VISTA_Format(dfResult)

#Now we make a quick sample summary that will be saved out with our formatted data
dfSummary = dfResult.copy()
keepcols = ['sys_sample_code','PARENT_SAMPLE_CODE','SAMPLE_DELIVERY_GROUP','lab_anl_method_name', 'analysis_date','analysis_time','SAMPLE_DATE', 'SAMPLE_TIME','SAMPLE_RECEIPT_DATE']
droplist = [col for col in dfSummary.columns if col not in keepcols]
dfSummary = dfSummary.drop(droplist,axis=1).groupby(['sys_sample_code','SAMPLE_DATE']).first().reset_index()
dfSummary['SAMPLE_DATE'] = pandas.to_datetime(dfSummary['SAMPLE_DATE']).dt.date
dfSummary['analysis_date'] = pandas.to_datetime(dfSummary['analysis_date']).dt.date

dfSummary = dfSummary[['sys_sample_code','PARENT_SAMPLE_CODE','SAMPLE_DELIVERY_GROUP','lab_anl_method_name','SAMPLE_DATE', 'SAMPLE_TIME','SAMPLE_RECEIPT_DATE', 'analysis_date','analysis_time']]

dfSummary['Sample_to_Analysis_Days'] = (dfSummary['analysis_date'] - dfSummary['SAMPLE_DATE']).dt.days

if dfGW.empty == True:
    print('no GW results match')
else:
    dfGW = VISTA_Format(dfGW)

if dfSW.empty == True:
    print('no SW results match')
else:
    dfSW = VISTA_Format(dfSW)
    
if dfStorm.empty == True:
    print('no Storm results match')
else:
    dfStorm = VISTA_Format(dfStorm)

if dfUnassigned.empty == True:
    print('no Unassigned results match')
else:
    dfUnassigned = VISTA_Format(dfUnassigned)

dfs = {'GW Results': dfGW, 'SW Results': dfSW, 'Storm Results': dfStorm, 'UNASSIGNED RESULTS': dfUnassigned,
       'Sample Summary': dfSummary}

writer = pandas.ExcelWriter(SaveName, engine='xlsxwriter')
for sheetname, df in dfs.items():  # loop through `dict` of dataframes
    df.to_excel(writer, sheet_name=sheetname)  # send df to writer
    worksheet = writer.sheets[sheetname]  # pull worksheet object
    for idx, col in enumerate(df):  # loop through all columns
        series = df[col]
        if isinstance(series.name, tuple):
            max_len = max(series.astype(str).map(len).max(),(len(series.name[1]))) + 1
        else:
            max_len = max(series.astype(str).map(len).max(),(len(series.name))) + 1   
        worksheet.set_column(idx, idx, max_len)  # set column width
writer.save()
writer.close()
print(str(SaveName)+' saved to folder')
input('press any key to exit')
