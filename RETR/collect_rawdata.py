#%%
import pandas as pd
import sys 
import numpy as numpy
import re
from pathlib import Path
import os
import datetime as dt
import pickle 
from pprint import pprint
from RETR.col_clean import *
from colorama import Fore, Style
"""
Code Contents:
1. city_code    : 列出各城市與代碼列表 
2. list_loc     : 列出資料夾(Quarterly_Data or sep_data)中的所有實價登錄的"上傳期間"資料夾
3. merge_csv    : 將"上傳期間"資料夾內符合條件的 csv檔案進行整併
4. collect_data : 合併所有資料夾內符合條件的CSV檔案,  並整理成Dict格式
5. select_col   : 依據不同的成交資料 (成屋/預售屋/租賃) 選取不同的欄位
6. save_file    : 給定位置與檔案名稱, 儲存整理的檔案
7. load_pickle  : 給定位置與檔案名稱, 匯出整理的檔案
"""

loc0 = Path(__file__).parent


def city_code():
     """
     There are city code in manifest.csv, 
     This function converts manifest to dict file
     """
     man_loc  = loc0 /'manifest.csv'
     manifest = pd.read_csv(man_loc, encoding='utf8')
     pattern_sale ="(^.{3})" # take first three words
     manifest.loc[:,'description'] = manifest['description'].str.extract(pattern_sale)
     manifest['name'] = manifest['name'].apply(lambda x: x[0])
     manifest = manifest.drop('schema', axis=1).drop_duplicates(ignore_index=True)
     manifest = dict(zip(manifest['name'], manifest['description']))
     return manifest


# List all directories in root1
def get_subfolder(loc):
    Subfolders_info = [ [Path(loc) /jj, jj] for jj in os.listdir(loc) if not os.path.isdir(jj)]
    return Subfolders_info

 
 
def merge_csv(csv_id, loc0):
     """
     Load all .csv files that in the element of Subfolders 
     csv_key = ['land_a.csv','land_b.csv','land_c.csv']
     0: _a.csv: 成屋 ; 1: _b.csv: 預售屋 ;  2: _c.csv
     """
     csv_key = ['land_a.csv','land_b.csv','land_c.csv']
     csv_file = [jj for  jj in os.listdir(loc0) if re.search(csv_key[csv_id],jj)]
     DF = pd.DataFrame() 

     err_list = [] # add  
     if len(csv_file)==0:
            state1 = '{cwd}資料夾中沒資料, 跳過...\n'.format(cwd=loc0)
            print(state1)
     else:
           for cc in csv_file:
              csv_loc = loc0 /cc  # csv位置
              city= city_code()[cc[0]]
              try:  # 檢查讀取csv檔是否有錯誤
                 df0 = pd.read_csv(csv_loc, index_col = None, 
                                     encoding='utf8', low_memory=False).iloc[1:,]
                 new_col = [jj.replace('(','').replace(')','') for jj in df0.columns]
                 df0.columns = new_col
                 df0['城市'] = city
                 DF = pd.concat([DF, df0], join='outer', ignore_index=True)
              except Exception as ee:
                 textt = '讀取資料檔{a}有誤,請檢察'.format(a = csv_loc)
                 #print(textt)
                 #print(type(ee).__name__)
                 error_loc =  str(loc0) +'\\'+ cc +':  Row '+ re.findall(r'line (\d+)',str(ee))[0]
                 err_list.append(error_loc)
                 print('\n')
                 continue
           state1 = '共有{b}個csv檔案; {c}筆資料\n'.format(b= len(csv_file), c=DF.shape[0])           
           print(state1)
     return DF, err_list
 

def select_col(file_key=0):
    if file_key==1:
        col  = ['data','城市','鄉鎮市區', '交易標的', 
                '土地位置建物門牌', '土地移轉總面積平方公尺', '都市土地使用分區',
                '交易年月日', '建物型態', '主要用途', '主要建材',
                '建物移轉總面積平方公尺', '總價元', '單價元平方公尺', '車位移轉總面積平方公尺', '車位總價元',
                '備註', '編號']
    elif file_key==2:
        col  = ['data','城市', '鄉鎮市區', '交易標的', '租賃年月日', 
                 '建物型態', '主要用途', '主要建材',
                '建築完成年月', '建物總面積平方公尺', '有無管理組織', '有無附傢俱', '總額元', 
                '單價元平方公尺', '車位類別', '車位面積平方公尺', '車位總額元', '備註',
                '編號', '出租型態', '有無管理員', '租賃期間', '有無電梯', '附屬設備', '租賃住宅服務']
    else:
        col  = ['data','城市', '鄉鎮市區', '交易標的', '土地移轉總面積平方公尺', '都市土地使用分區', 
                '交易年月日', '建物型態', '主要用途', '主要建材','建築完成年月', '建物移轉總面積平方公尺', 
                '建物現況格局-隔間', '總價元', '單價元平方公尺', '車位移轉總面積平方公尺', '車位總價元',
                '備註', '編號', '主建物面積']
    return col



def collect_data(csv_id, loc, start=0):
    """
    1.合併所有資料夾root1 內符合條件的csv檔案
    2.完成合併後, 轉為Dict格式
    """   
    csv_id = int(csv_id) # convert to integer
    csv_dict ={0:'成屋',1:'預售屋',2:'租賃'}
    print('執行{data_type}的資料清理合併...\n'.format(data_type = csv_dict[csv_id]))
    Total_DF = pd.DataFrame()
    err_total_list = []
    Subfolders_info = get_subfolder(loc)
    items = Subfolders_info[start:]
    if not isinstance(items,list):
         items= [items]    
    for ss in items: 
           print('進入{a}資料夾...'.format(a=ss[0]))  
           DF0, err_list  = merge_csv(csv_id, ss[0])
           DF0['data'] = ss[1]
           Total_DF = pd.concat([Total_DF, DF0], join='outer', axis=0, ignore_index=True)
           err_total_list.extend(err_list)

    if len(err_total_list) !=0:
         print(Style.BRIGHT +
                Fore.YELLOW +'\n There is something wrong in zip raw file(s):\n'+ 
                Style.RESET_ALL)
         pprint(err_total_list)

    col = select_col(csv_id)
    if csv_id != 2: # 成屋與預售屋
        Total_DF = Total_DF[col]
        Total_DF = convert_date(Total_DF) #交易年月日
        Total_DF = set_numeric_col(Total_DF)
    
    if csv_id ==0: # 成屋, 計算屋齡
        Total_DF = convert_build_date(Total_DF)
        Total_DF['屋齡'] = Total_DF['交易年月日'] - Total_DF['建築完成年月']
        Total_DF['屋齡'] = Total_DF['屋齡'].apply(lambda x: round(x.days/365,0)).astype(int)

    return Total_DF, err_total_list

 

def save_file(DF_file, foldername,file_key, folder_key):
    """
    將Dataframe 在foldername資料夾存成 pickle檔案, 命名為...
    foldername 為str
    """
    try:
        """
        如果沒有資料夾,直接創建一個, 如果已經有, 直接忽略
        """
        os.mkdir(foldername)
    except:
        pass
    file_dict   = {0:"estate",1:'presale',2:'rent'}
    folder_dict = {0:"old",1:"new"}  # 新資料與舊資料
    filetype =  file_dict[file_key]+'_'+ folder_dict[folder_key] +'_'
    save_time = dt.datetime.today().strftime('%Y%m') #儲存時間
    file0 =  filetype + save_time + r'.pickle' #儲存格式
    filename = Path(foldername) /file0
    with open(filename, 'wb') as ff:
        pickle.dump(DF_file, ff,protocol=pickle.HIGHEST_PROTOCOL)
    print('檔案於{a}儲存於{b}, 檔案名稱為~~{c}~~'.format(a=save_time, b=foldername, c=file0))
    

def save_pkl(csv_id , loc0, save_loc):
    """
    給定資料夾與csv_id (預售屋,成屋)與csv位置loc0, 
    將檔案存成pickle檔並放在save_loc內
    """
    if not isinstance(loc0, list):
        loc0 = [loc0]
    for ii,jj in enumerate(loc0):
        DF, _ = collect_data(csv_id, jj)
        save_file(DF, save_loc,csv_id, folder_key=ii)


           
def load_pickle(dir, key_id):
    DF = pd.DataFrame()
    key_file = {0:'estate', 1:'presale', 2:'rent'}
    pickle_files = [jj for jj in os.listdir(dir) if re.search(key_file[key_id],jj)]
    if isinstance(pickle_files, str):
        pickle_files = [pickle_files] 
    if not pickle_files ==[]: 
        for jj in pickle_files: 
            file_dir = dir+'/'+ jj
            with open(file_dir, 'rb') as ff:
                result = pickle.load(ff)
                DF = pd.concat([result, DF], axis=0, join='outer')
    else:
        result = 'The file empty!'
        print(result)
        DF = None
    return DF




if __name__ =='__main__':
    variables = sys.argv
    root1 = variables[1]
    key_id = int(variables[2]) # 0: 成屋, 1:預售屋, 2:租賃
    DF_file,B= collect_data(key_id,root1) # 讀取資料夾
 
