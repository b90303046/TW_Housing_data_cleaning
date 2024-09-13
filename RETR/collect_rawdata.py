#%%
import pandas as pd 
import numpy as numpy
import re
from pathlib import Path
import os
import datetime as dt
import pickle 
"""
函數說明
1. city_code    : 列出各城市與代碼列表 
2. list_loc     : 列出資料夾(Quarterly_Data or sep_data)中的所有實價登錄的"上傳期間"資料夾
3. merge_csv    : 將"上傳期間"資料夾內符合條件的 csv檔案進行整併
4. collect_data : 合併所有資料夾內符合條件的CSV檔案,  並整理成Dict格式
5. select_col   : 依據不同的成交資料 (成屋/預售屋/租賃) 選取不同的欄位
6. save_file    : 給定位置與檔案名稱, 儲存整理的檔案
7. load_pickle  : 給定位置與檔案名稱, 匯出整理的檔案
"""




#file_saved_loc = Path(':\\Housing')  # 檔案存放位置
file_saved_loc = Path('D:\\Housing')

def city_code():
    """
    城市代碼, 確保位置內要有manifest.csv
    """
    man_loc  = file_saved_loc /'Data' /'manifest.csv'
    manifest = pd.read_csv(man_loc, encoding='utf8')
    pattern_sale ="(^.{3})" # take first three words
    manifest.loc[:,'description'] = manifest['description'].\
     str.extract(pattern_sale)
    manifest['name'] = manifest['name'].apply(lambda x: x[0])
    manifest = manifest.drop('schema', axis=1).drop_duplicates(ignore_index=True)
    manifest = dict(zip(manifest['name'], manifest['description']))
    return manifest


def list_loc(fname):
    """
    列出資料夾中的所有實價登錄的"上傳期間"資料夾
    """
    parent_file = file_saved_loc /fname
    all_loc = [ [jj, parent_file /jj] for jj in os.listdir(parent_file) if  not re.search('.csv',jj)]
    return  all_loc

 
def merge_csv(csv_key_id, loc0):
    """
    進入子資料夾loc0後, 讀取所有該資料夾內符合條件的的csv檔案
    """
    csv_file = [jj for  jj in os.listdir(loc0[1]) if re.search(csv_key_id,jj)]
    DF = pd.DataFrame() 
    if len(csv_file)==0:
        state1 = '{loc}資料夾中沒資料, 跳過...'.format(loc=loc0[0])
    else:
        for cc in csv_file:
            csv_loc = loc0[1] /cc  # csv位置
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
                print(textt)
                print(type(ee).__name__)
                print(ee)
                continue
        state1 = '進入{a}資料夾:共有{b}個csv檔案; {c}筆資料'.format(a=loc0[0], 
                                                   b= len(csv_file), c=DF.shape[0])
    print(state1)
    return DF


def collect_data(csv_key_id, fname):
    """
    1.合併所有資料夾內符合條件的csv檔案
    2.完成合併後, 轉為Dict格式
    """
    Temp_Dict = {}
    subfolders = list_loc(fname)
    for ss in subfolders:     
          DF0 = merge_csv(csv_key_id, ss)
          Temp_Dict[ss[0]] = DF0
    return Temp_Dict


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


def save_file(DF_file, foldername,file_key, folder_key,file_name=""):
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
    file0 = file_name + filetype + save_time + r'.pickle' #儲存格是
    filename = Path(foldername) /file0
    with open(filename, 'wb') as ff:
        pickle.dump(DF_file, ff,protocol=pickle.HIGHEST_PROTOCOL)
    print('檔案於{a}儲存於{b}, 檔案名稱為~~{c}~~'.format(a=save_time, b=foldername, c=file0))
    

def load_pickle(dir, key_id):
    DF = pd.DataFrame()
    key_file = {0:'estate', 1:'presale', 2:'rent'}
    pickle_files = [jj for jj in os.listdir(dir) if re.search(key_file[key_id],jj)]
    #print(pickle_files)
    if isinstance(pickle_files, str):
        pickle_files = [pickle_files] 
    if not pickle_files ==[]: 
        for jj in pickle_files: #可針對不同的pickle進行清理
            file_dir = dir+'/'+ jj
            with open(file_dir, 'rb') as ff:
                result = pickle.load(ff)
                DF = pd.concat([result, DF], axis=0, join='outer')
    else:
        result = 'The file empty!'
        print(result)
        DF = None
    return DF
 