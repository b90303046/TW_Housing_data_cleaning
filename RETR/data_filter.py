#%%

import numpy as np
from pprint import pprint
import pandas as pd
import datetime as dt
#import xlwings as xw


#%%
"""
設定時間, 區域排序, 計算面積
Function list
1. add_timef
2. time_range
3. filter_obj
4. compute_area_p
5. filter_special
"""
end_year, end_month, end_day = (dt.datetime.now().year,
        dt.datetime.now().month, dt.datetime.now().day)



build_type =['透天厝', '華廈', '住宅大樓', '公寓','套房']
trade_type = '房地'


def add_timef(df,tfreq ='M', columns='交易年月日'):
    """
    將交易或建築完成日期改為月資料或季資料
    """
    df1 = df.copy()
    text = columns[0:2]
    year = df1[columns].apply(lambda x: str(x.year)[0:4])
    month = df1[columns].apply(lambda x: x.month)
    if tfreq.upper() =='M':
        colname= text + '-'+ tfreq.upper()
        df1[colname] = df1[columns].apply(lambda x : x.strftime(format='%Y-%m'))
    elif tfreq.upper() =='Y':
        colname= text + '-'+ tfreq.upper()
        df1[colname] = year #+ '-' + month.astype(str)
    elif tfreq.upper() =='Q':
        colname= text + '-'+ tfreq.upper()
        Qdata = ((month)-1)//3 +1 
        df1[colname] = year + 'Q' + Qdata.astype(str)
    else:
        pass  
    return df1



def time_range(df, start=(2011,1,1), end=(end_year, end_month, end_day), col='交易年月日'):
    """
    篩選交易日期
    """
    start_date =dt.datetime(start[0],start[1], start[2])
    end_date = dt.datetime(end[0], end[1], end[2])
    df1 = df.copy()
    bool1 = df1[col] >= start_date
    bool2 = df1[col] < end_date
    df1 = df1[bool1&bool2]
    return df1


def filter_info(func):
    def inner_fun(*args,**kwarg):
        row0 = args[0].shape[0]  # 期初篩選資料筆數
        print('篩選欄位{a1}, 篩選條件為{a2}'.format(a1 = kwarg['column'],
                                           a2 =kwarg['keywords']))
        result = func(*args, **kwarg)
        row1 = result.shape[0] # After filter
        state2 = '共刪除{ans}筆'.format(ans=row0-row1)
        print(state2)
        return result
    return inner_fun


@filter_info
def filter_obj(df, column,  keywords, negate=False): 
    """
    篩選欄位內特定關鍵詞:
    """
    df1 = df.copy()
    if isinstance(keywords,str): # 如果輸入字串
        keywords = [keywords]
    key_cols = '|'.join(keywords)
    if not negate:
        bool_mask = df1[column].str.contains(key_cols, na=False)
    else:
        bool_mask = ~df1[column].str.contains(key_cols, na=False)
    df1 = df1[bool_mask]
    #print(df1[column].value_counts()) 檢驗篩選結果
    print('過濾"{item}"交易後剩餘{row}筆資料'.format(item=column,
                                            row=df1.shape[0]))
    return(df1)



ping = 3.30579

def compute_area_p(df):
     """
     計算每坪價格, 以及每單位平方公尺價格, 並移除原資料單價平方公尺=0 等異常資料
     """
     df1 = df.copy()
     park_bool = (df1['車位總價元'] == 0) | (df1['建物移轉總面積平方公尺'] == 0)
     df1['總價萬'] = np.where(park_bool,df1['總價元'], df1['總價元']-df1['車位總價元'])/10000
     df1['面積m2'] = np.where(park_bool, df1['建物移轉總面積平方公尺'],  df1['建物移轉總面積平方公尺']-df1['車位移轉總面積平方公尺'])
     price_bool  = (df1['總價萬'] >0 ) & (df1['面積m2']>0)
     df1 = df1[price_bool]
     df1['面積坪'] =df1['面積m2']/ping
     df1['每坪單價(萬)'] =df1['總價萬']/df1['面積坪'] 
     print('調整車位價格與車位面積計算建物平均價格,並刪除單價為0之異常值, 剩餘{}筆'.format(df1.shape[0]))
     df1 = df1.drop(['面積m2'], axis=1)
     return df1


def filter_special(df, special=['親友','地上權','員工','特殊'], negate=True):
    """
    移除特殊交易案件
    """
    df1 = df.copy()
    #remove_tranaction=['親友','員工','地上權','特殊']
    #remove_tranaction=['地上權','特殊']
    pat2 = '|'.join(special)
    df1.fillna({'備註':'NA'}, inplace=True) 
    if negate:
        case_bool2 =  ~df1['備註'].str.contains(pat2, na=False)
    else:
        case_bool2 =  df1['備註'].str.contains(pat2, na=False)
     # 補上null value 值
    df2 = df1[case_bool2]
    state = '排除親友,員工或地上權之案件,剩餘{}筆'.format(df2.shape[0])
    print(state)
    return df2

 


def combine_region(x):
     """
     將八大都會區以外的地區集合成一項
     """
     city_set = ['苗栗縣','彰化縣','基隆市','雲林縣',
                 '屏東縣','花蓮縣','南投縣','嘉義市',
                 '臺東縣','嘉義縣','宜蘭縣','金門縣','澎湖縣','連江縣']
     other_city_set = set(city_set)
     if x  in other_city_set:
          y = '其他'
     else:
          y = x
     return y





def remove_tail_case(df,group_set,col,quant):
    """
    移除col欄位之下分位quant 以上及以下的分位資料,  
    groupby 時間(tfreq) 城市
    如果要根據時間篩選, 請先執行 add_timef
    example group_set: ['交易-Q','城市']
    """
    df1 = df.copy()
    fgroup = df1.groupby(group_set, observed=False)
    result = fgroup.apply(lambda x : 
      x[ (x[col] < x[col].quantile(1-quant))  & \
         (x[col] > x[col].quantile(quant))]).drop(group_set, axis=1).reset_index()
    #result = result.drop('level_2', axis=1) 
    return result


def compute_transaction(df, group_col):
    '''
    計算交易量
    group_col : 城市/ 建物類型, 自行unstack
    '''
    df1 = df.copy()
    city_trans = df1.groupby(group_col, observed=False).size()
    return city_trans 



def cut_transaction(df,price_type, group_set, cut_range):
     """
     根據df的結果, 
     在price_type 的 cut_range的區間設定下, 在group_set 進行分隔
     也可用來處理屋齡
     """
     df1 = df.copy()
     df1['區間'] =pd.cut(df1[price_type], bins= cut_range, right=False)
     Result = df1.groupby(group_set, observed=False)['區間'].value_counts()
     return Result


"""
計算價格部分
"""

def group_mean_p(df, price_type, group_set , col=None, quant=0):
    """
    刪除給定的極端分位數後計算各地區總價(每坪單價)的中位數或平均數
    home_p: 每坪單價, 總價元
    col   : 排除極端值的欄位選擇
    tfreq : 期間分群
    quant : 排除樣本極端值比例
    記得要先執行 add_timef
    """
    df1 = df.copy()
    if col is not None:
        df1 = remove_tail_case(df1,group_set,col,quant)
    else:  
        pass  
    result_mean = df1.groupby(group_set, observed=False)[price_type].mean().apply(lambda x : round(x/10000,2))
    return result_mean


def group_quant_p(df, price_type,group_set,quantp):
    """
    計算不同群組之下的分量價格
    group_col: 分群類別, output_col: 計算哪一個變數的分量(總價, 單價,)
    是否要unstack自行斟酌
    """
    df1 = df.copy()
    if isinstance(group_set, str):
        group_set = [group_set]
    df1 = df1.groupby(group_set, observed=False)[price_type].quantile(quantp)/10000
    return df1 


def set_city_order(city_list):
    """
    根據df0 設定
    """ 
    city_set = set(city_list)
    city_order_8= ['臺北市','新北市','桃園市','臺中市',
    '臺南市','高雄市','新竹市','新竹縣']

    other_city_set = ['苗栗縣','彰化縣','基隆市','雲林縣',
                       '屏東縣','花蓮縣','南投縣','嘉義市',
                       '臺東縣','嘉義縣','宜蘭縣','金門縣']
    if bool(city_set &  set(other_city_set)):
        city_new_list = ['全國'] + city_order_8 + other_city_set
    else:
        city_new_list = ['全國'] + city_order_8 + ['其他']
    return city_new_list
    
    



def table1_expand(df_table):
    """
    將先前計算的table1 轉為Dict
    """
    Temp_dict ={}
    time_col = df_table.columns[0]
    for ii in df_table.columns[2:]:
        df_new = df_table[[time_col,'城市',ii]]
        df_temp = pd.pivot(df_new, index= time_col, columns = '城市', values=ii)
        new_order = set_city_order(df_table['城市'].unique())
        Temp_dict[ii] = df_temp[new_order]
    return Temp_dict


def classify_region(city):
    """
    設定高價住宅區域認定規則
    """
    city_set ={'桃園市','臺中市','臺南市','高雄市','新竹市','新竹縣'}
    #city_set2 ={'苗栗縣','彰化縣','基隆市','雲林縣','屏東縣','花蓮縣',
    #            '南投縣','嘉義市','臺東縣','嘉義縣','宜蘭縣','金門縣'}
    if city =='臺北市':
        region = 'R1'
    elif city== '新北市':
        region = 'R2'
    elif city in city_set:
        region ='R3'
    else:
        region = 'R4'
    return region


# def export_dict_excel(Dict_file, data_loc, data_name):
#     """
#     將Dict內的dataframe轉至excel, 內部檔案為dataframe
#     """
#     file1 = xw.Book()
#     for jj in Dict_file.keys():
#         file1.sheets.add(jj)
#         sheet =  file1.sheets(jj)
#         sheet.range('A1').value = Dict_file[jj]
#         file_name = data_loc + '\\' + data_name  + '.xlsx'
#         file1.save(file_name)
 
# def summarize_p(df,home_p,tfreq,col,quant=0.00):
#     """
#     刪除給定的極端分位數後計算各地區總價(每坪單價)的中位數或平均數
#     home_p: 每坪單價, 總價元
#     col   : 排除極端值的欄位選擇
#     tfreq : 期間分群
#     quant : 排除樣本極端值比例
#     記得要先執行 add_timef
#     """
#     df1 = df.copy()
#     gpname1 = '交易-'+tfreq
#     group_set =[gpname1,'城市'] # 設定集合項目
#     result = reomve_extreme_case(df1,tfreq,col,quant)
#     result_mean = result.groupby(group_set, observed=False)[home_p].mean().unstack('城市').round(2)
#     result_median = result.groupby(group_set, observed=False)[home_p].quantile(0.5).unstack('城市').round(2)
#     result_mean   = result_mean[city_order]
#     result_median = result_median[city_order]
#     total_result_mean = result.groupby(group_set[0], observed=False)[home_p].mean().round(2)
#     total_result_mean.name ='全國'
#     total_result_median = result.groupby(group_set[0], observed=False)[home_p].quantile(0.5).round(2)
#     total_result_median.name='全國'
#     result_dict={}
#     result1 = pd.merge(left=total_result_mean, right = result_mean, how='left',
#                         left_index=True, right_index=True)
#     result2 = pd.merge(left=total_result_median, right = result_median, how='left',
#                         left_index=True, right_index=True)
#     result_dict[home_p+'_mean'] = result1.apply(lambda x:  x/10000)
#     result_dict[home_p+'_median'] = result2.apply(lambda x:  x/10000)
#     return result_dict
 



# def group_quant_p(df, price_type, tfreq, col, quan_value):
#         """
#     刪除給定的極端分位數後計算各地區總價(每坪單價)的中位數或平均數
#     home_p: 每坪單價, 總價元
#     col   : 排除極端值的欄位選擇
#     tfreq : 期間分群
#     quant : 排除樣本極端值比例
#     記得要先執行 add_timef
#     """
#     df1 = df.copy
#     pass 
 
 
 
# Export the file to excel

#wb.close()    

# import xlwings as xw
# # export to excel?
# try:
#     wb = xw.Book()
#     wb.save(excel_file_name)
# except:
#     wb = xw.Book(excel_file_name)


# for jj in Result_dict.keys():
#     try:
#         wb.sheets.add(jj)
#     except:
#         pass

#     sheet = wb.sheets[jj]
#     sheet.range('A1').value = Result_dict[jj]


# del_sheet = [jj for jj in wb.sheet_names if re.search('工作', jj)]

# for jj in del_sheet:
#     try:
#         wb.sheets[jj].delete()
#     except:
#         pass

# wb.save(excel_file_name) 