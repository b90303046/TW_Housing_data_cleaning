
from RETR.collect_rawdata import load_pickle
from RETR.col_clean import *
from RETR.data_filter import *
from pathlib import Path
from pprint import pprint


# # 我應該寫一個 decorator來寫篩選後的結果


def clean_estate(df, qq, spp = ['親友','地上','特殊']):
    """
    盡量依照信義房價指數篩選
    """
    df_temp = add_timef(df, tfreq=qq)
    df_temp = time_range(df_temp, start=(2014,7,1), end=(2024,1,1))
    df_temp = filter_obj(df_temp, column='交易標的',keywords= ['房地'])
    df_temp = filter_obj(df_temp, column='建物型態',
                            keywords=['住宅大樓','透天厝','華廈','公寓','套房','農舍'])    
    df_temp = filter_obj(df_temp, column='主要用途', keywords=['住','其他']) #['住','其他']
    print(df_temp.info())
    # df_temp = df_temp[df_temp['屋齡'] > 1 ]  #移除新成屋
    # df_temp = compute_area_p(df_temp)
    # bool_area =  df_temp['單價元平方公尺']>0
    # df_temp = df_temp[bool_area]
    # df_temp = filter_special(df_temp, special=spp)
    #df_temp['Region'] = df_temp['城市'].apply(classify_region)
    return df_temp



load_loc =  r'D:\\check' # Locations with pickle files
key_num =0  # 0: estate, 1: presale
House_data = load_pickle(load_loc, key_id= key_num)


  

tfreq='Q'
time_freq = '交易-'+tfreq
df0 = House_data.copy() 
df0 = clean_estate(df0, tfreq, spp=['親友','地上','特殊','預售'])

# print(df0.info()) 