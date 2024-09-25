import pandas as pd 
import datetime as dt
import re


"""
Function list:
1. extract_num :  取出欄位內的所有數字(\d)
2. classify_string_date: 將日期格式進行分類
3. convert_date: 將 '交易年月日' 欄位轉為dt.datetime格式
4. convert_build_date:  將 "建築完成年月" 轉成dt.datetime格式
5. to_days : 計算屋齡專用 (交易年月日 - 建築完成日期)
6. classify_build_date:  將 "建築完成年月" 的成可讀取的'字串日期'格式
7. set_numeric_col: 將部分欄位轉為 numeric
####　選用部分　＃＃＃＃
8. set_bool_col : 將部分欄位轉為 boolean格式

"""


categorical_col =['鄉鎮市區','交易標的','data','城市']


# def convert_categorical(df, cols= categorical_col):
#     df1 = df.copy()
#     for jj in cols:
#         df1[jj].fillna('None',inplace=True)
#         df1[jj] = df1[jj].astype('category')
#     return df1

 
"""
分析交易與建築完成日期函式
"""

def extract_num(x):
    """
    移除非數字字串
    """
    pattern = r'^(\d+)'
    y = re.search(pattern, str(x))
    try:
        z = y.group() # 取出數字部分
    except:
        z = None  # 裡面有一些很怪的東西
    return z


def classify_string_date(x):
    """
     將(交易年月日/建築完成年月)的字串轉成可讀取的'字串日期'格式
    """
    x1 = extract_num(x)
    if x1 is not None:
        len_y = len(x1)
        if len_y ==5:
            year,mon = x1[0:3],x1[3:5]
            dd ='01'
        else:
            year,mon,dd=x1[0:3],x1[3:5],x1[5:]
            year = str(int(year)+1911)
        y = '-'.join([year,mon,dd])
        return y


def convert_date(df):
    """
    將日期欄位轉為日期字串格式後, 在進一步轉成 dt格式
    """
    df1 = df.copy()
    col = '交易年月日'
    df1[col] = df1[col].apply(classify_string_date)
    df1[col] = pd.to_datetime(df1[col], format='%Y-%m-%d',
                              errors='coerce')
    start_date = dt.datetime(2010,12,31)
    bool1 = df1[col] > start_date
    df1 = df1.loc[bool1,:]
    return df1
    

def to_days(td):
    """
    將 timedelta 格式資料轉為日
    """
    return td.days


###############################################################
################   建築日期         ################
# def check_datenum(df,lenn=7,col='建築完成年月'):
#     """
#     檢查原始 (交易完成日期/建築完成日期)格式
#     正常: 字串長度 =7
#     異常: 5 : yyy, mm
#     異常: 6 : yy, mm, dd, 或是?? 可以檢查一下
#     其他: 格式不全, 直接賦予None
#     """
#     df1 = df.copy()
#     df1['lenn'] = df1[col].apply(lambda x:len(str(x))) # 計算長度
#     z = df1['lenn'].value_counts() # 根據長度進行統計
#     bool_lenn = df1['lenn'] == lenn
#     y = df1.loc[bool_lenn, col]
#     return y,z


def classify_build_date(x):
    """
     將建築完成年月的字串轉成可讀取的'字串日期'格式
    """
    x1 = extract_num(x)
    if x1 is not None:
        len_y = len(x1)
        if len_y ==5:
            year,mon = x1[0:3],x1[3:5]
            dd ='01'
        elif len_y == 6 or 7:
            year,mon,dd=x1[0:3],x1[3:5],x1[5:]
            year = str(int(year)+1911)
        else:
             year,mon,dd = '1911','01','01'
        y = '-'.join([year,mon,dd])
        return y

def convert_build_date(df):
    col = '建築完成年月'
    df1= df.copy()
    df1[col] = df1[col].apply(classify_build_date)
    df1[col] = pd.to_datetime(df1[col], errors='coerce')
    df1[col]  = df1[col].fillna(dt.datetime(1911,1,1))
    return df1


numeric_col = ['土地移轉總面積平方公尺','總價元','建物移轉總面積平方公尺',
               '單價元平方公尺','車位移轉總面積平方公尺',
               '車位移轉總面積平方公尺','車位總價元']



def set_numeric_col(df, cols=numeric_col):
    df1 = df.copy()
    for jj in cols:
        df1[jj] = pd.to_numeric(df1[jj], errors='coerce')
    return df1


########### 以下選用 #############


def set_bool_col(df, columns=['建物現況格局-隔間','有無管理組織','電梯'], 
                dictt={'有':True,'無':False}):
    """
    將有/無欄位的資料轉為 Boolean值
    ['建物現況格局-隔間','有無管理組織']
    """
    df1 = df.copy()
    for jj in columns:
         df1[jj].fillna('無', inplace=True)
         df1[jj] = df1[jj].map(dictt)
    return df1


def set_int_col(df, col=['建物現況格局-房','建物現況格局-廳',
                         '建物現況格局-衛']):
    df1 = df.copy()
    for jj in col:
        df1[jj] = df1[jj].astype(int)
    return df1


def convert_main_use(df):
    """
    將主要用途的內容進行初步分類
    """
    df1 = df.copy()
    df1['主要用途'].fillna('NIL', inplace=True)
    bool1 = df1['主要用途'].str.contains('住宅')
    bool2 = df1['主要用途'].str.contains('住[屋房家]')
    bool3 = df1['主要用途'].str.contains('其他')
    bool4 = df1['主要用途'].str.contains('[店廠辦]')
    bool5 = df1['主要用途'].str.contains('停車')
    bool6 = df1['主要用途'].str.contains('[農雞豬畜]')
    df1.loc[bool1,'主要用途'] = '住宅相關'
    df1.loc[bool2,'主要用途'] = '住房相關'
    df1.loc[bool3,'主要用途'] = '其他'
    df1.loc[bool4,'主要用途'] = '廠辦相關'
    df1.loc[bool5,'主要用途'] = '停車相關'
    df1.loc[bool6,'主要用途'] = '農業相關'
    return df1

    
def fill_null(df, col):
    df1 = df.copy()
    bool1 = df1[col].isnull()
    df1.loc[bool1, col] = 'NIL'
    return df1


def rm_col(df, columns = ['非都市土地使用分區','非都市土地使用編定']):
    """
    移除不需要的欄位
    """
    df1 = df.copy()
    for jj in columns:
         df1 = df1.drop(jj,axis=1)
    return df1

 