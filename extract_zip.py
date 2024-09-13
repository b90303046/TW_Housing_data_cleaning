import os
import zipfile 
import re 
from pprint import pprint
import sys
import os 
from pathlib import Path


"""
輸入邏輯: 我要將D:\Housing 中的新(舊)檔案存到裡面的資料夾 sep_data中
python extract_zip.py 'D:\Housing' 'sep_data' 1
"""

if len(sys.argv) != 4:
    print("輸入變數不足, 離開程式...")
    sys.exit(1)


basic_loc = sys.argv[1]
basic_loc_folder = sys.argv[2]
basic_file_num = int(sys.argv[3])  # 可能為6 或 8
filetype =[6,8]
new_folder = basic_loc  + basic_loc_folder

print(new_folder)

try:
    os.mkdir(new_folder)
except Exception as EEE:
    print(EEE)
    print(type(EEE).__name__)
    print('資料夾已存在')


zipfile_list = [ [jj[0:-4], jj] for jj in os.listdir(basic_loc) if re.search(r'\.zip',jj)]
zipfile_list = [jj for jj in zipfile_list if len(jj[0])== filetype[basic_file_num]]
 

for kk in zipfile_list:
    extract_file = basic_loc+ '\\'+kk[1]
    save_loc = new_folder + kk[0]
    pprint(extract_file) 
    with zipfile.ZipFile(extract_file,'r') as zzz:
           zzz.extractall(save_loc)       