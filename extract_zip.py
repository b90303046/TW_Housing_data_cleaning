import os
import zipfile 
import re 
from pprint import pprint
import sys
import os 
from pathlib import Path

"""
Simple direction:
Command in powershell : 'python extract_zip.py  loc1 loc2  num'
Extract the zip file in loc1 to loc2 with data type num
num = 0 :  quarterly data
num = 1 :  latest data (release every 10 days)
"""

if len(sys.argv) != 4:
    print("Not enough input, exit the program")
    sys.exit(1)


basic_loc = sys.argv[1]
basic_loc_folder = sys.argv[2]
basic_file_num = int(sys.argv[3])  # 可能為6 或 8
filetype =[6,8]
new_folder =  basic_loc_folder

print(new_folder)

try:
    os.mkdir(new_folder)
except Exception as EEE:
    #print(EEE)
    #print(type(EEE).__name__)
    print('The directory has existed')


zipfile_list = [ [jj[0:-4], jj] for jj in os.listdir(basic_loc) if re.search(r'\.zip',jj)]
zipfile_list = [jj for jj in zipfile_list if len(jj[0])== filetype[basic_file_num]]
 

for kk in zipfile_list:
     extract_file = basic_loc+ '\\'+kk[1]
     save_loc = new_folder+'\\' + kk[0]
     print(save_loc)
     with zipfile.ZipFile(extract_file,'r') as zzz:
             zzz.extractall(save_loc)       
