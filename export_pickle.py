# Using some functions in RETR to export csv files to pickle  
from RETR.collect_rawdata import *
from pathlib import Path
import pickle
import os
 

#####
# Set some variables
read_loc = 'D:\\House_test'
loc_save = 'D:\\check'
key_id = 0  #  0:成屋, 1:新成屋
folder_id = ['Quarterly','sep_data']  #or folder_id = ['sep_data']
#####


set_loc = [Path(read_loc) /ii for ii in folder_id]
save_pkl(key_id,set_loc, loc_save)

 

 
