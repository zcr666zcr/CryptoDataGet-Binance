import pandas as pd
import os
from pathlib import Path
from tqdm import tqdm
class BanYun():
    def __init__(self):
        self.path ={

            '分钟频率数据': [  Path(rf'E:\Quant\data\现货行情数据1min') ],

            '小时频率数据': [
                             Path(rf'E:\Quant\data\现货行情数据1h')],

            '日频频率数据': [
                             Path(rf'E:\Quant\data\现货行情数据1d')],

        }

    def tran(self,pinlv):

        old_path = self.path[pinlv][0]
        new_path = self.path[pinlv][1]
        file_list = os.listdir(old_path)
        for f in tqdm(file_list):
            df  = pd.read_csv(old_path/f)
            df.to_feather(new_path/f.replace(rf'.csv','.feather'))


if __name__=='__main__':
    t = BanYun()
    t.tran('分钟频率数据')



