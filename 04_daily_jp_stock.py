#!/usr/bin/env python
# -*- coding: utf-8 -*-
#%matplotlib inline
import pandas_datareader.data as web
import numpy as np
#import statsmodels.api as sm
import matplotlib.pyplot as plt
import pandas as pd
import pandas.tseries as pdt
from datetime import date
import common
import datetime
import os,csv
import shutil
import common_profit as compf
#common.env_time()[1][0:10]
class profit:
    def __init__(self,num):
        #ROOT出力フォルダチェック
        if os.path.exists(compf.OUT_DIR) == False:
            os.mkdir(compf.OUT_DIR)
        S_DIR = os.path.join(compf.OUT_DIR,num) #C:\data\90_profit\06_output
        if os.path.exists(S_DIR) == False:
            os.mkdir(S_DIR)
        #作業フォルダチェック
        self.S_DIR = os.path.join(S_DIR,common.env_time()[0][:14])
        os.mkdir(str(self.S_DIR))
        #スクリプトコピー
        shutil.copy2(__file__, self.S_DIR)

    def interval(self,all,priod):
        a = all.resample(priod).first()
        return a
    def save_to_csv(self,save_name,title,backreport):
        #ヘッダー追加
        if os.path.exists(save_name) == False:
            dic_name = ",".join([str(k[0]).replace(",","")  for k in backreport.items()])+"\n"
            with open(save_name, 'w', encoding="cp932") as f:
                f.write("now,stockname,"+dic_name)
        #1列目からデータ挿入
        dic_val = ",".join([str(round(k[1],3)).replace(",","")  for k in backreport.items()])+"\n"
        with open(save_name, 'a', encoding="cp932") as f:
            f.write(common.env_time()[1] +"," + title+","+dic_val)

    def Monthly_last(self,code,df):
        try:
            df.columns =['O','H','L','C','V','C2','SS']
            df = compf.add_avg_rng(df,'C','L','H')
            y = df.dropna()
            y['O1']=(y.O / y.O.shift(1)-1) * 1000000
            y['O2']=(y.O / y.O.shift(2)-1) * 1000000
            y['O3']=(y.O / y.O.shift(3)-1) * 1000000
            y['O4']=(y.O / y.O.shift(4)-1) * 1000000
        except:
            print(code,"Monthly_lastエラー")
            return pd.DataFrame({})
        #レポート用
        N = len(y) #FXデータのサイズ
        LongPL = np.zeros(N) # 買いポジションの損益
        ShortPL = np.zeros(N) # 売りポジションの損益
        SumLong = np.zeros(N) # 売りポジションの損益
        SumShort = np.zeros(N) # 売りポジションの損益
        pl = np.zeros(N) # 売りポジションの損益

        for i in range(10,len(y)-1):
            SumLong[i]=SumLong[i-1]
            SumShort[i]=SumShort[i-1]
            LongPL[i] = 0
            if y.index[i].month != y.index[i+1].month:
                pl[i] = y.O4[i]
                if float(y.rng200[i-1]) > 0.9:
                    LongPL[i] = y.O4[i]
                if float(y.rng200[i-1]) < 0.3:
                    ShortPL[i] = y.O4[i]
            SumLong[i]=SumLong[i]+LongPL[i] #レポート用
            SumShort[i]=SumShort[i]+ShortPL[i] #レポート用

        y['sumlong'] = SumLong
        y['sumshort'] = SumShort
        y['pl'] = pl

        save_name = os.path.join(self.S_DIR, str(code)  + "_Monthly_last_detail.csv")
        y.to_csv(save_name)
        return pd.DataFrame({'LongPL':LongPL, 'ShortPL':ShortPL,'Sumlong':SumLong,'Sumshort':SumShort}, index=y.index)

    def main_exec2(self,file_csv):
        sqls = "select *,rowid from kabu_list"
        sql_pd = common.select_sql('B01_stock.sqlite', sqls)
        for i, row in sql_pd.iterrows():
            code = row['コード']
            if common.stock_req(code, 1) == 1: #売りフラグあり
                print(code)
                code_text = os.path.join(compf.CODE_DIR, str(code) + '.txt')
                if os.path.exists(code_text):
                    df = pd.DataFrame(index=pd.date_range('2007/01/01', common.env_time()[1][0:10]))
                    df = df.join(pd.read_csv(code_text,index_col=0, parse_dates=True, encoding="cp932", header=None))
                    df = df.dropna()
                    if len(df) > 1500:
                        if file_csv == '_Monthly_last.csv':
                            PL = self.Monthly_last(code,df)
                        if file_csv == '_vora_stg.csv':
                            PL = self.vora_stg(code,df)
#                        if file_csv == '_ATR_stg.csv':
#                            PL = self.ATR_stg(code,df)
                        if file_csv == '_day_stg.csv':
                            PL = self.day_stg(code,df)

                        if len(PL) > 0:
                            if row['市場'].count(","):
                                sp = row['市場'].split(",")
                                row['市場'] = sp[0]
                            title = str(row['コード']) + "_" + str(row['銘柄名']) + "_" + str(row['セクタ']) + "_" + str(row['市場']) + file_csv
                            Equity, backreport = compf.BacktestReport(PL, title,self.S_DIR,1.1,"フィルター除外") #"フィルター除外"

    def vora_stg(self,code,df):
        try:
            df.columns =['O','H','L','C','V','C2','SS']
            df = compf.add_avg_rng(df,'C','L','H')
            y = df.dropna()
            y['C1']=round((y.C / y.C.shift(1)-1),4)
        except:
            print(code,"voraエラー")
            return pd.DataFrame({})
        #レポート用
        N = len(y) #FXデータのサイズ
        LongPL = np.zeros(N) # 買いポジションの損益
        ShortPL = np.zeros(N) # 売りポジションの損益
        SumLong = np.zeros(N) # 売りポジションの損益
        SumShort = np.zeros(N)  # 売りポジションの損益
        pl = np.zeros(N)
        for i in range(10,len(y)-1):
            SumLong[i]=SumLong[i-1]
            SumShort[i]=SumShort[i-1]
            LongPL[i] = 0
            ShortPL[i] = 0
            if float(y.C1[i]) > 0.15:
                LongPL[i] = int((y.O[i+1] / y.O[i] -1) * 1000000)
                pl[i] = LongPL[i]
            if 0.15 > float(y.C1[i]) > 0.05:
                ShortPL[i] = int((y.O[i+1] / y.O[i] -1) * 1000000)
                pl[i] = LongPL[i]
            SumLong[i]=SumLong[i]+LongPL[i] #レポート用
            SumShort[i]=SumShort[i]+ShortPL[i] #レポート用
        y['sumlong'] = SumLong
        y['sumshort'] = SumShort
        y['pl'] = pl

        save_name = os.path.join(self.S_DIR, str(code)  + "_detail.csv")
        y.to_csv(save_name)
        return pd.DataFrame({'LongPL':LongPL, 'ShortPL':ShortPL,'Sumlong':SumLong,'Sumshort':SumShort}, index=y.index)

    def ATR_stg(self, code, year_s, year_e, title):
        code_text = os.path.join(compf.CODE_DIR, str(code) + '.txt')
        if os.path.exists(code_text):
            df = pd.DataFrame(index=pd.date_range(year_s + '/01/01', year_e + '/12/31'))
            df = df.join(pd.read_csv(code_text,index_col=0, parse_dates=True, encoding="cp932", header=None))
            df = df.dropna()
            df.columns = ['O', 'H', 'L', 'C', 'V', 'C2', 'SS'][:len(df.columns)]
            df = compf.add_avg_rng(df, 'C', 'L', 'H')
        else:
            print(code,code_text + "が存在しない")
            return pd.DataFrame({})

        y = df
        y['CL'] = y.C.shift(1)
        y['MA']= y.C.rolling(10).mean().shift(1)

        #レポート用
        N = len(y) #FXデータのサイズ
        LongPL = np.zeros(N) # 買いポジションの損益
        ShortPL = np.zeros(N) # 売りポジションの損益
        SumPL = np.zeros(N)  # 売りポジションの損益
        SumB = np.zeros(N)  # 売りポジションの損益
        SumS = np.zeros(N)  # 売りポジションの損益

        BSTL = np.empty(N)
        SSTL = np.empty(N)
        vora = np.empty(N)
        for i in range(10,len(y)-1):
            vora[i] = int((max(y.H[i],y.CL[i]) - min(y.L[i],y.CL[i])) * 0.85) + common.haba_type(y.CL[i])
            BSTL[i] = y.CL[i] + vora[i-1]
            SSTL[i] = y.CL[i] - vora[i-1]

            SumPL[i]=SumPL[i-1]
            SumB[i]=SumB[i-1]
            SumS[i]=SumS[i-1]
            if y.O[i] < BSTL[i] and BSTL[i] <= y.H[i] and y.MA[i] < y.C[i]:
                LongPL[i] = int((y.O[i+1] / BSTL[i] -1) * 1000000)
            if y.O[i] > SSTL[i] and SSTL[i] >= y.L[i] and y.MA[i] > y.C[i]:
                ShortPL[i] = int((SSTL[i] / y.O[i+1] -1) * 1000000)

            SumPL[i]=SumPL[i]+ShortPL[i]+LongPL[i] #レポート用
            SumB[i]=SumB[i]+LongPL[i] #レポート用
            SumS[i]=SumS[i]+ShortPL[i] #レポート用
        y['BSTL']= BSTL
        y['SSTL']= SSTL
        y['vora']= vora
        y['plb'] = LongPL
        y['pls'] = ShortPL
        y['sumB'] = SumB
        y['sumS'] = SumS
        y['sum'] = SumPL

        save_name = os.path.join(self.S_DIR, str(code)  + title + ".csv")
        y.to_csv(save_name)
#        return pd.DataFrame({'LongPL':LongPL, 'ShortPL':ShortPL,'Sum':SumPL}, index=y.index)
        return y

    def day_stg(self,code,df):
        try:
            df.columns =['O','H','L','C','V','C2','SS']
            df = compf.add_avg_rng(df,'C','L','H')
            y = df.dropna()
            y['OC']=(y.C / y.O -1 ) * 1000000
            y['COL']=(y.O.shift(-1) / y.C -1) * 1000000
        except:
            print(code,"day_stgエラー")
            return pd.DataFrame({})
        #レポート用
        N = len(y) #FXデータのサイズ
        LongPL = np.zeros(N) # 買いポジションの損益
        ShortPL = np.zeros(N) # 売りポジションの損益
        SumLong = np.zeros(N) # 売りポジションの損益
        SumShort = np.zeros(N) # 売りポジションの損益
        pl = np.zeros(N) # 売りポジションの損益

        for i in range(10,len(y)-1):
            SumLong[i]=SumLong[i-1]
            SumShort[i]=SumShort[i-1]
            LongPL[i] = y.COL[i]
            ShortPL[i] = y.OC[i]

            SumLong[i]=SumLong[i]+LongPL[i] #レポート用
            SumShort[i]=SumShort[i]+ShortPL[i] #レポート用

        y['sumlong'] = SumLong
        y['sumshort'] = SumShort
        y['sum'] = SumShort + SumLong

        save_name = os.path.join(self.S_DIR, str(code)  + "_day_stg_detail.csv")
        y.to_csv(save_name)
        return pd.DataFrame({'LongPL':LongPL, 'ShortPL':ShortPL,'Sumlong':SumLong,'Sumshort':SumShort}, index=y.index)

    def all_avg(self, path):
        dict_w = {}
        if os.path.exists(path):
            print("path",path)
            df = pd.read_csv(path, index_col=0, parse_dates=True, encoding="cp932", header=0)
            for i in range(len(df.columns)):
                if df.columns[i] == "stockname":
                    pass
                elif df.columns[i] in ['CNT_A', 'CNT']:
                    dict_w[df.columns[i]] = int(df[df.columns[i]].mean())
                else:
                    dict_w[df.columns[i]] = round(df[df.columns[i]].mean(), 2)
                dict_w["amount"] = len(df)
        return dict_w

    def main4(self, title_t, filter="not_all"):
        files = os.listdir(compf.CODE_DIR)
        years_l = [2013, 2014, 2015, 2016, 2017]
        for i in files:
#        for i in ['1712', '1827', '1921', '2359', '2760', '4228', '6826', '8093', '8374', '8545', '8563', '2593', '4967', '6758', '8954']:
#        for i in ["1757"]:
            for year_e in years_l:
                code = i.replace(".txt", "")
                try:
                    y = self.ATR_stg(code, str(year_e - 4), str(year_e), "_base_" + str(year_e))
                except:
                    print(code,"不明なエラー発生")
                    continue
                if len(y) > 500 and int(y.O[1]) > 150 and common.stock_req(code, "SHELL") == 1:
                    dict_pl = {}

                    L_PL = compf.check_PL(y['plb'])
                    L_PL['MEMO'] = "L_PL"
                    S_PL = compf.check_PL(y['pls'])
                    S_PL['MEMO'] = "S_PL"
                    print(code,str(year_e),L_PL, S_PL)
                    for T_PL in [L_PL, S_PL]:
                        title = code + "_" + str(year_e) + T_PL['MEMO']
                        if T_PL['MEMO'] == "L_PL":
                            pl = 'plb'
                        if T_PL['MEMO'] == "S_PL":
                            pl = 'pls'

                        #5年間の成績
                        if (T_PL['WIN'] > 55 and T_PL['PL'] > 1.1 and T_PL['SUM_MAX_COMP'] > 3) or filter == 'all':
                            dict_pl.update(T_PL)
                            #前年の成績
                            y = self.ATR_stg(code, str(year_e - 0), str(year_e), "_" + title + "_" + str(year_e))
                            if len(y) == 0:
                                continue
                            T_PL = compf.check_PL(y[pl])
                            #前年と5年前を比較して前年の方が大きいこと
                            if (dict_pl['PL'] < T_PL['PL']) or filter == 'all':
                            #前年と5年前と比較して差分があまりないこと
#                            if (abs(dict_pl['WIN'] / T_PL['WIN'] -1) < 0.3 and abs(dict_pl['PL'] - 1) < 0.3 ) or filter == 'all':
                                y = self.ATR_stg(code, str(year_e + 1), str(year_e+1), "_"+ title + "_" + str(year_e))
                                if len(y) == 0:
                                    continue
                                T_PL = compf.check_PL(y[pl])
                                if T_PL['PL'] == 0:
                                    continue
                                dict_pl['CNT_A'] = T_PL['CNT']
                                dict_pl['WIN_A'] = T_PL['WIN']
                                dict_pl['PL_A'] = T_PL['PL']
                                dict_pl['AVG_A'] = T_PL['AVG']
                                dict_pl['WIN_COMP'] = dict_pl['WIN_A'] / dict_pl['WIN'] -1
                                dict_pl['PL_COMP'] = dict_pl['PL_A'] / dict_pl['PL'] -1
                                del dict_pl['MEMO']
                                self.save_to_csv(os.path.join(self.S_DIR, filter + "_.csv"), title, dict_pl)
        dict_w = self.all_avg(os.path.join(self.S_DIR, filter + "_.csv"))
        if len(dict_w) > 0:
            self.save_to_csv(os.path.join(compf.OUT_DIR,"reports_.csv"),title_t,dict_w)

    def STR_C(self):
        #カラムの初期化
        sqls = "update kabu_list set L_PL_085 = NULL ,S_PL_085 = NULL"
        common.db_update('B01_stock.sqlite', sqls)

        files = os.listdir(compf.CODE_DIR)
        for i in files:
            year_e = 2018
            code = i.replace(".txt", "")
            try:
                y = self.ATR_stg(code, str(year_e - 4), str(year_e), "_base_" + str(year_e))
            except:
                print(code,"不明なエラー発生")
                continue
            if len(y) > 500 and int(y.O[1]) > 150 and common.stock_req(code, "SHELL") == 1:
                dict_pl = {}
                dict_w = {}
                L_PL = compf.check_PL(y['plb'])
                L_PL['MEMO'] = "L_PL_085"
                S_PL = compf.check_PL(y['pls'])
                S_PL['MEMO'] = "S_PL_085"
                for T_PL in [L_PL, S_PL]:
                    dict_w = {}
                    title = code + "_" + str(year_e) + T_PL['MEMO']
                    if T_PL['MEMO'] == "L_PL_085":
                        pl = 'plb'
                    if T_PL['MEMO'] == "S_PL_085":
                        pl = 'pls'

                    #5年間の成績
                    if (T_PL['WIN'] > 58 and T_PL['PL'] > 1.1):
                        dict_pl.update(T_PL)
                        #前年の成績
                        y = self.ATR_stg(code, str(year_e - 0), str(year_e), "_" + title + "_" + str(year_e))
                        if len(y) == 0:
                            continue
                        T_PL = compf.check_PL(y[pl])
                        #前年と5年前を比較して前年の方が大きいこと
                        if (dict_pl['PL'] < T_PL['PL']):

                            dict_w[dict_pl['MEMO']] = dict_pl['WIN']
                            # rowid取得
                            sqls = "select *,rowid from %(table)s where コード = '%(key1)s' ;" % {'table': 'kabu_list','key1': code}
                            sql_pd = common.select_sql('B01_stock.sqlite', sqls)
                            sqls = common.create_update_sql('B01_stock.sqlite', dict_w, 'kabu_list', sql_pd['rowid'][0])  #最後の引数を削除すると自動的に最後の行


if __name__ == "__main__":
    info = profit('KABU')
    from time import sleep
#    print(info.ATR_stg('1301', '2014','2014','_1301_2013L_PL_2013'))
    info.STR_C() #"all"
    exit()
    info.main4("5年前のデータ使用_10移動平均上は買いフィルターあり","フィルターあり") #"all"
    exit()


    title = "_day_stg.csv"
    info.main_exec2(title)
    exit()

    title = "_ATR_stg.csv"
    title = "_Monthly_last.csv"
    info.main_exec2(title)
    title = "_vora_stg.csv"
    info.main_exec2(title)

    common.mail_send(u'投資戦略完了計算', title)


    print("end",__file__)
