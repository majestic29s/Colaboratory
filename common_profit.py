#!/usr/bin/env python
# -*- coding: utf-8 -*-
import pandas_datareader.data as web
import numpy as np
import pandas as pd
import pandas.tseries as pdt
from datetime import date
import os,csv
from datetime import datetime, timezone, timedelta

if os.name == "nt":
    OUT_DIR = r"C:\data\90_profit\06_output"
    INPUT_DIR = r"C:\data\90_profit\05_input"
    CODE_DIR = r"C:\Users\majestic29s\Dropbox\servers\ARISTO\data\ruby\jp"
    REP_DIR = r"C:\data\90_profit\06_output"
elif os.environ["HOME"] == '/content':
    OUT_DIR = os.path.join(os.environ["HOME"],'data/06_output')
    INPUT_DIR = os.path.join(os.environ["HOME"], 'data/05_input/')
    CODE_DIR = os.path.join(os.environ["HOME"], 'drive/home/jp')
    REP_DIR = os.path.join(os.environ["HOME"], 'drive/home')

else:
    OUT_DIR = os.path.join('/tmp/06_output')
    INPUT_DIR = os.path.join(os.environ["HOME"], 'data/python/90_profit/05_input/')
    CODE_DIR = os.path.join(os.environ["HOME"], 'data/ruby/jp')
    REP_DIR = os.path.join(os.environ["HOME"], 'data/python')
    """
    ROOT_DIR = os.path.join(os.environ["HOME"], 'python/06_output')
    ROOT_DATA = os.path.join(os.environ["HOME"], 'data/python')
    TDNET_FLAG = os.path.join(os.environ["HOME"], 'tmp/python/FLAG')
    DROP_DIR = os.path.join(os.environ["HOME"], 'Dropbox/servers/Hold/' + HOSTNAME)
    """

def BacktestReport(PL, title,S_DIR,pro_fit = 1.4,filter_f = 0,):
    backreport = {'総利益': "", '総損失': "", '総損益': "",'★総トレード数': "",'★勝率': "", '★プロフィットファクター': "",'★買いトレード数': "",'★buy勝率': "",
                    '★buyプロフィットファクター': "",'★売りトレード数': "",  '★sell勝率': "", '★sellプロフィットファクター': "",
                    '平均損益': "", '最大ドローダウン': "", 'リカバリーファクター': "", '勝トレード数': "", '最大勝トレード': "", '平均勝トレード': "", '負トレード数': "", '最大負トレード': "", '平均負トレード': "", 'buy勝トレード数': "", 'buy負トレード数': "", 'buy勝トレード利益': "", 'buy負トレード利益': "", 'buy合計損益': "",
                    'sell勝トレード数': "", 'sell負トレード数': "",'sell勝トレード利益': "", 'sell負トレード利益': "", 'sell合計損益': ""}
#        if int(PL.iloc[-1]['ShortPL']) == 0 or int(PL.iloc[-1]['LongPL']) == 0:
#            return 0, backreport

    #0を除去
    if 0 in (PL['LongPL'].clip_lower(0).sum(), PL['LongPL'].clip_upper(0).sum(), PL['ShortPL'].clip_lower(0).sum(), PL['ShortPL'].clip_upper(0).sum()):
        print("フィルター 0を除去_NG")
        return 0, backreport

    if filter_f == 0:
        #大きな利益損益除外フィルター
        tmp1 = max(abs(PL['ShortPL']) + abs(PL['LongPL']))
    #        tmp2 = min(PL['ShortPL'] + PL['LongPL'])
        tmp3 = (PL['ShortPL'] + PL['LongPL']).sum()
        if abs(tmp3) / tmp1 < 3:
            print("フィルター 大きな利益損益除外_NG",abs(tmp3) / tmp1)
            return 0, backreport


        #角度確認
        l1 = int(len(PL) * 0.3)
        l2 = int(len(PL) * 0.6)
        l3 = int(len(PL) * 0.8)

        if PL.iloc[0]['Sum'] < PL.iloc[l1]['Sum'] < PL.iloc[l2]['Sum'] < PL.iloc[l3]['Sum'] < PL.iloc[-1]['Sum']:
            pass
        elif PL.iloc[0]['Sum'] > PL.iloc[l1]['Sum'] > PL.iloc[l2]['Sum'] > PL.iloc[l3]['Sum'] > PL.iloc[-1]['Sum']:
            pass
        else:
            print("フィルター 曲線確認_NG")
            return 0, backreport

    LongPL = PL['LongPL']
    LongTrades = np.count_nonzero(PL['LongPL'])
    LongWinTrades = np.count_nonzero(LongPL.clip_lower(0))
    LongLoseTrades = np.count_nonzero(LongPL.clip_upper(0))
    if LongTrades == 0:
        LongTrades = 1
    if LongWinTrades == 0:
        LongWinTrades = 1
    if LongLoseTrades == 0:
        LongLoseTrades = 1

    backreport['★買いトレード数'] = LongTrades
    backreport['buy勝トレード数'] = LongWinTrades
    backreport['buy負トレード数'] = LongLoseTrades
    backreport['★buy勝率'] = round(LongWinTrades/LongTrades*100, 2)
    backreport['buy勝トレード利益'] = round(LongPL.clip_lower(0).sum(), 4)
    backreport['buy負トレード利益'] = round(LongPL.clip_upper(0).sum(), 4)
    backreport['buy合計損益'] = round(LongPL.sum()/LongTrades, 4)
    backreport['★buyプロフィットファクター'] = round(
        -LongPL.clip_lower(0).sum()/LongPL.clip_upper(0).sum(), 2)

    ShortPL = PL['ShortPL']
    ShortTrades = np.count_nonzero(PL['ShortPL'])
    ShortWinTrades = np.count_nonzero(ShortPL.clip_lower(0))
    ShortLoseTrades = np.count_nonzero(ShortPL.clip_upper(0))
    if ShortTrades == 0:
        ShortTrades = 1
    if ShortWinTrades == 0:
        ShortWinTrades = 1
    if ShortLoseTrades == 0:
        ShortLoseTrades = 1

    backreport['★売りトレード数'] = ShortTrades
    backreport['sell勝トレード数'] = ShortWinTrades
    backreport['sell負トレード数'] = ShortLoseTrades
    backreport['★sell勝率'] = round(ShortWinTrades/ShortTrades*100, 2)
    backreport['sell勝トレード利益'] = round(ShortPL.clip_lower(0).sum(), 4)
    backreport['sell負トレード利益'] = round(ShortPL.clip_upper(0).sum(), 4)
    backreport['sell合計損益'] = round(ShortPL.sum()/ShortTrades, 4)
    backreport['★sellプロフィットファクター'] = round(
        -ShortPL.clip_lower(0).sum()/ShortPL.clip_upper(0).sum(), 2)

    Trades = LongTrades + ShortTrades
    WinTrades = LongWinTrades+ShortWinTrades
    LoseTrades = LongLoseTrades+ShortLoseTrades
    if Trades == 0:
        Trades = 1
    if WinTrades == 0:
        WinTrades = 1
    if LoseTrades == 0:
        LoseTrades = 1

    backreport['★総トレード数'] = Trades
    backreport['勝トレード数'] = WinTrades
    backreport['最大勝トレード'] = max(LongPL.max(), ShortPL.max())
    backreport['平均勝トレード'] = round(
        (LongPL.clip_lower(0).sum()+ShortPL.clip_lower(0).sum())/WinTrades, 2)
    backreport['負トレード数'] = LoseTrades
    backreport['最大負トレード'] = min(LongPL.min(), ShortPL.min())
    backreport['平均負トレード'] = round(
        (LongPL.clip_upper(0).sum()+ShortPL.clip_upper(0).sum())/LoseTrades, 2)
    backreport['★勝率'] = round(WinTrades/Trades*100, 2)

    GrossProfit = LongPL.clip_lower(0).sum()+ShortPL.clip_lower(0).sum()
    GrossLoss = LongPL.clip_upper(0).sum()+ShortPL.clip_upper(0).sum()

    Profit = GrossProfit+GrossLoss
    Equity = (LongPL+ShortPL).sum()
    backreport['総利益'] = round(GrossProfit, 4)
    backreport['総損失'] = round(GrossLoss, 4)
    backreport['総損益'] = round(Profit, 4)
    backreport['★プロフィットファクター'] = round(-GrossProfit/GrossLoss, 4)
    backreport['平均損益'] = round(Profit/Trades, 4)
    backreport['最大ドローダウン'] = 0
    backreport['リカバリーファクター'] = 0

    if filter_f == 0:
        if 0.6 < backreport['★プロフィットファクター'] < pro_fit:
            print("フィルター プロフィットファクター_NG")
            return 0, backreport
        if  backreport['★総トレード数'] < 50:
            print("フィルター ★総トレード数_NG")
            return 0, backreport

    print("Report_Write!!!", title)
    PL = PL[(PL['ShortPL'] != 0.0) | (PL['LongPL'] != 0.0)]
    save_name = os.path.join(S_DIR, "_all_report.csv")
    save_to_csv(save_name, title, backreport)

    save_name = os.path.join(REP_DIR, "all_reports.csv")
    save_to_csv(save_name, title, backreport)

    PL.to_csv(os.path.join(S_DIR, title))
    return Equity, backreport


def save_to_csv(save_name,title,backreport):
    #ヘッダー追加
    if os.path.exists(save_name) == False:
        dic_name = ",".join([str(k[0]).replace(",","")  for k in backreport.items()])+"\n"
        with open(save_name, 'w', encoding="cp932") as f:
            f.write("now,stockname,"+dic_name)
    #1列目からデータ挿入
    dic_val = ",".join([str(round(k[1],3)).replace(",","")  for k in backreport.items()])+"\n"
    with open(save_name, 'a', encoding="cp932") as f:#code = 'cp932' utf-8
        f.write(env_time()[1] +"," + title+","+dic_val)

def env_time():
    t = datetime.datetime.now()
    date = t.strftime("%Y%m%d%H%M%S")
    timef = t.strftime("%Y/%m/%d %H:%M:%S")
    return date, timef

def code_hist(code,start_day,end_day):
    code = str(code)
    file_name = os.path.join(CODE_DIR, code) + ".txt"
    print(file_name)
    if os.path.exists(file_name):
        tsd = pd.read_csv(file_name, engine='python',parse_dates=True)
        tsd.columns = ['Date', 'Open', 'High','Low', 'Close', 'AdjClose', 'Volume','etc']
        tsd = tsd.set_index('Date')
        tsd = tsd.loc[start_day:end_day,:]
        return tsd
    else:
        tsd['Date'] = 0
        return tsd

def kairi(code,start_day,end):
    work_d = code_hist(code,start_day,end)
    T_max = (work_d['Close'][-1:]- work_d['Close'].min()) / (work_d['Close'].max() - work_d['Close'].min())
    return T_max

def add_avg_rng(tsd,C,L,H):
        #乖離平均追加
        for t in [7,30,200,365]:
            if len(tsd) > t:
                tsd['rng'+str(t)]=round((tsd[C].shift(1)-tsd[L].rolling(t).min().shift(1))/(tsd[H].rolling(t).max().shift(1)-tsd[L].rolling(t).min().shift(1)),2)
                tsd['avg' + str(t)] = round(tsd[C].shift(1) / tsd[C].rolling(t).mean().shift(1) - 1, 2)

        return tsd

def check_PL(PL):
    dict_w = {}
    win = np.count_nonzero(PL.clip_lower(0))
    lose = np.count_nonzero(PL.clip_upper(0))
    if win == 0:
        win = 1
    dict_w['CNT'] = win + lose
    dict_w['WIN'] = round(win / dict_w['CNT'] * 100, 2)
    try:
        dict_w['PL'] = round(-PL.clip_lower(0).sum() / PL.clip_upper(0).sum(), 2)
    except:
        dict_w['PL'] = -1
    dict_w['AVG'] = int(PL.sum() / dict_w['CNT'])
    dict_w['SUM_MAX_COMP'] = PL.sum() / PL.max()
    return dict_w

if __name__ == "__main__":
    hist = code_hist(9449, str(2015), '2017/01/01')
    print(hist)
    kikan = 30
