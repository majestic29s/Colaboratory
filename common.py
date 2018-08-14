#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
from bs4 import BeautifulSoup
import requests
import urllib.request
import csv
import smtplib
from email.mime.text import MIMEText
from email.header import Header
from email.utils import formatdate
import chardet
from chardet.universaldetector import UniversalDetector
import socket
import sys
import traceback
import datetime
import sqlite3
import jsm
import pandas as pd
import re
import socket
from time import sleep
import pandas_datareader.data as web
import math
from selenium import webdriver
from selenium.webdriver.chrome.options import Options

if os.name == "nt":
    ROOT_DIR = os.environ["PY_HOME"]
    ROOT_DATA = r'C:\data\python'
    DB_DIR = r'C:\data\python\db'
    TEMP_DIR = r'C:\temp'
    CODE_LIST = r'C:\autobyby\00_KABU_DATA\download\ruby\04_list\_tosho_list.txt'
    CODE_LIST_US = r'C:\autobyby\00_KABU_DATA\download\ruby\04_list\_stock_us.txt'
    TDNET_CFG = r'C:\tools\tdnet_check\list.txt'
    TDNET_FLAG = r'C:\tools\tdnet_check\FLAG'
    LOG_FILE = r'01_log\ope.log'
    HOSTNAME = os.environ["computername"]
    BYBY_LOG = r'c:\log\byby_Hist.csv'
    SHIKI = r'C:\autobyby\001_四季報'
    PASS_FILE = os.path.join(os.environ["PY_HOME"], r'03_lib','secret.conf')
    STOP_FLAG = os.path.join(os.environ["PY_HOME"], r'03_lib','stop')
    FLAG_DIR = os.path.join(os.environ["PY_HOME"], r'04_def')
    RUBY_LIST = r'C:\data\ruby\04_list'
    HORIDAY =  os.path.join(os.environ["PY_HOME"], r'03_lib','NO_WORKDAY.TXT')
    DROP_DIR = r'C:\temp\Hold'
#    RUBY_DATA = r'C:\trade_sumulator\05_data'
    RUBY_DATA = r"C:\Users\majestic29s\Dropbox\servers\ARISTO\data\ruby"

    LIB_DIR = os.path.join(os.environ["PY_HOME"], r'02_bin','lib')
    LOG_FILE = os.path.join(r'C:\log', LOG_FILE)
    PROFIT_DIR = os.path.join(r'C:\python_tool\90_profit\02_bin')
elif os.environ["HOME"] == '/content':#drive/python/
    ROOT_DIR = os.environ["HOME"]
    ROOT_DATA = os.environ["HOME"]
    DB_DIR = os.path.join(os.environ["HOME"], 'drive/home/db')
    TEMP_DIR = os.environ["HOME"]
    HOSTNAME = socket.gethostname()
    PROFIT_DIR = os.environ["HOME"]

else:
    ROOT_DIR = os.path.join(os.environ["HOME"], 'python')
    ROOT_DATA = os.path.join(os.environ["HOME"], 'data/python')
    DB_DIR = os.path.join(os.environ["HOME"], 'db')
    TEMP_DIR = os.path.join(os.environ["HOME"], 'tmp')
    CODE_LIST = os.path.join(os.environ["HOME"], 'ruby/04_list/_tosho_list.txt')
    CODE_LIST_US = os.path.join(os.environ["HOME"], 'ruby/04_list/_stock_us.txt')
    TDNET_CFG = os.path.join(os.environ["HOME"], 'python/03_lib/tdnet_list.txt')
    TDNET_FLAG = os.path.join(os.environ["HOME"], 'tmp/python/FLAG')
    LOG_FILE = r'01_log/ope.log'
    HOSTNAME = socket.gethostname()
    BYBY_LOG = r'01_log/byby_Hist.csv'
    PASS_FILE = os.path.join(os.environ["HOME"], 'python/03_lib/secret.conf')
    STOP_FLAG = os.path.join(os.environ["HOME"], 'python/03_lib/stop')
    FLAG_DIR = os.path.join(os.environ["HOME"], 'python/08_flag')
    RUBY_LIST = os.path.join(os.environ["HOME"], 'ruby/04_list')
    HORIDAY = os.path.join(os.environ["HOME"], 'python/03_lib/NO_WORKDAY.TXT')
    DROP_DIR = os.path.join(os.environ["HOME"], 'Dropbox/servers/Hold/' + HOSTNAME)
    RUBY_DATA = os.path.join(os.environ["HOME"], 'data/ruby')
    LIB_DIR = os.path.join(os.environ["HOME"], 'python/02_bin/lib')
    SHIKI = os.path.join(os.environ["HOME"], 'data/python')
    LOG_FILE = os.path.join(ROOT_DIR, LOG_FILE)
    PROFIT_DIR = os.path.join(os.environ["HOME"], 'python/90_profit/02_bin')


# passconfig
import configparser
config = configparser.ConfigParser()
config.read([PASS_FILE])

# リアルSQL追加
REAL_STOK = ('四季報好業績_NEW','東1昇格銘柄_決算月外_予測×中止', '優待戦略', '高配当銘柄', '銘柄異動', '第三者割当増資', '公募・売出', '立会外分売', '東証2部昇格候補',
            'もうすぐストップ高買', 'dnetメッセージ監視', '優待戦略権利取', 'ストップ高決済', 'ストップ高逆指値11', 'all', 'TDNETトレード', "DB未登録決済",  'DB未登録決済_現物',
            '夜間下降売りVer2', '日次引け成り_CSV', 'キャンセルcancel',"STR_0.85_TEST",'マザーズ月末戦略_TEST')

REAL_SQL = "select *,rowid from bybyhist where (終了日付 IS NULL or 終了日付 = '') and 損益 IS NOT NULL and タイトル in %(key1)s" % {'key1': REAL_STOK}


def real_byby(msg):  # あとで入れ替え
    list = REAL_STOK
    for i in list:
        if msg.find(i) == 0:
            return 1
    return 0


def env_time():
    t = datetime.datetime.now()
    date = t.strftime("%Y%m%d%H%M%S")
    timef = t.strftime("%Y/%m/%d %H:%M:%S")
    return date, timef


def full_path(file):
    return os.path.join(ROOT_DIR, file)


def temp_path(dir, file=None):
    if file == None:
        file_path = os.path.join(TEMP_DIR, dir)
    else:
        file_path = os.path.join(TEMP_DIR, dir, file)
    create_dir(file_path)
    return file_path


def save_path(dir, file=None):
    if dir.count('sqlite'):
        file_path = os.path.join(DB_DIR, dir)
    else:
        if file == None:
            file_path = os.path.join(ROOT_DATA, dir)
        else:
            file_path = os.path.join(ROOT_DATA, dir, file)
    create_dir(file_path)
    return file_path


def flag_path(dir, file=None):
    if file == None:
        file_path = os.path.join(ROOT_DATA, dir)
    else:
        file_path = os.path.join(ROOT_DATA, dir, file)
    create_dir(file_path)
    return file_path


def log_write(MSG=None, process=__file__, LEVEL="INFO", F_FILE=LOG_FILE):
    import datetime
    process = os.path.basename(process)
    t = datetime.datetime.now()
    DATE = t.strftime("%Y/%m/%d %H:%M:%S")
    with open(F_FILE, 'a', encoding="utf-8") as f:
        f.write(DATE + " " + LEVEL + " " + process + " " + str(MSG) + "\n")


def check_encoding(file_path):
    if os.path.exists(file_path):
        detector = UniversalDetector()
        with open(file_path, mode='rb') as f:
            for binary in f:
                detector.feed(binary)
                if detector.done:
                    break
        detector.close()
        return detector.result['encoding']
    else:
        return 0

# 削除する予定


def code_detail(code):
    code = str(code)
    import csv
    with open(CODE_LIST, "r") as fr:
        for line in fr.readlines():
            sp_msg = line.split(",")
            if code == sp_msg[0]:
                return sp_msg
    return 0


def haba_type(HABA):
    HABA = float(HABA)
    if HABA < 3000 - 3000 * 0.05:
        return 1
    elif HABA < 5000 - 5000 * 0.05:
        return 5
    elif HABA < 30000 - 30000 * 0.05:
        return 10
    elif HABA < 50000 - 50000 * 0.05:
        return 100
    elif HABA < 300000 - 300000 * 0.05:
        return 500
    elif HABA < 500000 - 500000 * 0.05:
        return 1000
    else:
        return 5000


def create_error(info):
    # エラーの情報をsysモジュールから取得sys.exc_info()
    exc_type, exc_value, exc_traceback = info
    temp_msg = "error_type:" + str(exc_type) + "\n"
    temp_msg += "error_value:" + str(exc_value) + "\n"
    # tracebackモジュールのformat_tbメソッドで特定の書式に変換
    tbinfo = traceback.format_tb(exc_traceback)
    for tbi in tbinfo:
        temp_msg += tbi
    return temp_msg


def real_stock2(code):
    try:
        code = code
        base_url = "https://stocks.finance.yahoo.co.jp/stocks/detail/"
        query = {}
        yahoo_info = {'LastDay': "", 'Open': "", 'High': "", 'Low': "", 'Volume': "", 'Buyingsellprice': "", 'LimitH': "", 'LimitL': "", 'price': "", 'name': "", "jikaso": "","hakokabu": "", "dividend": "", "dividend_1k": "", "PER": "", "PBR": "", "EPS": "", "BPS": "", "lowamount": "", "amount": "", "High_Y": "", "Low_Y": "", "trust": ""}

        query["code"] = code
        ret = requests.get(base_url, params=query)
        soup = BeautifulSoup(ret.content, "lxml")
        stocktable = soup.find('div', {'class': 'innerDate'})
        i = 0
        for k, v in yahoo_info.items():
            if i == 6:
                temp = stocktable.findAll('dd', {'class': 'ymuiEditLink mar0'})[i].strong.text.replace(",", "").split("～")
                yahoo_info['LimitH'] = temp[1]
                yahoo_info['LimitL'] = temp[0]
                break
            else:
                yahoo_info[k] = stocktable.findAll('dd', {'class': 'ymuiEditLink mar0'})[i].strong.text.replace(",", "")
            i += 1
        stocktable = soup.find('table', {'class': 'stocksTable'})
        yahoo_info['price'] = stocktable.findAll('td', {'class': 'stoksPrice'})[1].text.replace(",", "")
        yahoo_info['name'] = stocktable.findAll('th', {'class': 'symbol'})[0].text

        stocktable = soup.find('div', {'class': 'clearFix ymuiDotLine'})
        yahoo_info['trust'] = stocktable.findAll('div', {'class': 'yjMS clearfix'})[0].strong.text.replace(",", "")
        # 時価総額-年初来安値
        stocktable = soup.find('div', {'class': 'main2colR clearFix'})
        i = 0
        try:
            for k, v in yahoo_info.items():
                if i > 9:
                    yahoo_info[k] = stocktable.findAll('dd', {'class': 'ymuiEditLink mar0'})[i-10].strong.text.replace(",", "").replace('\n', '')
                i += 1
        except:
            pass

        tsd = kabu_search(code)
        if len(tsd) > 0:
            if tsd['セクタ'] == "ETF":
                yahoo_info['amount'] = yahoo_info['dividend']
                yahoo_info['High_Y'] = yahoo_info['dividend_1k']
                yahoo_info['Low_Y'] = yahoo_info['PER']
                yahoo_info['jikaso'] = ""
                yahoo_info['hakokabu'] = ""
                yahoo_info['dividend'] = ""
                yahoo_info['dividend_1k'] = ""
                yahoo_info['PER'] = ""
                yahoo_info['PBR'] = ""
                yahoo_info['EPS'] = ""
                yahoo_info['BPS'] = ""
                yahoo_info['lowamount'] = ""
        if yahoo_info['amount'] == '---':
            yahoo_info['amount'] = 1
        to_number(yahoo_info)
        try:
            yahoo_info['price'] = int(float(yahoo_info['price']))
            yahoo_info['Open'] = int(float(yahoo_info['Open']))
            yahoo_info['High'] = int(float(yahoo_info['High']))
            yahoo_info['Low'] = int(float(yahoo_info['Low']))
        except:
            try:
                aaa = float(yahoo_info['LimitH']) - float(yahoo_info['LimitL']) / 2
                yahoo_info['price'] = int(float(yahoo_info['LimitL']) + aaa)
                yahoo_info['Open'] = int(float(yahoo_info['Open']))
            except:
                yahoo_info['LastDay'] = 0
        return yahoo_info
    except:
        to_number(yahoo_info)
        if code == 25935:
            yahoo_info['amount'] = 100
        else:
            yahoo_info['LastDay'] = 0
        return yahoo_info


def real_stock_us(code):
    UURL = "https://finance.yahoo.com/quote/" + code + "?ltr=1"
    yahoo_info = {}
    # 最新株価取得
    ret = requests.get(UURL)
    l = ret.text.split(',')
    for i in l:
        if i.count("currentPrice"):
            val = i.split(':')
            yahoo_info['price'] = float(val[2])
            break
    else:
        result, date, open1, high, low, close, volume = get_stock_usd(code)
        yahoo_info['Previous_Close'] = close[1]
        yahoo_info['Open'] = open1[0]
        yahoo_info['High'] = high[0]
        yahoo_info['Low'] = low[0]
        yahoo_info['price'] = close[0]
        yahoo_info['Volume'] = volume[0]
        return yahoo_info

    # 最新データ取得
    dfs = read_html2(UURL, None)  # header=0,skiprows=0(省略可能)
    for i in range(len(dfs)):
        for idx, row in dfs[i].iterrows():
            if row[0] in ['Bid', 'Ask', ]:
                pass
            elif row[0] == "Day's Range":
                sp_work = row[1].split("-")
                yahoo_info['High'] = sp_work[0].strip()
                yahoo_info['Low'] = sp_work[1].strip()
            else:
                title = row[0].replace(")", "").replace("(", "").replace("&", "").replace("'s ", "").replace(".", "").replace("-", "").replace(" ", "_").\
                    replace("1y", "y1").replace("52_Week_Range", "Week_Range52")
                try:
                    if str(row[1]) == "nan":
                        yahoo_info[title] = ""
                    else:
                        yahoo_info[title] = row[1].replace("%", "")
                except:
                    yahoo_info[title] = row[1]
    return yahoo_info


def create_file(path, msg="",encod = "utf-8"):  # 何も書き込まない
    f = open(path, "w", encoding=encod)
    f.write(msg)
    f.close()


def mail_send(title, msg):
    if msg == "":
        return
    FROM_ADDR = config.get('mail', 'FROM_ADDR')
    TO_ADDR = config.get('mail', 'TO_ADDR').replace("python", HOSTNAME)
    ENCODING = 'iso-2022-jp'  # メールの文字コード
    msg = msg.replace(u'\uff5e', u'\u301c').replace(u'\xa0', u'').replace(u'\u2028', u'').replace(u'\u2029', u'')
    message = MIMEText(msg.encode(ENCODING, "ignore"),'plain',ENCODING,)
    message['Subject'] = str(Header(title + HOSTNAME, ENCODING))
    message['From'] = FROM_ADDR
    message['To'] = TO_ADDR
    message['Date'] = formatdate()
#        sendding(message)
    s = smtplib.SMTP('smtp.gmail.com:587')
    s.ehlo()  # "EHLO"を用いてESMTPサーバに身元を明かす
    s.starttls()  # TLSモードで通信
    s.ehlo()  # ehlo()はもう一度呼ばなければならない
    s.login(config.get('mail', 'USER_ID'), config.get('mail', 'PASSWORD'))   # SMTPサーバにログイン
    s.sendmail(FROM_ADDR,[TO_ADDR],message.as_string(),) #attachment(path),
    s.close()


def get_lastrow(filename, col):
    f = open(filename, 'r', encoding=check_encoding(filename))
    dataReader = csv.reader(f)
    for row in dataReader:
        last_row = row
    last_row = row[col]
    return last_row


def get_arry(filename, col):
    row_arry = []
    f = open(filename, 'r')
    dataReader = csv.reader(f)
    for row in dataReader:
        row_arry.append(float(row[col]))
    return row_arry


def TRA_calc(filename, col):
    price = get_arry(filename, col-2)
    high = price[len(price)-1]
    price = get_arry(filename, col-1)
    low = price[len(price)-1]
    price = get_arry(filename, col)
    last1 = price[len(price)-2]
    TR1 = high - low
    TR2 = high - last1
    TR3 = last1 - low
    TR = max(TR1, TR2, TR3)
    TRA = round(TR * 0.85, 1)
    return TR, TRA


def csv_save(filename, data, code="utf-8"):  # 辞書をCSVに出力する。
    # ヘッダー追加

    dic_name = ",".join([str(k[0]).replace(",", "") for k in data.items()])+"\n"
    dic_val = ",".join([str(k[1]).replace(",", "") for k in data.items()])+"\n"

    print(dic_val)
    if os.path.exists(filename) == False:
        with open(filename, 'w', encoding=code) as f:
            f.write("now,"+dic_name)
    # データ追加部分
    t = datetime.datetime.now()
    ctime = t.strftime("%Y/%m/%d %H:%M:%S")
    with open(filename, 'a', encoding=code) as f:
        f.write(ctime+","+dic_val)
    print(filename)


def insertDB3(DB, table, dic):
    # nowの確認ある場合は削除
    if dic.get('now'):
        del dic["now"]
    if dic.get('銘柄名_英語'):
        dic["銘柄名_英語"] = dic["銘柄名_英語"].replace(" ", "").replace("%", "").replace("$", "").replace("-", "").replace(",", "").replace("/", "_").replace(".", "").replace("'", "")
    if len(os.path.dirname(DB)) == 0:
        DB = os.path.join(DB_DIR, DB)
    t = datetime.datetime.now().strftime("%Y/%m/%d %H:%M:%S")
    # カーソル生成
    conn = sqlite3.connect(DB)
    # 自動コミット
    cursor = conn.isolation_level = None

    # CREATE
    col_name = ', '.join(["'"+k+"'" for k in dic.keys()])
    sql = "CREATE TABLE IF NOT EXISTS " + table + " (now," + col_name + ")"
    conn.execute(sql)

    # nowカラムの確認・追加
    sqls = "select * from %(table)s where rowid=(select max(rowid) from %(table)s) ;" % {'table': table}
    sql_pd = select_sql(DB, sqls)
    db_list = set([i for i in sql_pd.columns if i == 'now'])
    if len(db_list) == 0:
        db_list = set([i for i in sql_pd.columns if i != 'now'])
        sqls = "alter table %(table)s add column now" % {'table': table}
        db_update(DB, sqls)

    # DBへのカラムの確認・追加
    new_list = [l for l in dic]
    column_check(DB, table, new_list)

    # INSERT
    # データフレーム→リスト
    #     col_data = ', '.join(["'"+str(v).replace("'","")+"'" for v in dic.values()])
    col_data = ', '.join(["'"+str(round_check(v)).replace("'","")+"'" for v in dic.values()])
    sql = "INSERT INTO " + table + " (now," + col_name + ") VALUES ('" + t + "'," + col_data + ")"
    print(sql)


    try:
        conn.execute(sql, dic)
    except sqlite3.OperationalError:
        sleep(5)
        conn.execute(sql, dic)
    # 閉じる
    conn.commit
    conn.close()


def kabu_search(code):
    sqls = "select *,rowid from %(table)s where コード = '%(key1)s' and 株価 IS NOT NULL" % {'table': 'kabu_list', 'key1': code}
    tsd = select_sql('B01_stock.sqlite', sqls)

    if len(tsd) == 0:
        save_dir = save_path(TEMP_DIR,"kabu_search")
        file_name = flag_path(save_dir, str(code))
        if os.path.exists(file_name) == False:
            create_file(file_name)
        return {}
    header_row = {'株価':'','前日始値':'','前日高値':'','前日安値':''}
    for v in range(len(tsd.columns)):
        header_row[tsd.columns[v]] = tsd.ix[0, tsd.columns[v]]
    header_row['株価'] = int(float(header_row['株価']))
    try:
        header_row['前日始値'] = int(float(header_row['前日始値']))
        header_row['前日高値'] = int(float(header_row['前日高値']))
        header_row['前日安値'] = int(float(header_row['前日安値']))
    except:
        pass
    return header_row


def kabu_search_usd(code):
    DB_BYBY = save_path('I09_stock_usd.sqlite')
    dict = {'table': 'kabu_list', 'key1': str(code)}
    sqls = "select *,rowid from %(table)s where コード = '%(key1)s'" % dict
    tsd = select_sql(DB_BYBY, sqls)
    header_row = {}

    if len(tsd) == 0:
        return header_row
    for v in range(len(tsd.columns)):
        header_row[tsd.columns[v]] = tsd.ix[0, tsd.columns[v]]
    return header_row


# 切り上げ 取引量
def ceil(code, mount, opt=1):  # 切り捨ての場合はoptを0
    tsd = kabu_search(code)
    if len(tsd) > 0:
        try:
            temp1 = mount / tsd['株価']
            ttt = int(tsd['単位'])
            if ttt == 0:
                tsd['単位'] = 10000
        except:
            return 1
        return int(temp1 / int(tsd['単位']) + opt) * int(tsd['単位'])
    return 1


def ceil_usd(code, mount, opt=1):  # 切り捨ての場合はoptを0
    tsd = kabu_search(code)
    if len(tsd) > 0:
        try:
            temp1 = mount / int(tsd['price'])
        except:
            return 1
        return int(temp1 / 1) + opt
    return 1


def real_stock3(code):
    try:
        yahoo_info = {}
        base_url = "https://stocks.finance.yahoo.co.jp/stocks/profile/?code=" + str(code)
        tables = read_html2(base_url, None)  # header=0,skiprows=0(省略可能)
        temp = temp_path("csv", os.path.basename(__file__) + "tempfile.csv")
        tables[1].to_csv(temp)
        f = open(temp, 'r')
        dataReader = csv.reader(f)
        for row in dataReader:
            yahoo_info[row[1]] = row[2]
    except:
        return yahoo_info
    return yahoo_info


def create_dir(file_path):
    sp_dir = os.path.split(file_path)
    if os.path.exists(sp_dir[0]):
        return 1
    else:
        os.makedirs(sp_dir[0])
        return 0


def select_sql(DB, sqls,t_row = None):
    if len(os.path.dirname(DB)) == 0:
        DB = save_path(DB)

    try:
        conn = sqlite3.connect(DB)
        df = pd.read_sql(sqls, conn)
        conn.close()
    except:
        return pd.DataFrame({})
    if len(df) == 1:
        for i in range(len(df.columns) - 1):
            if type(df.iat[0, i]) is str:
                try:
                    if df.iat[0, i].count("."):
                        df.iat[0, i] = float(df.iat[0, i])
                    else:
                        df.iat[0, i] = int(df.iat[0, i])
                except:
                    pass
    #最後の行を0行に取得
    if t_row != None:
        num = len(df) -1
        for i in range(len(df.columns) - 1):
            df.iat[0, i] = df.iat[num, i]
    return df


def db_update(DB, sqls):
    if DB.count('sqlite'):
        DB = os.path.join(DB_DIR, DB)

    log_write("UPDATE_SQL", str(sqls))
    conn = sqlite3.connect(DB, isolation_level=None)
    cur = conn.cursor()
    try:
        cur.execute(sqls)
    except sqlite3.OperationalError:
        sleep(5)
        cur.execute(sqls)
    conn.close()


def high_check1(code, day1):
    # 高値更新チェック
    today = datetime.date.today()
    yest_day = today - datetime.timedelta(days=day1)
    result, date, open, high, low, close, volume, adj_close = get_stock(code, yest_day, today)
    if result != 1:
        return 0
    t_colse = close[0]
    # 前日の高値安値取得
    today = datetime.datetime.strptime(date[1], '%Y/%m/%d')
    result, date, open, high, low, close, volume, adj_close = get_stock(code, yest_day, today)
    if result != 1:
        return 0

    if max(high) < t_colse:
        return 1
    elif min(low) > t_colse:
        return -1
    else:
        return 0


def high_check1_usd(code, day1):
    # 高値更新チェック
    today = datetime.date.today()
    yest_day = today - datetime.timedelta(days=day1)
    result, date, open, high, low, close, volume = get_stock_usd(code)
    if result != 1:
        return 0
    t_colse = close[0]

    # 最新行削除
    del date[0]
    del open[0]
    del high[0]
    del low[0]
    del close[0]
    del volume[0]
    if result != 1:
        return 0
    print(max(high[:day1]), t_colse)
    if max(high[:day1]) < t_colse:
        return 1
    elif min(low[:day1]) > t_colse:
        return -1
    else:
        return 0


def date_diff(date_, today=None):
    try:
        # today計算
        if today == None:
            spwk = str(datetime.date.today()).split(" ")
            today = spwk[0].replace("/", ",").replace("-", ",")
            t_sp = today.split(",")
        else:
            if type(today) == datetime.date or type(today) == datetime.datetime:
                spwk = str(today).split(" ")
                today = spwk[0].replace("/", ",").replace("-", ",")
                t_sp = today.split(",")
            elif type(today) == str:
                spwk = today.split(" ")
                today = spwk[0].replace("/", ",").replace("-", ",")
                t_sp = today.split(",")

        # date_計算
        if type(date_) == datetime.date or type(date_) == datetime.datetime:
            spwk = str(date_).split(" ")
            src_day = spwk[0].replace("/", ",").replace("-", ",")
            s_sp = src_day.split(",")
        elif type(date_) == str:
            spwk = date_.split(" ")
            src_day = spwk[0].replace("/", ",").replace("-", ",")
            s_sp = src_day.split(",")

        a = datetime.date(int(t_sp[0]), int(t_sp[1]), int(t_sp[2]))
        b = datetime.date(int(s_sp[0]), int(s_sp[1]), int(s_sp[2]))
        return (b-a).days
    except:
        return -999


def next_day():
    #    HORIDAY
    today = datetime.date.today()
    if today.weekday() == 4:  # 金曜日
        day = 3
    elif today.weekday() == 5:  # 土曜日
        day = 2
    else:
        day = 1
    # ホリデーリスト取得
    ld = open(HORIDAY)
    lines = ld.readlines()
    ld.close()
    for i in range(7):
        next_day = today + datetime.timedelta(days=day+i)
        str_next = str(next_day).replace("-", "")
        for line in lines:
            if line.find(str_next) >= 0:
                print("OK", line[:-1])
            else:
                return str(next_day).replace("-", "/")


def last_day():
    #    HORIDAY
    today = datetime.date.today()
    # ホリデーリスト取得
    ld = open(HORIDAY)
    lines = ld.readlines()
    ld.close()
    # ラストビジネスデー
    for i in range(7):
        last_day = today - datetime.timedelta(days=1+i)
        if last_day.weekday() in (5, 6):
            continue
        str_next = str(last_day).replace("-", "")
        for line in lines:
            if line.find(str_next) >= 0:
                break
        else:
            return str(last_day).replace("-", "/")
    return str(last_day).replace("-", "/")

def weeks():
    today = datetime.date.today()
    w_list = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sum']
    for i in range(7):
        if today.weekday() == i:  # 金曜日
            return w_list[i]

def sum_clce(DB, table, row1, t_sum, FX=None):
    dict = {'table': table, 'key1': row1, 'key2': t_sum}
    sqls = "select %(key1)s,%(key2)s,rowid from %(table)s" % dict
    sql_pd = select_sql(DB, sqls)
    if len(sql_pd) < 2:
        return
    l_sum = 0
    cnt = 0

    for i, row in sql_pd.iterrows():
        to_number(row)
        try:
            #Nan (Not a number) を判定
            if math.isnan(row[row1]):
                row[t_sum] = l_sum
                l_sum = row[t_sum]
            else:
            # FXは四捨五入の有無チェック
                if FX == None:
                    if row[t_sum] == int(row[row1]) + int(l_sum):
                        l_sum = row[t_sum]
                        continu
                    row[t_sum] = int(row[row1]) + int(l_sum)
                else:
                    if row[t_sum] == round(row[row1] + l_sum, FX):
                        l_sum = row[t_sum]
                        continue
                    row[t_sum] = round(row[row1] + l_sum, FX)
                l_sum = row[t_sum]
        except:
            row[t_sum] = l_sum
            l_sum = row[t_sum]
        if cnt <= i:
            sqls = "UPDATE %(table)s SET %(key3)s = '%(key1)s' where rowid = '%(key2)s'" % {'table': table, 'key1': row[t_sum], 'key2': row['rowid'], 'key3': t_sum}
            db_update(DB, sqls)


def to_str(data):
    for kk, vv in data.items():
        try:
            data[kk] = str(vv)
        except:
            pass
    return data


def row_check(DB, dict_w):
    sqls = "select * from %(table_name)s where rowid in(select max(rowid) from %(table_name)s)" % {'table_name':dict_w['table_name']}
    del dict_w["table_name"]
    sql_pd = select_sql(DB, sqls)
    for k, v in dict_w.items():
        for i, row in sql_pd.iterrows():
            if str(row[k]) == str(dict_w[k]):
                print("OK",dict_w[k])
                break
        else:
            print("NG",dict_w[k],row[k])
            return 0
    return 1

def create_update_sql(DB, dict_w, table_name, rid = None):
    if len(os.path.dirname(DB)) == 0:
        DB = save_path(DB)
    if rid is None:
        rid = last_rowid(DB, table_name)
    # 更新日時設定
    t = datetime.datetime.now().strftime("%Y/%m/%d %H:%M:%S")
    dict_w['uptime'] = str(t)
    # カラムの確認・追加
    new_list = [l for l in dict_w]
    aaa = column_check(DB, table_name, new_list)

    sqls = "UPDATE " + table_name + " SET "
    cnt = 0
    for kk, vv in dict_w.items():
        cnt += 1
        if len(dict_w) == cnt:
            sqls += kk + " = '" + str(round_check(vv)) + "'"
        else:
            sqls += kk + " = '" + str(round_check(vv)) + "' ,"
    sqls += " WHERE rowid = %s" % str(rid)
    print(env_time()[0],DB,sqls)
    db_update(DB, sqls)
    return 0


def stock_req(code, sell=None):
    try:
        tsd = kabu_search(code)
        if tsd['市場'][:1] in ("名", "札", "福"):
            return -1
        if tsd['AVG20出来高指数300以上OK'] < 10:
            return -1
        if tsd['株価'] < 150:
            return -1
        if len(tsd) == 0:
            return -1
        if sell == None:
            return 0
        else:
            print(tsd['貸借区分'])
            if tsd['貸借区分'] == 1:
                return 1
            else:
                return 0
    except:
        return -1


def dir_join(list):
    if os.name == "nt":
        return '\\'.join(list)
    else:
        return '/'.join(list)


def add_dict(code, dict):
    #インデックス取得
    sqls = "select *,rowid from %(table)s where rowid=(select max(rowid) from %(table)s) ;" % {'table': 'rashio'}
    sql_pd = select_sql('I01_all.sqlite', sqls)

    tsd = kabu_search(code)
    if len(tsd) > 0:
        add = {'前日': tsd['update'], '前日始値': tsd['前日始値'], '前日終値': tsd['株価'], '当日': '', '当日始値': '', '当日終値': '', '更新日': '', '貸借区分': tsd['貸借区分'],'逆日歩': tsd['逆日歩'], '信用倍率': tsd['信用倍率'], '配当利回り': tsd['配当利回り'],'セクタ': tsd['セクタ'], 'AVG20出来高': tsd['AVG20出来高'], 'AVG20出来高300': tsd['AVG20出来高指数300以上OK'], '時価総額': tsd['時価総額'], '発行株数': tsd['発行株数'], '配当1株': tsd['配当1株'],'HighLow30': tsd['HighLow30'],'HighLow90': tsd['HighLow90'], 'HighLow180': tsd['HighLow180'],'HighLow365': tsd['HighLow365'],'HighLow700': tsd['HighLow700'], '乖離avg30': tsd['乖離avg30'], '乖離avg90': tsd['乖離avg90'], '乖離avg180': tsd['乖離avg180'], '乖離avg365': tsd['乖離avg365'], '乖離avg700': tsd['乖離avg700'],  '変動率90': tsd['変動率90'], 'memo': '','N225_乖離avg30':sql_pd['N225_乖離avg30'][0],'N225_HighLow30':sql_pd['N225_HighLow30'][0],'TOPIX_乖離avg30':sql_pd['TOPIX_乖離avg30'][0],'TOPIX_HighLow30':sql_pd['TOPIX_HighLow30'][0],'sel_comp1': tsd['sel_comp1'], 'sel_comp2': tsd['sel_comp2'], 'kei_comp1': tsd['kei_comp1'], 'kei_comp2': tsd['kei_comp2'],'bef_comp': tsd['bef_comp']}
        for kkk, vvv in add.items():
            dict[kkk] = vvv
    else:
        add = {'前日': '', '前日始値': '', '前日終値': '', '当日': '', '当日始値': '', '当日終値': '', '更新日': '', '貸借区分': '','逆日歩': '', '信用倍率': '', '配当利回り': '', 'HighLow30': '','HighLow90': '', '乖離avg30': '', '乖離avg90': '', '変動率90': '', 'memo': '','N225_乖離avg30':sql_pd['N225_乖離avg30'][0],'N225_HighLow30':sql_pd['N225_HighLow30'][0],'TOPIX_乖離avg30':sql_pd['TOPIX_乖離avg30'][0],'TOPIX_HighLow30':sql_pd['TOPIX_HighLow30'][0]}
        for kkk, vvv in add.items():
            dict[kkk] = vvv
    return dict

def add_dict_usd(code, dict, OPT="JP"):
    tsd = kabu_search_usd(code)
    if len(tsd) > 0:
        add = {'前日': '', '前日始値': '', '前日終値': '', '当日': '', '当日始値': '', '当日終値': '', '更新日': '', '配当利回り': tsd['ExDividend_Date'],'HighLow30': tsd['HighLow30'], 'HighLow90': tsd['HighLow90'], '乖離avg30': tsd['乖離avg30'], '乖離avg90': tsd['乖離avg90'], 'memo': ''}
        for kkk, vvv in add.items():
            dict[kkk] = vvv
    else:
        add = {'前日': '', '前日始値': '', '前日終値': '', '当日': '', '当日始値': '', '当日終値': '', '配当利回り': '', 'HighLow30': '','HighLow90': '', '乖離avg30': '', '乖離avg90': '', 'memo': ''}
        for kkk, vvv in add.items():
            dict[kkk] = vvv
    return dict


def to_number(data):
    if type(data) == list:
        data = [str(x) for x in data]
        for i in range(len(data)):
            if type(data[i]) == str:
                try:
                    if data[i].count("."):
                        data[i] = float(data[i])
                    else:
                        data[i] = int(data[i])
                except:
                    pass
        return data

    for kk, vv in data.items():
        if type(data[kk]) == str:
            try:
                if kk in ['仕掛け値', '値', '株価', '玉', '追加建玉']:
                    data[kk] = int(float(vv))
                elif vv.count("."):
                    data[kk] = float(vv)
                else:
                    data[kk] = int(vv)
            except:
                pass
    return data


def to_int(data):
    if type(data) == list:
        data = [str(x) for x in data]
        for i in range(len(data)):
            try:
                data[i] = int(float(data[i]))
            except:
                pass
        return data

    for kk, vv in data.items():
        try:
            data[kk] = int(float(vv))
        except:
            pass
    return data


def column_check(DB, table_name, new_list):
    msg = ""
    # DBからカラム情報取得。ない場合は追加する。
    set_new = set(new_list)
    sqls = "select * from %(table)s where rowid=(select max(rowid) from %(table)s) ;" % {'table': table_name}
    sql_pd = select_sql(DB, sqls)
    if len(sql_pd) > 0:
        db_list = set([i for i in sql_pd.columns if i != 'now'])
        diff = list(filter(lambda x: x not in db_list, set_new))
        for i in diff:
            dict = {'table': table_name, 'key1': i}
            sqls = "alter table %(table)s add column %(key1)s" % dict
            db_update(DB, sqls)
            msg += sqls + "\n"
        mail_send(u'カラム追加情報_' + table_name, msg)
        return 1
    return 0


def last_rowid(DB, table_name):
    sqls = "select rowid from %(table)s where rowid=(select max(rowid) from %(table)s) ;" % {'table': table_name}
    sql_pd = select_sql(DB, sqls)
    if len(sql_pd) > 0:
        return sql_pd.loc[0, 'rowid']
    else:
        return -1


def real_index(code):
    BYBY_DB = 'I01_all.sqlite'
    ua = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_3) ' 'AppleWebKit/537.36 (KHTML, like Gecko) ' 'Chrome/55.0.2883.95 Safari/537.36 '
    s = requests.session()
    s.headers.update({'User-Agent': ua})
    UURL = "https://www.bloomberg.co.jp/quote/" + code + ":IND"
    ret = s.get(UURL)
    soup = BeautifulSoup(ret.content, "lxml")
    stocktable = soup.find('div', {'class': 'price'})

    try:
        v = float(stocktable.text.replace(",", "").replace(".00", ""))
        sqls = "select %(key1)s,rowid from %(table)s where rowid=(select max(rowid) from %(table)s) ;" % {'table': 'rashio','key1':code +'_IND'}
        sql_pd = select_sql(BYBY_DB, sqls)
        #rashioテーブルがNULLだったら更新する。
        if sql_pd[code + '_IND'][0] is None:
            dict_w = {}
            dict_w[code + '_IND'] = v
            sqls = create_update_sql(BYBY_DB, dict_w, 'rashio', sql_pd['rowid'][0]) #最後の引数を削除すると自動的に最後の行
    except:
        #rashioテーブルのデータで代用
        sqls = "select %(key1)s from %(table)s where rowid=(select max(rowid) from %(table)s);" % {'table': 'rashio','key1':code +'_IND'}
        sql_pd = select_sql(BYBY_DB, sqls)
        v = float(sql_pd[code +'_IND'][0])
    return v

def stop_event_check(code):
    sqls = "select * from %(table)s where now like '%(key1)s' and コード = '%(key2)s'" % {'table': 'YAHOO_stophigh', 'key1': env_time()[1][0:10]+'%', 'key2': code}
    DB = save_path('I02_event.sqlite')
    sql_pd = select_sql(DB, sqls)
    if len(sql_pd) > 0:
        return 1
    return 0


def week_start_day():
    today = datetime.date.today()
    week_start = datetime.datetime.strptime(last_day(), '%Y/%m/%d')
    if today.weekday() < week_start.weekday():  # 週初
        return 1
    else:  # 週初以外
        return 0


def read_html2(UURL, header_cnt, skiprows_cnt=0):
    # HTTPError: HTTP Error 400: Bad Request 対策
    for i in range(3):
        try:
            dfs = pd.read_html(UURL, header=header_cnt, skiprows=skiprows_cnt)
            return dfs
        except:
            sleep(5)
    else:
        if UURL.count("https://info.finance.yahoo.co.jp/ranking") == False and UURL.count("https://www.barchart.com/stocks/quotes") == False and \
        UURL.count("http://www.stockboard.jp/flash/sel") == False:
            mail_send(u'URL更新失敗', UURL)
        return 0




def get_stock_usd(code,iconic='iex'):# iconic = ['iex','morningstar','robinhood']
    today = datetime.date.today()
    yest_day = today - datetime.timedelta(days=1100)
    list_w = ['date', 'open', 'high', 'low', 'close', 'volume']
    if iconic == 'morningstar':
        list_w = ['Open', 'High', 'Low', 'Close', 'Volume']
    if iconic == 'robinhood':
        list_w = ['open_price', 'high_price', 'low_price', 'close_price', 'volume']
    try:
        data_list = web.DataReader(code, iconic, yest_day, today)
        list_w = list(data_list.columns)
        if iconic == "morningstar" or iconic == "robinhood":
            date = [str(data[1])[:10].replace("-", "/") for data in data_list.index]
        else:
            date = [data.replace("-","/") for data in data_list.index]
        open = [round(float(data), 2) for data in data_list[list_w[0]]]
        high = [round(float(data), 2) for data in data_list[list_w[1]]]
        low = [round(float(data), 2) for data in data_list[list_w[2]]]
        close = [round(float(data), 2) for data in data_list[list_w[3]]]
        volume = [data for data in data_list[list_w[4]]]
        print("OK時系列取得", code)
        return 1, date[::-1], open[::-1], high[::-1], low[::-1], close[::-1], volume[::-1]
    except:
        print("NG時系列取得", code)
        return -1, 1, 1, 1, 1, 1, 1

def week_end_day():
    today = datetime.date.today()
    week_start = datetime.datetime.strptime(next_day(), '%Y/%m/%d')
    if today.weekday() > week_start.weekday():  # 週末
        return 1
    else:  # 週末以外
        return 0

def info_index():
    dict_w = {}
    # VIX取得
    code = "VIX"
    dict_w[code] = real_index(code)
    # TNX米10年国債
    code = "USGG10YR"
    dict_w[code] = real_index(code)
    # fxstreet_USDJPY
    dict_w['USDJPY'] = real_info('USD/JPY')
    dict_w['EURUSD'] = real_info('EUR/USD')
    return dict_w

def round_check(numbers,num=5):
    try:
        if str(numbers).count("."):
            numbers = float(numbers)
            return round(numbers,num)
    except:
        pass
    return numbers

def bloomberg_real():
    dict_w = {}
    BB = ["energy", "markets/commodities/futures/metals","markets/commodities/futures/agriculture"]
    for num in range(len(BB)):
        table_name = 'bloomberg_list'
        UURL = r"https://www.bloomberg.co.jp/" + BB[num]
        dfs = read_html2(UURL, 0)  # header=0,skiprows=0(省略可能)
        for i in range(len(dfs)):
            for idx, row in dfs[i].iterrows():
                for ii in range(len(row)):
                    if dfs[i].columns[ii] == "名称":
                        sp_msg = row[ii].split(" ")
                        name = str(sp_msg[1]).replace("(","").replace(")","").replace("/","")
                        if name in ['WTI原油','ブレント先物','金ドル','プラチナ米ドル']:
                            pass
                        else:
                            break
                    elif dfs[i].columns[ii] == "価格":
                        dict_w[name] = row[ii]
    return dict_w

def real_info(code):
    sqls = "select now,\"" + code + "\" from gmofx where rowid=(select max(rowid) from gmofx) ;"
    sql_pd = select_sql('I07_fx.sqlite', sqls)
    return sql_pd[code][0]

def Chorme_get(UURL):
    try:
        options = Options()
        # ヘッドレスモードを有効にする（次の行をコメントアウトすると画面が表示される）。
        options.add_argument('--headless')
        # ChromeのWebDriverオブジェクトを作成する。
        browser = webdriver.Chrome(chrome_options=options)
        browser.get(UURL)
        ret = browser.page_source
        browser.quit()
        return ret
    except:
        browser.quit()
        mail_send(u'Chorme_get_エラー発生', UURL)
        return -1

# 株価のデータ取得（銘柄コード, 開始日, 終了日）
def get_stock(code, start_date, end_date):
    try:
        aaa = int(code)
    except:
        # 米国株
        print("米国")
        result, date, open1, high, low, close, volume = get_stock_usd(code)
        return result, date, open1, high, low, close, volume, 0

    try:
        # 期間設定
        start_date = str(start_date).replace("-","/")
        end_date = str(end_date).replace("-","/")
        year, month, day = start_date.split("/")
        start = datetime.date(int(year), int(month), int(day))
        year, month, day = end_date.split("/")
        end = datetime.date(int(year), int(month), int(day))
        # 株価データ取得
        q = jsm.Quotes()
        target = q.get_historical_prices(code, jsm.DAILY, start_date=start, end_date=end)
        # 項目ごとにリストに格納して返す
        date = [data.date.strftime("%Y/%m/%d") for data in target]
        open = [data.open for data in target]
        high = [data.high for data in target]
        low = [data.low for data in target]
        close = [data.close for data in target]
        volume = [data.volume for data in target]
        adj_close = [data._adj_close for data in target]
        return 1, date, open, high, low, close, volume, adj_close
    except:
        return -1, 1, 1, 1, 1, 1, 1, 1

def sum_update(DB):
    # 全テーブル情報取得
    sqls = "select name from sqlite_master where type='table'"
    sql_pd = select_sql(DB, sqls)
    for i, rrow in sql_pd.iterrows():
        table_name = rrow['name']
        cnt = 0
        # ゴムは小数点まで
        if table_name.count("GOMU"):
            cnt = 1
        sqls = "select *,rowid from %(table)s where rowid=(select max(rowid) from %(table)s) ;" % {'table': table_name}
        sql_pd2 = select_sql(DB, sqls)
        for row_name in ["PL","L_PL","S_PL","S_PL_M","S_PL_N"]:
            if row_name in sql_pd2.columns:
                print(table_name, row_name, row_name.replace("PL","SUM"))
                sum_clce(DB, table_name, row_name, row_name.replace("PL","SUM"), cnt)

def get_stock3(code, S_DATA, E_DATA,day_chk = None):
    today = env_time()[1][0:10]
    S_DATA = str(S_DATA).replace("-","/")
    E_DATA = str(E_DATA).replace("-","/")
    l_D = [];l_O = [];l_H = [];l_L = [];l_C = [];l_V = [];l_AC = []
    try:
        aaa = int(code)
        contory = 'jp'
    except:
        contory = 'us'
    # 期間設定
    file_path = os.path.join(RUBY_DATA, contory, str(code)) + ".txt"
    print(file_path)
    # ファイルの更新日時チェック
    if os.path.exists(file_path):
        f = open(file_path, 'r', encoding=check_encoding(file_path))
        dataReader = csv.reader(f)
        for row in dataReader:
            last_row = row
            if str(S_DATA) <= str(row[0]) <= str(E_DATA):
                l_D.append(row[0])
                l_O.append(int(float(row[1])))
                l_H.append(int(float(row[2])))
                l_L.append(int(float(row[3])))
                l_C.append(int(float(row[4])))
                l_V.append(int(float(row[5])))
                l_AC.append(int(float(row[6])))
                today_temp = row[0]
        if day_chk is None or today == today_temp :
            return 1, l_D[::-1], l_O[::-1], l_H[::-1], l_L[::-1], l_C[::-1], l_V[::-1], l_AC[::-1]

    result, l_D, l_O, l_H, l_L, l_C, l_V, l_AC = get_stock(code, S_DATA, E_DATA)
    return result, l_D, l_O, l_H, l_L, l_C, l_V, l_AC

def get_stock4(code, S_DATA, E_DATA, day_chk=None):
    code = str(code)
    S_DATA = str(S_DATA).replace("-","/")
    E_DATA = str(E_DATA).replace("-","/")
    try:
        aaa = int(code)
        contory = 'jp'
    except:
        contory = 'us'
    file_path = os.path.join(RUBY_DATA, contory, str(code)) + ".txt"
    # ファイルの更新日時チェック
    if os.path.exists(file_path):
        df = pd.DataFrame(index=pd.date_range(str(S_DATA), str(E_DATA)))
        df = df.join(pd.read_csv(file_path,index_col=0, parse_dates=True, encoding="cp932", header=None))
        df = df.dropna()
        df.columns = ['l_O', 'l_H', 'l_L', 'l_C', 'l_V', 'l_AC', 'l_SS'][:len(df.columns)]
        df = df.astype(int)
        df = df.sort_index(ascending=False)

#        for i in df.index:
#            df = df.rename(index={i: str(i)[:10].replace("-", "/")})
#            df['l_D'] = df.rename(index={i: str(i)[:10].replace("-", "/")})
#        print(df.index)

        today = str(df.index[0])[0:10].replace("-","/")
        # 当日ファイルの更新日時チェック
        if day_chk is None or env_time()[1][0:10] == today:
            return 1, df
    #別の方法でデータ取得
    result, l_D, l_O, l_H, l_L, l_C, l_V, l_AC = get_stock(code, S_DATA, E_DATA)
    if result == 1:
        df = pd.DataFrame({'l_D':l_D,'l_O':l_O,'l_H':l_H,'l_L':l_L,'l_C':l_C,'l_V':l_V,'l_AC':l_AC})
        df = df.set_index('l_D')

    return result, pd.DataFrame({})


if __name__ == "__main__":
    code = 9449
    today = datetime.date.today()
    yest_day = today - datetime.timedelta(days=50)
    today = today - datetime.timedelta(days=40)

    print(env_time()[1])
    result, df = get_stock4(code, '2017/03/10', '2017/03/20')
    print(df.index[0],df['l_O'][0])
#    print(df.index,df['l_O'][0])

    print(__file__)
