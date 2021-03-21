import pandas as pd
from datetime import datetime
import re
from db_manager import DBManager
import pymysql
import os
from tqdm import tqdm
from data_parser import parse_price


def read_data(filename="2021/T210105.csv"):
    col_names = ["date", "code", "x", "name", "open", "high",
                 "low", "close", "vol", "type"]
    data = pd.read_csv(filename, encoding="shift_jis", header=None,
                       names=col_names)
    data['name'] = data['name'].apply(extract_name)
    data['date'] = data['date'].apply(format_date)
    data = data[['date', 'code', 'name', 'open', 'high',
                 'low', 'close', 'vol']]
    return data


def extract_name(pattern="6740　日デイスプレイ"):
    r_pattern = r"\d+(.*)"
    return re.findall(r_pattern, pattern)[0].strip()


def format_date(pattern="2021/1/4"):
    date = datetime.strptime(pattern, "%Y/%m/%d")
    return date.strftime("%Y-%m-%d")


def insert_db(db, data):
    for row, item in data.iterrows():
        # stock_id, name, price_at, open_val, high, low,
        # close_val, vol
        if item['vol'] == 0:
            continue
        try:
            db.insert_stock_log(str(item['code']), item['name'],
                                item['date'], item['open'],
                                item['high'], item['low'],
                                item['close'], item['vol'])
        except pymysql.IntegrityError:
            db.update_stock(str(item['code']), item['date'],
                            item['open'], item['high'], item['low'],
                            item['close'], item['vol'])
    db.commit()


def read_dir(db, path="2021"):
    for root, dirs, files in os.walk(path):
        for item in tqdm(files):
            target = f"{root}/{item}"
            insert_db(db, read_data(target))


def update2today():
    with DBManager() as db:
        latest_date = db.get_latest_date()
        now = datetime.now()
        for y in range(latest_date.year, now.year+1):
            for m in range(latest_date.month, now.month+1):
                for d in range(latest_date.da+1, now.day+1):
                    target = parse_price(y, m, d)
                    if target is not None:
                        print(f"Insert stock data @{y}-{m}-{d}")
                        insert_db(db, read_data(f"csv/{target}"))


def read_stock_df(db, stock_id, start=None, end=None):
    col = ['index', 'code_id', 'date', 'open', 'high', 'low', 'close', 'vol']
    rst = db.read_stock(stock_id=stock_id, start=start, end=end)
    df = pd.DataFrame(rst, columns=col)
    return df


if __name__ == '__main__':
    # rst = read_stock_df("8591")
    