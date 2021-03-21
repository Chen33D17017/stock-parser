import requests
import os
import zipfile
import time
import random


def main():
    for m in range(1, 13):
        for d in range(1, 32):
            time.sleep(random.randrange(3, 5))
            parse_price(2014, m, d)


def get_name(y=2021, m=1, d=1):
    m = str(m).zfill(2)
    y = str(y)
    d = str(d).zfill(2)
    return f"{y}/{y[2:4]}_{m}/T{y[2:4]}{m}{d}"


def parse_price(y=2021, m=1, d=15):
    time = get_name(y, m, d)
    url_zip = f"https://mujinzou.com/k_data/{time}.zip"
    req_url_zip = requests.get(url_zip, timeout=3.5)
    if req_url_zip.status_code == 200:
        filename = os.path.basename(url_zip)
        with open(f"zip/{filename}", 'wb') as f:
            f.write(req_url_zip.content)

        with zipfile.ZipFile(f"zip/{filename}") as zf:
            zf.extractall("csv/")
        
        csv_file = filename.split('.')[0] + ".csv"
        return csv_file
    else:
        return None

if __name__ == '__main__':
    main()