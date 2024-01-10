import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import re

import gspread
from oauth2client.service_account import ServiceAccountCredentials
from gspread_dataframe import set_with_dataframe

from dotenv import load_dotenv
load_dotenv()
import os


def extract_maxfloor(floor_string):
    match = re.search(r'地上(\d+)階建', floor_string)
    try:
        if match:
            return int(match.group(1))
        else:
            return int(floor_string.replace('階建',''))
    except:
        return 0

# 「地下」という文字が含まれるときに地下階数を抽出する関数
def extract_basement(floor_string):
    match = re.search(r'地下(\d+)', floor_string)
    if match:
        return int(match.group(1))
    else:
        return 0

# 部屋の階数を抽出する関数
# 自分用のメモ：tiによってtbody_itemの[0]個目から順に代入される
def extract_floor(tbody_item):
    suumo_floor_raw = tbody_item.select('tr.js-cassette_link')[0].text.split()[0]
    try:
        suumo_floor = int(suumo_floor_raw.replace('階','').replace('B','-'))
    except:
        suumo_floor = 0
        print(suumo_floor_raw)
    return suumo_floor

def extract_rent(tbody_item):
    suumo_rent_raw = tbody_item.select('span.cassetteitem_other-emphasis, span.ui-text--bold')[0].text
    try:
        suumo_rent = float(suumo_rent_raw.replace('万円',''))
    except:
        suumo_rent = 0
        print(suumo_rent_raw)
    return suumo_rent

def extract_size(tbody_item):
    suumo_size_raw = tbody_item.select('span.cassetteitem_menseki')[0].text
    try:
        suumo_size = float(suumo_size_raw.replace('m2',''))
    except:
        suumo_size = 0
        print(suumo_size_raw)
    return suumo_size


SCOPES = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
SERVICE_ACCOUNT_FILE=""
credentials = ServiceAccountCredentials.from_json_keyfile_name(SERVICE_ACCOUNT_FILE,SCOPES)

gs = gspread.authorize(credentials)
SPREADSHEET_KEY = os.getenv('SPREADSHEET_KEY')
workbook = gs.open_by_key(SPREADSHEET_KEY)
worksheet = workbook.worksheet("DB")


REQUEST_URL = 'https://suumo.jp/jj/chintai/ichiran/FR301FC001/?ar=030&bs=040&pc=50&smk=r01&po1=25&po2=99&shkr1=03&shkr2=03&shkr3=03&shkr4=03&ekInput=25620&ta=13&kskbn=01&tj=40&nk=1&cb=0.0&ct=20.0&md=05&md=06&md=07&md=08&md=09&md=10&ts=1&ts=2&et=5&mb=0&mt=9999999&cn=9999999&fw2='
res = requests.get(REQUEST_URL)
res.encoding = "utf-8"


soup = BeautifulSoup(res.text, 'html.parser')
maxpage = int(soup.select("ol.pagination-parts a")[-1].text)


if res.status_code == 200:

    names = []
    addresses = []
    station_times = []
    ages = []
    maxfloors = []
    basements = []

    floors = []
    rents = []
    layouts = []
    sizes = []

    for k in range(maxpage):
        res = requests.get(REQUEST_URL+"&page="+str(k))
        res.encoding = "utf-8"
        soup = BeautifulSoup(res.text, "html.parser")
        suumo_cassetteitem_from_html = soup.select('div.cassetteitem')
        time.sleep(1)

        for sc in suumo_cassetteitem_from_html:
            suumo_name = sc.select('div.cassetteitem_content-title')[0].text
            suumo_address = sc.select("li.cassetteitem_detail-col1")[0].text
            suumo_station_time = sc.select('li.cassetteitem_detail-col2')[0].text.replace('\n',',').strip(',')
            suumo_age_raw, suumo_maxfloor_raw = sc.select("li.cassetteitem_detail-col3")[0].text.split()
            suumo_age = int(suumo_age_raw.replace('築','').replace('年','').replace('新','0'))
            suumo_maxfloor = extract_maxfloor(suumo_maxfloor_raw)
            suumo_basement = extract_basement(suumo_maxfloor_raw)

            tbody_items = sc.select('tbody')

            for ti in tbody_items:
                suumo_floor = extract_floor(ti)
                suumo_rent = extract_rent(ti)
                suumo_layout = ti.select('span.cassetteitem_madori')[0].text
                suumo_size = extract_size(ti)

                names.append(suumo_name)
                addresses.append(suumo_address)
                station_times.append(suumo_station_time)
                ages.append(suumo_age)
                maxfloors.append(suumo_maxfloor)
                basements.append(suumo_basement)

                floors.append(suumo_floor)
                rents.append(suumo_rent)
                layouts.append(suumo_layout)
                sizes.append(suumo_size)

    data = {
        'Name': names,
        'Address': addresses,
        'Station_Times': station_times,
        'Age': ages,
        'Max_Floor': maxfloors,
        'Basement': basements,
        'Floor': floors,
        'Rent': rents,
        'Layout': layouts,
        'Size': sizes
    }
    df = pd.DataFrame(data)


df.drop_duplicates(subset=['Address', 'Floor', 'Rent', 'Layout', 'Size'], inplace=True)

values = [df.columns.values.tolist()] + df.values.tolist()

worksheet.update("A1", values)