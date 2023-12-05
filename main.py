import requests
from bs4 import BeautifulSoup
import pandas as pd
import time

import gspread
from oauth2client.service_account import ServiceAccountCredentials
from gspread_dataframe import set_with_dataframe

from dotenv import load_dotenv
load_dotenv()
import os


SCOPES = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
SERVICE_ACCOUNT_FILE=""
credentials = ServiceAccountCredentials.from_json_keyfile_name(SERVICE_ACCOUNT_FILE,SCOPES)

gs = gspread.authorize(credentials)
SPREADSHEET_KEY = os.getenv('SPREADSHEET_KEY')
workbook = gs.open_by_key(SPREADSHEET_KEY)
worksheet = workbook.worksheet("DB")


REQUEST_URL = 'https://suumo.jp/jj/chintai/ichiran/FR301FC001/?ar=030&bs=040&pc=30&smk=r01&po1=25&po2=99&shkr1=03&shkr2=03&shkr3=03&shkr4=03&ekInput=25620&ta=13&kskbn=01&tj=40&nk=1&cb=0.0&ct=20.0&md=05&md=06&md=07&md=08&md=09&md=10&ts=1&ts=2&et=10&mb=0&mt=9999999&cn=9999999&fw2='
res = requests.get(REQUEST_URL)
res.encoding = "utf-8"


soup = BeautifulSoup(res.text, 'html.parser')
maxpage = int(soup.select("ol.pagination-parts a")[-1].text)

suumo_cassetteitem_from_html = soup.select('div.cassetteitem')

if res.status_code == 200:

    names = []
    addresses = []
    station_times = []
    ages = []
    maxfloors = []

    floors = []
    rents = []
    layouts = []
    sizes = []

    for k in range(maxpage):
        res = requests.get(REQUEST_URL+"&page="+str(k))
        res.encoding = "utf-8"
        soup = BeautifulSoup(res.text, "html.parser")
        details = soup.select("div.cassetteitem")
        time.sleep(1)

        for sc in suumo_cassetteitem_from_html:
            suumo_name = sc.select('div.cassetteitem_content-title')[0].text
            suumo_address = sc.select("li.cassetteitem_detail-col1")[0].text
            suumo_station_time = sc.select('li.cassetteitem_detail-col2')[0].text.replace('\n',',').strip(',')
            suumo_age, suumo_maxfloor = sc.select("li.cassetteitem_detail-col3")[0].text.split()

            tbody_items = sc.select('tbody')

            for ti in tbody_items:
                suumo_floor = ti.select('tr.js-cassette_link')[0].text.split()[0]
                suumo_rent = ti.select('span.cassetteitem_other-emphasis, span.ui-text--bold')[0].text
                suumo_layout = ti.select('span.cassetteitem_madori')[0].text
                suumo_size = ti.select('span.cassetteitem_menseki')[0].text
                

                names.append(suumo_name)
                addresses.append(suumo_address)
                station_times.append(suumo_station_time)
                ages.append(suumo_age)
                maxfloors.append(suumo_maxfloor)

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
        'Floor': floors,
        'Rent': rents,
        'Layout': layouts,
        'Size': sizes
    }
    df = pd.DataFrame(data)


df.drop_duplicates(subset=['Address', 'Floor', 'Rent', 'Layout', 'Size'], inplace=True)

values = [df.columns.values.tolist()] + df.values.tolist()

worksheet.update("A1", values)