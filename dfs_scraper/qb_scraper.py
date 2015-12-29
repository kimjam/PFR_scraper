import requests
from bs4 import BeautifulSoup
import re
import numpy as np
import pandas as pd
import itertools
import sqlalchemy


def qb_scraper(qb_dict, target_date, dbload=True):
    def PlayerScrape(name, playerlink, final_headers):
        url = playerlink
        r = requests.get(url)
        soup = BeautifulSoup(r.content)
        data = []
        table = soup.find(class_='sortable stats_table row_summable')
        for row in table.find_all('tr')[1:]:
            col = row.find_all('td')
            if len(col) > 0:
                for c in col:
                    data.append(c.text)

        header = soup.find_all("tr", {"class": ''})[1]
        regex = r'<th .*>(.*)<\/th>'

        clmheaders = [re.compile(regex).findall(str(h)) for h in header]
        chain = itertools.chain.from_iterable(clmheaders)
        clmheaders = list(chain)
        clmheaders[5] = 'Home'

        cols = len(clmheaders)
        rows = len(data)/cols

        vals = pd.DataFrame(columns=clmheaders[0:22])
        breaks = np.linspace(0, len(data), (rows+1), dtype="int16")

        if rows == 1:
            for i in range(1):
                vals.loc[i] = [d for d in data[breaks[i]:breaks[i+1]]][0:22]
        else:
            for i in range(rows-1):
                vals.loc[i] = [d for d in data[breaks[i]:breaks[i+1]]][0:22]

        vals.insert(0, 'Name', name)
        if vals.columns[9] != 'Cmp' and vals.columns[10] != 'Cmp':
            return vals
        else:
            place = vals.shape[1]
            vals.columns = final_headers[0:place]

            start = vals.shape[1]
            end = len(final_headers)
            if vals.shape[1] < 23 and ('Cmp' in clmheaders[8:10]):
                for col in final_headers[start:end]:
                    vals[col] = pd.Series([0])

        vals = vals[final_headers]
        return vals

    clmheaders = [
        'Name', 'Rk', 'G#', 'Date', 'Age', 'Tm', 'Home', 'Opp',
        'Result', 'GS', 'Cmp', 'Att', 'Cmp%', 'Yds', 'TD', 'Int', 'Rate',
        'Y/A', 'AY/A', 'ru_Att', 'ru_Yds', 'ru_Y/A', 'ru_TD', 'Tgt', 'Rec',
        'rec_Yds', 'rec_Y/R', 'rec_TD'
    ]

    final = clmheaders[0:23]
    QBS = pd.DataFrame(columns=clmheaders[0:23])
    for key in qb_dict:
        print key
        p = PlayerScrape(
            name=key,
            playerlink=qb_dict[key],
            final_headers=final
        )
        if p.shape[1] == QBS.shape[1]:
            QBS = QBS.append(p)
        else:
            continue
        print key

    target_date = pd.to_datetime(target_date)
    QBS = QBS.fillna(0)
    QBS['Home'] = QBS['Home'].replace('@', 0).replace('', 1)
    QBS['GS'] = QBS['GS'].replace('*', 1).replace('', 0)
    QBS['Date'] = QBS['Date'].apply(pd.to_datetime)
    QBS = QBS.replace('', 0)
    QBS = QBS.drop(
        ['Rk', 'Age', 'Result', 'Cmp%', 'Rate', 'Y/A', 'AY/A', 'ru_Y/A'],
        axis=1
    )
    QBS.columns = [
        'name', 'g', 'date', 'team', 'home', 'opp', 'gs', 'cmp',
        'att', 'yds', 'td', 'ints', 'ru_att', 'ru_yds', 'ru_td'
    ]
    QBS[
        ['cmp', 'att', 'yds', 'td', 'ints', 'ru_att', 'ru_yds', 'ru_td']
    ] = QBS[
        ['cmp', 'att', 'yds', 'td', 'ints', 'ru_att', 'ru_yds', 'ru_td']
    ].astype(int)
    QBS['pts'] = (
        QBS['yds'] / 25. + QBS['ru_yds'] / 10. +
        QBS['td'] * 4. - QBS['ints'] + QBS['ru_td'] * 6.
    )

    QBS['pts'] = [round(x, 1) for x in QBS['pts']]

    QBS = QBS[QBS.date >= target_date]
    QBS['date'] = QBS['date'].apply(lambda x: x.strftime('%Y-%m-%d'))
    QBS['name'] = QBS['name'].apply(lambda x: x.replace("\'", "").lower())

    if dbload:
        f = open('secret.txt', 'r')
        secret = f.read()

        con_string = 'mysql+pymysql://root:%s@127.0.0.1/nfl?charset=utf8mb4'
        con_string = con_string % (secret)
        engine = sqlalchemy.create_engine(con_string, echo=False)
        QBS.to_sql(con=engine, name='qb', if_exists='append', index=False)

    return QBS
