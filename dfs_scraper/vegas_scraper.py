import requests
from bs4 import BeautifulSoup
import re
import pandas as pd
import itertools
import sqlalchemy


def vegas_scraper(year, target_date, week):
    def schedscrape(url):
        r = requests.get(url)
        soup = BeautifulSoup(r.content)

        dates = []
        home = []
        table = soup.find('table', {'id': 'team_gamelogs'})
        date_regex = r'<td align="left" csk="(.*)">.*</td>'
        home_regex = r'<td align="right">(.*)</td>'
        for row in table.find_all('tr')[2:]:
            cols = row.find_all('td')
            dates.append(re.compile(date_regex).findall(str(cols[2])))
            h = re.compile(home_regex).findall(str(cols[7]))[0]
            if h:
                h = 0
            else:
                h = 1
            home.append(h)

        remove = [ind for ind, val in enumerate(dates) if val == []]
        chain = itertools.chain.from_iterable(dates)
        dates = list(chain)

        for ind in remove:
            del home[ind]

        return dates, home

    def pagescrape(url, team_dict, week):
        r = requests.get(url)
        soup = BeautifulSoup(r.content)

        regex = r'http://www.pro-football-reference.com/teams/(.*)\
            /.*_lines.htm'
        team = re.compile(regex).findall(url)
        team = str(team[0])
        team = team_dict[team]
        table = soup.find(class_='stats_table')
        data = []
        for row in table.find_all('tr')[1:]:
            col = row.find_all('td')[0:16]
            if len(col) > 0:
                for c in col:
                    data.append(c.text)

        ind = [
            'Opp', 'Spread', 'Over/Under', 'Result', 'vs. Line', 'OU Result'
        ]
        vals = pd.DataFrame(index=ind, columns=range(1))

        data_inds = [num + (week - 1) * 6 for num in range(6)]

        if week == 1:
            for i in data_inds:
                vals.ix[i] = pd.Series(data)[i]
        else:
            for i in data_inds:
                vals.ix[i - ((week - 1) * 6)] = pd.Series(data)[i]

        vals = vals.transpose()
        vals['Opp'] = vals['Opp'].map(lambda x: x.lstrip('@'))
        vals['Team'] = team

        sched_url = url.replace('lines', 'games')
        dates, homes = schedscrape(sched_url)
        vals['Date'] = dates[week - 1]
        vals['Date'] = vals['Date'].apply(lambda x: pd.to_datetime(x).date())
        vals['Home'] = homes[week - 1]

        return vals

    team_dict = {
        'crd': 'ARI', 'atl': 'ATL', 'rav': 'BAL', 'buf': 'BUF', 'car': 'CAR',
        'chi': 'CHI', 'cin': 'CIN', 'cle': 'CLE', 'dal': 'DAL', 'den': 'DEN',
        'det': 'DET', 'gnb': 'GNB', 'htx': 'HOU', 'clt': 'IND', 'jax': 'JAX',
        'kan': 'KAN', 'mia': 'MIA', 'min': 'MIN', 'nor': 'NOR', 'nwe': 'NWE',
        'nyg': 'NYG', 'nyj': 'NYJ', 'rai': 'OAK', 'phi': 'PHI', 'pit': 'PIT',
        'sdg': 'SDG', 'sea': 'SEA', 'sfo': 'SFO', 'ram': 'STL', 'tam': 'TAM',
        'oti': 'TEN', 'was': 'WAS'
    }

    u = 'http://www.pro-football-reference.com/teams/'
    end = '_lines.htm'

    target_date = pd.to_datetime(target_date).date()
    links = [u + key + '/' + str(year) + end for key in team_dict.keys()]

    headers = [
        'Opp', 'Spread', 'Over/Under', 'Result', 'vs. Line', 'OU Result'
    ]

    lines = pd.DataFrame(columns=headers)

    for link in links:
        v = pagescrape(link, team_dict, week)
        lines = lines.append(v)

    lines = lines[lines.Date >= target_date]

    # return lines
    f = open('secret.txt', 'r')
    secret = f.read()

    connect_string = 'mysql+pymysql://root:%s@127.0.0.1/nfl?charset=utf8mb4'
    connect_string = connect_string % (secret)
    engine = sqlalchemy.create_engine(connect_string, echo=False)

    lines.to_sql(
        con=engine,
        name='historic_vegas',
        if_exists='append',
        index=False
    )

    print 'Loaded into database.'

if __name__ == '__main__':
    import sys
    vegas_scraper(year=sys.argv[1], target_date=sys.argv[2])
