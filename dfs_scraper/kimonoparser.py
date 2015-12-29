import json
import urllib
import pandas as pd
import itertools
from time import mktime, strptime
from datetime import datetime
import sqlalchemy


def kimono_lines_parser(url, num_thurs=1, num_sat=0, num_mon=1, dbload=True):
    """
    url to kimonolabs api
    num_thurs number of thursday night games
    num_sat number of saturday games
    num_mon number of monday night games
    """
    results = json.loads(urllib.urlopen(url).read().decode("utf-8"))

    dates = results['results']['collection1']
    rows = results['results']['collection2']

    dates = [strptime(date['date'], '%B %d, %Y') for date in dates]

    dates = [
        datetime.strftime(datetime.fromtimestamp(mktime(date)), '%Y-%m-%d')
        for date in dates
    ]

    if len(dates) == 3:
        dates = [
            [dates[0]] * (2 * num_thurs),
            [dates[1]] * (len(rows) - 2 * (num_thurs + num_mon)),
            [dates[2]] * (2 * num_mon)
        ]
    elif len(dates) == 4:
        dates = [
            [dates[0]] * (2 * num_thurs),
            [dates[1]] * (2 * num_sat),
            [dates[2]] * (len(rows) - 2 * (num_thurs + num_sat + num_mon)),
            [dates[3]] * (2 * num_mon)
        ]

    chain = itertools.chain.from_iterable(dates)
    dates = list(chain)

    teams = [row['team'].split(' ')[-1] for row in rows]

    team_dict = {
        'Broncos': 'DEN', 'Chiefs': 'KAN', 'Texans': 'HOU', 'Panthers': 'CAR',
        'Buccaneers': 'TAM', 'Saints': 'NOR', '49ers': 'SFO', 'Bills': 'BUF',
        'Lions': 'DET', 'Vikings': 'MIN', 'Patriots': 'NWE', 'Steelers': 'PIT',
        'Cardinals': 'ARI', 'Bears': 'CHI', 'Titans': 'TEN', 'Browns': 'CLE',
        'Chargers': 'SDG', 'Bengals': 'CIN', 'Rams': 'STL', 'Redskins': 'WAS',
        'Falcons': 'ATL', 'Giants': 'NYG', 'Dolphins': 'MIA', 'Jaguars': 'JAX',
        'Cowboys': 'DAL', 'Eagles': 'PHI', 'Seahawks': 'SEA', 'Packers': 'GNB',
        'Jets': 'NYJ', 'Colts': 'IND', 'Raiders': 'OAK', 'Ravens': 'BAL'
    }

    teams = [team_dict[key] for key in teams]
    spread = [float(row['spread']['text'].split('\n')[0]) for row in rows]
    over_under = [
        float(row['over.under']['text'].split('\n')[0].split(' ')[1])
        for row in rows
    ]

    parsed_data = {
        'team': pd.Series(teams),
        'date': pd.Series(dates),
        'spread': pd.Series(spread),
        'over.under': pd.Series(over_under)
    }

    parsed_df = pd.DataFrame(parsed_data)
    parsed_df = parsed_df[['team', 'date', 'spread', 'over.under']]

    if dbload:
        f = open('secret.txt', 'r')
        secret = f.read()

        con_string = 'mysql+pymysql://root:%s@127.0.0.1/nfl?charset=utf8mb4'
        con_string = con_string % (secret)
        engine = sqlalchemy.create_engine(con_string, echo=False)

        parsed_df.to_sql(
            con=engine,
            name='sportsbook',
            if_exists='append',
            index=False
        )

    return parsed_df
