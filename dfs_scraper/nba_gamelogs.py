import requests
import pandas as pd
import urllib
from collections import OrderedDict
from datetime import datetime
from datetime import timedelta
from datetime import date
import sqlalchemy


def nba_scraper(daily=False, season='2015-16'):
    url = 'http://stats.nba.com/stats/leaguegamelog?'
    parameters = {
        'Counter': 1000,
        'Direction': 'DESC',
        'LeagueID': '00',
        'PlayerOrTeam': 'P',
        'Season': season,
        'SeasonType': 'Regular Season',
        'Sorter': 'PTS'
    }

    parameters = OrderedDict(sorted(parameters.items()))
    url += urllib.urlencode(parameters)

    r = requests.get(url)
    data = r.json()

    columns = [col.lower() for col in data['resultSets'][0]['headers']]
    df = pd.DataFrame(data['resultSets'][0]['rowSet'], columns=columns)

    if daily:
        cutoff = date.strftime(
            datetime.now() - timedelta(days=1),
            format='%Y-%m-%d'
        )

        df = df[df.game_date >= cutoff]

    # clean data
    home = df['matchup'].apply(lambda x: x.split(' ')[1])
    df['home'] = home.replace(to_replace=['vs.', '@'], value=[1, 0])
    df['wl'] = df['wl'].replace(to_replace=['W', 'L'], value=[1, 0])
    df['opp'] = df['matchup'].apply(lambda x: x.split(' ')[2])

    def double(cat):
        return cat > 9

    cats = df[['pts', 'reb', 'ast', 'blk', 'stl']]
    cats = cats.apply(func=double, axis=0)
    cats['n_dub'] = cats.sum(axis=1)

    df['dub_dub'] = cats['n_dub'].apply(lambda x: int(bool(x > 1)))
    df['trip_dub'] = cats['n_dub'].apply(lambda x: int(bool(x > 2)))
    df['dk_pts'] = (
        df['pts'] + df['fg3m'] * .5 + df['reb'] * 1.25 + df['ast'] * 1.5 +
        df['stl'] * 2 + df['blk'] * 2 - df['tov'] * .5 + df['dub_dub'] * 1.5 +
        df['trip_dub'] * 3
    )

    cols = df.columns.tolist()
    cols[cols.index('team_abbreviation')] = u'team'
    df.columns = cols

    keep = [col for col in cols if col not in ['video_available', 'matchup']]

    df = df[keep]

    f = open('secret.txt', 'r')
    secret = f.read()

    connect_string = 'mysql+pymysql://root:%s@127.0.0.1/nba?charset=utf8mb4'
    connect_string = connect_string % (secret)
    engine = sqlalchemy.create_engine(connect_string, echo=False)
    df.to_sql(con=engine, name='game_logs', if_exists='append', index=False)

    return df
