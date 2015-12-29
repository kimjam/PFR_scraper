import json
import urllib
import pandas as pd
import re
import sqlalchemy
import os


def espn_parser(week, url=None, filename=None, dbload=True):
    """
    url to kimonolab api
    filename if kimono is not working
    week current week
    """
    teams = [
        'Eagles', 'Redskins', 'Giants', 'Cowboys',
        'Panthers', 'Buccaneers', 'Saints', 'Falcons',
        'Packers', 'Bears', 'Vikings', 'Lions',
        '49ers', 'Seahawks', 'Rams', 'Cardinals',
        'Patriots', 'Jets', 'Bills', 'Dolphins',
        'Colts', 'Texans', 'Jaguars', 'Titans',
        'Chiefs', 'Raiders', 'Broncos', 'Chargers',
        'Steelers', 'Browns', 'Ravens', 'Bengals'
    ]
    teams = [' '.join([team, 'D/ST']) for team in teams]

    if url:
        results = json.load(urllib.urlopen(url))
        data = results['results']['collection1']

        names = [
            player['player']['text'].split(',')[0]
            for player in data
        ]

        names = [
            name.split(' ')[0]
            if name in teams
            else name
            for name in names
        ]
        names = [re.sub('\xa0D/ST', '', name) for name in names]

        positions = [player['player']['text'] for player in data]
        positions = [re.sub('\xa0D/ST', '', player) for player in positions]
        positions = [re.sub('\xa0', ' ', player) for player in positions]
        positions = [
            player.split(',')[1].split(' ')[2]
            if player not in teams
            else u'DST'
            for player in positions
        ]

        projections = [float(proj['projection']) for proj in data]

    if filename:
        os.chdir('..')
        os.chdir('fantasy_sports/nfl/raw_data/espn')
        data = pd.read_csv(filename)
        # data = data[data.projection != '--']

        names = [
            player.split(',')[0]
            for player in data['player'].tolist()
            ]

        names = [
            name.split(' ')[0]
            if name in teams
            else name
            for name in names
        ]
        names = [re.sub('\xa0D/ST', '', name) for name in names]

        positions = [player for player in data['player'].tolist()]
        positions = [re.sub('\xa0D/ST', '', player) for player in positions]
        positions = [re.sub('\xa0', ' ', player) for player in positions]
        positions = [
            player.split(',')[1].split(' ')[2]
            if player not in teams
            else u'DST'
            for player in positions
        ]

        projections = [
            float(proj)
            for proj in data['projection'].tolist()
            ]

        os.chdir('..')
        os.chdir('..')
        os.chdir('..')
        os.chdir('..')
        os.chdir('dfs_scraper')

    parsed_data = {
        'name': pd.Series(names),
        'position': pd.Series(positions),
        'projection': pd.Series(projections),
        'week': week
    }

    parsed_df = pd.DataFrame(parsed_data)

    if dbload:
        f = open('secret.txt', 'r')
        secret = f.read()

        con_string = 'mysql+pymysql://root:%s@127.0.0.1/nfl?charset=utf8mb4'
        con_string = con_string % (secret)
        engine = sqlalchemy.create_engine(con_string, echo=False)

        parsed_df.to_sql(
            con=engine,
            name='espn',
            if_exists='append',
            index=False
        )

    return parsed_df
