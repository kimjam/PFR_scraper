def rb_scraper(rb_dict, target_date):
    def PlayerScrape(name, playerlink, attributes):
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

        vals = pd.DataFrame(columns=clmheaders[0:17])
        breaks = np.linspace(0, len(data), (rows+1), dtype="int16")

        if rows == 1:
            for i in range(1):
                vals.loc[i] = [d for d in data[breaks[i]:breaks[i+1]]][0:17]
        else:
            for i in range(rows-1):
                vals.loc[i] = [d for d in data[breaks[i]:breaks[i+1]]][0:17]

        vals.insert(0, 'Name', name)

        fixyds = [i for i in range(vals.shape[1]) if vals.columns[i] == 'Yds']

        for f in fixyds:
            if vals.columns[f-1] == 'Rec':
                new_columns = vals.columns.values
                new_columns[f] = 'rec_Yds'
                vals.columns = new_columns
            elif vals.columns[f-1] == 'Att':
                new_columns = vals.columns.values
                new_columns[f] = 'ru_Yds'
                vals.columns = new_columns

        fixtd = [i for i in range(vals.shape[1]) if vals.columns[i] == 'TD']

        for f in fixtd:
            if vals.columns[f-1] == 'Y/R':
                new_columns = vals.columns.values
                new_columns[f] = 'rec_TD'
                vals.columns = new_columns
            elif vals.columns[f-1] == 'Y/A':
                new_columns = vals.columns.values
                new_columns[f] = 'ru_TD'
                vals.columns = new_columns

        for c in vals.columns.values:
            if c not in attributes:
                vals = vals.drop([c], axis=1)

        if vals.shape[1] < 18:
            add = [c for c in attributes if c not in vals.columns]
            for col in add:
                vals[col] = pd.Series([0])

        vals = vals[attributes]
        return vals

    attributes = [
        'Name', 'Rk', 'G#', 'Date', 'Age', 'Tm', 'Home', 'Opp', 'Result',
        'Att', 'ru_Yds', 'Y/A', 'ru_TD', 'Tgt', 'Rec', 'rec_Yds', 'Y/R',
        'rec_TD'
    ]
    RBS = pd.DataFrame(columns=attributes)

    for key in rb_dict:
        print key
        p = PlayerScrape(key, rb_dict[key], attributes)
        if p.shape[1] == RBS.shape[1]:
            RBS = RBS.append(p)
            print key

    target_date = pd.to_datetime(target_date)
    RBS = RBS.fillna(0)
    RBS['Home'] = RBS['Home'].replace('@', 0).replace('', 1)
    RBS['Date'] = RBS['Date'].apply(pd.to_datetime)
    RBS = RBS.drop(['Rk', 'Age', 'Result', 'Y/A', 'Y/R'], axis=1)
    RBS = RBS.replace('', 0)
    RBS.columns = [
        'name', 'g', 'date', 'team', 'home', 'opp', 'att',
        'ru_yds', 'ru_td', 'tgt', 'rec', 'rec_yds', 'rec_td'
    ]

    RBS[['ru_yds', 'ru_td', 'rec', 'rec_yds', 'rec_td']] = RBS[
        [
            'ru_yds', 'ru_td', 'rec', 'rec_yds', 'rec_td'
        ]
    ].astype(int)
    RBS['pts'] = (
        (RBS['ru_yds'] + RBS['rec_yds']) / 10. +
        RBS['rec'] * .5 + (RBS['ru_td'] + RBS['rec_td']) * 6
    )

    RBS = RBS[RBS.date >= target_date]
    RBS['date'] = RBS['date'].apply(lambda x: x.strftime('%Y-%m-%d'))
    RBS['name'] = RBS['name'].apply(lambda x: x.replace("\'", "").lower())

    f = open('secret.txt', 'r')
    secret = f.read()

    connect_string = 'mysql+pymysql://root:%s@127.0.0.1/nfl?charset=utf8mb4'
    connect_string = connect_string % (secret)
    engine = sqlalchemy.create_engine(connect_string, echo=False)
    RBS.to_sql(con=engine, name='rb', if_exists='append', index=False)
