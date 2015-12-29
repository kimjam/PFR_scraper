def te_scraper(te_dict, target_date):
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

        vals = pd.DataFrame(columns=clmheaders[0:13])
        breaks = np.linspace(0, len(data), (rows+1), dtype="int16")

        if rows == 1:
            for i in range(1):
                vals.loc[i] = [d for d in data[breaks[i]:breaks[i+1]]][0:13]
        else:
            for i in range(rows-1):
                vals.loc[i] = [d for d in data[breaks[i]:breaks[i+1]]][0:13]

        vals.insert(0, 'Name', name)

        fixyds = [i for i in range(vals.shape[1]) if vals.columns[i] == 'Yds']

        for f in fixyds:
            if vals.columns[f-1] == 'Rec':
                new_columns = vals.columns.values
                new_columns[f] = 'rec_Yds'
                vals.columns = new_columns

        fixtd = [i for i in range(vals.shape[1]) if vals.columns[i] == 'TD']

        for f in fixtd:
            if vals.columns[f-1] == 'Y/R':
                new_columns = vals.columns.values
                new_columns[f] = 'rec_TD'
                vals.columns = new_columns

        for c in vals.columns.values:
            if c not in attributes:
                vals = vals.drop([c], axis=1)

        if vals.shape[1] < 14:
            add = [c for c in attributes if c not in vals.columns]
            for col in add:
                vals[col] = pd.Series([0])

        vals = vals[attributes]
        return vals

    attributes = [
        'Name', 'Rk', 'G#', 'Date', 'Age', 'Tm', 'Home',
        'Opp', 'Result', 'Tgt', 'Rec', 'rec_Yds', 'Y/R', 'rec_TD'
    ]
    TES = pd.DataFrame(columns=attributes)

    for key in te_dict:
        print key
        p = PlayerScrape(key, te_dict[key], attributes)
        TES = TES.append(p)
        print key

    target_date = pd.to_datetime(target_date)
    TES = TES.fillna(0)
    TES['Home'] = TES['Home'].replace('@', 0).replace('', 0)
    TES['Date'] = TES['Date'].apply(pd.to_datetime)
    TES = TES.drop(['Rk', 'Age', 'Result', 'Y/R'], axis=1)
    TES.columns = [
        'name', 'g', 'date', 'team', 'home', 'opp', 'tgt',
        'rec', 'rec_yds', 'rec_td'
    ]
    TES = TES.replace('', 0)
    TES[['tgt', 'rec', 'rec_yds', 'rec_td']] = TES[
        ['tgt', 'rec', 'rec_yds', 'rec_td']
    ].astype(int)
    TES['pts'] = TES['rec_yds'] / 10. + TES['rec'] * .5 + TES['rec_td'] * 6

    TES = TES[TES.date >= target_date]
    TES['date'] = TES['date'].apply(lambda x: x.strftime('%Y-%m-%d'))
    TES['name'] = TES['name'].apply(lambda x: x.replace("\'", "").lower())

    f = open('secret.txt', 'r')
    secret = f.read()

    connect_string = 'mysql+pymysql://root:%s@127.0.0.1/nfl?charset=utf8mb4'
    connect_string = connect_string % (secret)
    engine = sqlalchemy.create_engine(connect_string, echo=False)
    TES.to_sql(con=engine, name='te', if_exists='append', index=False)
