import requests
from bs4 import BeautifulSoup
import re
import numpy as np
import pandas as pd
import itertools
import os
import pymysql
import sqlalchemy

def qb_scraper(qb_dict, target_date):
	def PlayerScrape(name, playerlink):
		url = playerlink
		r = requests.get(url)
		soup = BeautifulSoup(r.content)
		data=[]
		table = soup.find(class_ = 'sortable stats_table row_summable')
		#table = soup.find('table', {'id': 'stats'}) #class_ = 'sortable stats_table row_summable')
		for row in table.find_all('tr')[1:]:
			col = row.find_all('td')
			if len(col) > 0:
				for c in col:
					data.append(c.text)
		header = soup.find_all("tr", {"class":''})[1]
		regex = r'<th .*>(.*)<\/th>'
		clmheaders = [re.compile(regex).findall(str(h)) for h in header]
		chain = itertools.chain.from_iterable(clmheaders)
		clmheaders = list(chain)
		clmheaders[5] = 'Home'
		
		cols = len(clmheaders)
		rows = len(data)/cols
		
		vals = pd.DataFrame(index = [name]*(rows-1), columns=clmheaders[0:22])
		breaks = np.linspace(0,len(data), (rows+1), dtype="int16")
		
		for i in range(rows-1):
			vals.ix[i] = [d for d in data[breaks[i]:breaks[i+1]]][0:22]

		vals.index.name = 'Name'
		vals.reset_index(level = 0, inplace = True)
		return vals

	clmheaders = [
		'Name', 'Rk', 'G#', 'Date', 'Age', 'Tm', 'Home', 'Opp',
		'Result', 'GS', 'Cmp', 'Att', 'Cmp%', 'Yds', 'TD', 'Int', 'Rate', 'Y/A',
		'AY/A', 'Att', 'Yds', 'Y/A', 'TD', 'Tgt', 'Rec', 'Yds', 'Y/R', 'TD'
	]

	QBS = pd.DataFrame(columns = clmheaders[0:23])
	for key in qb_dict:
		print key
		p = PlayerScrape(key, qb_dict[key])
		if p.shape[1] == QBS.shape[1]:
			QBS = QBS.append(p)
		else:
			continue
		print key

	target_date = pd.to_datetime(target_date)
	#QBS.index.name = 'name'
	QBS = QBS.fillna(0)
	QBS['Home'] = QBS['Home'].replace('@', 0).replace('', 1)
	QBS['GS'] = QBS['GS'].replace('*', 1).replace('', 0)
	QBS['Date'] = QBS['Date'].apply(pd.to_datetime)
	QBS = QBS.replace('', 0)
	QBS = QBS.drop(['Rk', 'Age', 'Result', 'Cmp%', 'Rate', 'Y/A', 'AY/A'], axis = 1)
	QBS.columns = ['name', 'g', 'date', 'team', 'home', 'opp', 'gs', 'cmp',
		'att', 'yds', 'td', 'ints', 'ru_att', 'ru_yds', 'ru_td']
	QBS[['cmp', 'att', 'yds', 'td', 'ints', 'ru_att', 'ru_yds', 'ru_td']] = QBS[['cmp', 'att', 'yds', 'td', 'ints', 'ru_att', 'ru_yds', 'ru_td']].astype(int)
	QBS['pts'] = QBS['yds'] / 25 + QBS['ru_yds'] / 10 + QBS['td'] * 4 - QBS['ints'] + QBS['ru_td'] * 6
	QBS['pts'] = [round(x, 1) for x in QBS['pts']]

	QBS = QBS[QBS.date >= target_date]
	QBS['date'] = QBS['date'].apply(lambda x: x.strftime('%Y-%m-%d'))
	
	f = open('secret.txt', 'r')
	secret = f.read()

	connect_string = 'mysql+pymysql://root:%s@127.0.0.1/nfl?charset=utf8mb4' % (secret)
	engine = sqlalchemy.create_engine(connect_string, echo = False)
	QBS.to_sql(con = engine, name = 'qb', if_exists = 'append', index = False)