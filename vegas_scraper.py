import requests
from bs4 import BeautifulSoup
import re
import numpy as np
import pandas as pd
import itertools
import pymysql
import sqlalchemy

def vegas_scraper(year, target_date):
	def schedscrape(url):
		r = requests.get(url)
		soup = BeautifulSoup(r.content)

		dates = []
		table = soup.find('table', {'id': 'team_gamelogs'})
		regex = r'<td align="left" csk="(.*)">.*</td>'
		for row in table.find_all('tr')[2:]:
			cols = row.find_all('td', {'align': 'left'})
			filtered = [re.compile(regex).findall(str(col)) for col in cols]
			chain = itertools.chain.from_iterable(filtered)
			date = list(chain)
			if date:
				dates.append(date[0])

		return dates

	def pagescrape(url, team_dict):
		r = requests.get(url)
		soup = BeautifulSoup(r.content)
		
		regex = r'http://www.pro-football-reference.com/teams/(.*)/.*_lines.htm'
		team = re.compile(regex).findall(url)
		team = str(team[0])
		team = team_dict[team]
		table = soup.find(class_ = 'stats_table')
		data = []
		for row in table.find_all('tr')[1:]:
			col = row.find_all('td')[0:16]
			if len(col) > 0:
				for c in col:
					data.append(c.text)
					
		breaks = np.linspace(0,96,7,dtype = int)
		ind = ['Opp', 'Spread', 'Over/Under', 'Result', 'vs. Line', 'OU Result']
		vals = pd.DataFrame(index = ind ,columns=range(1,17))
		
		for i in range(6):
			vals.ix[i] = [d for d in data[breaks[i]:breaks[i+1]]]
			
		vals = vals.transpose()
		vals['Opp'] = vals['Opp'].map(lambda x: x.lstrip('@'))
		vals['Team'] = team

		sched_url = url.replace('lines', 'games')
		vals['Date'] = schedscrape(sched_url)
		vals['Date'] = vals['Date'].apply(lambda x: pd.to_datetime(x).date())

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

	headers = ['Opp', 'Spread', 'Over/Under', 'Result', 'vs. Line', 'OU Result']

	lines = pd.DataFrame(columns = headers)

	for link in links:
		v = pagescrape(link, team_dict)
		lines = lines.append(v)

	lines = lines[lines.Date >= target_date]

	f = open('secret.txt', 'r')
	secret = f.read()

	connect_string = 'mysql+pymysql://root:%s@127.0.0.1/nfl?charset=utf8mb4' % (secret)
	engine = sqlalchemy.create_engine(connect_string, echo = False)

	lines.to_sql(con = engine, name = 'vegas2', if_exists = 'append', index = False)

	print 'Loaded into database.'

if __name__ == '__main__':
	import sys
	vegas_scraper(year = sys.argv[1], target_date = sys.argv[2])