import requests
from collections import OrderedDict
from datetime import datetime
from datetime import timedelta
from datetime import date


# url = 'http://stats.nba.com/stats/leaguegamelog?Counter=1000&Direction=DESC&LeagueID=00&PlayerOrTeam=P&Season=2015-16&SeasonType=Regular+Season&Sorter=PTS'
def nba_scraper():
	url = 'http://stats.nba.com/stats/leaguegamelog?'
	parameters = {
		'Counter': 1000,
		'Direction': 'DESC',
		'LeagueID': '00',
		'PlayerOrTeam': 'P',
		'Season': '2015-16',
		'SeasonType': 'Regular Season',
		'Sorter': 'PTS'
	}

	parameters = OrderedDict(sorted(parameters.items()))
	url += urllib.urlencode(parameters)

	r = requests.get(url)
	data = r.json()

	columns = [col.lower() for col in data['resultSets'][0]['headers']]
	df = pd.DataFrame(data['resultSets'][0]['rowSet'], columns=columns)

	cutoff = date.strftime(
		datetime.now() - timedelta(days=1),
		format='%Y-%m-%d'
	)

	df = df[df.game_date >= cutoff]
	#add database loading
