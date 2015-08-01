import requests
from bs4 import BeautifulSoup
import re
import numpy as np
import pandas as pd
import itertools

def boxscore_scraper(url, target_week, year):
	def playerscraper(url, year):
		r = requests.get(url)
		soup = BeautifulSoup(r.content)
		
		table = soup.find('table', {'id': 'skill_stats'})
		players = table.find_all('td', {'align': 'left', 'style': 'border: 1px dotted #AAA !important;'})
		
		names = [p.text for p in players]
		
		regex = r'<a href="(.*)">.*<\/a>'
		links = [re.compile(regex).findall(str(p)) for p in players]
		chain = itertools.chain.from_iterable(links)
		links = list(chain)
		u = 'http://www.pro-football-reference.com'
		g = 'gamelog/'
		
		links = [u + l.replace('.htm', '/') + g + year for l in links]
		
		playerlinks = dict(zip(names, links))
		return(playerlinks)

	def positionscraper(url):
		r = requests.get(url)
		soup = BeautifulSoup(r.content)
		
		regex = r'<strong>Position:</strong>(.*)'
		pos = [re.compile(regex).findall(str(s)) for s in soup]
		
		chain = itertools.chain.from_iterable(pos)
		pos = list(chain)
		pos = filter(None, pos)
		if pos:
			pos = pos[0].replace(' ', '')
		else:
			pos = 'FIND ME BRUH'
		
		return(pos)

	# url = 'http://www.pro-football-reference.com/years/2014/games.htm'
	r = requests.get(url)
	soup = BeautifulSoup(r.content)

	tds = soup.find_all('td', {'align': 'center'})
	regex = r'<a href="(.*)">boxscore</a>'

	links = [re.compile(regex).findall(str(td)) for td in tds]
	chain = itertools.chain.from_iterable(links)
	links = list(chain)

	u = 'http://www.pro-football-reference.com'

	boxscorelinks = [u + l for l in links]

	tds = soup.find_all('td', {'align': 'right'})
	regex = r'<td align="right" csk=".*">(.*)</td>'

	weeks = [re.compile(regex).findall(str(td)) for td in tds]
	weeks = filter(None, weeks)
	chain = itertools.chain.from_iterable(weeks)
	weeks = list(chain)
	weeks = [week for week in weeks if week != '']

	boxscore = pd.DataFrame(zip(weeks, boxscorelinks), columns = ['week', 'boxscorelink'])
	boxscore = boxscore[boxscore.week == str(target_week)]

	qbs = {}
	rbs = {}
	wrs = {}
	tes = {}
	find = {}
	for link in boxscore['boxscorelink']:
		playerlinks = playerscraper(link, str(year))
		for key in playerlinks:
			pos = positionscraper(playerlinks[key])
			if pos == 'QB':
				print(key)
				qbs[key] = playerlinks[key]
			elif pos == 'RB' or pos == 'FB':
				print(key)
				rbs[key] = playerlinks[key]
			elif pos == 'WR':
				print(key)
				wrs[key] = playerlinks[key]
			elif pos == 'TE':
				print(key)
				tes[key] = playerlinks[key]
			else:
				print(key)
				find[key] = playerlinks[key]

	return {'qbs': qbs, 'rbs': rbs, 'wrs': wrs, 'tes': tes, 'find': find }