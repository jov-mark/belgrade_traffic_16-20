import time, os
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup
import csv
import re

def generate_dates():
	year = [2015,2016,2017,2018,2019,2020]
	months = [(1,31),(2,28),(3,31),(4,30),(5,31),(6,30),(7,31),(8,31),(9,30),(10,31),(11,30),(12,31)]
	dates = []
	for y in year:
		for m,ds in months:
			form = ("%s-%s-"%(y,m))
			for d in range(1,ds+1):
				date = form + str(d)
				dates.append(date)
			if (y==2016 or y==2020) and m==2:
				date = form + str(d+1)
				dates.append(date)	
	return dates

def write_dict(file, columns, lst, fun):
	with open(file, fun, newline='') as f:
	    writer = csv.DictWriter(f, fieldnames=columns)

	    if fun=='w':
	    	writer.writeheader()
	    for data in lst:
	    	writer.writerow(data)


def parse_summary(table_summary, fun):
	values = ['temp', 'precipitation','wind','pressure']
	replace = [('day average','average'),('wind','max wind'),('precipitation','precipitation'),('sea level pressure','pressure')]

	summary = []
	date_summary = {"date":date}

	tbs = table_summary.find_all("tbody")
	for tbody in tbs:
		for tr in tbody.find_all("tr"):
			th = tr.find('th').text.lower()
			for v in values:
				tds = tr.find_all('td')
				if v in th:
					for exp in replace:
						if exp[0] in th:
							th = exp[1]
					date_summary.update({th:tds[0].text})
					date_summary.update({th+" historic":tds[1].text})
					break
				elif 'time' in th:
					date_summary.update({"sunrise":tds[1].text})
					date_summary.update({"sunset":tds[2].text})
					break

	summary.append(date_summary)

	write_dict(file_sum, list(date_summary.keys()), summary, fun)


def parse_observation(table_observation, fun):
	observations = []

	tbody = table_observation.find("tbody")
	for tr in tbody.find_all("tr"):
		obs = {"date":date}
		tds = tr.find_all("td")
		tds = tr.find_all("td")
		obs.update({"time":tds[0].text})
		obs.update({"temperature":tds[1].text[:-2]})
		obs.update({"humidity":tds[3].text[:-2]})
		obs.update({"wind":tds[5].text[:-4]})
		obs.update({"pressure":tds[7].text[:-3]})
		obs.update({"precipitation":tds[8].text[:-3]})
		obs.update({"condition":tds[9].text})
		observations.append(obs)

	write_dict(file_obs, list(obs.keys()), observations, fun)


file_sum = "summary.csv"
file_obs = "observations.csv"

url = "https://www.wunderground.com/history/daily/rs/sur%C4%8Din/LYBE/date/"
filename = "source"
page = ""

dates = generate_dates()
start = 52
end = len(dates)
fun = 'a'

opts = Options()
opts.add_argument("-headless")
chrome_driver = os.getcwd() + "\\chromedriver.exe"
driver = webdriver.Chrome(options = opts, executable_path=chrome_driver)

for date in dates[start:end]:
	print("Getting content for %s"%date)
	while True:
		driver.get(url+date)
		time.sleep(5)

		soup  = BeautifulSoup(driver.page_source,features="lxml")
		table_summary = soup.find("lib-city-history-summary").find("table") 
		table_observation = soup.find("lib-city-history-observation").find("table")
		
		if table_summary!=None and table_observation!=None:
			break

	parse_summary(table_summary,fun)
	parse_observation(table_observation,fun)

	# if fun=='w':	fun='a'

driver.quit()
print("Data for period between %s and %s is downloaded" %(dates[start],dates[end]))