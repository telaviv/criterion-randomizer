import random
import requests
from termcolor import colored
from bs4 import BeautifulSoup

url = 'https://films.criterionchannel.com'
directory_html = requests.get(url).text
directory_soup = BeautifulSoup(directory_html, 'html.parser')
links = [v.attrs['data-href'] for v in directory_soup.find_all(class_='criterion-channel__tr')]
link = random.choice(links)

film_html = requests.get(link).text
film_soup = BeautifulSoup(film_html, 'html.parser')
title = film_soup.find(class_='collection-title').text.strip()
duration = film_soup.find(class_='duration-container').text.strip()

print(colored(title, 'blue') + '\t' + colored(duration, 'yellow'))
