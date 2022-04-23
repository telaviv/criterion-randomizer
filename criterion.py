import ipdb

from bs4 import BeautifulSoup
import random
import re
import requests
import sqlite3
from termcolor import colored
import time

link_match = re.compile(r'https://www.criterionchannel.com/(.*)')

column_types = [
    'id integer primary key autoincrement',
    'tag text unique',
    'movie_id integer',
    'title text',
    'duration text',
    'created_at integer',
    'selected_at integer',
    'watched_at integer',
    'hydrated_at integer',
    'updated_at integer',
]

def select_random_movie():
    url = 'https://films.criterionchannel.com'
    directory_html = requests.get(url).text
    directory_soup = BeautifulSoup(directory_html, 'html.parser')
    links = [v.attrs['data-href'] for v in directory_soup.find_all(class_='criterion-channel__tr')]
    link = random.choice(links)

    film_html = requests.get(link).text
    film_soup = BeautifulSoup(film_html, 'html.parser')
    movie_id = film_soup.find(class_='js-collection-item').attrs['data-item-id']
    title = film_soup.find(class_='collection-title').text.strip()
    duration = film_soup.find(class_='duration-container').text.strip()
    link_tag = link_match.match(film_soup.find(rel='canonical').attrs['href']).group(1)
    return {'movie_id': movie_id, 'title': title, 'duration': duration, 'link_tag': link_tag}

def get_tags_from_criterion():
    url = 'https://films.criterionchannel.com'
    directory_html = requests.get(url).text
    directory_soup = BeautifulSoup(directory_html, 'html.parser')
    return [link_match.match(v.find('a').attrs['href']).group(1) for v in directory_soup.find_all(class_='criterion-channel__tr')]

def print_movie(movie):
    print(colored(movie['title'], 'blue') + '\t' + colored(movie['duration'], 'yellow'))

def randomize():
    movie = select_random_movie()
    print_movie(movie)

def get_cursor():
    return sqlite3.connect('criterion.db').cursor()

def initialize():
    cursor = get_cursor()
    cursor.execute(f'create table if not exists movies ({", ".join(column_types)});')
    return cursor

def add_tags_to_db(tags, cursor):
    now = get_time()
    for chunk in chunks(tags, 500):
        values = ', '.join([f"('{tag}', {now})" for tag in tags])
        cursor.execute(f'insert into movies(tag, created_at) values {values} on conflict(tag) do nothing;')

def get_time():
    return int(time.time())

def chunks(v, n):
    for i in range(0, len(v), n):
        yield v[i:i + n]

def add_tags_from_criterion():
    cursor = initialize()
    tags = get_tags_from_criterion()
    add_tags_to_db(tags, cursor)

def list_rows(cursor):
    for row in cursor.execute('select * from movies;'):
        print(row)


if __name__ == '__main__':
    cursor = initialize()
    list_rows(cursor)
