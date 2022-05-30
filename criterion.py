#!/usr/bin/env python3
import ipdb

import argparse
from datetime import datetime, timedelta
import os
import random
import re
import requests
import sqlite3
import time

from bs4 import BeautifulSoup
from termcolor import colored
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

link_match = re.compile(r'https://www.criterionchannel.com/(.*)')
DIRECTORY_FILENAME = 'criterion-directory.html'
DIRECTORY_URL = 'https://films.criterionchannel.com'


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



def get_criterion_directory_html():
    if is_directory_cached():
        return get_directory_from_cache()
    html = get_directory_from_criterion()
    save_directory_to_file(html)
    return html

def get_directory_from_criterion():
    return requests.get(DIRECTORY_URL, verify=False).text

def get_directory_from_cache():
    with open(DIRECTORY_FILENAME) as f:
        return f.read()

def save_directory_to_file(html):
    with open(DIRECTORY_FILENAME, 'w') as f:
        f.write(html)

def is_directory_cached():
    try:
        mtime = int(os.path.getmtime(DIRECTORY_FILENAME))
        current_time = get_time()
        return current_time - mtime <  60 * 60 * 24
    except OSError:
        return False


def get_tags_from_criterion():
    directory_html = get_criterion_directory_html()
    directory_soup = BeautifulSoup(directory_html, 'html.parser')
    return [link_match.match(v.find('a').attrs['href']).group(1) for v in directory_soup.find_all(class_='criterion-channel__tr')]

def print_movie(movie):
    print(colored(movie['title'], 'blue') + '\t' + colored(movie['duration'], 'yellow'))

def randomize():
    movie = select_random_movie()
    print_movie(movie)

def get_cursor():
    return sqlite3.connect('criterion.db', isolation_level='IMMEDIATE').cursor()

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

def add_tags_from_criterion(cursor):
    tags = get_tags_from_criterion()
    add_tags_to_db(tags, cursor)

def select_movie_to_watch(cursor):
    selected_movie = find_currently_selected_movie(cursor)
    if selected_movie is not None:
        resolve_selected_movie(selected_movie, cursor)
    query = 'select id, tag, hydrated_at from movies where watched_at is NULL;'
    result = cursor.execute(query).fetchall()
    id, tag, hydrated_at = random.choice(result)
    if hydrated_at is None:
        hydrate_movie(tag, cursor, selected=True)
        cursor.connection.commit()
    select_movie(id, cursor)
    movie = get_movie_data_by_id(id, cursor)
    return movie

def hydrate_movie(tag, cursor, selected=False):
    url = get_movie_url(tag)
    movie = get_movie_data_from_url(url)
    movie_id, title, duration = movie['movie_id'], movie['title'], movie['duration']
    now = get_time()
    cursor.execute(f"update movies "
                   f"set "
                   f"movie_id='{movie_id}', "
                   f"title='{title}', "
                   f"duration='{duration}', "
                   f"hydrated_at={now}, "
                   f"updated_at={now} "
                   f"where tag='{tag}';")

def get_movie_url(tag):
    return f'https://www.criterionchannel.com/{tag}'

def select_movie(id, cursor):
    now = get_time()
    cursor.execute(f'update movies set selected_at = {now} where id={id};')

def unselect_all_movies(cursor):
    cursor.execute(f'update movies set selected_at = null;')

def watch_movie(id, cursor):
    now = get_time()
    cursor.execute(f'update movies set watched_at = {now}, selected_at = null where id={id};')

def find_currently_selected_movie(cursor):
    query = f'select id, tag, title, duration, selected_at, watched_at from movies where selected_at is not null;'
    result = cursor.execute(query).fetchone()
    if result is None:
        return result

    return normalize_movie(result)

def normalize_movie(query_result):
    id, tag, title, duration, selected_at, watched_at = query_result;
    return {
        'id': id,
        'tag': tag,
        'title': title,
        'duration': duration,
        'selected_at': selected_at,
        'watched_at': watched_at,
    }

def resolve_selected_movie(movie, cursor):
    selection = None

    while selection not in ['1', '2', '3']:
        print(f"The movie \"{movie['title']}\" has already been selected.")
        print("Would you like to:")
        print("1) Mark the movie as watched")
        print("2) Unselect the movie")
        print("3) Cancel the operation")
        print()
        selection = input('🍿 => ')

    if selection == '1':
        watch_movie(movie['id'], cursor)
    elif selection == '2':
        unselect_all_movies(cursor)
    else:
        exit(0)

def display_all_watched_movies():
    cursor = initialize()
    for movie in get_all_watched_movies(cursor):
        print(movie['title'])

def get_all_watched_movies(cursor):
    query = f'''
    select id, tag, title, duration, selected_at, watched_at
    from movies
    where watched_at is not null
    order by watched_at desc;
    '''
    movies = []
    for movie_result in cursor.execute(query).fetchall():
        movies.append(normalize_movie(movie_result))
    return movies

def get_movie_data_from_url(url):
    film_html = requests.get(url, verify=False).text
    film_soup = BeautifulSoup(film_html, 'html.parser')
    movie_id = film_soup.find(class_='js-collection-item').attrs['data-item-id']
    title = film_soup.find(class_='collection-title').text.strip()
    duration = film_soup.find(class_='duration-container').text.strip()
    return {'movie_id': movie_id, 'title': title, 'duration': duration}

def get_movie_data_by_id(id, cursor):
    query = f'select id, tag, title, duration, selected_at, watched_at from movies where id = {id};'
    result = cursor.execute(query).fetchone()
    return normalize_movie(result)

def select_random_movie():
    cursor = initialize()
    add_tags_from_criterion(cursor)
    cursor.connection.commit()
    movie = select_movie_to_watch(cursor)
    cursor.connection.commit()
    print_movie(movie)

def parse_arguments():
    parser = argparse.ArgumentParser(description='Select a random criterion movie!')
    help_string = 'List all of your watched movies, most recent first.'
    parser.add_argument('-lw', '--list-watched', help=help_string, action='store_true')
    args = parser.parse_args()

    if args.list_watched:
        display_all_watched_movies()
    else:
        select_random_movie()

if __name__ == '__main__':
    parse_arguments()
