#!/usr/bin/env python3
import time
import requests
import sqlite3

from argparse import ArgumentParser
from datetime import datetime, timedelta, date
from sqlite3 import Error

"""
  Scans submissions and comments: collects ids with its dates and/or ranks by author
  'Count' and 'ranking' tables are ready for visualization
"""

parser = ArgumentParser()
parser.add_argument('-d', type=str, help='database to use', metavar='')
parser.add_argument('-r', type=str, required=True, help='subreddit to scan', metavar='')
parser.add_argument('--since', type=str, required=True, help='since what date to scan', metavar='')
parser.add_argument('--until', type=str, required=True, help='until what date scan', metavar='')
parser.add_argument('--user', type=str, default='Canillita', help='your username', metavar='')
args = parser.parse_args()

db_name = args.d or f"{args.r}.db"
subreddit = args.r

try:
  date_from  = datetime.strptime(args.since, '%Y-%m-%d')
  date_until = datetime.strptime(args.until, '%Y-%m-%d')
except ValueError:
  raise ValueError("Incorrect date format. Should be: YYYY-MM-DD")

if date_from > date_until:
  raise Exception("Start date (--since) can't be after finish date (--until)")

enable_raw_stats = True
enable_counting  = True
enable_ranking   = True

types = ['submission', 'comment']
url = "https://api.pushshift.io/reddit/{}/search?&limit=1000&sort=desc&subreddit={}&before={}"
headers = {'User-Agent': f"Object counter by u/{args.user}"}

columns = ['s', 'c'] # submissions, comments - columns names for counting

date_until += timedelta(days=1) # add 1 day so --until is included in time period
time_from  = int(date_from.strftime("%s"))
time_until = int(date_until.strftime("%s"))
time_diff  = time_until - time_from
hours_diff = (min(int(time.time()), time_until) - time_from) / 60 / 60
total_days = int((date_until - date_from).days)

requests_made   = 0
requests_failed = 0

con = sqlite3.connect(db_name)
db = con.cursor()

def create_table(table_definition):
  try:
    db.execute(f"CREATE TABLE {table_definition}")
    con.commit()
  except Error:
    pass

enable_raw_stats and create_table("raw(id text PRIMARY KEY, date text, type text)")
enable_counting  and create_table("count(date text PRIMARY KEY, s text, c text, total text, users text)")
enable_ranking   and create_table("ranking(user text PRIMARY KEY, s integer DEFAULT 0, c integer DEFAULT 0)")

print(f"Database: {db_name}")

def calc_percent(actual_time):
  actual_time = max(actual_time, time_from)
  return abs(float(actual_time - time_until) / float(time_diff))

def show_progress(type, time):
  percent = calc_percent(time)
  print("Scanning {}s: {:.2%}".format(type, percent), end="\r")

def print_msg(message):
  print(f"-> {message}")

def msg_finish_for(column):
  print(f"Finished processing column: {column}")

def range_dates():
  for n in range(total_days):
    yield date_from + timedelta(n)

def count_for(column):
  count = 1
  for day_obj in range_dates():
    show_progress(column, count)
    day = day_obj.strftime("%y-%m-%d")
    db.execute(f"UPDATE count SET {column} = (SELECT COUNT(date) FROM raw WHERE date = '{day}' AND type = '{column}' GROUP BY type) WHERE date = '{day}'")
    count += 1
  con.commit()
  msg_finish_for(column)

def calc_total():
  for day_obj in range_dates():
    day = day_obj.strftime("%y-%m-%d")
    db.execute(f"UPDATE count SET total = (SELECT COUNT(date) FROM raw WHERE date = '{day}') WHERE date = '{day}'")
  msg_finish_for("total")
  con.commit()

# collect data
for type in types:
  count = 0
  break_out = False
  previous_epoch = time_until

  # keep track of consecutive failed attemps
  cons_failed_connection_count = 0

  while True:
    show_progress(type, previous_epoch)
    actual_url = url.format(type, subreddit, previous_epoch)
    request = requests.get(actual_url, headers=headers)
    requests_made += 1

    # delay if server error
    # if request.status_code == 502:
    status = request.status_code # TODO

    if status != 200 and status != 403:
      delay = 5 * cons_failed_connection_count
      if status == 429: delay *= 2
      cons_failed_connection_count += 1
      requests_failed += 1
      print(f"Got {request}, retrying in {delay}s...", end="\x1b[1K\r")
      time.sleep(delay)
      continue

    cons_failed_connection_count = 0
    entries = request.json()['data']

    for entry in entries:
      entry_timestamp = entry['created_utc']
      previous_epoch = entry_timestamp - 1
      if previous_epoch < time_from:
        break_out = True
        break

      col = type[0]
      user = entry['author']

      if enable_raw_stats:
        query = "INSERT OR IGNORE INTO raw(id, date, type) VALUES (?, ?, ?);"
        date  = datetime.fromtimestamp(entry_timestamp).strftime('%y-%m-%d')
        entry_data = (entry['id'], date, type[0])
        db.execute(query, entry_data)

      if enable_ranking:
        # update existing row
        db.execute(f"UPDATE ranking SET {col} = {col} + 1 WHERE user='{user}';")
        # if no update (row didn't exists) then insert
        db.execute(f"INSERT INTO ranking (user, {col}) SELECT '{user}', 1 WHERE (Select Changes() = 0);")

      count += 1

    if break_out:
      print(f"r/{subreddit}: scanned {count} {type}s")
      con.commit()
      break

# when finished collecting data
print_msg(f"Data from {round(hours_diff, 2)}hs time period [{total_days} day{'s'[:total_days != 1]}]")
print_msg(f"API requests fail count: [{requests_failed}/{requests_made}]")
enable_ranking and print_msg("With user ranking")


if enable_counting:
  # fill date column
  for day_obj in range_dates():
    day = day_obj.strftime("%y-%m-%d")
    db.execute(f"INSERT OR IGNORE INTO count(date) VALUES ('{day}')")
    con.commit()

  # fill rows data
  for column in columns:
    count_for(column)
  calc_total()

