## Simple work in progress script to retrieve activity stats from subreddits

### Usage example
```shell
 python3 scanner.py -r uruguay --since 2021-01-01 --until 2021-01-31
```
It would scan submissions and comments published between those specified dates (inclusive). Based on local time. It is going to generate (or update if already exists) a database named after the subreddit.

You can specify a database using the flag `-d`.

Generated `ranking` and `count` tables are ready to visualize. 

> It does use Pushshift API. It's recommended to specify your reddit username passing it with `--user youruser` so that name would be part of the user-agent for the requests.

