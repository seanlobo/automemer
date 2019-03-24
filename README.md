# AutoMemer

A bot to deliver a constant stream of memes to your slack group of choice. It uses Python3.6+, as well as [Praw](https://praw.readthedocs.io/en/latest/) to
query user defined subreddits and [the Slack Python API](https://github.com/slackapi/python-slackclient) to post messages. It can
be customized with respect to subreddits scraped, scrape interval, post (to slack) interval, subreddit upvote thresholds and more.

## Setup
### Slack
1) Follow [this guide](https://get.slack.help/hc/en-us/articles/115005265703-Create-a-bot-for-your-workspace) to setup a bot for your workspace. You'll want to connect to the Real Time Messaging (RTM) API

2) Run `print_bot_id.py` to determine the ID of your bot (change the name from `automemer` to whatever the name of the bot is)

3) Fill out `ids.sh` with the relevant information and run `source ids.sh`

### Reddit
1) Create a reddit application by following [this guide](https://github.com/reddit-archive/reddit/wiki/OAuth2)

2) Add the client secret and id to a `praw.ini` file as shown [here](https://praw.readthedocs.io/en/latest/getting_started/configuration/prawini.html)

3) Modify the `Reddit` instance in `scrape_reddit.py` as needed.

### Running

1) run `pip install requirements.txt`, in a virtualenv if desired

2) Run `python3 setup.py` to setup the db and directory structure

3) Run `python3 slackbot.py` to begin the bot
