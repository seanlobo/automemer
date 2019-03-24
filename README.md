# AutoMemer

A bot to deliver a constant stream of memes to your slack group of choice. Uses [Praw](https://praw.readthedocs.io/en/latest/) to
query user defined subreddits and [the Slack Python API](https://github.com/slackapi/python-slackclient) to post messages. Can
be customized with respect to subreddits scraped, scrape interval, post (to slack) interval, subreddit upvote thresholds and more.

### TODO
- Make project simpler to set up (currently requires personal keys for querying the reddit and slack APIs)
- Instructions on how to set up
