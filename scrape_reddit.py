import json
import sqlite3
from datetime import datetime
from multiprocessing import Lock

import praw

import utils


# loading praw agent
reddit = praw.Reddit(
    'automemer',
    user_agent='Python/praw:automemer:v1.0 (by /u/AutoMemer)',
)


def scrape(cursor, connection, lock=Lock()):
    """Queries Praw to scrape subs according to preferences file"""
    # loading in subreddit list
    lock.acquire()
    try:
        with open('memes/settings.json', mode='r', encoding='utf-8') as f:
            settings = json.loads(f.read())
        sub_names = settings.get('subs', ['me_irl'])
        subreddits = [reddit.subreddit(name) for name in sorted(list(sub_names))]
        subreddits = [sub for sub in subreddits if not sub.over18]
        NUM_MEMES = settings.get('num_memes', 50)
    except OSError as e:  # logging errors and loading default sub of me_irl
        utils.log_error(e)
        subreddits = [reddit.subreddit('me_irl')]
        NUM_MEMES = 50
    finally:
        lock.release()

    scraped_memes_path = 'memes/scraped.json'

    # querying praw without lock acquired, because this takes a long time
    reddit_memes = []
    for sub in subreddits:
        sub_memes = []
        for post in sub.hot(limit=NUM_MEMES):
            data = {
                'over_18': post.over_18,
                'id': post.id,
                'ups': post.ups,
                'title': post.title,
                'url': post.url,
                'link': post.shortlink,
                'highest_ups': post.ups,
                'posted_to_slack': False,
                'author': str(post.author),
                'sub': post.subreddit.display_name,
                'upvote_ratio': post.upvote_ratio,
                'recorded': datetime.utcnow().isoformat(),
                'created_utc': datetime.fromtimestamp(post.created_utc).isoformat(),
                'last_updated': datetime.utcnow().isoformat(),
            }
            sub_memes.append(data)
        reddit_memes.append(sub_memes)

    # update scraped list and database with lock acquired
    lock.acquire()
    try:
        # load scraped memes
        try:
            with open(scraped_memes_path, mode='r', encoding='utf-8') as scraped:
                new_memes = json.loads(scraped.read())  # the memes we've scraped but not yet posted
        except OSError as e:
            utils.log_error(e)
            new_memes = {}

        # add scraped memes to our database and scraped.json file
        for i, sub in enumerate(subreddits):
            try:
                for post in reddit_memes[i]:
                    previous_data = utils.get_meme_data(cursor, post['id'])
                    if not previous_data:  # this meme is new, add it to our list
                        utils.add_meme_data(cursor, post, connection)
                        if not post['over_18']:
                            # if the meme is sfw then add it to scraped.json
                            new_memes[post['url']] = post
                    else:
                        # this meme is old, update data in SQLite
                        previous_data['highest_ups'] = max(
                            post.get('ups') or 1,
                            previous_data.get('highest_ups') or 1,
                            previous_data.get('ups') or 1,
                        )
                        previous_data['ups'] = post['ups']
                        previous_data['upvote_ratio'] = post['upvote_ratio']
                        previous_data['last_updated'] = post['last_updated']
                        utils.update_meme_data(cursor, previous_data, connection)

                        # if this url hasn't ever been posted, add it to the list
                        if not (previous_data['over_18'] or
                                utils.has_been_posted_to_slack(cursor, previous_data)):
                            new_memes[post['url']] = post
            except Exception as e:
                utils.log_error(e)

        # update scraped memes file
        with open(scraped_memes_path, mode='w', encoding='utf-8') as f:
            f.write(json.dumps(new_memes, indent=2))

    finally:
        lock.release()


def update_reddit_meme(cursor, connection, meme_url, lock):
    """
    Retrieves every meme matching the passed url, and queries Praw to update data.
    Returns updated data
    :param cursor: a database cursor object
    :param connection: a database connection object
    :param meme_url: a url to match memes' stored urls with in the database
    :param lock: a multiprocessing.Lock object
    :return: a list of memes whose urls matched the passed
    """
    lock.acquire()
    try:
        matching_memes = utils.get_meme_data_from_url(cursor, meme_url)
        for meme_data in matching_memes:
            post = reddit.submission(id=meme_data['id'])
            meme_data['ups'] = post.ups
            meme_data['highest_ups'] = max(meme_data.get('highest_ups', 0), post.ups)
            meme_data['upvote_ratio'] = post.upvote_ratio
            meme_data['last_updated'] = datetime.utcnow().isoformat()

            utils.update_meme_data(cursor, meme_data, connection)

        return matching_memes
    except Exception as e:
        utils.log_error(e)
    finally:
        lock.release()


if __name__ == '__main__':
    conn = sqlite3.connect('memes/memes.sqlite3')
    cursor = conn.cursor()
    scrape(cursor, conn)
