import datetime
import json
import os
import shutil
import subprocess
import sqlite3
import traceback
import time
from multiprocessing import Lock

import praw


# loading praw agent
reddit = praw.Reddit('automemer', user_agent='meme scraper')


def log_error(error):
    """Writes the given error to a log file"""
    error_datetime = datetime.datetime.now().isoformat()
    with open("memes/errors.txt", mode='a', encoding='utf-8') as f:
        f.write('\n{name} at {time}\n'.format(name=str(error), time=error_datetime))
        f.write(traceback.format_exc() + '\n')
        f.write('-' * 75 + '\n')


def get_meme_data(cursor, meme_id):
    cursor.execute(
        '''
        SELECT *
        FROM memes
        WHERE id = ?
        ''',
        (meme_id,)
    )
    row = cursor.fetchone()
    if row:
        keys = [
            'id', 'over_18', 'ups', 'highest_ups', 'title', 'url', 'link',
            'author', 'sub', 'upvote_ratio', 'created_utc', 'last_updated',
            'recorded', 'posted_to_slack',
        ]

        return {keys[i]: row[i] for i in range(len(keys))}
    else:
        return dict()


def get_meme_data_from_url(cursor, url):
    cursor.execute(
        '''
        SELECT *
        FROM memes
        WHERE url = ?
        ''',
        (url,)
    )
    rows = cursor.fetchall()
    if rows:
        keys = [
            'id', 'over_18', 'ups', 'highest_ups', 'title', 'url', 'link',
            'author', 'sub', 'upvote_ratio', 'created_utc', 'last_updated',
            'recorded', 'posted_to_slack',
        ]

        return [{keys[i]: row[i] for i in range(len(keys))} for row in rows]
    else:
        return []


def add_meme_data(cursor, meme_dict, connection, replace=False):
    replace_str = 'REPLACE' if replace else 'IGNORE'
    cursor.execute(
        '''
        INSERT OR {replace_str} INTO memes VALUES (
            :id,
            :over_18,
            :ups,
            :highest_ups,
            :title,
            :url,
            :link,
            :author,
            :sub,
            :upvote_ratio,
            :created_utc,
            :last_updated,
            :recorded,
            :posted_to_slack
        );
        '''.format(replace_str=replace_str),
        meme_dict
    )
    connection.commit()


def update_meme_data(cursor, meme_dict, connection):
    cursor.execute(
        '''
        UPDATE memes
        SET ups = ?, highest_ups = ?, last_updated = ?, posted_to_slack = ?,
            upvote_ratio = ?
        WHERE id = ?
        ''',
        (
            meme_dict['ups'],
            meme_dict['highest_ups'],
            meme_dict['last_updated'],
            meme_dict['posted_to_slack'],
            meme_dict['upvote_ratio'],
            meme_dict['id'],
        )
    )
    connection.commit()


def set_posted_to_slack(cursor, meme_id, connection, val):
    cursor.execute(
        '''
        UPDATE memes
        SET posted_to_slack = ?
        WHERE id = ?
        ''',
        (val, meme_id)
    )
    connection.commit()


def has_been_posted_to_slack(cursor, meme):
    cursor.execute(
        '''
        SELECT posted_to_slack
        FROM memes
        WHERE url = ?
        ''',
        (meme['url'],)
    )
    values = cursor.fetchall()
    for v in values:
        if v[0]:
            return True
    return False


def scrape(cursor, connection, lock=Lock()):
    """Queries Praw to scrape subs according to preferences file"""
    # loading in subreddit list
    lock.acquire()
    try:
        with open('memes/settings.txt', mode='r', encoding='utf-8') as f:
            settings = json.loads(f.read())
        sub_names = settings.get('subs', ['me_irl'])
        subreddits = [reddit.subreddit(name) for name in sorted(list(sub_names))]
        subreddits = [sub for sub in subreddits if not sub.over18]
        NUM_MEMES = settings.get('num_memes', 50)
        thresholds = settings.get('threshold_upvotes')
    except OSError as e: # logging errors and loading default sub of me_irl
        log_error(e)
        subreddits = [reddit.subreddit('me_irl')]
        NUM_MEMES = 50
    finally:
        lock.release()

    scraped_memes_path =  'memes/scraped.json'                # scraped memes file

    # querying praw without lock acquired, because this takes a long time
    reddit_memes = []
    for sub in subreddits:
        sub_memes = []
        for post in sub.hot(limit=NUM_MEMES):
            data = {
                'over_18' : post.over_18,
                'id' : post.id,
                'ups' : post.ups,
                'title' : post.title,
                'url' : post.url,
                'link' : post.shortlink,
                'highest_ups' : post.ups,
                'posted_to_slack' : False,
                'author' : str(post.author),
                'sub' : post.subreddit.display_name,
                'upvote_ratio' : post.upvote_ratio,
                'recorded' : datetime.datetime.utcnow().isoformat(),
                'created_utc' : datetime.datetime.fromtimestamp(post.created_utc).isoformat(),
                'last_updated' : datetime.datetime.utcnow().isoformat(),
            }
            sub_memes.append(data)
        reddit_memes.append(sub_memes)

    lock.acquire()
    try:
        # load scraped memes
        try:
            with open(scraped_memes_path, mode='r', encoding='utf-8') as scraped:
                new_memes = json.loads(scraped.read())  # the memes we've scraped today
        except OSError as e:
            log_error(e)
            new_memes = dict()

        # add scraped memes to our database and scraped.json file
        for i, sub in enumerate(subreddits):
            sub_threshold = thresholds.get(sub, thresholds.get('global'))
            try:
                for post in reddit_memes[i]:
                    previous_data = get_meme_data(cursor, post['id'])
                    if not previous_data:  # this meme is new, add it to our list
                        add_meme_data(cursor, post, connection)
                        if not post['over_18']:
                            new_memes[post['url']] = post
                    else:  # this meme is old
                        # update data in sqlite
                        previous_data['highest_ups'] = max(
                            post.get('ups', 0),
                            previous_data.get('highest_ups', 0),
                            previous_data.get('ups', 0)
                        )
                        previous_data['ups'] = post['ups']
                        previous_data['upvote_ratio'] = post['upvote_ratio']
                        previous_data['last_updated'] = post['last_updated']
                        update_meme_data(cursor, previous_data, connection)

                        # if this url hasn't ever been posted, add it to the list
                        if not (previous_data['over_18']
                         or has_been_posted_to_slack(cursor, previous_data)):
                            new_memes[post['url']] = post
            except Exception as e:
                log_error(e)

        # update scraped memes file
        with open(scraped_memes_path, mode='w', encoding='utf-8') as f:
            f.write(json.dumps(new_memes, indent=2))

    finally:
        lock.release()


def update_meme(cursor, connection, meme_url, lock):
    """Updates meme json file for the given meme, and returns given meme's data"""
    lock.acquire()
    try:
        matching_memes = get_meme_data_from_url(cursor, meme_url)
        if not matching_memes:  # no memes found for the passed url
            return
        for meme_data in matching_memes:
            post                      = reddit.submission(id=meme_data['id'])
            meme_data['ups']          = post.ups
            meme_data['highest_ups']  = max(meme_data.get('highest_ups', 0), post.ups)
            meme_data['upvote_ratio'] = post.upvote_ratio
            meme_data['last_updated'] = datetime.datetime.utcnow().isoformat()

            update_meme_data(cursor, meme_data, connection)

        return matching_memes
    except Exception as e:
        log_error(e)
    finally:
        lock.release()

if __name__ == '__main__':
    import sqlite3
    conn = sqlite3.connect('memes/memes.sqlite3')
    cursor = conn.cursor()
    scrape(cursor, conn)
