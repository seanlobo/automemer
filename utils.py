import datetime
import logging
import os
import threading
import traceback
from logging import handlers
from pathlib import Path

import pymysql


SCRAPED_PATH = 'memes/scraped.json'
SETTINGS_PATH = 'memes/settings.json'
SQLITE_FILE = 'memes/memes.sqlite3'
ERROR_LOG_FILE = 'memes/errors.log'
SLACK_LOG_FILE = 'memes/comments.log'
USAGE_LOG_FILE = 'memes/usage.log'

# set up logging
os.makedirs('memes', exist_ok=True)
Path(ERROR_LOG_FILE).touch()
Path(USAGE_LOG_FILE).touch()
logger = logging.getLogger(__name__)
rfh = handlers.RotatingFileHandler(
    ERROR_LOG_FILE,
    maxBytes=1024 * 1024 * 20,
    backupCount=1,
)
rfh.setLevel(logging.DEBUG)
rfh.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
logger.addHandler(rfh)


def log_error(error):
    """Writes the given error to a log file"""
    error_type_string = type(error).__name__
    traceback_string = traceback.format_exc()
    logger.error('%s\n%s', error_type_string, traceback_string)


def log_usage(log_str):
    time_str = str(datetime.datetime.now())

    with open(USAGE_LOG_FILE, 'a') as f:
        f.write(f'{time_str} - {threading.get_ident()} - {log_str}\n')


def get_connection(
    user,
    password,
    db,
    host,
    cursorclass=pymysql.cursors.DictCursor,
    autocommit=True,
    charset='utf8mb4',
    **kwargs,
):

    return pymysql.connect(
        user=user,
        password=password,
        db=db,
        host=host,
        cursorclass=cursorclass,
        charset=charset,
        autocommit=autocommit,
        **kwargs,
    )


def get_meme_data(cursor, meme_id):
    """
    Queries SQLite for data associated with the passed Reddit post id.
    :param cursor: a database cursor object
    :param meme_id: the id associated with a post on reddit / a row in the database
    :return: a dictionary with the data for the appropriate post if it exists, else an empty dict
    """
    cursor.execute(
        '''
        SELECT *
        FROM posts
        WHERE id = %s
        ''',
        (meme_id,),
    )
    return cursor.fetchone()


def get_meme_data_from_url(cursor, url):
    """
    Queries SQLite for data associated with the given url
    :param cursor: a database cursor object
    :param url: a url for an image / post on Reddit
    :return: a list of dictionaries corresponding to each post having the appropriate url,
    or an empty list if no data matches.
    """
    cursor.execute(
        '''
        SELECT *
        FROM posts
        WHERE url = %s
        ''',
        (url,),
    )
    return cursor.fetchall()


def add_meme_data(cursor, meme_dict, connection):
    """
    Inserts data for the passed dict into the database. Will always insert if
    the data doesn't exist, will update existing data if replace=True, and do nothing
    if replace=False
    :param cursor: a database cursor object
    :param meme_dict: a dictionary with data for a given meme
    :param connection: a database connection object
    :param replace: whether to replace existing data or do nothing when existing data is found
    """
    cursor.execute(
        '''
        INSERT INTO posts VALUES (
            %(id)s,
            %(over_18)s,
            %(ups)s,
            %(highest_ups)s,
            %(title)s,
            %(url)s,
            %(link)s,
            %(author)s,
            %(sub)s,
            %(upvote_ratio)s,
            %(created_utc)s,
            %(last_updated)s,
            %(recorded)s,
            %(posted_to_slack)s
        );
        ''',
        meme_dict,
    )
    connection.commit()


def update_meme_data(cursor, meme_dict, connection):
    """
    Updates the following fields in database for the row corresponding to meme_dict[id] :
    ups, highest_ups, last_updated, posted_to_slack
    :param cursor: a database cursor object
    :param meme_dict: a dictionary with appropriate data for a meme
    :param connection: a database connection object
    """
    cursor.execute(
        '''
        UPDATE posts
        SET ups = %s, highest_ups = %s, last_updated = %s, posted_to_slack = %s,
            upvote_ratio = %s
        WHERE id = %s
        ''',
        (
            meme_dict['ups'],
            meme_dict['highest_ups'],
            meme_dict['last_updated'],
            meme_dict['posted_to_slack'],
            meme_dict['upvote_ratio'],
            meme_dict['id'],
        ),
    )


def set_posted_to_slack(cursor, meme_id, connection, val):
    """
    Updates the value of row meme_id to have a posted_to_slack value of val. Should typically be used
    to specify a meme has been posted (aka val = True)
    :param cursor: a database cursor object
    :param meme_id: the (Reddit / database row) id of the meme to update
    :param connection: a database connection object
    :param val: a boolean represnting whether the meme has been posted to reddit
    """
    cursor.execute(
        '''
        UPDATE posts
        SET posted_to_slack = %s
        WHERE id = %s
        ''',
        (val, meme_id),
    )
    connection.commit()


def has_been_posted_to_slack(cursor, meme_dict):
    """
    Returns whether the passed meme has been posted to slack. NOTE: while `set_posted_to_slack`
    only sets a single row (based on Reddit / database row id) this function returns True
    if any row with the same url as the passed meme has been posted to slack.
    :param cursor: a database cursor object
    :param meme_dict: a dictionary with a url to check
    :return:
    """
    cursor.execute(
        '''
        SELECT posted_to_slack
        FROM posts
        WHERE url = %s
        ''',
        (meme_dict['url'],),
    )
    values = cursor.fetchall()
    for v in values:
        if v['posted_to_slack']:
            return True
    return False
