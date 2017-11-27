import logging
import traceback
from logging import handlers


# set up logging
logger = logging.getLogger(__name__)
rfh = handlers.RotatingFileHandler(
    'memes/automemer.log',
    maxBytes=1024 * 1024 * 20,
    backupCount=1
)
rfh.setLevel(logging.DEBUG)
rfh.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
logger.addHandler(rfh)


def log_error(error):
    """Writes the given error to a log file"""
    error_type_string = type(error).__name__
    traceback_string = traceback.format_exc()
    logger.error("%s\n%s", error_type_string, traceback_string)


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
    """
    Inserts data for the passed dict into the database. Will always insert if
    the data doesn't exist, will update existing data if replace=True, and do nothing
    if replace=False
    :param cursor: a database cursor object
    :param meme_dict: a dictionary with data for a given meme
    :param connection: a database connection object
    :param replace: whether to replace existing data or do nothing when existing data is found
    """
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
    """
    Updates the following fields in database for the row corresponding to meme_dict[id] :
    ups, highest_ups, last_updated, posted_to_slack
    :param cursor: a database cursor object
    :param meme_dict: a dictionary with appropriate data for a meme
    :param connection: a database connection object
    """
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
        UPDATE memes
        SET posted_to_slack = ?
        WHERE id = ?
        ''',
        (val, meme_id)
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
        FROM memes
        WHERE url = ?
        ''',
        (meme_dict['url'],)
    )
    values = cursor.fetchall()
    for v in values:
        if v[0]:
            return True
    return False
