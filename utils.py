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

