import json
import os
import sqlite3

import utils


if __name__ == '__main__':
    # creating directories and files
    if not os.path.isfile(utils.SCRAPED_PATH):
        file = open(utils.SCRAPED_PATH, 'x')
        file.write(json.dumps({}))
        file.close()
    if not os.path.isfile(utils.SETTINGS_PATH):
        file = open(utils.SETTINGS_PATH, 'x')
        file.write(json.dumps({}))
        file.close()

    # create sqlite db
    if not os.path.isfile(utils.SQLITE_FILE):
        conn = sqlite3.connect(utils.SQLITE_FILE)
        cursor = conn.cursor()

        cursor.execute('''
            CREATE TABLE posts (
                id              STRING,
                over_18         BOOLEAN,
                ups             INTEGER,
                highest_ups     INTEGER,
                title           STRING,
                url             STRING,
                link            STRING,
                author          STRING,
                sub             STRING,
                upvote_ratio    FLOAT,
                created_utc     DATETIME,
                last_updated    DATETIME,
                recorded        STRING,
                posted_to_slack BOOLEAN
            )
        ''')

        conn.commit()
