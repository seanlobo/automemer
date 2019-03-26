import json
import os

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
        with open('db.json', 'r') as f:
            db_info = json.loads(f.read())
        conn = utils.get_connection(
            db_info['user'],
            db_info['password'],
            db_info['db'],
            db_info['host'],
        )
        cursor = conn.cursor()

        cursor.execute('''
            CREATE TABLE IF NOT EXISTS posts (
                id              TEXT,
                over_18         BOOLEAN,
                ups             INTEGER,
                highest_ups     INTEGER,
                title           TEXT,
                url             TEXT,
                link            TEXT,
                author          TEXT,
                sub             TEXT,
                upvote_ratio    FLOAT,
                created_utc     DATETIME,
                last_updated    DATETIME,
                recorded        TEXT,
                posted_to_slack BOOLEAN
            );
        ''')

        conn.commit()
