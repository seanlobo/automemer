import datetime
import os
import shutil
import subprocess
import ast
import traceback
import json
import time
from multiprocessing import Lock

import praw


# loading praw agent
reddit = praw.Reddit('automemer', user_agent='meme scraper')
ABSOLUTE_PATH = 'memes/'


def log_error(error):
    """Writes the given error to a log file"""
    error_datetime = datetime.datetime.now().isoformat()
    with open("memes/errors.txt", mode='a', encoding='utf-8') as f:
        f.write('\n{name} at {time}\n'.format(name=str(error), time=error_datetime))
        f.write(traceback.format_exc() + '\n')
        f.write('-' * 75 + '\n')


def scrape(lock=Lock()):
    """Queries Praw to scrape subs according to preferences file"""
    # loading in subreddit list
    lock.acquire()
    try:
        with open(ABSOLUTE_PATH + 'settings.txt', mode='r', encoding='utf-8') as f:
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

    meme_dict_path = ABSOLUTE_PATH + 'MEMES.json'                        # meme file
    scraped_memes_path =  ABSOLUTE_PATH + 'scraped.json'                # scraped memes file

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
        try:
            with open(meme_dict_path, mode='r', encoding='utf-8') as f:
                meme_dict = json.loads(f.read())
        except Exception as e:  # except any error and print the error to a file
            log_error(e)
            meme_dict = dict()
            if not os.path.isfile(meme_dict_path):
                with open(meme_dict_path, 'w') as f:
                        f.write(json.dumps({}))

        # writing new memes to scraped.json
        try:
            with open(scraped_memes_path, mode='r', encoding='utf-8') as scraped:
                new_memes = json.loads(scraped.read())  # the other memes we've scraped today
        except OSError as e:
            log_error(e)
            new_memes = dict()

        for i, sub in enumerate(subreddits):
            sub_threshold = thresholds.get(sub, thresholds.get('global'))
            try:
                for post in reddit_memes[i]:
                    if post['url'] not in meme_dict:  # add this meme to our list
                        meme_dict[post['url']] = [post]
                        if not post['over_18']:
                            new_memes[post['url']] = post
                    else:  # this meme (url) is old
                        previous_versions = meme_dict[post['url']]
                        old_meme, ind = None, None
                        for i, prev in enumerate(previous_versions):
                            if prev.get('id') == post['id']:
                                old_meme = prev
                                ind = i
                        if old_meme is None:  # this post is new ( a new post to an old url), so add it to the list
                            previous_versions.append(post)
                        else:  # this exact post was previously recorded, update the values
                            post['highest_ups'] = max(
                                old_meme.get('highest_ups', 0),
                                post['ups'],
                                post['highest_ups']
                            )
                            post['posted_to_slack'] = old_meme.get('posted_to_slack', False)
                            post['recorded'] = old_meme.get('recorded', post['recorded'])
                            meme_dict[post['url']][ind] = post

                        previously_posted = False
                        for prev in previous_versions:
                            if prev.get('posted_to_slack'):
                                previously_posted = True
                                break
                        if (post['highest_ups'] > sub_threshold and
                                not previously_posted and
                                not post['over_18']):
                            new_memes[post['url']] = post
            except Exception as e:
                log_error(e)

        # update scraped memes file
        with open(scraped_memes_path, mode='w', encoding='utf-8') as f:
            f.write(json.dumps(new_memes, indent=2))

        # writing updated meme_dict to file
        with open(meme_dict_path, mode='w', encoding='utf-8') as f:
            f.write(json.dumps(meme_dict, indent=2))
    finally:
        lock.release()


def update_meme(meme_url, lock):
    """Updates meme json file for the given meme, and returns given meme's data"""
    lock.acquire()
    try:
        with open(ABSOLUTE_PATH + 'MEMES.json', 'r', encoding='utf-8') as f:
            memes = f.read()
        memes = json.loads(memes)
        matching_memes = memes.get(meme_url)
        if matching_memes is None:  # can't update without the meme and id
            return
        for meme_data in matching_memes:
            if 'id' in meme_data:
                post                      = reddit.submission(id=meme_data['id'])
                meme_data['ups']          = post.ups
                meme_data['highest_ups']  = max(meme_data.get('highest_ups', 0), post.ups)
                meme_data['upvote_ratio'] = post.upvote_ratio
                meme_data['last_updated'] = datetime.datetime.utcnow().isoformat()

        with open(ABSOLUTE_PATH + 'MEMES.json', 'w', encoding='utf-8') as f:
            f.write(json.dumps(memes))

        return matching_memes
    except Exception as e:
        log_error(e)
    finally:
        lock.release()


if __name__ == '__main__':
    import time
    i = 0
    while True:
        i += 1
        print("Round {}".format(i))
        scrape()
        time.sleep(10 * 60)

