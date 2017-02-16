# https://www.fullstackpython.com/blog/build-first-slack-bot-python.html

import os
import shutil
import sys
import time
import datetime
import ast
import json
import traceback
import html
from collections import Counter
from multiprocessing import Lock, Process

from slackclient import SlackClient

import scrape_reddit


# Get the bot id from an environment variable
BOT_ID = os.environ.get("BOT_ID")

# constants
AT_BOT = "<@{}>".format(BOT_ID)

# meme spam channel id
MEME_SPAM_CHANNEL = "C2M6KKPNG"

# instantiate Slack and Twilio clients
slack_client = SlackClient(os.environ.get('SLACK_BOT_TOKEN'))

def current_time_as_min():
    now = datetime.datetime.now()
    midnight = now.replace(hour=0, minute=0, second=0, microsecond=0)
    return (now - midnight).seconds // 60


class Queue:
    def __init__(self):
        self.data = []

    def push(self, item):
        self.data.insert(0, item)

    def pop(self):
        return self.data.pop()

    def is_empty(self):
        return len(self.data) == 0

# ----------------------- SPECIFIC COMMANDS ---------------------------
bot_commands = {
    "help": "Prints a list of commands and short descriptions",
    "list-subreddits": "Prints a list of subreddits currently being scraped",
    "add-subreddit <sub>": "Adds <sub> to the list of subreddits scraped",
    "delete-subreddit <sub>": "Deletes <sub> from the list of subreddits scraped",
    "list-settings": "Prints out all settings",
    "set-threshold <threshold> {optional_subreddit}": (
        "Sets threshold upvotes a meme must meet to be scraped. If {optional_subreddit} "
        "is specified, sets <threshold> specifically for that sub, otherwise a global "
        "threshold is set (applied to subs without a specific threshold)"
    ),
    "details <meme_url>": "Gives details for a meme if meme_url has been scraped",
    "set scrape-interval <int>": "sets the scrape interval to <int> minutes",
    "pop {num}": "pops {num} memes (or as many as there are) from the queue",
    "num-memes {dank_only} {by_sub}": (
        "Prints the number of memes currently waiting to be posted. To only post dank memes use "
        "`num-memes dank_only`, to get a breakdown by subreddit use `num-memes by_sub`"
    ),
    "kill": "Kills automemer. Program is stopped, no scraping, no posting. ded :rip:",
}
def handle_command(command, channel, bot_responses, lock):
    """
    Receives commands directed at the bot and determines if they
    are valid commands. If so, then acts on the commands. If not,
    returns back what it needs for clarification.
    """
    response = '>{}\n'.format(command)
    # specific command responses
    if command.lower() == "help":
        for command in sorted(bot_commands.keys()):
            response += '`{}` - {}\n'.format(command, bot_commands[command])
    elif command.lower() == "list-subreddits":
        try:
            settings = json.loads(open('./memes/settings.txt').read())
            subs = sorted(settings.get('subs'))
            response += (
                'The following subreddits are currently being collected: {}'.format(
                str(subs))
            )
        except OSError as e:
            response += ':sadparrot: error\n'
            response += str(e)
    elif command.lower().startswith("add-subreddit"):
        command = command.lower().split()
        if len(command) != 2:
            response += "command must be in the form 'add-subreddit [name]'"
        else:
            command = command[1]
            settings = json.loads(open('./memes/settings.txt').read())
            settings['subs'].append(command)
            lock.acquire()
            with open('memes/settings.txt', mode='w', encoding='utf-8') as f:
                f.write(json.dumps(settings, indent=2))
            lock.release()
            response += "{} has been added!".format(command)
    elif command.lower().startswith("delete-subreddit"):
        command = command.lower().split()
        if len(command) != 2:
            response += "command must be in the form 'delete-subreddit [name]'"
        else:
            sub = command[1]
            settings = json.loads(open('./memes/settings.txt').read())
            previous_subs = settings['subs']
            if sub not in previous_subs:
                response += "`{}` is not currently being followed, nothing was done".format(sub)
            else:
                previous_subs.remove(sub)
                settings['subs'] = previous_subs
                lock.acquire()
                with open('memes/settings.txt', mode='w', encoding='utf-8') as f:
                    f.write(json.dumps(settings, indent=2))
                lock.release()
                response += "`{}` has been removed".format(sub)
    elif command.lower() == "list-settings":
        lock.acquire()
        with open("memes/settings.txt", mode='r', encoding='utf-8') as f:
            settings = json.loads(f.read())
        lock.release()
        for key, val in sorted(settings.items()):
            if key == "subs":
                val = sorted(val)
            response += "`{key}`: {val}\n".format(key=key, val=val)
    elif command.lower().startswith("set-threshold"):
        lock.acquire()
        command = command.lower().split()
        if len(command) not in [2, 3]:
            response += "command must be in the form 'add-subreddit [name] [optional-sub]'"
        elif len(command) == 2 or command[2].lower() == 'global':
            threshold = command[1]
            try:
                threshold = int(threshold)
            except ValueError:
                response = "{threshold} is not a valid integer".format(threshold=threshold)
            else:
                with open("memes/settings.txt", mode='r', encoding='utf-8') as f:
                    settings = json.loads(f.read())
                settings['threshold_upvotes']['global'] = threshold
                with open("memes/settings.txt", mode='w', encoding='utf-8') as f:
                    f.write(json.dumps(settings, indent=2))
                response = "The global threshold has been set to {threshold}!".format(threshold=threshold)
        else:
            sub = command[2].lower()
            with open("memes/settings.txt", mode='r', encoding='utf-8') as f:
                settings = json.loads(f.read())
            if sub not in settings['subs']:
                response = "{} is not in the list of subreddits. run `list-subreddits` to view a list".format(sub)
            else:
                threshold = command[1]
                if threshold.lower() == 'none':
                    del settings['threshold_upvotes'][sub]
                    with open("memes/settings.txt", mode='w', encoding='utf-8') as f:
                        f.write(json.dumps(settings, indent=2))
                else:
                    try:
                        threshold = int(threshold)
                    except ValueError:
                        response = "{threshold} is not a valid integer".format(threshold=threshold)
                    else:
                        settings['threshold_upvotes'][sub] = threshold
                        with open("memes/settings.txt", mode='w', encoding='utf-8') as f:
                            f.write(json.dumps(settings, indent=2))
                        response = "The threshold upvotes for _{sub}_ has been set to *{threshold}*!".format(sub=sub, threshold=threshold)
        lock.release()
    elif command.lower().startswith("details"):
        command = command.split()
        if len(command) != 2:
            response += "command must be in the form `details <meme_url>`\n"
        else:
            meme_url = html.unescape(command[1][1:-1])
            meme_data = scrape_reddit.update_meme(meme_url, lock)
            if meme_data is None:
                response += "I could find any data for this url: `{}`, sorry\n".format(meme_url)
            else:
                for key, val in sorted(meme_data.items()):
                    response += "`{key}`: {data}\n".format(key=key, data=val)

    elif command.lower().startswith("set scrape-interval"):
        interval = command.split()
        if len(interval) != 3:
            response += "command must be in the form `set scrape-interval <integer>`"
        else:
            interval = interval[-1]
            try:
                interval = int(interval)
            except ValueError:
                response += "{} is not an integer :parrotcop:".format(interval)
            else:
                if interval >= 1440:
                    response += (
                        "```\n"
                        ">>> minutes_per_day()\n"
                        "1440"
                        "```\n"
                        "Too many minutes!"
                    )
                elif interval <= 0:
                    response += "I see someone's trying to be a smart aleck. Enter a number greater than 0"
                else:
                    lock.acquire()
                    with open("memes/settings.txt", mode='r', encoding='utf-8') as s:
                        settings = s.read()
                    settings = json.loads(settings)
                    settings['scrape_interval'] = interval
                    global scrape_interval
                    scrape_interval = interval
                    with open("memes/settings.txt", mode='w', encoding='utf-8') as s:
                        s.write(json.dumps(settings, indent=2))
                    response += "scrape_interval has been set to *{}*!".format(str(interval))
                    lock.release()

    elif command.lower().startswith("pop"):
        command = command.split()
        if len(command) == 2:
            try:
                limit = int(command[1])
            except ValueError:
                response += "{} isn't a number!".format(str(command[1]))
            else:
                if limit <= 0:
                    response += "You can't pop 0 or fewer memes........"
                else:
                    add_new_memes_to_queue(bot_responses, lock, limit, user_prompt=True)
                    return
        else:
            add_new_memes_to_queue(bot_responses, lock, user_prompt=True)
            return

    elif command.lower().startswith('num-memes'):
        command = command.lower().split()
        by_sub = 'by_sub' in command
        dank_only = 'dank_only' in command
        total, postable = count_memes(lock)
        subs_lower_to_title = {sub.lower() : sub for sub in total}

        if not by_sub:
            if not dank_only:
                text = "Total memes: {}\nPostable memes: {}".format(str(sum(total.values())),
                                                                    str(sum(postable.values())))
                response += text
            else:
                response += "Postable memes: {}".format(str(sum(postable.values())))
        else:
            if not dank_only:
                for sub in sorted(list(map(lambda x: x.lower(), total.keys()))):
                    response += "*{sub}*: {good}   ({tot})\n".format(
                        sub=subs_lower_to_title[sub],
                        good=postable[subs_lower_to_title[sub]],
                        tot=total[subs_lower_to_title[sub]]
                    )
                response += "\n*Combined*: {}    ({})".format(str(sum(postable.values())),
                                                                   str(sum(total.values())))
            else:
                for sub in sorted(list(map(lambda x: x.lower(), total.keys()))):
                    response += "*{sub}*: {ups}\n".format(
                            sub=sub_lower_to_title[sub],
                            ups=postable[sub_lower_to_title[sub]])
                response += "\n*Combined*: {}".format(str(sum(postable.values())))

    elif command.lower() == "kill":
        slack_client.api_call("chat.postMessage", channel=MEME_SPAM_CHANNEL,
                              text="have it your way", as_user=True)
        sys.exit()

    elif command.lower().startswith("echo "):
        response = ''.join(command.split()[1:])
        slack_client.api_call(
            "chat.postMessage",
            channel=channel,
            text=response,
            link_names=1,
            as_user=True
        )
        return

    else:  # a default response
        response =  ('>*' + command + '*\n'
             "I don't know this command :dealwithitparrot:\n"
        )
    bot_responses.push((channel, response))


def count_memes(lock):
    meme_path = 'memes/scraped.json'
    try:
        lock.acquire()
        with open(meme_path, mode='r', encoding='utf-8') as f:
            memes = f.read()
        with open('memes/settings.txt', mode='r', encoding='utf-8') as f:
            settings = f.read()
        memes = json.loads(memes)
        settings = json.loads(settings)
        thresholds = settings['threshold_upvotes']

        total, postable = Counter(), Counter()
        for post, data in memes.items():
            if not data.get('over_18'):
                sub = data.get('sub')
                ups = data.get('highest_ups')
                sub_threshold = thresholds[sub] if sub is not None and sub in thresholds else thresholds['global']

                total[sub] += 1
                if ups >= sub_threshold:
                    postable[sub] += 1
        return total, postable
    except OSError:
        return Counter(), Counter()
    finally:
        lock.release()



def add_new_memes_to_queue(bot_responses, lock, limit=10, user_prompt=False):
    _, postable = count_memes(lock)
    lock.acquire()
    if sum(postable.values()) == 0 and user_prompt:
        bot_responses.push((MEME_SPAM_CHANNEL, 'Sorry, we ran out of memes :('))
        return
    meme_path = 'memes/scraped.json'
    all_meme_path = 'memes/MEMES.txt'
    try:
        with open(meme_path, mode='r', encoding='utf-8') as f:
            text = f.read()
        with open(all_meme_path, 'r', encoding='utf-8') as f:
            all_memes = f.read()
        all_memes = json.loads(all_memes)
        memes = json.loads(text)
        with open('memes/settings.txt', mode='r', encoding='utf-8') as f:
            settings = json.loads(f.read())
        thresholds = settings['threshold_upvotes']
        for post, data in sorted(list(memes.items()), key=lambda x: x[1]['created_utc']):
            del memes[post]
            ups = data.get('highest_ups')
            sub = data.get('sub')
            sub_threshold = thresholds.get(sub, thresholds['global'])
            if ups > sub_threshold:
                all_memes[post]['posted_to_slack'] = True
                limit -= 1
                meme_text = (
                    "*{title}* _(from /r/{sub})_ `{ups}`\n{url}".format(
                        title=data.get('title'),
                        sub=sub,
                        ups=data.get('ups'),
                        url=post)
                    )
                bot_responses.push((MEME_SPAM_CHANNEL, meme_text))
                if limit <= 0:
                    break
        with open(meme_path, mode='w', encoding='utf-8') as f:
            f.write(json.dumps(memes, indent=2))
        with open(all_meme_path, mode='w', encoding='utf-8') as f:
            f.write(json.dumps(all_memes))
        if 0 < limit and user_prompt:
            bot_responses.push((MEME_SPAM_CHANNEL, 'Sorry, we ran out of memes :('))
    except Exception as e:
        bot_responses.push((MEME_SPAM_CHANNEL, (
            "There was an error :sadparrot:\n"
            ">`{}`".format(str(e))
        )))
        with open('memes/errors.txt', mode='a', encoding='utf-8') as errors:
            date = datetime.datetime.today().date().isoformat()
            minutes = current_time_as_min()
            errors.write(date + ' {}: {}\n'.format(str(minutes), type(e)))
            errors.write(traceback.format_exc() + '\n')
            errors.write('-' * 100 + '\n')
    finally:
        lock.release()


def load_scrape_interval(lock):
    minutes = current_time_as_min()
    date = datetime.datetime.today().date().isoformat()
    lock.acquire()
    try:
        with open('memes/settings.txt', mode='r', encoding='utf-8') as f:
            settings = f.read()
        settings = json.loads(settings)
        interval = settings['scrape_interval']
        return interval
    except Exception as e:
        bot_responses.push((MEME_SPAM_CHANNEL, (
            "There was an error setting the scrape interval :sadparrot:\n"
            "Setting the interval to a default 1 hour (60 minutes)"
            ">`{}`".format(str(e))
        )))
        with open('memes/errors.txt', mode='a', encoding='utf-8') as errors:
            errors.write(date + ' {}: {}\n'.format(str(minutes), type(e)))
            errors.write(traceback.format_exc() + '\n')
            errors.write('-' * 100 + '\n')
        return 60
    finally:
        lock.release()


def pop_queue(bot_responses):
    if not bot_responses.is_empty():
        channel, response = bot_responses.pop()
        slack_client.api_call("chat.postMessage", channel=channel,
                              text=response, as_user=True)

def parse_slack_output(slack_rtm_output):
    """
    the Slack Real Time Messaging API is an events firehose.
    this parsing function returns None unless a message is
    directed at the Bot, based on its ID.
    """
    if slack_rtm_output:
        print(json.dumps(slack_rtm_output, indent=2))
    output_list = slack_rtm_output
    if output_list and len(output_list) > 0:
        for output in output_list:
            if output and 'text' in output and AT_BOT in output['text']:
                # return text after the @ mention, whitespace removed
                return output['text'].split(AT_BOT)[1].strip(), \
                       output['channel']
    return None, None

if __name__ == "__main__":
    READ_WEBSOCKET_DELAY = 1 # 1 second delay between reading from firehose
    if slack_client.rtm_connect():
        for i in range(1, 4):
            try:
                print("AutoMemer connected and running!")
                bot_responses = Queue()
                added_memes = False  # have we added the 30 minute memes
                scraped_memes = False  # have we popped the memes
                lock = Lock()
                scrape_interval = load_scrape_interval(lock)
                while True:
                    command, channel = parse_slack_output(slack_client.rtm_read())
                    if command and channel:  # if a user tagged @automemer with a command
                        handle_command(command, channel, bot_responses, lock)
                    time_as_minutes = current_time_as_min()
                    if time_as_minutes % 10 == 0 and not added_memes:
                        Process(target=scrape_reddit.scrape, args=(lock,)).start()
                        added_memes = True  # we just added the memes
                    elif time_as_minutes % 10 > 0:  # the minute passed, reset added_memes
                        added_memes = False

                    if time_as_minutes % scrape_interval == 0 and not scraped_memes:
                        add_new_memes_to_queue(bot_responses, lock)
                        scraped_memes = True
                    elif time_as_minutes % scrape_interval > 0 and scraped_memes:
                        scraped_memes = False

                    pop_queue(bot_responses)
                    time.sleep(READ_WEBSOCKET_DELAY)
            except Exception as e:
                lock.acquire()
                with open('memes/errors.txt', mode='a', encoding='utf-8') as errors:
                    errors.write(datetime.datetime.now().isoformat() + '{}\n'.format(type(e)))
                    errors.write(traceback.format_exc() + '\n')
                    errors.write('-' * 100 + '\n')
                lock.release()
                text = (
                    "error!!! I'm dying check me sean\n`{}`\n".format(str(e)) +
                    "_I have resurrected myself {} out of 3 times, to kill me use the `kill` command_"
                        .format(str(i))
                )
                slack_client.api_call("chat.postMessage", channel=MEME_SPAM_CHANNEL,
                                  text=text, as_user=True)
                time.sleep(READ_WEBSOCKET_DELAY)

    else:
        print("Connection failed. Invalid Slack token or bot ID?")

