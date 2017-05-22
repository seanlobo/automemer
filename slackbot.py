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
from collections import Counter, defaultdict
from multiprocessing import Lock, Process

from slackclient import SlackClient

import scrape_reddit
from scrape_reddit import log_error


class AutoMemer:
    bot_commands = {
        "add <sub>": "Adds <sub> to the list of subreddits scraped",
        "delete <sub>": "Deletes <sub> from the list of subreddits scraped",
        "details <meme_url>": "Gives details for a meme if meme_url has been scraped",
        "help": "Prints a list of commands and short descriptions",
        "increase threshold <threshold> {optional_subreddit}": (
            "Sets threshold for {optional_subreddit} to the old threshold + or - the <threshold> "
            "value passed. Defaults to global"
        ),
        "kill": "Kills automemer. Program is stopped, no scraping, no posting. ded :rip:",
        "link <url>": "Prints the link associated with the url passed",
        "list settings": "Prints out all settings",
        "list subreddits": "Prints a list of subreddits currently being scraped",
        "list thresholds": "Prints the thresholds for subs",
        "num-memes {dank_only} {by_sub}": (
            "Prints the number of memes currently waiting to be posted. To only post dank memes use "
            "`num-memes dank_only`, to get a breakdown by subreddit use `num-memes by_sub`"
        ),
        "pop {num}": "pops {num} memes (or as many as there are) from the queue",
        "set scrape interval <int>": "sets the scrape interval to <int> minutes",
        "set threshold <threshold> {optional_subreddit}": (
            "Sets threshold upvotes a meme must meet to be scraped. If {optional_subreddit} "
            "is specified, sets <threshold> specifically for that sub, otherwise a global "
            "threshold is set (applied to subs without a specific threshold)"
        ),

    }

    def __init__(self, bot_id, channel_id, debug=False):
        self.bot_id     = bot_id
        self.at_bot     = "<@" + bot_id + ">"
        self.channel_id = channel_id
        self.client     = SlackClient(os.environ.get('SLACK_BOT_TOKEN'))
        self.messages   = Queue()
        self.lock       = Lock()
        self.debug      = debug
        self.users_list = None

        self.log_file       = './memes/log_file.txt'
        self.scraped_path   = './memes/scraped.json'
        self.settings_path  = './memes/settings.txt'
        self.all_memes_path = './memes/MEMES.json'

        # creating directories and files
        os.makedirs('memes', exist_ok=True)
        if not os.path.isfile('memes/errors.txt'):
            open('memes/errors.txt', 'x').close()
        if not os.path.isfile(self.all_memes_path):
            file = open(self.all_memes_path, 'x')
            file.write(json.dumps({}))
            file.close()
        if not os.path.isfile(self.scraped_path):
            file = open(self.scraped_path, 'x')
            file.write(json.dumps({}))
            file.close()
        if not os.path.isfile('memes/settings.txt'):
            file = open('memes/settings.txt', 'x')
            file.write(json.dumps({}))
            file.close()

    @staticmethod
    def current_time_as_min():
        now = datetime.datetime.now()
        midnight = now.replace(hour=0, minute=0, second=0, microsecond=0)
        return (now - midnight).seconds // 60

    def run(self):
        READ_WEBSOCKET_DELAY = 1  # 1 second delay between reading from firehose
        if self.client.rtm_connect():
            self.users_list = self.client.api_call("users.list")
            num_tries = 3
            for i in range(1, num_tries + 1):
                try:
                    print("AutoMemer connected and running!")
                    scraped_reddit = False  # have we scraped reddit yet
                    added_memes_to_queue = False  # have we added the contents of scraped.json to our queue
                    scrape_interval = self.load_scrape_interval()
                    while True:
                        command, channel = self.parse_slack_output(self.client.rtm_read())
                        if command and channel:  # if a user tagged @automemer with a command
                            self.handle_command(command, channel)
                        time_as_minutes = self.current_time_as_min()
                        if time_as_minutes % 10 == 0 and not scraped_reddit:
                            Process(target=scrape_reddit.scrape, args=(self.lock,)).start()
                            scraped_reddit = True  # we just added the memes
                        elif time_as_minutes % 10 > 0:  # the minute passed, reset scraped_reddit
                            scraped_reddit = False

                        if time_as_minutes % scrape_interval == 0 and not added_memes_to_queue:
                            self.add_new_memes_to_queue()
                            added_memes_to_queue = True
                        elif time_as_minutes % scrape_interval > 0 and added_memes_to_queue:
                            added_memes_to_queue = False

                        self.pop_queue()
                        time.sleep(READ_WEBSOCKET_DELAY)
                except Exception as e:
                    self.lock.acquire()
                    try:
                        log_error(e)
                    finally:
                        self.lock.release()
                    if i == num_tries:
                        return e
                    text = (
                        "error!!! I'm dying check me\n`{}`\n".format(str(e)) +
                        "_I have resurrected myself {} out of 2 times, to kill me use the `kill` command_"
                        .format(str(i))
                    )
                    self.client.api_call("chat.postMessage", channel=MEME_SPAM_CHANNEL,
                                          text=text, as_user=True)
                    time.sleep(READ_WEBSOCKET_DELAY)

        else:
            print("Connection failed. Invalid Slack token or bot ID?")

    def handle_command(self, command, channel):
        """
        Receives commands directed at the bot and determines if they
        are valid commands. If so, then acts on the commands. If not,
        returns back what it needs for clarification.
        """
        response = '>{}\n'.format(command)
        # specific command responses
        if command.lower().startswith("add"):
            response += self._command_add_sub(command)
        elif command.lower().startswith("delete"):
            response += self._command_delete_sub(command)
        elif command.lower().startswith("details"):
            response += self._command_details(command)
        elif command.lower() == "help":
            response += self._command_help()
        elif command.lower().startswith("increase threshold"):
            response += self._command_set_threshold(command, mode="+")
        elif command.lower().startswith("list thresholds"):
            response += self._command_list_thresholds()
        elif command.lower().startswith("link"):
            response += self._command_details(command, link_only=True)
        elif command.lower() == "list settings":
            response += self._command_list_settings()
        elif command.lower() == "list subreddits":
            response += self._command_list_subs()
        elif command.lower().startswith("set threshold"):
            response += self._command_set_threshold(command)
        elif command.lower().startswith("set post interval"):
            response += self._command_set_post_interval(command)
        elif command.lower().startswith("pop"):
            reply = self._command_pop(command)
            if reply == "":  # if we get an empty string back we've already popped the memes
                return
            else:
                response += reply
        elif command.lower().startswith('num-memes'):
            response += self._command_num_memes(command)
        elif command.lower() == "kill":
            slack_client.api_call("chat.postMessage", channel=MEME_SPAM_CHANNEL,
                                  text="have it your way", as_user=True)
            sys.exit()
        elif command.lower().startswith("echo "):
            response = ''.join(command.split()[1:])
            self.client.api_call(
                "chat.postMessage",
                channel=channel,
                text=response,
                link_names=1,
                as_user=True
            )
            return
        elif command.lower().startswith("users.list"):
            response = type(
                self.client.api_call("users.list")
            )
            self.client.api_call(
                "chat.postMessage",
                channel=channel,
                text=response,
                link_names=1,
                as_user=True
            )
            return

        else:  # a default response
            response = (
                '>*' + command + "*\nI don't know this command :dealwithitparrot:\n"
            )
        self.messages.push((channel, response))

    def load_scrape_interval(self):
        self.lock.acquire()
        try:
            with open('memes/settings.txt', mode='r', encoding='utf-8') as f:
                settings = f.read()
            settings = json.loads(settings)
            interval = settings['scrape_interval']
            return interval
        except Exception as e:
            self.messages.push((MEME_SPAM_CHANNEL, (
                "There was an error setting the scrape interval :sadparrot:\n"
                "Setting the interval to a default 1 hour (60 minutes)\n"
                ">`{}`".format(str(e))
            )))
            log_error(e)
            return 60
        finally:
            self.lock.release()

    def add_new_memes_to_queue(self, limit=15, user_prompt=False):
        _, postable = self.count_memes()
        self.lock.acquire()
        if sum(postable.values()) == 0 and user_prompt:
            self.messages.push((MEME_SPAM_CHANNEL, 'Sorry, we ran out of memes :('))
            return
        try:
            with open(self.scraped_path, mode='r', encoding='utf-8') as f:
                text = f.read()
            with open(self.all_memes_path, 'r', encoding='utf-8') as f:
                all_memes = f.read()
            all_memes = json.loads(all_memes)
            memes = json.loads(text)
            with open(self.settings_path, mode='r', encoding='utf-8') as f:
                settings = json.loads(f.read())
            thresholds = settings['threshold_upvotes']
            memes_by_sub = defaultdict(lambda: list())
            for post, data in sorted(list(memes.items()), key=lambda x: x[1]['created_utc']):
                memes_by_sub[data['sub']].append(data)

            list_of_subs = list(memes_by_sub.keys())
            sub_ind = 0
            while limit > 0 and sum(len(lst) for lst in memes_by_sub) > 0:  # while we want and have more memes to pop
                sub = list_of_subs[sub_ind]
                sub_threshold = thresholds.get(sub.lower(), thresholds['global'])
                while memes_by_sub[sub]:  # while there are memes from this sub
                    meme = memes_by_sub[sub].pop(0)
                    del memes[meme['url']]
                    ups = int(meme.get('highest_ups'))
                    if ups > sub_threshold:  # this meme is DAAANK
                        all_memes[meme['url']]['posted_to_slack'] = True
                        limit -= 1
                        meme_text = (
                            "*{title}* _(from /r/{sub})_ `{ups:,d}`\n{url}".format(
                            title=meme.get('title'),
                            sub=sub,
                            ups=ups,
                            url=meme['url'])
                        )
                        self.messages.push((MEME_SPAM_CHANNEL, meme_text))
                        break
                sub_ind = (sub_ind + 1) % len(list_of_subs)

            with open(self.scraped_path, mode='w', encoding='utf-8') as f:
                f.write(json.dumps(memes, indent=2))
            with open(self.all_memes_path, mode='w', encoding='utf-8') as f:
                f.write(json.dumps(all_memes))
            if 0 < limit and user_prompt:
                self.messages.push((MEME_SPAM_CHANNEL, 'Sorry, we ran out of memes :('))
        except Exception as e:
            self.messages.push((MEME_SPAM_CHANNEL, (
                "There was an error :sadparrot:\n"
                ">`{}`".format(str(e))
            )))
            log_error(e)
        finally:
            self.lock.release()

    def pop_queue(self):
        if not self.messages.is_empty():
            channel, response = self.messages.pop()
            if not self.debug:
                self.client.api_call("chat.postMessage", channel=channel,
                                      text=response, as_user=True)
            else:
                d = dict(api="chat.postMessage", channel=channel, text=response, as_user=True)
                d['time'] = datetime.datetime.now().isoformat()
                with open(self.log_file, 'a') as f:
                    f.write(json.dumps(d, indent=2) + ',\n')

    def parse_slack_output(self, slack_rtm_output):
         """
         the Slack Real Time Messaging API is an events firehose.
         this parsing function returns None unless a message is
         directed at the Bot, based on its ID.
         """
         if slack_rtm_output:
             for output in slack_rtm_output:
                 if 'user' in output:
                     output['username'] = self._get_name(output['user'])
             if output.get('type') not in ['presence_change', 'reconnect_url']:
                print(json.dumps(slack_rtm_output, indent=2))
         output_list = slack_rtm_output
         if output_list and len(output_list) > 0:
             for output in output_list:
                 if output and 'text' in output and self.at_bot in output['text']:
                     # return text after the @ mention, whitespace removed
                     return output['text'].split(self.at_bot)[1].strip(), \
                           output['channel']
         return None, None


    def count_memes(self):
        self.lock.acquire()
        try:
            with open(self.scraped_path, mode='r', encoding='utf-8') as f:
                memes = f.read()
            with open('memes/settings.txt', mode='r', encoding='utf-8') as f:
                settings = f.read()
            memes = json.loads(memes)
            settings = json.loads(settings)
            thresholds = settings['threshold_upvotes']

            total, postable = Counter(), Counter()
            for post, data in memes.items():
                if not data.get('over_18'):
                    sub = data.get('sub', '').lower()
                    ups = data.get('highest_ups')
                    sub_threshold = thresholds.get(sub, thresholds['global'])

                    total[sub] += 1
                    if ups >= sub_threshold:
                        postable[sub] += 1
            return total, postable
        except OSError:
            return Counter(), Counter()
        finally:
            self.lock.release()

    def _command_help(self):
        text = ""
        for command, description in sorted(AutoMemer.bot_commands.items(), key=lambda x: x[0]):
            text += '`{}` - {}\n'.format(command, description)
        return text

    def _command_add_sub(self, command):
        response = ""
        command = command.lower().split()
        if len(command) != 2:
            response += "command must be in the form `add [name]`"
        else:
            command = command[1]
            settings = json.loads(open(self.settings_path).read())
            settings['subs'].append(command)
            self.lock.acquire()
            try:
                with open('memes/settings.txt', mode='w', encoding='utf-8') as f:
                    f.write(json.dumps(settings, indent=2))
            finally:
                self.lock.release()
            response += "_/r/{}_ has been added!".format(command)
        return response

    def _command_delete_sub(self, command):
        response = ""
        command = command.lower().split()
        if len(command) != 2:
            response += "command must be in the form `delete [name]`"
        else:
            sub = command[1]
            settings = json.loads(open(self.settings_path).read())
            previous_subs = settings['subs']
            previous_thresholds = settings['threshold_upvotes']
            if sub not in previous_subs:
                response += (
                    "_/r/{0}_ is not currently being followed, to add it use the"
                    " command `add {0}`".format(sub)
                )
            else:
                previous_subs.remove(sub)
                settings['subs'] = previous_subs
                if sub in previous_thresholds:
                    del previous_thresholds[sub]
                self.lock.acquire()
                try:
                    with open('memes/settings.txt', mode='w', encoding='utf-8') as f:
                        f.write(json.dumps(settings, indent=2))
                finally:
                    self.lock.release()
                response += "_/r/{}_ has been removed".format(sub)
        return response

    def _command_list_settings(self):
        response = ""
        self.lock.acquire()
        try:
            with open("memes/settings.txt", mode='r', encoding='utf-8') as f:
                settings = json.loads(f.read())
        finally:
            self.lock.release()
        for key, val in sorted(settings.items()):
            if key == "subs":
                val = sorted(val)
            response += "`{key}`: {val}\n".format(key=key, val=json.dumps(val, indent=2))
        return response

    def _command_list_thresholds(self):
        response = ""
        self.lock.acquire()
        try:
            settings = json.loads(open(self.settings_path).read())
            thresholds = settings.get('threshold_upvotes')
            response += json.dumps(thresholds, indent=2)
        except OSError as e:
            response += ':sadparrot: error\n'
            response += str(e)
        finally:
            self.lock.release()
        return response

    def _command_list_subs(self):
        response = ""
        self.lock.acquire()
        try:
            settings = json.loads(open(self.settings_path).read())
            subs = sorted(settings.get('subs'))
            response += (
                'The following subreddits are currently being collected: {}'.format(
                    str(subs))
            )
        except OSError as e:
            response += ':sadparrot: error\n'
            response += str(e)
        finally:
            self.lock.release()
        return response

    def _command_set_threshold(self, command, mode=None):
        command_str = 'set'
        if mode =='+':
            command_str = 'increase'
        response = ""
        command = command.lower().split()
        if len(command) not in [3, 4]:
            response += (
                "command must be in the form '{change} threshold {threshold} <optional-sub>'"
                .format(change=command_str)
            )
        elif len(command) == 3 or command[-1].lower() == 'global':
            threshold = command[2]
            try:
                threshold = int(threshold)
            except ValueError:
                response += "{threshold} is not a valid integer".format(threshold=threshold)
            else:
                old_t, new_t = self._command_set_threshold_to(threshold, mode=mode)
                response += (
                    "The global threshold has been set to *{threshold}*! (previously {old})"
                    .format(threshold=new_t, old=old_t)
                )
        else:
            sub = command[-1].lower()
            with open("memes/settings.txt", mode='r', encoding='utf-8') as f:
                settings = json.loads(f.read())
            if sub not in settings['subs']:
                response += "{} is not in the list of subreddits. run `list subreddits` to view a list".format(sub)
            else:
                threshold = command[2]
                try:
                    threshold = int(threshold)
                except ValueError:
                    response += "{threshold} is not a valid integer".format(threshold=threshold)
                else:
                    old_t, new_t = self._command_set_threshold_to(threshold, sub=sub, mode=mode)
                    response += (
                        "The threshold upvotes for _{sub}_ has been set to *{threshold}*! (previously {old})"
                        .format(sub=sub, threshold=new_t, old=old_t)
                    )
        return response

    def _command_set_threshold_to(self, upvote_value, sub='global', mode=None):
        self.lock.acquire()
        try:
            settings = json.loads(open(self.settings_path).read())
            old_t = settings['threshold_upvotes'].get(sub, 'global')
            new_t = upvote_value
            if mode == '+':
                if old_t == 'global':
                    new_t += settings['threshold_upvotes']['global']
                else:
                    new_t += old_t
            new_t = max(1, new_t)
            settings['threshold_upvotes'][sub] = new_t
            with open(self.settings_path, 'w') as f:
                f.write(json.dumps(settings, indent=2))
            return old_t, new_t
        finally:
            self.lock.release()

    def _command_details(self, command, link_only=False):
        response = ""
        command = command.split()
        if len(command) != 2:
            response += "command must be in the form `details <meme_url>`\n"
        else:
            meme_url = html.unescape(command[1][1:-1])
            meme_data = scrape_reddit.update_meme(meme_url, self.lock)
            if meme_data is None:
                response += "I could find any data for this url: `{}`, sorry\n".format(meme_url)
            else:
                if link_only:
                    response += meme_data.get('link')
                else:
                    for key, val in sorted(meme_data.items()):
                        response += "`{key}`: {data}\n".format(key=key, data=val)
        return response

    def _command_set_post_interval(self, command):
        response = ""
        interval = command.split()
        if len(interval) != 4:
            response += "command must be in the form `set post interval <integer>`"
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
                    self.lock.acquire()
                    try:
                        with open("memes/settings.txt", mode='r', encoding='utf-8') as s:
                            settings = s.read()
                        settings = json.loads(settings)
                        settings['scrape_interval'] = interval
                        global scrape_interval
                        scrape_interval = interval
                        with open("memes/settings.txt", mode='w', encoding='utf-8') as s:
                            s.write(json.dumps(settings, indent=2))
                        response += "scrape_interval has been set to *{}*!".format(str(interval))
                    finally:
                        self.lock.release()
        return response

    def _command_pop(self, command):
        response = ""
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
                    self.add_new_memes_to_queue(limit, user_prompt=True)
        else:
            self.add_new_memes_to_queue(user_prompt=True)

        return response

    def _command_num_memes(self, command):
        response = ""
        command = command.lower().split()
        by_sub = 'by_sub' in command
        dank_only = 'dank_only' in command
        total, postable = self.count_memes()
        subs_lower_to_title = {sub.lower(): sub for sub in total}

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
                        sub=subs_lower_to_title[sub],
                        ups=postable[subs_lower_to_title[sub]])
                response += "\n*Combined*: {}".format(str(sum(postable.values())))
        return response

    def _get_name(self, user_id):
        if self.users_list is None:
            return user_id
        for member in self.users_list['members']:
            if member['id'] == user_id:
                name = member.get('name')
                if name is not None:
                    return name
                profile = members.get('profile')
                if profile is not None:
                    name = profile.get('real_name')
                    if name is not None:
                        return name
                return user_id
        return user_id

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


if __name__ == "__main__":
    BOT_ID = os.environ.get("BOT_ID")
    MEME_SPAM_CHANNEL = os.environ.get("MEME_SPAM_CHANNEL")

    meme_bot = AutoMemer(BOT_ID, MEME_SPAM_CHANNEL)
    meme_bot.run()

