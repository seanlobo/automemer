import datetime
import html
import json
import os
import queue
import sys
import time
from collections import Counter
from collections import defaultdict
from multiprocessing import Lock
from threading import Thread

import websocket
from slackclient import SlackClient

import scrape_reddit
import utils


class AutoMemer:
    bot_commands = {
        'add <sub>': 'Adds <sub> to the list of subreddits scraped',
        'delete <sub>': 'Deletes <sub> from the list of subreddits scraped',
        'details <meme_url>': (
            'Gives details for a meme if meme_url has been scraped',
        ),
        'help': 'Prints a list of commands and short descriptions',
        'increase threshold <threshold> {optional_subreddit}': (
            'Sets threshold for {optional_subreddit} to the old threshold + '
            'or - the <threshold> value passed. Defaults to global'
        ),
        'kill': 'Kills automemer. Program is stopped, no scraping, no posting',
        'link <url>': 'Prints the link associated with the url passed',
        'list settings': 'Prints out all settings',
        'list subreddits': 'Prints a list of subreddits currently being scraped',
        'list thresholds': 'Prints the thresholds for subs',
        'num-memes {postable_only} {by_sub}': (
            'Prints the number of memes currently waiting to be posted. '
            'To only post memes with enough upvotes use `num-memes postable_only`, to get a '
            'breakdown by subreddit use `num-memes by_sub`'
        ),
        'pop {num}': 'pops {num} memes (or as many as there are) from the queue',
        'set threshold <threshold> {optional_subreddit}': (
            'Sets threshold upvotes a meme must meet to be scraped. If '
            '{optional_subreddit} is specified, sets <threshold> specifically '
            'for that sub, otherwise a global threshold is set (applied to '
            'subs without a specific threshold)'
        ),
        'scrape reddit': 'manually starts a reddit scrape, which usually occurs every 30 minutes',
    }

    def __init__(
        self, bot_id, channel_id, bot_token, dbuser, dbpassword,
        dbname, dbhost, debug=False,
    ):
        self.bot_id = bot_id
        self.at_bot = '<@' + bot_id + '>'
        self.channel_id = channel_id
        self.client = SlackClient(bot_token)
        self.messages = queue.Queue()
        self.lock = Lock()
        self.debug = debug
        self.users_list = self.client.api_call('users.list')

        self.conn = utils.get_connection(dbuser, dbpassword, dbname, dbhost)
        self.cursor = self.conn.cursor()

        # how often to post to slack
        self.post_to_slack_interval = self.load_post_to_slack_interval()

        # creating directories and files
        os.makedirs('memes', exist_ok=True)
        if not os.path.isfile(utils.SCRAPED_PATH):
            file = open(utils.SCRAPED_PATH, 'x')
            file.write(json.dumps({}))
            file.close()
        if not os.path.isfile(utils.SETTINGS_PATH):
            file = open(utils.SETTINGS_PATH, 'x')
            file.write(json.dumps({}))
            file.close()

        utils.log_usage('Running init')

    @staticmethod
    def current_time_as_min():
        now = datetime.datetime.now()
        midnight = now.replace(hour=0, minute=0, second=0, microsecond=0)
        return (now - midnight).seconds // 60

    def run(self):
        utils.log_usage('run()')
        while True:
            try:
                if self.client.rtm_connect():
                    print('AutoMemer connected and running!')
                    utils.log_usage('run() - self.client.rtm_connect()')

                    # scraping thread, which scrapes reddit every 30 minutes
                    t_scrape = Thread(
                        target=self.scrape_repeatedly,
                    )
                    t_scrape.daemon = True
                    t_scrape.start()

                    # command handling thread, which handles slack queries and
                    # posts memes once per second
                    t_command = Thread(
                        target=self.handle_commands_repeatedly,
                    )
                    t_command.daemon = True
                    t_command.start()

                    # meme popping thread, which adds memes to be posted to the queue
                    # once every self.post_to_slack_interval minutes
                    t_post = Thread(
                        target=self.post_to_slack_repeatedly,
                    )
                    t_post.daemon = True
                    t_post.start()

                    # wait (forever) until the 3 threads terminate, by a user killed the program
                    t_scrape.join()
                    t_command.join()
                    t_post.join()
                else:
                    print('Connection failed. Invalid Slack token or bot ID?')
                    break
            except (websocket._exceptions.WebSocketConnectionClosedException, BrokenPipeError):
                pass

    def scrape_repeatedly(self):
        """Scrapes reddit forever, once per interval, until the thread is killed"""
        # sleep until it is an interval of 30 minutes
        cur_time = self.current_time_as_min()
        time.sleep(30 - (cur_time % 30))
        while True:
            # scrape reddit
            t = Thread(
                target=scrape_reddit.scrape,
                args=(self.cursor, self.conn, self.lock),
            )
            t.daemon = True
            t.start()

            # sleep for 30 minutes
            time.sleep(60 * 30)

    def handle_commands_repeatedly(self):
        """
        Handles all commands from slack forever (until killed), and posts memes
        at most once per second when there are any
        """
        while True:
            slack_outputs = self.parse_slack_output(self.client.rtm_read())
            for output in slack_outputs:
                # handle all the commands
                t = Thread(
                    target=self.handle_command,
                    args=(output,),
                )
                t.daemon = True
                t.start()

            # pop a meme if there is one
            self.pop_queue()

            # sleep to rate limit slack api queries
            time.sleep(1)

    def post_to_slack_repeatedly(self):
        """Adds memes to our post queue once per post interval, forever (until killed)"""
        while True:
            if self.current_time_as_min() % self.post_to_slack_interval == 0:
                self.add_new_memes_to_queue()

            # sleep 1 minute
            time.sleep(60)

    def handle_command(self, output):
        """
        Receives commands directed at the bot and determines if they
        are valid commands. If so, then acts on the commands. If not,
        returns back what it needs for clarification.
        """
        command = output.get('@mention')
        if command is None:
            return

        utils.log_usage('handle_command')
        response = f'>{command}\n'
        command = command.lower()
        # specific command responses
        if command.startswith('add'):
            response += self._command_add_sub(output)
        elif command.startswith('delete') or command.startswith('remove'):
            response += self._command_delete_sub(output)
        elif command.startswith('details'):
            response += self._command_details(output)
        elif command == 'help':
            response += self._command_help()
        elif command.startswith('increase threshold'):
            response += self._command_set_threshold(output, mode='+')
        elif command.startswith('list thresholds'):
            response += self._command_list_thresholds()
        elif command.startswith('link'):
            response += self._command_details(output, link_only=True)
        elif command == 'list settings':
            response += self._command_list_settings()
        elif command == 'list subreddits':
            response += self._command_list_subs()
        elif command.startswith('set threshold'):
            response += self._command_set_threshold(output)
        elif command.startswith('set post interval'):
            response += self._command_set_post_interval(output)
        elif command.startswith('pop'):
            reply = self._command_pop(output)
            if reply == '':
                # if we get an empty string back we've already popped the memes
                return
            else:
                response += reply
        elif command.startswith('num-memes'):
            response += self._command_num_memes(output)
        elif command == 'kill':
            self.client.api_call(
                'chat.postMessage', channel=MEME_SPAM_CHANNEL,
                text='have it your way', as_user=True,
            )
            sys.exit(0)
        elif command.startswith('echo '):
            response = ''.join(output.get('@mention').split()[1:])
        elif 'less memes' in command:
            response = '*fewer'
        elif 'fewer time' in command:
            response = '*less'
        elif command == 'scrape reddit':
            t = Thread(
                target=scrape_reddit.scrape,
                args=(self.cursor, self.conn, self.lock),
            )
            t.daemon = True
            t.start()
            response = '+:+1:'
        else:  # a default response
            response = (
                ">*{}*\nI don't know this command :dealwithitparrot:\n"
                .format(command)
            )

        # construct the response
        msg = {
            'channel': output['channel'],
            'text': response,
        }
        if 'thread_ts' in output:
            # the message we are responding too was threaded, so thread
            msg['thread_ts'] = output['thread_ts']
        else:
            msg['thread_ts'] = output['ts']
        self.messages.put(msg)

    def load_post_to_slack_interval(self):
        self.lock.acquire()
        try:
            with open(utils.SETTINGS_PATH, mode='r', encoding='utf-8') as f:
                settings = f.read()
            settings = json.loads(settings)
            interval = settings['scrape_interval']
            return interval
        except Exception as e:
            utils.log_error(e)
            return 60
        finally:
            self.lock.release()

    def add_new_memes_to_queue(self, limit=None, user_prompt=False):
        utils.log_usage(f'add_new_memes_to_queue(limit={limit}, user_prompt={user_prompt})')
        _, postable = self.count_memes()
        # post 50% of the current number of memes in the queue
        limit = limit or int(0.5 * sum(postable.values()))
        utils.log_usage(f'add_new_memes_to_queue - postable_memes={sum(postable.values())}, limit={limit}')
        self.lock.acquire()
        try:
            with open(utils.SCRAPED_PATH, mode='r', encoding='utf-8') as f:
                scraped = f.read()
            scraped_memes = json.loads(scraped)
            with open(utils.SETTINGS_PATH, mode='r', encoding='utf-8') as f:
                settings = json.loads(f.read())
            thresholds = settings['threshold_upvotes']
            memes_by_sub = defaultdict(list)
            for post, data in sorted(
                list(scraped_memes.items()),
                key=lambda x: x[1]['created_utc'],
            ):
                memes_by_sub[data['sub']].append(data)

            list_of_subs = list(memes_by_sub.keys())
            sub_ind = 0
            while limit > 0 and any(memes_by_sub.values()):
                # while we haven't reached the limit and have more memes to post
                sub = list_of_subs[sub_ind]
                sub_threshold = thresholds.get(sub.lower(), thresholds['global'])
                while memes_by_sub[sub]:  # while there are memes from this sub
                    meme = memes_by_sub[sub].pop(0)
                    del scraped_memes[meme['url']]
                    ups = int(meme.get('highest_ups'))
                    if ups > sub_threshold:
                        utils.set_posted_to_slack(
                            self.cursor,
                            meme['id'],
                            self.conn,
                            True,
                        )

                        limit -= 1
                        meme_text = (
                            '*{title}* _(from /r/{sub})_ `{ups:,d}`\n{url}'
                            .format(
                                title=meme.get('title').strip('*'),
                                sub=sub.strip('_'),
                                ups=ups,
                                url=meme['url'],
                            )
                        )
                        self.messages.put({
                            'channel': MEME_SPAM_CHANNEL,
                            'text': meme_text,
                        })
                        break
                sub_ind = (sub_ind + 1) % len(list_of_subs)

            with open(utils.SCRAPED_PATH, mode='w', encoding='utf-8') as f:
                f.write(json.dumps(scraped_memes, indent=2))
            if limit > 0 and user_prompt:
                self.messages.put({
                    'channel': MEME_SPAM_CHANNEL,
                    'text': 'Sorry, we ran out of memes :(',
                })
        except Exception as e:
            self.messages.put({
                'channel': MEME_SPAM_CHANNEL,
                'text': (
                    'There was an error :sadparrot:\n'
                    '>`{}`'.format(str(e))
                ),
            })
            utils.log_error(e)
        finally:
            self.lock.release()

    def pop_queue(self):
        if not self.messages.empty():
            msg = self.messages.get()
            if not self.debug:
                self.client.api_call('chat.postMessage', **msg, as_user=True)
            else:
                msg['api'] = 'chat.postMessage'
                msg['as_user'] = True
                msg['time'] = datetime.datetime.now().isoformat(),
                with open(utils.SLACK_LOG_FILE, 'a') as f:
                    f.write(json.dumps(msg, indent=2) + ',\n')

    def parse_slack_output(self, slack_rtm_output):
        """
        the Slack Real Time Messaging API is an events firehose.
        this parsing function returns None unless a message is
        directed at the Bot, based on its ID.
        """
        if slack_rtm_output:
            for output in slack_rtm_output:
                output['time'] = datetime.datetime.now().isoformat()
                if 'user' in output:
                    output['username'] = self._get_name(output['user'])
                if 'text' in output and self.at_bot in output['text']:
                    # return text after the @ mention, whitespace removed
                    output['@mention'] = output['text'].split(self.at_bot)[1].strip()

            self.log_slack_rtm(slack_rtm_output)
        return slack_rtm_output

    def log_slack_rtm(self, message):
        if not isinstance(message, str):
            message = json.dumps(message, indent=2)

        with open(utils.SLACK_LOG_FILE, 'a') as f:
            f.write(message + ',\n')

    def count_memes(self):
        utils.log_usage('count_memes')
        self.lock.acquire()
        utils.log_usage('count_memes - lock acquired')
        try:
            with open(utils.SCRAPED_PATH, mode='r', encoding='utf-8') as f:
                memes = f.read()
            with open(utils.SETTINGS_PATH, mode='r', encoding='utf-8') as f:
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
            utils.log_usage('count_memes - lock released')

    def _command_help(self):
        text = ''
        for command, description in sorted(AutoMemer.bot_commands.items(), key=lambda x: x[0]):
            text += f'`{command}` - {description}\n'
        return text

    def _command_add_sub(self, output):
        response = ''
        command = output.get('@mention').lower().split()
        if len(command) != 2:
            response += 'command must be in the form `add [name]`'
        else:
            command = command[1]
            settings = json.loads(open(utils.SETTINGS_PATH).read())
            settings['subs'].append(command)
            self.lock.acquire()
            try:
                with open(utils.SETTINGS_PATH, mode='w', encoding='utf-8') as f:
                    f.write(json.dumps(settings, indent=2))
            finally:
                self.lock.release()
            response += f'_/r/{command}_ has been added!'
        return response

    def _command_delete_sub(self, output):
        response = ''
        command = output.get('@mention').lower().split()
        if len(command) != 2:
            response += 'command must be in the form `delete [name]`'
        else:
            sub = command[1]
            settings = json.loads(open(utils.SETTINGS_PATH).read())
            previous_subs = settings['subs']
            previous_thresholds = settings['threshold_upvotes']
            if sub not in previous_subs:
                response += (
                    '_/r/{0}_ is not currently being followed, to add it use the'
                    ' command `add {0}`'.format(sub)
                )
            else:
                previous_subs.remove(sub)
                settings['subs'] = previous_subs
                if sub in previous_thresholds:
                    del previous_thresholds[sub]
                self.lock.acquire()
                try:
                    with open(utils.SETTINGS_PATH, mode='w', encoding='utf-8') as f:
                        f.write(json.dumps(settings, indent=2))
                finally:
                    self.lock.release()
                response += f'_/r/{sub}_ has been removed'
        return response

    def _command_list_settings(self):
        response = ''
        self.lock.acquire()
        try:
            with open(utils.SETTINGS_PATH, mode='r', encoding='utf-8') as f:
                settings = json.loads(f.read())
        finally:
            self.lock.release()
        for key, val in sorted(settings.items()):
            if key == 'subs':
                val = sorted(val)
            response += '`{key}`: {val}\n'.format(key=key, val=json.dumps(val, indent=2))
        return response

    def _command_list_thresholds(self):
        response = ''
        self.lock.acquire()
        try:
            settings = json.loads(open(utils.SETTINGS_PATH).read())
            thresholds = settings.get('threshold_upvotes')
            response += json.dumps(thresholds, indent=2)
        except OSError as e:
            response += ':sadparrot: error\n'
            response += str(e)
        finally:
            self.lock.release()
        return response

    def _command_list_subs(self):
        response = ''
        self.lock.acquire()
        try:
            settings = json.loads(open(utils.SETTINGS_PATH).read())
            subs = sorted(settings.get('subs'))
            response += (
                'The following subreddits are currently being followed: {}'.format(
                    str(subs),
                )
            )
        except OSError as e:
            response += ':sadparrot: error\n'
            response += str(e)
        finally:
            self.lock.release()
        return response

    def _command_set_threshold(self, output, mode=None):
        command_str = 'set'
        if mode == '+':
            command_str = 'increase'
        response = ''
        command = output.get('@mention').lower().split()
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
                response += f'{threshold} is not a valid integer'
            else:
                old_t, new_t = self._command_set_threshold_to(threshold, mode=mode)
                response += (
                    'The global threshold has been set to *{threshold}*! (previously {old})'
                    .format(threshold=new_t, old=old_t)
                )
        else:
            sub = command[-1].lower()
            with open(utils.SETTINGS_PATH, mode='r', encoding='utf-8') as f:
                settings = json.loads(f.read())
            if sub not in settings['subs']:
                response += f'{sub} is not in the list of subreddits. run `list subreddits` to view a list'
            else:
                threshold = command[2]
                try:
                    threshold = int(threshold)
                except ValueError:
                    response += f'{threshold} is not a valid integer'
                else:
                    old_t, new_t = self._command_set_threshold_to(threshold, sub=sub, mode=mode)
                    response += (
                        'The threshold upvotes for _{sub}_ has been set to *{threshold}*! (previously {old})'
                        .format(sub=sub, threshold=new_t, old=old_t)
                    )
        return response

    def _command_set_threshold_to(self, upvote_value, sub='global', mode=None):
        self.lock.acquire()
        try:
            settings = json.loads(open(utils.SETTINGS_PATH).read())
            old_t = settings['threshold_upvotes'].get(sub, 'global')
            new_t = upvote_value
            if mode == '+':
                if old_t == 'global':
                    new_t += settings['threshold_upvotes']['global']
                else:
                    new_t += old_t
            new_t = max(1, new_t)
            settings['threshold_upvotes'][sub] = new_t
            with open(utils.SETTINGS_PATH, 'w') as f:
                f.write(json.dumps(settings, indent=2))
            return old_t, new_t
        finally:
            self.lock.release()

    def _command_details(self, output, link_only=False):
        response = ''
        command = output.get('@mention').split()
        if len(command) != 2:
            response += 'command must be in the form `details <meme_url>`\n'
        else:
            meme_url = html.unescape(command[1][1:-1])
            meme_data = scrape_reddit.update_reddit_meme(
                self.cursor, self.conn, meme_url, self.lock,
            )
            if meme_data is None:
                response += f'I could find any data for this url: `{meme_url}`, sorry\n'
            else:
                if link_only:
                    for meme in meme_data:
                        response += meme.get('link') + '\n'
                else:
                    for meme in meme_data:
                        for key, val in sorted(meme.items()):
                            response += f'`{key}`: {val}\n'
                        response += '\n'
        return response

    def _command_set_post_interval(self, command):
        response = ''
        interval = command.get('@mention').split()
        if len(interval) != 4:
            response += 'command must be in the form `set post interval <integer>`'
        else:
            interval = interval[-1]
            try:
                interval = int(interval)
            except ValueError:
                response += f'{interval} is not an integer :parrotcop:'
            else:
                if interval >= 1440:
                    response += (
                        '```\n'
                        '>>> minutes_per_day()\n'
                        '1440'
                        '```\n'
                        'Too many minutes!'
                    )
                elif interval <= 0:
                    response += 'Please enter a number greater than 0'
                else:
                    self.lock.acquire()
                    try:
                        with open(utils.SETTINGS_PATH, mode='r', encoding='utf-8') as s:
                            settings = s.read()
                        settings = json.loads(settings)
                        settings['scrape_interval'] = interval
                        global scrape_interval
                        scrape_interval = interval
                        with open(utils.SETTINGS_PATH, mode='w', encoding='utf-8') as s:
                            s.write(json.dumps(settings, indent=2))
                        response += 'scrape_interval has been set to *{}*!'.format(str(interval))
                    finally:
                        self.lock.release()
        return response

    def _command_pop(self, output):
        response = ''
        command = output.get('@mention').split()
        if len(command) == 2:
            try:
                limit = int(command[1])
            except ValueError:
                response += "{} isn't a number!".format(str(command[1]))
            else:
                if limit <= 0:
                    response += "You can't pop 0 or fewer memes.."
                else:
                    self.add_new_memes_to_queue(limit, user_prompt=True)
        else:
            self.add_new_memes_to_queue(user_prompt=True)

        return response

    def _command_num_memes(self, output):
        utils.log_usage('handle_command - num-memes - start')
        response = ''
        command = output.get('@mention').lower().split()
        by_sub = 'by_sub' in command
        postable_only = 'postable_only' in command
        total, postable = self.count_memes()
        subs_lower_to_title = {sub.lower(): sub for sub in total}

        if not by_sub:
            if not postable_only:
                text = 'Total memes: {}\nPostable memes: {}'.format(
                    str(sum(total.values())),
                    str(sum(postable.values())),
                )
                response += text
            else:
                response += 'Postable memes: {}'.format(str(sum(postable.values())))
        else:
            if not postable_only:
                for sub in sorted(list(map(lambda x: x.lower(), total.keys()))):
                    response += '*{sub}*: {good}   ({tot})\n'.format(
                        sub=subs_lower_to_title[sub],
                        good=postable[subs_lower_to_title[sub]],
                        tot=total[subs_lower_to_title[sub]],
                    )
                response += '\n*Combined*: {}    ({})'.format(
                    str(sum(postable.values())),
                    str(sum(total.values())),
                )
            else:
                for sub in sorted(list(map(lambda x: x.lower(), total.keys()))):
                    response += '*{sub}*: {ups}\n'.format(
                        sub=subs_lower_to_title[sub],
                        ups=postable[subs_lower_to_title[sub]],
                    )
                response += '\n*Combined*: {}'.format(str(sum(postable.values())))
        utils.log_usage('handle_command - num-memes - end')
        return response

    def _get_name(self, user_id):
        if self.users_list is None:
            return user_id
        for member in self.users_list['members']:
            if member['id'] == user_id:
                name = member.get('name')
                if name is not None:
                    return name
                profile = member.get('profile')
                if profile is not None:
                    name = profile.get('real_name')
                    if name is not None:
                        return name
                return user_id
        return user_id

# ----------------------- SPECIFIC COMMANDS ---------------------------


if __name__ == '__main__':
    BOT_ID = os.environ.get('BOT_ID')
    MEME_SPAM_CHANNEL = os.environ.get('MEME_SPAM_CHANNEL')
    BOT_TOKEN = os.environ.get('SLACK_BOT_TOKEN')

    with open('db.json', 'r') as f:
        db_info = json.loads(f.read())

    meme_bot = AutoMemer(
        BOT_ID,
        MEME_SPAM_CHANNEL,
        BOT_TOKEN,
        db_info['user'],
        db_info['password'],
        db_info['db'],
        db_info['host'],
    )
    try:
        meme_bot.run()
    except Exception as e:
        utils.log_error(e)
    else:
        meme_bot.client.api_call(
            'chat.postMessage',
            channel=MEME_SPAM_CHANNEL,
            text='exiting gracefully',
            as_user=True,
        )
