# -*- coding: utf-8 -*-
import sys
import io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
import os
import requests
import json
import time
from pprint import pprint
from requests.auth import AuthBase
from requests.auth import HTTPBasicAuth
from logger import logger
from utility import insert_image_as_binary
from Models.TwitterTweets import TwitterTweets
from Models.Watchers import Watchers
from Models.WatcherAlertsConf import WatcherAlertsConf
from Models.WatcherAlertHits import WatcherAlertHits
import database
from datetime import datetime, timedelta
from pytz import timezone, utc
from dateutil import parser
from time import sleep, time
from multiprocessing import Process, Queue, Pool
import asyncio
import math
from socket import error as SocketError
import errno
import re
import unicodedata

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH = os.path.join(ROOT_DIR, 'config.json')

# customer token setting
config = json.load(open(CONFIG_PATH, mode='r'))
CK = config['TWITTER']['consumer_token']
CS = config['TWITTER']['consumer_secret']
AT = config['TWITTER']['access_token']
ATS = config['TWITTER']['access_token_secret']

consumer_key = CK  # Add your API key here
consumer_secret = CS  # Add your API secret key here

stream_url = "https://api.twitter.com/labs/1/tweets/stream/filter?format=detailed&expansions=attachments.media_keys,author_id,entities.mentions.username,in_reply_to_user_id,referenced_tweets.id,referenced_tweets.id.author_id"
rules_url = "https://api.twitter.com/labs/1/tweets/stream/filter/rules"

tweet_que = Queue()

# Gets a bearer token
class BearerTokenAuth(AuthBase):
    def __init__(self, consumer_key, consumer_secret):
        self.bearer_token_url = "https://api.twitter.com/oauth2/token"
        self.consumer_key = consumer_key
        self.consumer_secret = consumer_secret
        self.bearer_token = self.get_bearer_token()
        # self.bearer_token = ''

    def get_bearer_token(self):
        response = requests.post(
            self.bearer_token_url, 
            auth=(self.consumer_key, self.consumer_secret),
            data={'grant_type': 'client_credentials'},
            headers={'User-Agent': 'TwitterDevFilteredStreamQuickStartPython'}
        )
        if response.status_code is not 200:
            print(response.status)
            print(response.text)
            raise Exception(f"Cannot get a Bearer token (HTTP %d): %s " % (response.status_code, response.text))

        body = response.json()
        return body['access_token']

    def __call__(self, r):
        r.headers['Authorization'] = f"Bearer %s" % self.bearer_token
        r.headers['User-Agent'] = 'TwitterDevFilteredStreamQuickStartPython'
        return r


def get_all_rules(auth):
    response = requests.get(rules_url, auth=auth)

    if response.status_code is not 200:
        raise Exception(f"Cannot get rules (HTTP %d): %s " % (response.status_code, response.text))

    return response.json()


def delete_all_rules(rules, auth):
    if rules is None or 'data' not in rules:
        return None

    ids = list(map(lambda rule: rule['id'], rules['data']))

    payload = {
        'delete': {
          'ids': ids
        }
    }

    response = requests.post(rules_url, auth=auth, json=payload)

    if response.status_code is not 200:
        raise Exception(f"Cannot delete rules (HTTP %d): %s" % (response.status_code, response.text))

def set_rules(rules, auth):
    if rules is None:
        return

    payload = {
        'add': rules
    }

    response = requests.post(rules_url, auth=auth, json=payload)

    if response.status_code is not 201:
        raise Exception(f"Cannot create rules (HTTP %d): %s" % (response.status_code, response.text))


def parse_tweet_object():
    print('process id:', os.getpid())
    logger['access'].info(f"process id:{os.getpid()}")
    while True:
        if tweet_que.empty():
            print(' !!empty!! ')
            sleep(3)
            continue
        
        parsed = json.loads(tweet_que.get_nowait())
        if parsed is None:
            print(' !!parsed None!! ')
            sleep(3)
            continue
 
        try:
            data = parsed['data']
            print(data['id'])

            tweet = {
                'watcher_id': None,
                'tweet_id': data['id'],
                'message': None,
                'user_id': data['author_id'],
                'name': None,
                'user_name': None,
                'storage_id': None,
                'reply_to_user_id': None,
                'reply_to_name': None,
                'reply_to_user_name': None,
                'quote_tweet_id': None,
                'quote_message': None,
                'quote_user_id': None,
                'quote_name': None,
                'quote_user_name': None,
                'posted_at': parser.parse(data['created_at']).astimezone(timezone('Asia/Tokyo')),
                'created_at': datetime.now(),
                'retweets': 0,
                'likes': 0,
                'replies': 0
            }
            print(data['created_at'])
            print(f" tweet_id = {data['id']}")
            author_id = data['author_id']
            message = data['text']

            reply_to_user = []
            reply_to_user_ids = []
            reply_to_usernames = []
            reply_to_names = []
            in_reply_to_user_id = ''
            quoted_tweets = []
            quoted_author_id = ''
            quote_text = ''
            if 'referenced_tweets' in data:
                for referenced_tweet in data['referenced_tweets']:
                    print(referenced_tweet)

                    # リプライの場合はtypeがreplied_to
                    # だけど、返事をした先のツイート主のIDしかin_reply_to_user_idには保存されない
                    if referenced_tweet['type'] == 'replied_to':
                        if 'in_reply_to_user_id' in data:
                            print(f"  in_reply_to_user_id = {data['in_reply_to_user_id']}")
                            in_reply_to_user_id = data['in_reply_to_user_id'] if data['in_reply_to_user_id'] is not None else ''

                    elif referenced_tweet['type'] == 'quoted':
                        print('---quoted!!')
                        print(referenced_tweet['id'])
                        quoted_tweets.append(referenced_tweet['id'])
                        for include_tweet in parsed['includes']['tweets']:
                            if include_tweet['id'] == referenced_tweet['id']:
                                quoted_tweets.append(include_tweet)
                                quoted_author_id = include_tweet['author_id']
                                # quoted_text = include_tweet['text']
                                tweet['quote_tweet_id'] = include_tweet['id']
                                tweet['quote_message'] = include_tweet['text']
                                tweet['quote_user_id'] = include_tweet['author_id']
                                quote_text = tweet['quote_message']

            # ただのメンションの場合は本文に@hogeが入ったままでOK
            # リプライは本文から@を消して別カラムに保存    
            if in_reply_to_user_id != '':
                reply_start = 0
                if 'entities' in data:
                    if 'mentions' in data['entities']:
                        for mention in data['entities']['mentions']:
                            print(f"username={mention['username']}")
                            print(f"mention['start']={mention['start']}")
                            print(f"reply_start={reply_start}")
                            print(f"mention['end']={mention['end']}")
                            mention['start'] -= reply_start

                            print(data['text'])
                            if mention['start'] == 0:
                                # リプライ先に関する文章を削除
                                reply_to_user.append(mention)
                                mention['end'] -= reply_start
                                message = message[mention['end']+1:]
                                reply_start = mention['end']+ 1 + reply_start
                else:
                    print('データが変')
                    print(in_reply_to_user_id != '')
            print()

            users = parsed['includes']['users']
            for user in users:
                
                if user['id'] == author_id:
                    author_name = user['username']
                    author_screen_name = user['name']
                    print(author_id)
                    print(author_name)
                    print(author_screen_name)
                    # カラム名がパラメータ名と一致してないから気をつける
                    tweet['name'] = user['username']
                    tweet['user_name'] = user['name']
                
                for mention in reply_to_user:    
                    if user['username'] == mention['username']:
                        print(f" reply to {user['username']}  {user['name']}  {user['id']}")
                        reply_to_user_ids.append(user['id'])
                        reply_to_usernames.append(user['username'])
                        reply_to_names.append(user['name'])
                
                if user['id'] == quoted_author_id:
                    quoted_username = user['username']
                    quoted_screen_name = user['name']
                    # カラム名がパラメータ名と一致してないから気をつける
                    tweet['quote_name'] = user['username']
                    tweet['quote_user_name'] = user['name']
                    print(f"  quoted_username={quoted_username} quoted_screen_name={quoted_screen_name} quoted_author_id={quoted_author_id}")
                    quote_text = quote_text +' '+ tweet['quote_name']+ ' '+ tweet['quote_user_name']

            # FIXME カラム名がパラメータ名と一致してないから気をつける
            tweet['reply_to_user_id'] = json.dumps(reply_to_user_ids) if len(reply_to_user_ids)>0 else None
            tweet['reply_to_name'] = json.dumps(reply_to_usernames) if len(reply_to_usernames)>0 else None
            tweet['reply_to_user_name'] = json.dumps(reply_to_names) if len(reply_to_names)>0 else None

            media_keys = []
            if 'attachments' in data:
                print(f"   attachments = {data['attachments']}")
                if 'media_keys' in data['attachments']:
                    # 配列そのまま入れる
                    media_keys = data['attachments']['media_keys']

            if len(media_keys) > 0 :
                media_urls = []
                for media in parsed['includes']['media']:
                    if media['media_key'] in media_keys:
                        if media['type'] != 'photo':
                            # 画像じゃない場合はタイプ名をそのままstorage_idにいれる
                            print(f"  media type = {media['type']}")
                            tweet['storage_id'] = json.dumps(media['type'])
                            break
                        else:
                            media_urls.append(media['url'])
                
                # 全部のURL取れたら画像取得して保存
                if len(media_urls) > 0 and tweet['storage_id'] is None:
                    loop = asyncio.new_event_loop()
                    tweet['storage_id'] = insert_image_as_binary(media_urls, loop)
                    print(tweet['storage_id'])
            
            # どのwatcherに属するツイートなのか本文検索
            print(f"本文＝{message}")
            print(parsed['matching_rules'])

            # tagでどのワードにHITしたのか調べる　複数ルールにヒットした場合も複数入ってくるはず
            try:
                mysql = database.make_connection()
                t_alerts_conf = WatcherAlertsConf.where_null('deleted_at')

                for matching_rule in parsed['matching_rules']:
                    tweet['watcher_id'] = matching_rule['tag']
                    tweet['message'] = message

                    # logger['access'].info(tweet) 場所移動
                    # try:
                    #     TwitterTweets.insert(tweet)                
                    # except Exception as err:
                    #     print(f" !!tweet insert err!!  {err}")
                    #     logger['error'].exception(err)
                    #     pass

                    tweet_text = tweet['name'] +' '+ tweet['user_name'] +' '+ data['text'] +' '+ quote_text
                    tweet_text = unicodedata.normalize('NFKC', tweet_text)
                    watcher = Watchers.where('watcher_id', matching_rule['tag']).first()
                    alerts_conf = t_alerts_conf.where('title_id', watcher.to_dict()['title']).where('sns_type', watcher.to_dict()['sns_type'])
                    alert_array = []
                    if alerts_conf.count() > 0:
                        for conf in alerts_conf.get():
                            print(conf.to_dict()['keyword'])
                            if re.search(conf.to_dict()['keyword'], data['text'], flags=re.IGNORECASE):
                                # アラート設定キーワードが含まれていたのでhitにもinsertする
                                print('alert!')
                                try:
                                    alert_array.append(conf.to_dict()['id'])
                                    alert_hit = {
                                        'conf_id': conf.to_dict()['id'],
                                        'hit_id_child': tweet['tweet_id'],
                                        'created_at': datetime.now(),
                                    }
                                    WatcherAlertHits.insert(alert_hit)
                                except Exception as err:
                                    print(f" !!alert hits insert err!!  {err}")
                                    logger['error'].exception(err)
                                    pass
                    tweet['alert_conf_ids'] = alert_array if len(alert_array) > 0 else None

                    
                    logger['access'].info(tweet)
                    try:
                        TwitterTweets.insert(tweet)                
                    except Exception as err:
                        print(f" !!tweet insert err!!  {err}")
                        logger['error'].exception(err)
                        pass

            except Exception as err:
                print(f" !!err!!  {err}")
                logger['error'].exception(err)
                pass

            finally:
                database.destroy_connection(mysql)
            print('--------------')

        except Exception as e:
            print(f" !!e!!  {e}")
            logger['error'].exception(e)



def stream_connect(auth):
    try:
        response = requests.get(stream_url, auth=auth, stream=True)
        print(response)
    except SocketError as e:
        print(f" !!SocketError!!  {e}")
        logger['error'].exception(f" !!SocketError!!  {e}")
        if e.errno != errno.ECONNRESET:
            raise # Not error we are looking for
        pass # Handle error here.
        return

    if response.status_code is not 200:
        print(response.status_code)
        print(response.text)
        print(response.headers)
        logger['error'].exception(response.status_code)
        logger['error'].exception(response.text)
        logger['error'].exception(response.headers)
        return

    # プロセスプール
    with Pool(processes=5) as pool:
        pool.apply_async(parse_tweet_object)
        pool.apply_async(parse_tweet_object)
        pool.apply_async(parse_tweet_object)
        pool.apply_async(parse_tweet_object)
        pool.apply_async(parse_tweet_object)

        for response_line in response.iter_lines():
            if response_line:
                # なんか全部とりきれてない気がする?
                tweet_que.put_nowait(response_line)
                print("--put--")
                

            

bearer_token = BearerTokenAuth(consumer_key, consumer_secret)
def setup_rules(auth):
    current_rules = get_all_rules(auth)
    print('現在のルール')
    print(current_rules)

    new_rules = []
    try:
        mysql = database.make_connection()
         # watcher取得
        t_watchers = Watchers.where_null('deleted_at').where('sns_type',2)
        if int(t_watchers.count()) is 0:
            print('生きてる監視ワードがない')
            return None

        # t_watchers からルール作成
        watchers = t_watchers.get()
        for t_watchers_data in watchers:
            split_keyword = t_watchers_data.to_dict()['keyword'].split()
            if len(split_keyword) == 1:
                query = "{} OR bio_name:{}".format(split_keyword[0], split_keyword[0].lstrip('@'))
            else:
                search_text_list = []
                sequences = int(math.pow(2, len(split_keyword)))
                for sequence in range(sequences):
                    # 2進数に変換して計算
                    binary = format(sequence, "0{}b".format(len(split_keyword)))
                    keywords = []
                    for index, bit in enumerate(binary):
                        if bit == '0':
                            # 一番前に格納
                            keywords.insert(0, split_keyword[index])
                        else:
                            # 一番後ろに格納
                            bio_keyword = 'bio_name:{}'.format(split_keyword[index].lstrip('@'))
                            keywords.append(bio_keyword)
                    string = " ".join(keywords)
                    search_text_list.append("({})".format(string))
                query = "OR".join(search_text_list)
            
            query = '({}) -is:retweet'.format(query)
            rule = {'value':query, 'tag':t_watchers_data.to_dict()['watcher_id']}
            new_rules.append(rule)
        print('新しいルール')
        print(new_rules)
        print()
        logger['access'].info(f"new_rules = {new_rules}")

    except Exception as err:
        print(f" !!err!!  {err}")
        logger['error'].exception(err)
        pass
    finally:
        database.destroy_connection(mysql)

    delete_all_rules(current_rules, auth)
    set_rules(new_rules, auth)



logger = logger()
# worker_list =[]

# Comment this line if you already setup rules and want to keep them
# setup_rules(bearer_token)

# Listen to the stream.
# This reconnection logic will attempt to reconnect when a disconnection is detected.
# To avoid rate limites, this logic implements exponential backoff, so the wait time
# will increase if the client cannot reconnect to the stream.
timeout = 0

if __name__ == '__main__':
    # setup_rules(bearer_token)
    print(f" os.cpu_count() = { os.cpu_count() }")
    try:
        mysql = database.make_connection()
        while True:
            try:
                stream_connect(bearer_token)
                print('---------------接続切れた-----------------')
                logger['error'].exception('!!!接続が切れた!!!')
                logger['access'].info('!!!接続が切れた!!!')
                sleep(2 ** timeout)
                timeout += 1
            except Exception as ex:
                print(ex)
                logger['error'].exception(ex)
                logger['access'].info(ex)
                sleep(2 ** timeout)
                timeout += 1
                
    except TimeoutError as err:
        print('!! mysqlがtimeout !!')
        logger['error'].exception(err)

    except Exception as err:
        print(err)
        logger['error'].exception(err)

    finally:
        # １週終わったら必ずmysqlへのコネクションを切る
        database.destroy_connection(mysql)
        print('終わり')
