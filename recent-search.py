import os
import json
import urllib.parse
import requests
from requests.auth import AuthBase
import database
from utility import insert_image_as_binary
from datetime import datetime, timedelta
from dateutil import parser
from time import sleep, time
from pytz import timezone, utc
from requests_oauthlib import OAuth1Session
from Models.TwitterTweets import TwitterTweets
from Models.TwitterUsers import TwitterUsers
from Models.Watchers import Watchers
from logger import logger
import pathlib
import sys
import re
import random

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH = os.path.join(ROOT_DIR, 'config.json')

# customer token setting
config = json.load(open(CONFIG_PATH, mode='r'))
CK = config['TWITTER']['consumer_token']
CS = config['TWITTER']['consumer_secret']
AT = config['TWITTER']['access_token']
ATS = config['TWITTER']['access_token_secret']

# Generates a bearer token with consumer key and secret via https://api.twitter.com/oauth2/token.
class BearerTokenAuth(AuthBase):
    def __init__(self, consumer_key, consumer_secret):
        self.bearer_token_url = "https://api.twitter.com/oauth2/token"
        self.consumer_key = consumer_key
        self.consumer_secret = consumer_secret
        self.bearer_token = self.get_bearer_token()

    def get_bearer_token(self):
        response = requests.post(
            self.bearer_token_url,
            auth=(self.consumer_key, self.consumer_secret),
            data={'grant_type': 'client_credentials'},
            headers={'User-Agent': 'LabsRecentSearchQuickStartPython'}
            )

        if response.status_code is not 200:
            raise Exception("Cannot get a Bearer token (HTTP %d): %s" % (response.status_code, response.text))

        body = response.json()
        return body['access_token']

    def __call__(self, r):
        r.headers['Authorization'] = f"Bearer %s" % self.bearer_token
        r.headers['User-Agent'] = 'LabsRecentSearchQuickStartPython'
        return r

#Create Bearer Token for authenticating with recent search.
BEARER_TOKEN = BearerTokenAuth(CK, CS)

# OAuth認証(ユーザタイムライン取得API)
twitter_session = OAuth1Session(CK, CS, AT, ATS)

#Display the returned Tweet JSON.
# parsed = json.loads(response.text)
# pretty_print = json.dumps(parsed, indent=2, sort_keys=True)
# print (pretty_print)

search_url = "https://api.twitter.com/labs/1/tweets/search"
options = "&format=detailed&expansions=author_id,entities.mentions.username,in_reply_to_user_id,referenced_tweets.id,referenced_tweets.id.author_id,attachments.media_keys"
headers = {
    "Accept-Encoding": "gzip"
}

### APIアクセス
def get_tweets(auth, query, next_token=''):

    if next_token != '' :
        next_token = f"&next_token={next_token}"

    url = f"{search_url}?query={query}{options}&max_results=100{next_token}" #{urllib.parse.quote(REQUEST_PARAMETERS)}"
    response = requests.get(url, auth=auth, headers = headers)

    if response.status_code is not 200:
        #raise Exception(f"Error with request (HTTP error code: {response.status_code} - {response.reason}")
        print (f"Error with request (HTTP error code: {response.status_code} - {response.reason}")

    return response


### 2020-02時点の現行TweetObjectsのパース＆DB登録
def  parse_old_tweet_obj(res_text, watcher_id, watchers):
    res = json.loads(res_text)
    if len(res) == 0:
        print('0件')
        return

    print('ぱーすするよ')
    for data in res:
        print(data['created_at'])
        print(data['user']['name'])
        print(data['text'])
        message = data['text']

        reply_to_user_ids = []
        reply_to_names = []
        in_reply_to_user_id = data['in_reply_to_user_id'] if data['in_reply_to_user_id'] is not None else ''

        tweet = {
                    'watcher_id': watcher_id,
                    'tweet_id': data['id_str'],
                    'message': message,
                    'user_id': data['user']['id'],
                    'name': data['user']['screen_name'],
                    'user_name': data['user']['name'],
                    'storage_id': None,
                    'reply_to_user_id': None,
                    'reply_to_name': None,
                    'reply_to_user_name': None, #TODO このAPIだけでは取れない
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
        # TODO 古いツイートだと引用先が消えてたりするので、ちゃんとキーがあるか確認する

        # reply
        if in_reply_to_user_id != '':
            print('リプライあり')
            reply_start = 0
            if 'entities' in data:
                print(data['entities'])
                if 'user_mentions' in data['entities']:
                    for mention in data['entities']['user_mentions']:
                        print(f"screen_name={mention['screen_name']}")
                        print(f"mention['start']={mention['indices'][0]}")
                        reply_to_user_ids.append(mention['id_str'])
                        reply_to_names.append(mention['screen_name'])
                        print(f"reply_start={reply_start}")
                        print(f"mention['end']={mention['indices'][1]}")
                        mention['indices'][0] -= reply_start

                        print(data['text'])
                        if mention['indices'][0] == 0:
                            # リプライ先に関する文章を削除
                            # reply_to_user.append(mention)
                            mention['indices'][1] -= reply_start
                            end = mention['indices'][1] + 1
                            message = message[end:]
                            reply_start = end + reply_start
                            print('↓登録本文↓')
                            print(message)

                tweet['message'] = message
                tweet['reply_to_user_id'] = json.dumps(reply_to_user_ids)
                tweet['reply_to_name'] = json.dumps(reply_to_names)
            else:
                print('データが変')
        
        if 'quoted_status_id_str' in data:
            tweet['quote_tweet_id'] = data['quoted_status_id_str']

        if 'quoted_status' in data:
            tweet['quote_message'] = data['quoted_status']['text']
            tweet['quote_user_id'] = data['quoted_status']['user']['id']
            tweet['quote_name'] = data['quoted_status']['user']['screen_name']
            tweet['quote_user_name'] = data['quoted_status']['user']['name']


        storage_id = None
        if 'entities' in data:
            if 'media' in data['entities']:
                media_urls = []
                entities = data['entities']
                for media in entities['media']:
                    # photo, video, and animated_gif
                    if media['type'] != 'photo':
                        # TODO 画像じゃない場合はタイプ名をそのままstorage_idにいれる
                        print(f"  media type = {media['type']}")
                        storage_id = json.dumps(media['type'])
                        break
                    else:
                        media_urls.append(media['media_url_https'])
                
                # 全部のURL取れたら画像取得して保存
                # TODO 画像順序が正しくない
                print(media_urls)
                if len(media_urls) > 0 and storage_id is None:
                    storage_id = insert_image_as_binary(media_urls)
                tweet['storage_id'] = storage_id

        
        # どのwatcherに属するツイートなのか本文検索
        for watcher in watchers:
            split_keyword = watcher.to_dict()['keyword'].split()
            print(split_keyword)
            append_flg = False
            for keyword in split_keyword:
                # リプライ先も含んだ本文から探す
                # inを使うと大文字小文字区別して処理するから、現行と異なる
                # NG -> if keyword in data['text']:
                tweet_text = tweet['name'] +' '+ tweet['user_name'] +' '+ data['text']
                # print(tweet_text)
                if re.search(keyword, tweet_text, flags=re.IGNORECASE):
                    append_flg = True
                else:
                    append_flg = False
                    break

            if append_flg is True:
                tweet['watcher_id'] = watcher.to_dict()['watcher_id']
                tweets.append(tweet)
                print(tweet)
                print()
            
        try:
            TwitterTweets.insert(tweet)
        except Exception as err:
            print(err)
            logger['error'].exception(err)
            pass
        print('*******************************************')


#-----------------------------------------------------------------------------------------------------------------------

if __name__ == "__main__":
    logger = logger()
    bearer_token = BEARER_TOKEN

    # logger['access'].info('----------------------Start {} ----------------------'.format(cnt))
    # print('----------------------Start {} ----------------------'.format(cnt))
    start = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(start)
    starttime  = time()

    #As we page through results, we will be counting these: 
    request_count = 0
    tweet_count = 0

    try:
        mysql = database.make_connection()

        # watcher取得
        t_watchers = Watchers.where_null('deleted_at')

        # TODO 何回かこの中入ったら強制終了にしたい
        if int(t_watchers.count()) is 0:
            print('生きてる監視ワードがない')
            sys.exit()

        watchers = t_watchers.get()
        join_keyword = '('
        for t_watchers_data in watchers:
            print(t_watchers_data.to_dict())
            split_keyword = t_watchers_data.to_dict()['keyword'].split()
            print(split_keyword)
            # 空白区切りのキーワードを再構築
            if len(split_keyword) > 1:
                keyword = '('
                for sk in split_keyword:
                    keyword = keyword + ' ' + sk
                keyword = keyword + ')'
            else:
                keyword = t_watchers_data.to_dict()['keyword']

            if join_keyword == '(':
                join_keyword = join_keyword + keyword
                # if t_watchers_data.to_dict()['option']:
                #     join_keyword = join_keyword + ' ' +  t_watchers_data.to_dict()['option']

            else:
                join_keyword = join_keyword + ' OR ' + keyword
                # if t_watchers_data.to_dict()['option']:
                #     join_keyword = join_keyword + ' ' +  t_watchers_data.to_dict()['option']
        
        query = join_keyword + ") -is:retweet"
        # print(query)
        # query = urllib.parse.quote(query)
        # print(query)
        # print(len(query))


        # latest_tweet = TwitterTweets.order_by('tweet_id', 'desc').limit(1).first() # TODO posted_atのほうがいい？？
        # if latest_tweet: 
        #     tmp_dict = latest_tweet.to_dict()
        #     posted_at = tmp_dict.get('posted_at').astimezone(timezone('UTC')).isoformat()
        #     posted_at = posted_at.split('+')
        #     print(posted_at[0])

        #     since_id_option = "&start_time=" + str(posted_at[0] + "Z")
        #     query = query + since_id_option

        crawling_start_time = t_watchers_data.to_dict()['crawling_start_time'].astimezone(timezone('UTC'))
        if crawling_start_time is None:
            # 過去に実行されていない場合は1時間前からにする
            crawling_start_time = datetime.utcnow() - timedelta(hours=1)
        
        start_time_option = "&start_time=" + str(crawling_start_time.strftime("%Y-%m-%dT%H:%M:%SZ"))
        query = query + start_time_option


        # 次回の基準になる時間をt_watchersに保存する
        req_start_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        


        print(query)
        print(len(query))
        logger['access'].info(query)
        logger['access'].info(' query len = {}'.format(len(query)))
        print()

        tweets = []
        next_token = ''

        # next_tokenがある限りループ
        while True:
            #loop body
            response = get_tweets(bearer_token, query, next_token)
            if response.status_code is not 200:
                break
            parsed = json.loads(response.text)

            try:
                for data in parsed['data']:
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
                    if 'referenced_tweets' in data:
                        # print(data['referenced_tweets'])
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
                                # TODO FIXME author_idはやくこい
                                print(referenced_tweet['id'])
                                quoted_tweets.append(referenced_tweet['id'])
                                for include_tweet in parsed['includes']['tweets']:
                                    if include_tweet['id'] == referenced_tweet['id']:
                                        quoted_tweets.append(include_tweet)
                                        # print(include_tweet)
                                        print(include_tweet['author_id'])
                                        quoted_author_id = include_tweet['author_id']
                                        quoted_text = include_tweet['text']
                                        tweet['quote_tweet_id'] = include_tweet['id']
                                        tweet['quote_message'] = include_tweet['text']
                                        tweet['quote_user_id'] = include_tweet['author_id']

                    # ただのメンションの場合は本文に@hogeが入ったままでOK
                    # リプライは本文から@を消して別カラムに保存    
                    # print(f"  in_reply_to_user_id = {in_reply_to_user_id}")
                    if in_reply_to_user_id != '':
                        reply_start = 0
                        if 'entities' in data:
                            # print(data['entities'])
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
                    # print(users)
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
                            print(quoted_text)

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
                        # print(parsed['includes']['media'])
                        for media in parsed['includes']['media']:
                            if media['media_key'] in media_keys:
                                if media['type'] != 'photo':
                                    # TODO 画像じゃない場合はタイプ名をそのままstorage_idにいれる
                                    print(f"  media type = {media['type']}")
                                    tweet['storage_id'] = json.dumps(media['type'])
                                    break
                                else:
                                    media_urls.append(media['url'])
                        
                        # 全部のURL取れたら画像取得して保存
                        # print(media_urls)
                        if len(media_urls) > 0 and tweet['storage_id'] is None:
                            tweet['storage_id'] = insert_image_as_binary(media_urls)
                    
                    # どのwatcherに属するツイートなのか本文検索
                    print(f"本文＝{message}")
                    for watcher in watchers:
                        split_keyword = watcher.to_dict()['keyword'].split()
                        print(split_keyword)
                        append_flg = False
                        for keyword in split_keyword:
                            # リプライ先も含んだ本文から探す
                            # inを使うと大文字小文字区別して処理するから、現行と異なる
                            # NG -> if keyword in data['text']:
                            tweet_text = tweet['name'] +' '+ tweet['user_name'] +' '+ data['text']
                            if re.search(keyword, tweet_text, flags=re.IGNORECASE):
                                append_flg = True
                            else:
                                append_flg = False
                                break

                        if append_flg is True:
                            tweet['watcher_id'] = watcher.to_dict()['watcher_id']
                            tweet['message'] = message
                            tweets.append(tweet)
                            print(tweet)
                            print()

                    print('--------------')

            # except KeyError:
            #     print('!!!!keyErr!!!!')
            #     pass

            except Exception as err:
                print(err)
                logger['error'].exception(err)
                # pass

            request_count += 1
            tweet_count += parsed['meta']['result_count']

            # tweet_idが同じものがあるとテーブル制約的にだめなので一つずつ入れる
            for tweet in tweets:
                try:
                    TwitterTweets.insert(tweet)
                except Exception as err:
                    print(err)
                    logger['error'].exception(err)
                    pass

            print('insert OK')
            tweets = []

            # データがまだない場合はリクエスト2回まで(制限しないと無限に取得しようとするので)
            # if not latest_tweet and request_count == 2:
            #     break

            if 'next_token' in parsed['meta']:
                next_token  = parsed['meta']['next_token']
                print("---- next_token あり")
            else:
                print("---- ！！next_tokenなし")
                next_token = None
                break

        print(f"Made {request_count} requests and received {tweet_count} Tweets...")
        print("--------------------------------------------")

        # 次回の基準になる時間をt_watchersに保存する
        Watchers.where('watcher_id', t_watchers_data.to_dict()['watcher_id']).update({'crawling_start_time': req_start_time})
        

        # ユーザタイムライン取得APIでtweetを取得する
        twitter_users = TwitterUsers.where('watcher_id',63).where_null('deleted_at').where('protected',0).order_by('request_at','asc')
        twitter_users_count = twitter_users.count()
        print(f"user count = {twitter_users_count}")

        # offset = random.randint(1, twitter_users_count-900) # 必ず900は取る
        # print(offset)

        # twitter_users = twitter_users.limit(900).offset(offset)
        twitter_users = twitter_users.limit(900)
        twitter_user = twitter_users.get()
        print(twitter_user)# TODO 毎回違う人が選ばれるのを確認したい

        if twitter_user.count() > 0:
            for user in twitter_user:
                user_dict = user.to_dict()
                latest_tweet = TwitterTweets.where('user_id',user_dict['user_id']).order_by('tweet_id', 'desc').limit(1).first()
                if latest_tweet:
                    latest_tweet_dict = latest_tweet.to_dict()
                    # TODO　max_id 設定？
                    params ={
                        'user_id': user_dict['user_id'],
                        'since_id': latest_tweet_dict.get('tweet_id'), # DBに入ってるTweetより新しいものがあれば取得する
                        'include_rts':'false'
                    }
                else:
                    # 初めて取得するユーザタイムラインの場合、とりあえず件数10個まで取得する
                    # この件数はRT数を含む件数なので、RTを取得しないオプションを付けているときは必ず10件取得するとは限らない
                    params ={
                        'user_id': user_dict['user_id'],
                        'count': 10,
                        'include_rts':'false'
                    }
                
                req_time = start = datetime.now()
                req = twitter_session.get('https://api.twitter.com/1.1/statuses/user_timeline.json', params = params)
                if req.status_code == 200:
                    parse_old_tweet_obj(req.text, user_dict['watcher_id'], watchers)
                    TwitterUsers.where('watcher_id',user_dict['watcher_id']).where('user_id', user_dict['user_id']).update({
                        'request_at': req_time
                    })
                elif req.status_code == 429:
                    print('!!!user timeline API エラー!!!  rate limit 制限にかかった')
                    print(req.status_code)
                    logger['error'].exception('!!!user timeline API エラー!!!  rate limit 制限にかかった')
                    logger['error'].exception(req.status_code)
                    logger['error'].exception(json.dumps(req.text))
                    break
                else:
                    print('!!!user timeline API エラー!!!')
                    print(req.status_code)
                    print(params)
                    logger['error'].exception('!!!user timeline API エラー!!!')
                    logger['error'].exception(req.status_code)
                    logger['error'].exception(json.dumps(req.text))
    
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

      