import pandas as pd
import requests
from datetime import datetime
import time
import logging
from reddit_access import Reddit_access
from srp_decorators import request_execute

def hot_redditor(res):
    df = pd.DataFrame()
    for post in res.json()['data']['children']:
        df = df.append({
            'user_name': post['data']['display_name'],
            'profile_img': post['data']['icon_img'],
            'subscribers': post['data']['subscribers'],
            'name': post['data']['name'],
            'id': post['data']['id'],
            'created_utc': post['data']['created_utc'],
            'user_is_subscriber': post['data']['user_is_subscriber'],
            'user_is_contributor': post['data']['user_is_contributor'],
            'user_is_moderator': post['data']['user_is_moderator'],
            'subreddit_type': post['data']['subreddit_type'],
            'over18': post['data']['over18'],
            'url': post['data']['url']
        }, ignore_index=True)
    return df

def new_redditor(res):
    df = pd.DataFrame()
    for post in res.json()['data']['children']:
        df = df.append({
            'user_name': post['data']['display_name'],
            'profile_img': post['data']['icon_img'],
            'subscribers': post['data']['subscribers'],
            'name': post['data']['name'],
            'id': post['data']['id'],
            'created_utc': post['data']['created_utc'],
            'user_is_subscriber': post['data']['user_is_subscriber'],
            'user_is_contributor': post['data']['user_is_contributor'],
            'user_is_moderator': post['data']['user_is_moderator'],
            'subreddit_type': post['data']['subreddit_type'],
            'over18': post['data']['over18'],
            'url': post['data']['url']
        }, ignore_index=True)
    return df


def main():
    log = logging.getLogger("app")
    logging.basicConfig(level=logging.DEBUG, filename='redditor_data.log', filemode='w',
                        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', datefmt='%d/%m/%Y %H:%M:%S')

    log.info("Starting Application")
    access = Reddit_access()
    headers = access.oauth()
    url1 = access.new_redditor_url
    url2 = access.hot_redditor_url
    df1 = pd.DataFrame()
    df2 = pd.DataFrame()
    params = {'limit': 100}
    log.info("start popular redditors data fetch!")

    # hot redditors
    log.info("start popular redditors data fetch!")
    hot_df = hot_redditor(request_execute(url2, headers))
    if hot_df is None:
        logging.error("failed to fetch hot redditors data")
    else:
        #load into file
        hot_filename = "popular_redditors_" + str(datetime.now().strftime("%Y_%m_%d-%I_%M_%S_%p"))
        hot_df.to_csv(("{}.csv".format(hot_filename)))
        logging.info("hot redditors data fetch completed!")

    # new redditors
    log.info("start new redditors data fetch!")
    new_df = new_redditor(request_execute(url1, headers))
    if new_df is None:
        logging.error("failed to fetch new redditors data")
    else:
        # load into files
        new_filename = "new_redditors_" + str(datetime.now().strftime("%Y_%m_%d-%I_%M_%S_%p"))
        new_df.to_csv("{}.csv".format(new_filename))
        logging.info("new redditors data fetch completed!")

    print("Data fetch completed!!")


if __name__ == "__main__":
    main()

