import pandas as pd
import requests
from datetime import datetime
import logging
import time
from reddit_access import Reddit_access
from srp_decorators import request_execute

def hot_subreddits(res):
    df = pd.DataFrame()
    for post in res.json()['data']['children']:
        df = df.append({'data':post['data']},ignore_index=True)

    return df

def new_subreddits(res):
    df = pd.DataFrame()
    for post in res.json()['data']['children']:
        df = df.append({'data':post['data']},ignore_index=True)
    return df


def main():
    log = logging.getLogger("app")
    logging.basicConfig(level=logging.DEBUG, filename='subreddit_data.log', filemode='w',
                        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', datefmt='%d/%m/%Y %H:%M:%S')

    log.info("Starting Application")
    access = Reddit_access()
    headers = access.oauth()
    url1 = access.new_subreddit_url
    url2 = access.hot_subreddit_url
    df1 = pd.DataFrame()
    df2 = pd.DataFrame()
    params = {'limit': 100}


    # hot subreddits
    log.info("start popular subreddits data fetch!")
    hot_df = hot_subreddits(request_execute(url2, headers))
    if hot_df is None:
        logging.error("failed to fetch hot subreddits data")
    else:
        # load into file
        hot_filename = "popular_subreddits_" + str(datetime.now().strftime("%Y_%m_%d-%I_%M_%S_%p"))
        hot_df.to_csv(("{}.csv".format(hot_filename)))
        logging.info("popular subreddits data fetch completed!")

    # new redditors
    log.info("start new subreddits data fetch!")
    new_df = new_subreddits(request_execute(url1, headers))
    if new_df is None:
        logging.error("failed to fetch new subreddits data")
    else:
        # load into files
        new_filename = "new_subreddits_" + str(datetime.now().strftime("%Y_%m_%d-%I_%M_%S_%p"))
        new_df.to_csv("{}.csv".format(new_filename))
        logging.info("new subreddits data fetch completed!")

    print("Data fetch completed!!")


if __name__ == "__main__":
    main()

