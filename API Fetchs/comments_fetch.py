import pandas as pd
import requests
from datetime import datetime
import time
import logging
from reddit_access import Reddit_access
from srp_decorators import request_execute

def comments(res):
    df = pd.DataFrame()
    for post in res.json()['data']['children']:
        df = df.append({'data':post['data']},ignore_index=True)
    return df


def main():
    log = logging.getLogger("app")
    logging.basicConfig(level=logging.DEBUG, filename='comments_data.log', filemode='w',
                        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', datefmt='%d/%m/%Y %H:%M:%S')

    log.info("Starting Application")
    access = Reddit_access()
    headers = access.oauth()
    url1 = access.comments_url
    df1 = pd.DataFrame()
    params = {'limit': 100}
    log.info("start comments data fetch!")

    # hot redditors
    com_df = comments(request_execute(url1, headers))
    if com_df is None:
        logging.error("failed to fetch comments data")
    else:
        # load into file
        com_filename = "comments_" + str(datetime.now().strftime("%Y_%m_%d-%I_%M_%S_%p"))
        com_df.to_csv(("{}.csv".format(com_filename)))
        logging.info("comments data fetch completed!")


    print("Data fetch completed!!")


if __name__ == "__main__":
    main()

