import pandas as pd
import requests
import logging
from reddit_access import Reddit_access
from srp_decorators import request_execute,csv_to_json



def hotredditor(res2):
    df = pd.DataFrame()
    for post in res2.json()['data']['children']:
        df = df.append({
                'user_name':post['data']['display_name'],
                'profile_img':post['data']['icon_img'],
                'subscribers':post['data']['subscribers'],
                'name':post['data']['name'],
                'id':post['data']['id'],
                'created_utc':post['data']['created_utc'],
                'user_is_subscriber':post['data']['user_is_subscriber'],
                'user_is_contributor':post['data']['user_is_contributor'],
                'user_is_moderator':post['data']['user_is_moderator'],
                'subreddit_type':post['data']['subreddit_type'],
                'over18':post['data']['over18'],
                'url':post['data']['url']
            },ignore_index=True)
    return df

def newredditor(res1):
    df = pd.DataFrame()
    for post in res1.json()['data']['children']:
        df = df.append({
                'user_name':post['data']['display_name'],
                'profile_img':post['data']['icon_img'],
                'own_subscribers':post['data']['subscribers'],
                'name':post['data']['name'],
                'id':post['data']['id'],
                'created_utc':post['data']['created_utc'],
                'user_is_subscriber':post['data']['user_is_subscriber'],
                'user_is_contributor':post['data']['user_is_contributor'],
                'user_is_moderator':post['data']['user_is_moderator'],
                'subreddit_type':post['data']['subreddit_type'],
                'over18':post['data']['over18'],
                'url':post['data']['url']
            },ignore_index=True)
    return df


def main():
    log = logging.getLogger("app")
    logging.basicConfig(level=logging.DEBUG,filename='subreddit_data.log',filemode='w', format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', datefmt='%d/%m/%Y %H:%M:%S')

    log.info("Starting Application")
    access = Reddit_access()
    headers = access.oauth()
    url1 = access.new_redditor_url
    url2 = access.hot_redditor_url
    df1 = pd.DataFrame()
    df2 = pd.DataFrame()
    params = {'limit': 100}
    # hot redditors
    log.info("start popular redditors data fetch!")

    hot_df = hotredditor(request_execute(url2,headers))

    if hot_df is None:
        logging.error("failed to fetch hot redditors data")
    else:
        #load into file
        hot_df.to_csv(("hot_reddit.csv"))
        logging.info("hot redditors data fetch completed!")
    #new redditors
    log.info("start new redditors data fetch!")
    new_df = newredditor(request_execute(url1,headers))
    if new_df is None:
        logging.error("failed to fetch new redditors data")
    else:
        # load into files

        new_df.to_csv("new_reddit.csv")
        logging.info("new redditors data fetch completed!")
    file_csv = "new_reddit.csv"
    file_json = "new_reddit.json"
    new_converter = csv_to_json()
    new_schema = new_converter(file_csv,file_json)
    print(new_schema)
    logging.info("process log completed!")
    print("Data fetch completed!!")


if __name__ == "__main__":
    main()


