import requests

class Reddit_access:
    hot_redditor_url = "https://oauth.reddit.com/users/popular"
    new_redditor_url = "https://oauth.reddit.com/users/new"
    hot_subreddit_url = "https://oauth.reddit.com/subreddits/popular"
    new_subreddit_url =  "https://oauth.reddit.com/subreddits/new"
    hot_submission_url = "https://oauth.reddit.com/r/all/hot"
    new_submission_url = "https://oauth.reddit.com/r/all/new"
    comments_url = "https://oauth.reddit.com/r/all/comments"
    def __init__(self):
        # note that CLIENT_ID refers to 'personal use script' and SECRET_TOKEN to 'token'
        self.auth = requests.auth.HTTPBasicAuth('######', '#######')
        # here we pass our login method (password), username, and password
        self.params = {'grant_type': 'password',
                       'username': '#####',
                       'password': '######'}
        # setup our header info, which gives reddit a brief description of our app
        self.headers = {'User-Agent': 'SRP/0.0.1'}



    def oauth(self):
        res = requests.post('https://www.reddit.com/api/v1/access_token',
                            auth=self.auth, data=self.params, headers=self.headers)

        # convert response to JSON and pull access_token value
        TOKEN = res.json()['access_token']

        # add authorization to our headers dictionary
        headers = {**self.headers, **{'Authorization': f"bearer {TOKEN}"}}
        return headers