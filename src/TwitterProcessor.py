# -*- coding: utf-8 -*-
# @Author: Michael
# @Date:   2016-11-22 19:33:46
# @Last Modified by:   Macsnow
# @Last Modified time: 2017-06-04 17:10:51
from TwitterAPI import TwitterAPI
from TwitterAPI import TwitterError
import json
import time


class TwitterProcessor(object):
    """
    @brief      Class for access twitterAPI to get data.
    """

    def __init__(self, credentials, proxy_url='socks5://127.0.0.1:1080/'):
        super(TwitterProcessor, self).__init__()
        self.credentials = credentials
        self.connector = TwitterAPI(credentials['api_key'],
                                    credentials['api_secret'],
                                    credentials['access_token_key'],
                                    credentials['access_token_secret'],
                                    auth_type='oAuth1',
                                    proxy_url=proxy_url)

    def robustRequest(self, resource, params, max_tries=5):
        """
        If a Twitter request fails, sleep for 15 minutes.
        Do this at most max_tries times before quitting.
        Args:
          twitter .... A TwitterAPI object.
          resource ... A resource string to request.
          params ..... A parameter dictionary for the request.
          max_tries .. The maximum number of tries to attempt.
        Returns:
          A TwitterResponse object, or None if failed.
        """
        for i in range(max_tries):
            try:
                request = self.connector.request(resource, params)
            except TwitterError.TwitterConnectionError as e:
                print(e)
                i -= 1
                continue
            if request.status_code == 200:
                return request.text
            else:
                print('this is No.%d times try.' % (i + 1))
                error = json.loads(request.text)
                # print(error, error['errors'][0]['code'] == 88, 'code' in error)
                if 'code' in repr(error) and error['errors'][0]['code'] == 88:
                    print('Got error:', error['errors'][0]['message'], '\nsleeping for 15 minutes.')
                    print('time:', time.asctime(time.localtime(time.time())))
                    time.sleep(60 * 15)
                    continue
                elif 'code' in repr(error) and error['errors'][0]['code'] == 34 or 'error' in error and error['error'] == 'Not authorized.':
                    # 34 == user does not exist.
                    print('skipping bad request', resource, params)
                    return None

    def getFollowers(self, userId, count=5000):
        """
        Gets the followers.

        Arguments:
            userId {Id} -- The user identifier

        Keyword Arguments:
            count {int: up to 5000} -- The number of user tweets to return (default: {5000})

        Returns:
            {list} -- The followers.
        """
        followers = []
        request = self.robustRequest('followers/ids',
                                     {'user_id': userId, 'count': count})
        if request:
            followers += json.loads(request)['ids']
        return followers

    def getFriends(self, userId, count=5000):
        """
        Gets the friends.

        Arguments:
            userId {Id} -- The user identifier

        Keyword Arguments:
            count {int: up to 5000} -- The number of user tweets to return (default: {5000})

        Returns:
            {list} -- The friends.
        """
        friends = []
        request = self.robustRequest('friends/ids',
                                     {'user_id': userId, 'count': count})
        if request:
            friends += json.loads(request)['ids']
        return friends

    def getUser(self, userId):
        """
        Gets the user of given ID.

        Arguments:
            userId {Id} -- The user identifier

        Returns:
            {dict} -- user profile.
        """
        user = None
        request = self.robustRequest('users/show',
                                     {'user_id': userId})
        if request:
            user = json.loads(request)
        return user

    def getUserTimeline(self, userId, count=200):
        """
        Gets the user timeline.

        Arguments:
            userId {Id} -- The user identifier

        Keyword Arguments:
            count {int: up to 200} -- The number of user tweets to return (default: {200})

        Returns:
            {list} -- tweets of user
        """
        userTimeline = []
        request = self.robustRequest('statuses/user_timeline',
                                     {'user_id': userId, 'count': count})
        if request:
            userTimeline += json.loads(request)
        return userTimeline

    def getRetweets(self, userId, count=100):
        """
        Gets the user retweets of a user.

        Arguments:
            userId {Id} -- The user identifier

        Keyword Arguments:
            count {int: up to 100} -- The number of user tweets to return (default: {100})

        Returns:
            {list} -- retweets of user
        """
        userTimeline = []
        request = self.robustRequest('statuses/retweets_of_me',
                                     {'user_id': userId, 'count': count})
        if request:
            userTimeline += json.loads(request)
        return userTimeline

    def getfavorites(self, userId, count=200):
        """
        Gets the user retweets of a user.

        Arguments:
            userId {Id} -- The user identifier

        Keyword Arguments:
            count {int: up to 200} -- The number of user tweets to return (default: {200})

        Returns:
            {list} -- favorites of user
        """
        userTimeline = []
        request = self.robustRequest('favorites/list',
                                     {'user_id': userId, 'count': count})
        if request:
            userTimeline += json.loads(request)
        return userTimeline
