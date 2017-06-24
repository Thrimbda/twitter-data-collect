# -*- coding: utf-8 -*-
# @Author: Michael
# @Date:   2016-11-22 20:11:37
# @Last Modified by:   64509
# @Last Modified time: 2017-06-25 03:09:02
from .DbConnector import DbConnector
from .TwitterProcessor import TwitterProcessor
from .exceptions import ResoureceNotAvailableException


class workshop(object):

    def __init__(self, testmode=True):
        self.testmode = testmode

    def __call__(self, process):
        def wrapper(*args, **kwargs):
            if self.testmode:
                for i in range(3):
                    try:
                        process(*args, **kwargs)
                    except ResoureceNotAvailableException as e:
                        print(e)
                        break
            else:
                while True:
                    try:
                        process(*args, **kwargs)
                    except ResoureceNotAvailableException as e:
                        print(e)
                        break
        return wrapper


@workshop(testmode=False)
def processFollower(twitterProcessor, dbConnector):
    """
    @brief      Gets unprocessed brands from DB
                fetch 300 followers of those brands and
                adds it to DB.
                Halts when all brands are processed or cutt_off is reached

    @param      twitterProcessor    The twitter processor
    @param      dbConnector         The database connector
    """
    brand = dbConnector.getBrandToProcess()
    if brand:
        followers = twitterProcessor.getFollowers(brand['brand_id'], 300)
        dbConnector.addFollowersToDB(brand['brand_id'], followers)
        print('added %d followers of %s to DB' % (len(followers),
                                                  brand['brand_id']))
    else:
        raise ResoureceNotAvailableException('No more brands to process')


@workshop(testmode=False)
def processFriend(twitterProcessor, dbConnector):
    """
    @brief      Gets unprocessed follower and get
                5000 of their friends , adds it to DB
                Halts when all followers are processed or cutt_off is reached

    @param      twitterProcessor    The twitter processor
    @param      dbConnector         The database connector
    """
    follower = dbConnector.getFollowerToProcess()
    if follower:
        friends = twitterProcessor.getFollowers(follower['user_id'], 5000)
        dbConnector.addFriendsToDB(follower['user_id'], friends)
        print('added %d friends of %s to DB' % (len(friends),
                                                follower['user_id']))
        dbConnector.updateProcessedFlag(follower['user_id'])
    else:
        raise ResoureceNotAvailableException('No more followers to process')


@workshop(testmode=False)
def processUserProfile(twitterProcessor, dbConnector):
    user = dbConnector.getUserToProcess('profile')
    if user:
        profile = twitterProcessor.getUser(user['user_id'])
        dbConnector.addUserProfile(user['user_id'], profile)
        print("added user %s's profile to DB" % (user['user_id']))
    else:
        raise ResoureceNotAvailableException('No more followers to process')


@workshop(testmode=False)
def processUserTweets(twitterProcessor, dbConnector):
    user = dbConnector.getUserToProcess('tweets')
    if user:
        for i in range(3200 // 200):
            tweets = twitterProcessor.getUserTimeline(user['user_id'], 200)
            dbConnector.addUserTweets(user['user_id'], tweets)
            print("added user %s's %d tweets to DB" % (len(tweets),
                                                       user['user_id']))
    else:
        raise ResoureceNotAvailableException('No more followers to process')


@workshop(testmode=False)
def processUserRetweets(twitterProcessor, dbConnector):
    user = dbConnector.getUserToProcess('retweets')
    if user:
        for i in range(3200 // 200):
            retweets = twitterProcessor.getUserTimeline(user['user_id'], 100)
            dbConnector.addUserRetweets(user['user_id'], retweets)
            print("added user %s's %d retweets to DB" % (len(retweets),
                                                         user['user_id']))
    else:
        raise ResoureceNotAvailableException('No more followers to process')


@workshop(testmode=False)
def processUserFavorites(twitterProcessor, dbConnector):
    user = dbConnector.getUserToProcess('favorites')
    if user:
        for i in range(1000 // 200):
            favorites = twitterProcessor.getfavorites(user['user_id'], 200)
            dbConnector.addUserfavorites(user['user_id'], favorites)
            print("added user %s's %d favorites to DB" % (len(favorites),
                                                          user['user_id']))
    else:
        raise ResoureceNotAvailableException('No more followers to process')


if __name__ == '__main__':
    dbConnector = DbConnector()
    twitterProcessor = TwitterProcessor(dbConnector.getCredential())
    processUserProfile(twitterProcessor, dbConnector)
    processUserTweets(twitterProcessor, dbConnector)
    processUserRetweets(twitterProcessor, dbConnector)
    processUserFavorites(twitterProcessor, dbConnector)
