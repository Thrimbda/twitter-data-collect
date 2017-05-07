# -*- coding: utf-8 -*-
# @Author: Michael
# @Date:   2016-11-22 20:11:37
# @Last Modified by:   Macsnow
# @Last Modified time: 2017-05-07 12:56:52
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
        print('added %d followers of %s to DB' % (len(followers), brand['brand_id']))
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
        friends = twitterProcessor.getFollowers(follower['follower_id'], 5000)
        dbConnector.addFriendsToDB(follower['follower_id'], friends)
        print('added %d friends of %s to DB' % (len(friends), follower['follower_id']))
        dbConnector.updateProcessedFlag(follower['follower_id'])
    else:
        raise ResoureceNotAvailableException('No more followers to process')


def processUser(twitterProcessor, dbConnector):
    user = dbConnector.getFriendToProcess()
    if user:
        pass


if __name__ == '__main__':
    dbConnector = DbConnector()
    twitterProcessor = TwitterProcessor(dbConnector.getCredential())
    processFollower(twitterProcessor, dbConnector)
    processFriend(twitterProcessor, dbConnector)
    pass
