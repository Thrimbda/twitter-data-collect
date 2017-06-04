# -*- coding: utf-8 -*-
# @Author: Michael
# @Date:   2016-11-21 22:22:39
# @Last Modified by:   Macsnow
# @Last Modified time: 2017-06-04 18:04:00
from pymongo import MongoClient
import json


class DbConnector(object):
    """Class for database connector.

    use mongodb.
    """

    def __init__(self):
        super(DbConnector, self).__init__()
        self.initDB()

    def getDb(self):
        return self.db

    def getCollection(self, collection):
        return self.db[collection]

    def getBrandToProcess(self):
        brand = self.getCollection('brands').find_one_and_update(filter={'is_processed': False, 'is_taken': False}, update={'$set': {'is_taken': True}}, upsert=False, sort=None, full_response=False)
        if brand:
            brand['brand_id'] = brand.pop('_id')
        return brand

    def getFollowerToProcess(self):
        followersDtls = self.getCollection('follower').find_one_and_update(filter={'is_processed': False, 'is_taken': False}, update={'$set': {'is_taken': True}}, upsert=False, sort=None, full_response=False)
        if followersDtls:
            followersDtls['follower_id'] = followersDtls.pop('_id')
        return followersDtls

    def getFriendToProcess(self):
        firendDtls = self.getCollection('friend').find_one_and_update(filter={'is_processed': False, 'is_taken': False}, update={'$set': {'is_taken': True}}, upsert=False, sort=None, full_response=False)
        if firendDtls:
            firendDtls['friend_id'] = firendDtls.pop('_id')
        return firendDtls

    def getUserToProcess(self, processField):
        firendDtls = self.getCollection('friend').find_one_and_update(filter={processField: {'$exists': False}}, update={'$set': {'is_taken': True}}, upsert=False, sort=None, full_response=False)
        if firendDtls:
            firendDtls['friend_id'] = firendDtls.pop('_id')
        return firendDtls

    def getCredential(self):
        return self.db.twitter_cred.find_one()

    def addFollowersToDB(self, brandId, followersId):
        for followerId in followersId:
            self.getCollection('user').update_one({'_id': followerId}, {'$addToSet': {'follows': brandId}, '$set': {'is_processed': False, 'is_taken': False}}, True)

    def addFriendsToDB(self, userId, friendsList):
        self.getCollection('user').update_one({'_id': userId}, {'$addToSet': {'follows': {'$each': friendsList}}})

    def addUserProfile(self, userId, profile):
        self.getCollection('user').update_one({'_id': userId}, {'$addToSet': {'profile': profile}})

    def addUserTweets(self, userId, tweets):
        self.getCollection('user').update_one({'_id': userId}, {'$addToSet': {'tweets': {'$each': tweets}}})

    def addUserRetweets(self, userId, retweets):
        self.getCollection('user').update_one({'_id': userId}, {'$addToSet': {'retweets': {'$each': retweets}}})

    def addUserfavorites(self, userId, favorites):
        self.getCollection('user').update_one({'_id': userId}, {'$addToSet': {'favorites': {'$each': favorites}}})

    def updateProcessedFlag(self, userId):
        self.getCollection('follower').update_one({'_id': userId}, {'$set': {'is_taken': False, 'is_processed': True}})

    @classmethod
    def initDB(self, forceInit=False):
        self.con = MongoClient('localhost', 27017)
        self.db = self.con.twitter_action
        if not forceInit:
            # with open('../data/brands.json', 'r') as f:
            #     for line in f.readlines():
            #         self.db['brands'].insert_one(eval(line))

            with open('../data/twitter-cred.json', 'r') as f:
                for line in f.readlines():
                    user = json.loads(line)
                    user['is_taken'] = False
                    self.db['twitter_cred'].insert_one(user)
