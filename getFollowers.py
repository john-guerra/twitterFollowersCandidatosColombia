# coding: utf-8
"""Downloads all the followers for a Twitter handle into a Mongo db. 
It downloads 200 followers per minute, per Twitter restrictions

@author= duto_guerra"""


from twitter import *
from pymongo import MongoClient 
import datetime
import time as time2

# Expects a mongo server running on localhost:27017
# with a database twitter_followers, creates two tables
# users and followers
client = MongoClient('localhost', 27017)
dbFollowers = client.twitter_followers
colUsers = dbFollowers.users
colFollowers = dbFollowers.followers


# Put your twitter developer credentials here
auth = OAuth(
    consumer_key='YOUR_CONSUMER_KEY',
    consumer_secret='YOUR_CONSUMER_SECRET',
    token='YOUR_TOKEN',
    token_secret='YOUR_TOKEN_SECRET'
)

# Hack for speeding up, in case you have more than one credential,  you can add them here as a list
keys = [auth]
search = Twitter(auth=auth)


def getUserLastCursor(userId):
    """If the user exists returns it, if not creates it"""
    print "searching", userId
    user = colUsers.find_one({'user':userId})
    if (not user):
        print "Creating user"
        colUsers.insert_one({'user':userId, 
                             'followers':[], 
                             'last_cursor': 15,
                             'help': "John",
                             'i':0,
                             'last_query': datetime.datetime.now()
                            })
        return -1,0
    else:
        print "Found user, with " , len(user['followers']) , " followers"
        return user['last_cursor'],user['i']



TIMEOUT=60/len(keys)
def getFollowersForUser(userId):
    print "getting followers for %s"%(userId)
    cursor,i=getUserLastCursor(userId)
    print "Last cursor", cursor
    k=0
    errorCount = 0
    while True:
        k+=1
        search = Twitter(auth=keys[k%len(keys)])        
        try:
            res = search.followers.list(screen_name=userId, 
                                        cursor=cursor,
                                        count=200)
        except TwitterHTTPError as error:
            print "Error getting followers for " + userId +  " trying to continue "
            print error
            if "errors" in error.response_data.keys() and  \
                len(error.response_data["errors"])>0 and \
                "code" in error.response_data["errors"][0].keys() and \
                error.response_data["errors"][0]["code"] == 88:
                raise
            else:
                errorCount+=1
                print "Error count=",errorCount," trying again on ", TIMEOUT*errorCount
                time2.sleep(TIMEOUT*errorCount)

                continue
                
        print "Got results, inserting to db, next cursor", res['next_cursor'], " i", i
        colFollowers.insert_many(
            [{'user':userId, 'i':j+i, 'follower':f} for j,f in enumerate(res['users'])],
            ordered=True
        )
        i+=len(res["users"])
        colUsers.update_one(
            {'user': userId},
            {
             "$set" : {
                 "last_cursor": res["next_cursor"],
                 "i":i,
                 'last_query': datetime.datetime.now()
                 
             }
            }
            
        )
        cursor = res['next_cursor']

        errorCount=0
        if cursor == 0:
            print "Done!"
            break
        else:
            print "Sleeping"
            time2.sleep(TIMEOUT)


# Change for who you want to download
HANDLE_TO_DOWNLOAD = "duto_guerra"
getFollowersForUser(HANDLE_TO_DOWNLOAD)

