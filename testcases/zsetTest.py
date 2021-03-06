__author__ = 'Ma Haoxiang'

import requests
import json
import time

class zsetTest:
    def __init__(self):
        self.host, self.port = self.readTestConfig()

    # read test configuration from test.conf
    def readTestConfig(self):
        with open("test.conf", "r") as configFile:
            config = json.loads(configFile.read())
            try:
                host = config["HOST"]
                port = config["PORT"]
                return host, port
            except:
                return None, None

    # write test log
    def writeLog(self, url, params, response):
        with open("testLog.log", "a") as testLogFile:
            testLogFile.write("DATE: " + time.strftime("%Y/%m/%d %H:%M:%S",time.localtime(time.time())))
            testLogFile.write("\n")
            testLogFile.write("URL: " + url)
            testLogFile.write("\n")
            testLogFile.write("PARAMS: " + params)
            testLogFile.write("\n")
            testLogFile.write("RESPONSE: " + response)
            testLogFile.write("\n")

    def makeZSetTest(self):
        url = "http://" + self.host + ":" + str(self.port) + "/makeZSet"

        params = {
            "dbName": "db0",
            "zsetName": "zset1"
        }

        # case1 make a new zset
        response = requests.post(url=url, json=params)
        self.writeLog(url, json.dumps(params), response.content.decode())

        # case2 make a set repeatedly
        response = requests.post(url=url, json=params)
        self.writeLog(url, json.dumps(params), response.content.decode())

        # case3 unknown database name
        params["dbName"] = "db999"
        response = requests.post(url=url, json=params)
        self.writeLog(url, json.dumps(params), response.content.decode())

        # case4 invalid zset name
        params["dbName"] = "db0"
        params["zsetName"] = "!@abc"
        response = requests.post(url=url, json=params)
        self.writeLog(url, json.dumps(params), response.content.decode())

        # case5 error url
        errorUrl = "http://" + self.host + ":" + str(self.port) + "/makezset"
        response = requests.post(url=errorUrl, json=params)
        self.writeLog(url, json.dumps(params), response.content.decode())

    def getZSetTest(self):
        url = "http://" + self.host + ":" + str(self.port) + "/getZSet/{}/{}"

        # case1 create a set and then get it
        createUrl = "http://" + self.host + ":" + str(self.port) + "/makeZSet/{}/{}"
        response = requests.get(createUrl.format("db0", "zset1"))
        response = requests.get(url.format("db0","zset1"))
        self.writeLog(url.format("db0", "zset1"), "", response.content.decode())

        # case2 unknown database name
        response = requests.get(url.format("db999","zset1"))
        self.writeLog(url.format("db999","zset1"), "", response.content.decode())

        # case3 unknown set name
        response = requests.get(url.format("db0", "zset123"))
        self.writeLog(url.format("db0", "zset123"), "", response.content.decode())

        # case4 error url
        errorUrl = "http://" + self.host + ":" + str(self.port) + "/getzset/{}/{}"
        response = requests.get(errorUrl.format("db0", "zset1"))
        self.writeLog(errorUrl.format("db0", "zset1"), "", response.content.decode())

    def insertZSetTest(self):
        url = "http://" + self.host + ":" + str(self.port) + "/insertZSet"

        # case1 create a set and then insert
        params = {
            "dbName": "db0",
            "zsetName": "zset1"
        }
        createUrl = "http://" + self.host + ":" + str(self.port) + "/makeZSet"
        response = requests.post(url=createUrl, json=params)

        insertParams = {
            "dbName":"db0",
            "zsetName":"zset1",
            "value":"hello",
            "score":1
        }
        response = requests.put(url,json=insertParams)
        self.writeLog(url, json.dumps(insertParams), response.content.decode())

        # case2 insert an existed value
        response = requests.put(url,json=insertParams)
        self.writeLog(url, json.dumps(insertParams), response.content.decode())

        # case3 unknown database name
        insertParams["dbName"] = "db999"
        response = requests.put(url, json=insertParams)
        self.writeLog(url, json.dumps(insertParams), response.content.decode())

        # case4 unknown set name
        insertParams["dbName"] = "db0"
        insertParams["zsetName"] = "zset123"
        response = requests.put(url, json=insertParams)
        self.writeLog(url, json.dumps(insertParams), response.content.decode())

        # case5 error database name type
        insertParams["dbName"] = [7,8,9]
        response = requests.put(url, json=insertParams)
        self.writeLog(url, json.dumps(insertParams), response.content.decode())

        # case6 error set name type
        insertParams["dbName"] = "db0"
        insertParams["zsetName"] = [4,5,6]
        response = requests.put(url, json=insertParams)
        self.writeLog(url, json.dumps(insertParams), response.content.decode())

        # case7 error url
        errorUrl = "http://" + self.host + ":" + str(self.port) + "/insertzset"
        insertParams = {
            "dbName": "db0",
            "zsetName": "set1",
            "value": "hello",
            "score":1
        }
        response = requests.put(errorUrl, json=insertParams)
        self.writeLog(errorUrl, json.dumps(insertParams), response.content.decode())

    def rmFromZSetTest(self):
        url = "http://" + self.host + ":" + str(self.port) + "/rmFromZSet"

        # case1 create, insert, then remove
        createParams = {
            "dbName": "db0",
            "zsetName": "zset1"
        }
        createUrl = "http://" + self.host + ":" + str(self.port) + "/makeZSet"
        insertUrl = "http://" + self.host + ":" + str(self.port) + "/insertZSet"
        response = requests.post(url=createUrl, json=createParams)
        insertParams = {
            "dbName": "db0",
            "zsetName": "zset1",
            "value": "hello",
            "score": 1
        }
        response = requests.put(url=insertUrl, json=insertParams)
        removeParams = {
            "dbName":"db0",
            "zsetName":"zset1",
            "value":"hello"
        }
        response = requests.put(url=url, json=removeParams)
        self.writeLog(url, json.dumps(removeParams), response.content.decode())

        # case2 remove from an empty set
        response = requests.put(url=url, json=removeParams)
        self.writeLog(url, json.dumps(removeParams), response.content.decode())

        # case3 unknown database name
        removeParams["dbName"] = "db999"
        response = requests.put(url=url, json=removeParams)
        self.writeLog(url, json.dumps(removeParams), response.content.decode())

        # case4 unknown set name
        removeParams["dbName"] = "db0"
        removeParams["zsetName"] = "zset123"
        response = requests.put(url=url, json=removeParams)
        self.writeLog(url, json.dumps(removeParams), response.content.decode())

        # case5 error database name type
        removeParams["dbName"] = [1,2,3]
        removeParams["zsetName"] = "zset1"
        response = requests.put(url=url, json=removeParams)
        self.writeLog(url, json.dumps(removeParams), response.content.decode())

        # case6 error set name type
        removeParams["dbName"] = "db0"
        removeParams["zsetName"] = [1,2,3]
        response = requests.put(url=url, json=removeParams)
        self.writeLog(url, json.dumps(removeParams), response.content.decode())

        # case7 non-existed set value
        removeParams["dbName"] = "db0"
        removeParams["zsetName"] = "zset1"
        removeParams["value"] = "key"
        response = requests.put(url=url, json=removeParams)
        self.writeLog(url, json.dumps(removeParams), response.content.decode())

        # case8 error url
        errorUrl = "http://" + self.host + ":" + str(self.port) + "/rmfromzset"
        response = requests.put(url=errorUrl, json=removeParams)
        self.writeLog(errorUrl, json.dumps(removeParams), response.content.decode())

    def clearZSetTest(self):
        url = "http://" + self.host + ":" + str(self.port) + "/clearZSet/{}/{}"

        # case1 create, insert, then clear
        createParams = {
            "dbName": "db0",
            "zsetName": "zset1"
        }
        createUrl = "http://" + self.host + ":" + str(self.port) + "/makeZSet"
        insertUrl = "http://" + self.host + ":" + str(self.port) + "/insertZSet"
        response = requests.post(url=createUrl, json=createParams)
        insertParams = {
            "dbName": "db0",
            "zsetName": "zset1",
            "value": "hello",
            "score": 1
        }
        response = requests.put(url=insertUrl, json=insertParams)
        response = requests.put(url.format("db0","zset1"))
        self.writeLog(url.format("db0","zset1"), "", response.content.decode())

        # case2 clear an empty set
        response = requests.put(url.format("db0","zset1"))
        self.writeLog(url.format("db0","zset1"), "", response.content.decode())

        # case3 unknown database name
        response = requests.put(url.format("db123","zset1"))
        self.writeLog(url.format("db123","zset1"), "", response.content.decode())

        # case4 unknown set name
        response = requests.put(url.format("db0","zset123"))
        self.writeLog(url.format("db0","zset123"), "", response.content.decode())

        # case5 error url
        errorUrl = "http://" + self.host + ":" + str(self.port) + "/clearzset/{}/{}"
        response = requests.put(errorUrl.format("db0","zset1"))
        self.writeLog(errorUrl.format("db0","zset1"), "", response.content.decode())

    def deleteSetTest(self):
        url = "http://" + self.host + ":" + str(self.port) + "/deleteZSet/{}/{}"

        # case1 create, then delete a set
        createParams = {
            "dbName": "db0",
            "zsetName": "zset1"
        }
        createUrl = "http://" + self.host + ":" + str(self.port) + "/makeZSet"
        response = requests.post(url=createUrl, json=createParams)
        response = requests.delete(url.format("db0", "zset1"))
        self.writeLog(url.format("db0","zset1"), "", response.content.decode())

        # case2 delete a non-existed set
        response = requests.delete(url.format("db0","zset1"))
        self.writeLog(url.format("db0","zset1"), "", response.content.decode())

        # case3 unknown database name
        response = requests.delete(url.format("db789","zset1"))
        self.writeLog(url.format("db789","zset1"), "", response.content.decode())

        # case4 unknown set name
        response = requests.delete(url.format("db0","zset123"))
        self.writeLog(url.format("db0","zset123"), "", response.content.decode())

        # case5 error url
        errorUrl = "http://" + self.host + ":" + str(self.port) + "/deletezset/{}/{}"
        response = requests.delete(errorUrl.format("db0","zset123"))
        self.writeLog(errorUrl.format("db0","zset123"), "", response.content.decode())

    def searchZSetTest(self):
        url = "http://" + self.host + ":" + str(self.port) + "/searchZSet/{}/{}"

        # case1 create several sets, then search
        createParams = {
            "dbName": "db0",
            "zsetName": "zset1"
        }
        createUrl = "http://" + self.host + ":" + str(self.port) + "/makeZSet"
        response = requests.post(url=createUrl, json=createParams)
        createParams["zsetName"] = "zs"
        response = requests.post(url=createUrl, json=createParams)
        createParams["zsetName"] = "z1"
        response = requests.post(url=createUrl, json=createParams)
        createParams["zsetName"] = "abczs"
        response = requests.post(url=createUrl, json=createParams)
        createParams["zsetName"] = "set"
        response = requests.post(url=createUrl, json=createParams)
        createParams["zsetName"] = "z*"
        response = requests.post(url=createUrl, json=createParams)
        response = requests.get(url=url.format("db0", "z*"))
        self.writeLog(url.format("db0","z*"), "", response.content.decode())

        # case2 unknown database name
        response = requests.get(url.format("db999","z*"))
        self.writeLog(url.format("db999","z*"), "", response.content.decode())

        # case3 universal regular expression
        response = requests.get(url.format("db0","*"))
        self.writeLog(url.format("db0","*"), "", response.content.decode())

        # case4 error url
        errorUrl = "http://" + self.host + ":" + str(self.port) + "/searchzset/{}/{}"
        response = requests.get(errorUrl.format("db0","*"))
        self.writeLog(errorUrl.format("db0","*"), "", response.content.decode())

    def findMinTest(self):
        url = "http://" + self.host + ":" + str(self.port) + "/findMinFromZSet/{}/{}"

        # case1 create a zset, insert, then find min
        createParams = {
            "dbName": "db0",
            "zsetName": "zset1"
        }
        createUrl = "http://" + self.host + ":" + str(self.port) + "/makeZSet"
        response = requests.post(url=createUrl, json=createParams)

        insertUrl = "http://" + self.host + ":" + str(self.port) + "/insertZSet"
        insertParams = {
            "dbName": "db0",
            "zsetName": "zset1",
            "value": "hello",
            "score": 1
        }
        response = requests.put(url=insertUrl, json=insertParams)
        insertParams["value"] = "world"
        insertParams["score"] = 3
        response = requests.put(url=insertUrl, json=insertParams)
        response = requests.get(url.format("db0", "zset1"))
        self.writeLog(url.format("db0","zset1"), "", response.content.decode())

        # case2 find min in an empty zset
        createParams["zsetName"] = "zset2"
        response = requests.post(url=createUrl, json=createParams)
        response = requests.get(url.format("db0", "zset2"))
        self.writeLog(url.format("db0","zset2"), "", response.content.decode())

        # case3 unknown database name
        response = requests.get(url.format("db123", "zset2"))
        self.writeLog(url.format("db123", "zset2"), "", response.content.decode())

        # case4 unknown zset name
        response = requests.get(url.format("db0", "zset123"))
        self.writeLog(url.format("db0", "zset123"), "", response.content.decode())

        # case5 error url
        errorUrl = "http://" + self.host + ":" + str(self.port) + "/findminfromzset/{}/{}"
        response = requests.get(errorUrl.format("db0", "zset1"))
        self.writeLog(errorUrl.format("db0", "zset1"), "", response.content.decode())

    def findMaxTest(self):
        url = "http://" + self.host + ":" + str(self.port) + "/findMaxFromZSet/{}/{}"

        # case1 create a zset, insert, then find max
        createParams = {
            "dbName": "db0",
            "zsetName": "zset1"
        }
        createUrl = "http://" + self.host + ":" + str(self.port) + "/makeZSet"
        response = requests.post(url=createUrl, json=createParams)

        insertUrl = "http://" + self.host + ":" + str(self.port) + "/insertZSet"
        insertParams = {
            "dbName": "db0",
            "zsetName": "zset1",
            "value": "hello",
            "score": 1
        }
        response = requests.put(url=insertUrl, json=insertParams)
        insertParams["value"] = "world"
        insertParams["score"] = 3
        response = requests.put(url=insertUrl, json=insertParams)
        response = requests.get(url.format("db0", "zset1"))
        self.writeLog(url.format("db0", "zset1"), "", response.content.decode())

        # case2 find min in an empty zset
        response = requests.get(createUrl.format("db0", "zset2"))
        response = requests.get(url.format("db0", "zset2"))
        self.writeLog(url.format("db0", "zset2"), "", response.content.decode())

        # case3 unknown database name
        response = requests.get(url.format("db123", "zset2"))
        self.writeLog(url.format("db123", "zset2"), "", response.content.decode())

        # case4 unknown zset name
        response = requests.get(url.format("db0", "zset123"))
        self.writeLog(url.format("db0", "zset123"), "", response.content.decode())

        # case5 error url
        errorUrl = "http://" + self.host + ":" + str(self.port) + "/findmaxfromzset/{}/{}"
        response = requests.get(errorUrl.format("db0", "zset1"))
        self.writeLog(errorUrl.format("db0", "zset1"), "", response.content.decode())

    def getScoreTest(self):
        url = "http://" + self.host + ":" + str(self.port) + "/getScore/{}/{}/{}"

        # case1 create a zset, insert, then get score
        createParams = {
            "dbName": "db0",
            "zsetName": "zset1"
        }
        createUrl = "http://" + self.host + ":" + str(self.port) + "/makeZSet"
        response = requests.post(url=createUrl, json=createParams)

        insertUrl = "http://" + self.host + ":" + str(self.port) + "/insertZSet"
        insertParams = {
            "dbName": "db0",
            "zsetName": "zset1",
            "value": "hello",
            "score": 1
        }
        response = requests.put(url=insertUrl, json=insertParams)
        response = requests.get(url.format("db0", "zset1", "hello"))
        self.writeLog(url.format("db0", "zset1", "hello"), "", response.content.decode())

        # case2 get score from empty zset
        response = requests.get(createUrl.format("db0", "zset2"))
        response = requests.get(url.format("db0", "zset2", "hello"))
        self.writeLog(url.format("db0", "zset2", "hello"), "", response.content.decode())

        # case3 unknown database name
        response = requests.get(url.format("db123", "zset1", "hello"))
        self.writeLog(url.format("db123", "zset1", "hello"), "", response.content.decode())

        # case4 unknown zset name
        response = requests.get(url.format("db0", "zset123", "hello"))
        self.writeLog(url.format("db0", "zset123", "hello"), "", response.content.decode())

        # case5 error url
        errorUrl = "http://" + self.host + ":" + str(self.port) + "/getscore/{}/{}/{}"
        response = requests.get(errorUrl.format("db0", "zset1", "hello"))
        self.writeLog(errorUrl.format("db0", "zset1", "hello"), "", response.content.decode())

    def getValuesByRangeTest(self):
        url = "http://" + self.host + ":" + str(self.port) + "/getValuesByRange/{}/{}/{}/{}"

        # case1 create a zset, insert, then get values by range
        createParams = {
            "dbName": "db0",
            "zsetName": "zset1"
        }
        createUrl = "http://" + self.host + ":" + str(self.port) + "/makeZSet"
        response = requests.post(url=createUrl, json=createParams)
        insertUrl = "http://" + self.host + ":" + str(self.port) + "/insertZSet"
        insertParams = {
            "dbName": "db0",
            "zsetName": "zset1",
            "value": "a",
            "score": 1
        }
        response = requests.put(url=insertUrl, json=insertParams)
        insertParams["value"] = "b"
        insertParams["score"] = 3
        response = requests.put(url=insertUrl, json=insertParams)
        insertParams["value"] = "c"
        insertParams["score"] = 5
        response = requests.put(url=insertUrl, json=insertParams)

        response = requests.get(url.format("db0", "zset1", 1, 5))
        self.writeLog(url.format("db0", "zset1", 1, 5), "", response.content.decode())

        # case2 no value is inside the range
        response = requests.get(url.format("db0", "zset1", 10, 20))
        self.writeLog(url.format("db0", "zset1", 10, 20), "", response.content.decode())

        # case3 unknown database name
        response = requests.get(url.format("db123", "zset1", 1, 5))
        self.writeLog(url.format("db123", "zset1", 1, 5), "", response.content.decode())

        # case4 unknown zset name
        response = requests.get(url.format("db0", "zset123", 1, 5))
        self.writeLog(url.format("db0", "zset123", 1, 5), "", response.content.decode())

        # case5 error url
        errorUrl = "http://" + self.host + ":" + str(self.port) + "/getvaluesbyrange/{}/{}/{}/{}"
        response = requests.get(errorUrl.format("db0", "zset1", 1, 5))
        self.writeLog(errorUrl.format("db0", "zset1", 1, 5), "", response.content.decode())

    def getSizeTest(self):
        url = "http://" + self.host + ":" + str(self.port) + "/getZSetSize/{}/{}"

        # case1 create a zset, insert, then get size
        createParams = {
            "dbName": "db0",
            "zsetName": "zset1"
        }
        createUrl = "http://" + self.host + ":" + str(self.port) + "/makeZSet"
        response = requests.post(url=createUrl, json=createParams)
        insertUrl = "http://" + self.host + ":" + str(self.port) + "/insertZSet"
        insertParams = {
            "dbName": "db0",
            "zsetName": "zset1",
            "value": "a",
            "score": 1
        }
        response = requests.put(url=insertUrl, json=insertParams)
        insertParams["value"] = "b"
        insertParams["score"] = 3
        response = requests.put(url=insertUrl, json=insertParams)
        insertParams["value"] = "c"
        insertParams["score"] = 5
        response = requests.put(url=insertUrl, json=insertParams)

        response = requests.get(url.format("db0", "zset1"))
        self.writeLog(url.format("db0", "zset1"), "", response.content.decode())

        # case2 get size from empty zset
        response = requests.get(createUrl.format("db0", "zset2"))
        response = requests.get(url.format("db0", "zset2"))
        self.writeLog(url.format("db0", "zset2"), "", response.content.decode())

        # case3 unknown database name
        response = requests.get(url.format("db123", "zset1"))
        self.writeLog(url.format("db123", "zset1"), "", response.content.decode())

        # case4 unknown zset name
        response = requests.get(url.format("db0", "zset123"))
        self.writeLog(url.format("db0", "zset123"), "", response.content.decode())

        # case5 error url
        errorUrl = "http://" + self.host + ":" + str(self.port) + "/getzsetsize/{}/{}"
        response = requests.get(errorUrl.format("db0", "zset1"))
        self.writeLog(errorUrl.format("db0", "zset1"), "", response.content.decode())

    def getRankTest(self):
        url = "http://" + self.host + ":" + str(self.port) + "/getRank/{}/{}/{}"

        # case1 create a zset, insert
        createParams = {
            "dbName": "db0",
            "zsetName": "zset1"
        }
        createUrl = "http://" + self.host + ":" + str(self.port) + "/makeZSet"
        response = requests.post(url=createUrl, json=createParams)
        insertUrl = "http://" + self.host + ":" + str(self.port) + "/insertZSet"
        insertParams = {
            "dbName": "db0",
            "zsetName": "zset1",
            "value": "a",
            "score": 1
        }
        response = requests.put(url=insertUrl, json=insertParams)
        insertParams["value"] = "b"
        insertParams["score"] = 3
        response = requests.put(url=insertUrl, json=insertParams)
        insertParams["value"] = "c"
        insertParams["score"] = 5
        response = requests.put(url=insertUrl, json=insertParams)

        response = requests.get(url.format("db0", "zset1", "b"))
        self.writeLog(url.format("db0", "zset1", "b"), "", response.content.decode())

        # case2 get rank from empty zset
        response = requests.get(createUrl.format("db0", "zset2"))
        response = requests.get(url.format("db0", "zset2", "a"))
        self.writeLog(url.format("db0", "zset2", "a"), "", response.content.decode())

        # case3 unknown database name
        response = requests.get(url.format("db123", "zset1", "a"))
        self.writeLog(url.format("db123", "zset1", "a"), "", response.content.decode())

        # case4 unknown zset name
        response = requests.get(url.format("db0", "zset123", "b"))
        self.writeLog(url.format("db0", "zset123", "b"), "", response.content.decode())

        # case5 error url
        errorUrl = "http://" + self.host + ":" + str(self.port) + "/getrank/{}/{}/{}"
        response = requests.get(errorUrl.format("db0", "zset1", "a"))
        self.writeLog(errorUrl.format("db0", "zset1", "a"), "", response.content.decode())

    def rmByScoreTest(self):
        url = "http://" + self.host + ":" + str(self.port) + "/rmFromZSetByScore"

        # case1 create, insert, then remove
        createParams = {
            "dbName": "db0",
            "zsetName": "zset1"
        }
        createUrl = "http://" + self.host + ":" + str(self.port) + "/makeZSet"
        response = requests.post(url=createUrl, json=createParams)
        insertUrl = "http://" + self.host + ":" + str(self.port) + "/insertZSet"
        insertParams = {
            "dbName": "db0",
            "zsetName": "zset1",
            "value": "a",
            "score": 1
        }
        response = requests.put(url=insertUrl, json=insertParams)
        insertParams["value"] = "b"
        insertParams["score"] = 3
        response = requests.put(url=insertUrl, json=insertParams)
        insertParams["value"] = "c"
        insertParams["score"] = 5
        response = requests.put(url=insertUrl, json=insertParams)
        removeParam = {
            "dbName": "db0",
            "zsetName": "zset1",
            "start": 1,
            "end": 4
        }
        response = requests.put(url=url, json=removeParam)
        self.writeLog(url, json.dumps(removeParam), response.content.decode())

        # case2 score is out of range
        removeParam = {
            "dbName": "db0",
            "zsetName": "zset1",
            "start": 90,
            "end": 100
        }
        response = requests.put(url=url, json=removeParam)
        self.writeLog(url, json.dumps(removeParam), response.content.decode())

        # case3 unknown database name
        removeParam["dbName"] = "db123"
        response = requests.put(url=url, json=removeParam)
        self.writeLog(url, json.dumps(removeParam), response.content.decode())

        # case4 unknown zset name
        removeParam["dbName"] = "db0"
        removeParam["zsetName"] = "zset123"
        response = requests.put(url=url, json=removeParam)
        self.writeLog(url, json.dumps(removeParam), response.content.decode())

        # case5 error url
        errorUrl = "http://" + self.host + ":" + str(self.port) + "/rmfromzsetbyscore"
        response = requests.put(url=errorUrl, json=removeParam)
        self.writeLog(errorUrl, json.dumps(removeParam), response.content.decode())

    def setTTLTest(self):
        url = "http://" + self.host + ":" + str(self.port) + "/setTTL"

        # case1 create a set, set ttl
        createParams = {
            "dbName": "db0",
            "zsetName": "zset1"
        }
        createUrl = "http://" + self.host + ":" + str(self.port) + "/makeZSet"
        response = requests.post(url=createUrl, json=createParams)

        ttlParams = {
            "dataType": "ZSET",
            "dbName": "db0",
            "keyName": "zset1",
            "ttl": 60
        }
        response = requests.post(url=url, json=ttlParams)
        self.writeLog(url, json.dumps(ttlParams), response.content.decode())

        # case2 set ttl repeatedly
        response = requests.post(url=url, json=ttlParams)
        self.writeLog(url, json.dumps(ttlParams), response.content.decode())

        # case3 unknown database name
        ttlParams["dbName"] = "db999"
        response = requests.post(url=url, json=ttlParams)
        self.writeLog(url, json.dumps(ttlParams), response.content.decode())

        # case4 unknown set name
        ttlParams["dbName"] = "db0"
        ttlParams["keyName"] = "zset123"
        response = requests.post(url=url, json=ttlParams)
        self.writeLog(url, json.dumps(ttlParams), response.content.decode())

        # case5 ttl is not INT type
        ttlParams["keyName"] = "zset1"
        ttlParams["ttl"] = "hello"
        response = requests.post(url=url, json=ttlParams)
        self.writeLog(url, json.dumps(ttlParams), response.content.decode())

        # error url
        errorUrl = "http://" + self.host + ":" + str(self.port) + "/setttl"
        response = requests.post(url=errorUrl, json=ttlParams)
        self.writeLog(errorUrl, json.dumps(ttlParams), response.content.decode())

    def clearTTLTest(self):
        url = "http://" + self.host + ":" + str(self.port) + "/clearTTL"

        # case1 set a TTL and then clear it
        createParams = {
            "dbName": "db0",
            "zsetName": "zset1"
        }
        createUrl = "http://" + self.host + ":" + str(self.port) + "/makeZSet"
        response = requests.post(url=createUrl, json=createParams)

        setUrl = "http://" + self.host + ":" + str(self.port) + "/setTTL"
        ttlParams = {
            "dataType": "ZSET",
            "dbName": "db0",
            "keyName": "zset1",
            "ttl": 60
        }
        response = requests.post(url=setUrl, json=ttlParams)

        clearParams = {
            "dataType": "ZSET",
            "dbName": "db0",
            "keyName": "zset1"
        }
        response = requests.post(url=url, json=clearParams)
        self.writeLog(url, json.dumps(clearParams), response.content.decode())

        # case2 clear TTL repeatedly
        response = requests.post(url=url, json=clearParams)
        self.writeLog(url, json.dumps(clearParams), response.content.decode())

        # case3 clear non-existed TTL
        createParams["zsetName"] = "zset2"
        response = requests.post(url=createUrl, json=createParams)
        clearParams["keyName"] = "zset2"
        response = requests.post(url=url, json=clearParams)
        self.writeLog(url, json.dumps(clearParams), response.content.decode())

        # case4 unknown database name
        clearParams["dbName"] = "db999"
        clearParams["keyName"] = "zset1"
        response = requests.post(url=url, json=clearParams)
        self.writeLog(url, json.dumps(clearParams), response.content.decode())

        # case5 unknown element name
        clearParams["dbName"] = "db0"
        clearParams["keyName"] = "zset999"
        response = requests.post(url=url, json=clearParams)
        self.writeLog(url, json.dumps(clearParams), response.content.decode())

        # case6 error url
        errorUrl = "http://" + self.host + ":" + str(self.port) + "/clearttl"
        response = requests.post(url=errorUrl, json=clearParams)
        self.writeLog(errorUrl, json.dumps(clearParams), response.content.decode())

if __name__ == "__main__":
    test = zsetTest()

    # testing create zset function
    # test.makeZSetTest()

    # testing get zset function
    #test.getZSetTest()

    # testing insert zset function
    # test.insertZSetTest()

    # testing remove from zset function
    # test.rmFromZSetTest()

    # testing clear zset function
    # test.clearZSetTest()

    # testing delete zset function
    # test.deleteSetTest()

    # testing search zset function
    # test.searchZSetTest()

    # testing find min from zset function
    # test.findMinTest()

    # testing find max from zset function
    # test.findMaxTest()

    # testing get score function
    # test.getScoreTest()

    # testing get values by range function
    # test.getValuesByRangeTest()

    # testing get size function
    # test.getSizeTest()

    # testing rank function
    # test.getRankTest()

    # testing remove by score function
    # test.rmByScoreTest()

    # testing set ttl function
    # test.setTTLTest()

    # testing clear ttl function
    # test.clearTTLTest()