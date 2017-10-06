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
        url = "http://" + self.host + ":" + str(self.port) + "/makeZSet/{}/{}"

        # case1 make a new zset
        response = requests.get(url.format("db0","zset1"))
        self.writeLog(url.format("db0","zset1"), "", response.content.decode())

        # case2 make a set repeatedly
        response = requests.get(url.format("db0", "zset1"))
        self.writeLog(url.format("db0", "zset1"), "", response.content.decode())

        # case3 unknown database name
        response = requests.get(url.format("db999", "zset1"))
        self.writeLog(url.format("db999", "zset1"), "", response.content.decode())

        # case4 invalid zset name
        response = requests.get(url.format("db0", "!@abc"))
        self.writeLog(url.format("db0", "!@abc"), "", response.content.decode())

        # case5 error url
        errorUrl = "http://" + self.host + ":" + str(self.port) + "/makezset/{}/{}"
        response = requests.get(errorUrl.format("db0", "zset1"))
        self.writeLog(errorUrl.format("db0", "zset1"), "", response.content.decode())

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
        createUrl = "http://" + self.host + ":" + str(self.port) + "/makeZSet/{}/{}"
        response = requests.get(createUrl.format("db0", "zset1"))
        insertParams = {
            "dbName":"db0",
            "zsetName":"zset1",
            "value":"hello",
            "score":1
        }
        response = requests.post(url,json=insertParams)
        self.writeLog(url, json.dumps(insertParams), response.content.decode())

        # case2 insert an existed value
        response = requests.post(url,json=insertParams)
        self.writeLog(url, json.dumps(insertParams), response.content.decode())

        # case3 unknown database name
        insertParams["dbName"] = "db999"
        response = requests.post(url, json=insertParams)
        self.writeLog(url, json.dumps(insertParams), response.content.decode())

        # case4 unknown set name
        insertParams["dbName"] = "db0"
        insertParams["zsetName"] = "zset123"
        response = requests.post(url, json=insertParams)
        self.writeLog(url, json.dumps(insertParams), response.content.decode())

        # case5 error database name type
        insertParams["dbName"] = [7,8,9]
        response = requests.post(url, json=insertParams)
        self.writeLog(url, json.dumps(insertParams), response.content.decode())

        # case6 error set name type
        insertParams["dbName"] = "db0"
        insertParams["zsetName"] = [4,5,6]
        response = requests.post(url, json=insertParams)
        self.writeLog(url, json.dumps(insertParams), response.content.decode())

        # case7 error url
        errorUrl = "http://" + self.host + ":" + str(self.port) + "/insertzset"
        insertParams = {
            "dbName": "db0",
            "zsetName": "set1",
            "value": "hello",
            "score":1
        }
        response = requests.post(errorUrl, json=insertParams)
        self.writeLog(errorUrl, json.dumps(insertParams), response.content.decode())

    def rmFromZSetTest(self):
        url = "http://" + self.host + ":" + str(self.port) + "/rmFromZSet"

        # case1 create, insert, then remove
        createUrl = "http://" + self.host + ":" + str(self.port) + "/makeZSet/{}/{}"
        insertUrl = "http://" + self.host + ":" + str(self.port) + "/insertZSet"
        response = requests.get(createUrl.format("db0", "zset1"))
        insertParams = {
            "dbName": "db0",
            "zsetName": "zset1",
            "value": "hello",
            "score": 1
        }
        response = requests.post(insertUrl, json=insertParams)
        removeParams = {
            "dbName":"db0",
            "zsetName":"zset1",
            "value":"hello"
        }
        response = requests.post(url, json=removeParams)
        self.writeLog(url, json.dumps(removeParams), response.content.decode())

        # case2 remove from an empty set
        response = requests.post(url, json=removeParams)
        self.writeLog(url, json.dumps(removeParams), response.content.decode())

        # case3 unknown database name
        removeParams["dbName"] = "db999"
        response = requests.post(url, json=removeParams)
        self.writeLog(url, json.dumps(removeParams), response.content.decode())

        # case4 unknown set name
        removeParams["dbName"] = "db0"
        removeParams["zsetName"] = "zset123"
        response = requests.post(url, json=removeParams)
        self.writeLog(url, json.dumps(removeParams), response.content.decode())

        # case5 error database name type
        removeParams["dbName"] = [1,2,3]
        removeParams["zsetName"] = "zset1"
        response = requests.post(url, json=removeParams)
        self.writeLog(url, json.dumps(removeParams), response.content.decode())

        # case6 error set name type
        removeParams["dbName"] = "db0"
        removeParams["zsetName"] = [1,2,3]
        response = requests.post(url, json=removeParams)
        self.writeLog(url, json.dumps(removeParams), response.content.decode())

        # case7 non-existed set value
        removeParams["dbName"] = "db0"
        removeParams["zsetName"] = "zset1"
        removeParams["value"] = "key"
        response = requests.post(url, json=removeParams)
        self.writeLog(url, json.dumps(removeParams), response.content.decode())

        # case8 error url
        errorUrl = "http://" + self.host + ":" + str(self.port) + "/rmfromzset"
        response = requests.post(errorUrl, json=removeParams)
        self.writeLog(errorUrl, json.dumps(removeParams), response.content.decode())

    def clearZSetTest(self):
        url = "http://" + self.host + ":" + str(self.port) + "/clearZSet/{}/{}"

        # case1 create, insert, then clear
        createUrl = "http://" + self.host + ":" + str(self.port) + "/makeZSet/{}/{}"
        insertUrl = "http://" + self.host + ":" + str(self.port) + "/insertZSet"
        response = requests.get(createUrl.format("db0", "zset1"))
        insertParams = {
            "dbName": "db0",
            "zsetName": "zset1",
            "value": "hello",
            "score": 1
        }
        response = requests.post(insertUrl, json=insertParams)
        response = requests.get(url.format("db0","zset1"))
        self.writeLog(url.format("db0","zset1"), "", response.content.decode())

        # case2 clear an empty set
        response = requests.get(url.format("db0","zset1"))
        self.writeLog(url.format("db0","zset1"), "", response.content.decode())

        # case3 unknown database name
        response = requests.get(url.format("db123","zset1"))
        self.writeLog(url.format("db123","zset1"), "", response.content.decode())

        # case4 unknown set name
        response = requests.get(url.format("db0","zset123"))
        self.writeLog(url.format("db0","zset123"), "", response.content.decode())

        # case5 error url
        errorUrl = "http://" + self.host + ":" + str(self.port) + "/clearzset/{}/{}"
        response = requests.get(errorUrl.format("db0","zset1"))
        self.writeLog(errorUrl.format("db0","zset1"), "", response.content.decode())

    def deleteSetTest(self):
        url = "http://" + self.host + ":" + str(self.port) + "/deleteZSet/{}/{}"

        # case1 create, then delete a set
        createUrl = "http://" + self.host + ":" + str(self.port) + "/makeZSet/{}/{}"
        response = requests.get(createUrl.format("db0", "zset1"))
        response = requests.get(url.format("db0","zset1"))
        self.writeLog(url.format("db0","zset1"), "", response.content.decode())

        # case2 delete a non-existed set
        response = requests.get(url.format("db0","zset1"))
        self.writeLog(url.format("db0","zset1"), "", response.content.decode())

        # case3 unknown database name
        response = requests.get(url.format("db789","zset1"))
        self.writeLog(url.format("db789","zset1"), "", response.content.decode())

        # case4 unknown set name
        response = requests.get(url.format("db0","zset123"))
        self.writeLog(url.format("db0","zset123"), "", response.content.decode())

        # case5 error url
        errorUrl = "http://" + self.host + ":" + str(self.port) + "/deletezset/{}/{}"
        response = requests.get(errorUrl.format("db0","zset123"))
        self.writeLog(errorUrl.format("db0","zset123"), "", response.content.decode())

    def searchZSetTest(self):
        url = "http://" + self.host + ":" + str(self.port) + "/searchZSet/{}/{}"

        # case1 create several sets, then search
        createUrl = "http://" + self.host + ":" + str(self.port) + "/makeZSet/{}/{}"
        response = requests.get(createUrl.format("db0", "zset1"))
        response = requests.get(createUrl.format("db0", "zs"))
        response = requests.get(createUrl.format("db0", "z1"))
        response = requests.get(createUrl.format("db0", "abczs"))
        response = requests.get(createUrl.format("db0", "set"))
        response = requests.get(url.format("db0","z*"))
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
        createUrl = "http://" + self.host + ":" + str(self.port) + "/makeZSet/{}/{}"
        response = requests.get(createUrl.format("db0", "zset1"))
        insertUrl = "http://" + self.host + ":" + str(self.port) + "/insertZSet"
        insertParams = {
            "dbName": "db0",
            "zsetName": "zset1",
            "value": "hello",
            "score": 1
        }
        response = requests.post(insertUrl, json=insertParams)
        insertParams["value"] = "world"
        insertParams["score"] = 3
        response = requests.post(insertUrl, json=insertParams)
        response = requests.get(url.format("db0","zset1"))
        self.writeLog(url.format("db0","zset1"),"",response.content.decode())

        # case2 find min in an empty zset
        response = requests.get(createUrl.format("db0", "zset2"))
        response = requests.get(url.format("db0","zset2"))
        self.writeLog(url.format("db0","zset2"),"",response.content.decode())

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
        createUrl = "http://" + self.host + ":" + str(self.port) + "/makeZSet/{}/{}"
        response = requests.get(createUrl.format("db0", "zset1"))
        insertUrl = "http://" + self.host + ":" + str(self.port) + "/insertZSet"
        insertParams = {
            "dbName": "db0",
            "zsetName": "zset1",
            "value": "hello",
            "score": 1
        }
        response = requests.post(insertUrl, json=insertParams)
        insertParams["value"] = "world"
        insertParams["score"] = 3
        response = requests.post(insertUrl, json=insertParams)
        response = requests.get(url.format("db0","zset1"))
        self.writeLog(url.format("db0","zset1"),"",response.content.decode())

        # case2 find min in an empty zset
        response = requests.get(createUrl.format("db0", "zset2"))
        response = requests.get(url.format("db0","zset2"))
        self.writeLog(url.format("db0","zset2"),"",response.content.decode())

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


if __name__ == "__main__":
    test = zsetTest()

    # testing create zset function
    #test.makeZSetTest()

    # testing get zset function
    #test.getZSetTest()

    # testing insert zset function
    #test.insertZSetTest()

    # testing remove from zset function
    #test.rmFromZSetTest()

    # testing clear zset function
    #test.clearZSetTest()

    # testing delete zset function
    #test.deleteSetTest()

    # testing search zset function
    #test.searchZSetTest()

    # testing find min from zset function
    #test.findMinTest()

    # testing find max from zset function
    test.findMaxTest()