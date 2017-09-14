__author__ = 'Ma Haoxiang'

import requests
import json
import time

class hashTest:
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

    def makeSetTest(self):
        url = "http://" + self.host + ":" + str(self.port) + "/makeSet/{}/{}"

        # case1 make a new set
        response = requests.get(url.format("db0","set1"))
        self.writeLog(url.format("db0","set1"), "", response.content.decode())

        # case2 make a set repeatedly
        response = requests.get(url.format("db0", "set1"))
        self.writeLog(url.format("db0", "set1"), "", response.content.decode())

        # case3 unknown database name
        response = requests.get(url.format("db999", "set1"))
        self.writeLog(url.format("db999", "set1"), "", response.content.decode())

        # case4 invalid set name
        response = requests.get(url.format("db0", "!@abc"))
        self.writeLog(url.format("db0", "!@abc"), "", response.content.decode())

        # case5 error url
        errorUrl = "http://" + self.host + ":" + str(self.port) + "/makeset/{}/{}"
        response = requests.get(errorUrl.format("db0", "set1"))
        self.writeLog(errorUrl.format("db0", "set1"), "", response.content.decode())

    def getSetTest(self):
        url = "http://" + self.host + ":" + str(self.port) + "/getSet/{}/{}"

        # case1 create a set and then get it
        createUrl = "http://" + self.host + ":" + str(self.port) + "/makeSet/{}/{}"
        response = requests.get(createUrl.format("db0", "set1"))
        response = requests.get(url.format("db0","set1"))
        self.writeLog(url.format("db0", "set1"), "", response.content.decode())

        # case2 unknown database name
        response = requests.get(url.format("db999","set1"))
        self.writeLog(url.format("db999","set1"), "", response.content.decode())

        # case3 unknown set name
        response = requests.get(url.format("db0", "set123"))
        self.writeLog(url.format("db0", "set123"), "", response.content.decode())

        # case4 error url
        errorUrl = "http://" + self.host + ":" + str(self.port) + "/getset/{}/{}"
        response = requests.get(errorUrl.format("db0", "set1"))
        self.writeLog(errorUrl.format("db0", "set1"), "", response.content.decode())

    def insertSetTest(self):
        url = "http://" + self.host + ":" + str(self.port) + "/insertSet"

        # case1 create a set and then insert
        createUrl = "http://" + self.host + ":" + str(self.port) + "/makeSet/{}/{}"
        response = requests.get(createUrl.format("db0", "set1"))
        insertParams = {
            "dbName":"db0",
            "setName":"set1",
            "setValue":"1"
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
        insertParams["setName"] = "set123"
        response = requests.post(url, json=insertParams)
        self.writeLog(url, json.dumps(insertParams), response.content.decode())

        # case5 error database name type
        insertParams["dbName"] = [7,8,9]
        response = requests.post(url, json=insertParams)
        self.writeLog(url, json.dumps(insertParams), response.content.decode())

        # case6 error set name type
        insertParams["dbName"] = "db0"
        insertParams["setName"] = [4,5,6]
        response = requests.post(url, json=insertParams)
        self.writeLog(url, json.dumps(insertParams), response.content.decode())

        # case7 error url
        errorUrl = "http://" + self.host + ":" + str(self.port) + "/insertset"
        insertParams = {
            "dbName": "db0",
            "setName": "set1",
            "setValue": "1"
        }
        response = requests.post(errorUrl, json=insertParams)
        self.writeLog(errorUrl, json.dumps(insertParams), response.content.decode())

    def rmFromSetTest(self):
        url = "http://" + self.host + ":" + str(self.port) + "/rmFromSet"

        # case1 create, insert, then remove
        createUrl = "http://" + self.host + ":" + str(self.port) + "/makeSet/{}/{}"
        insertUrl = "http://" + self.host + ":" + str(self.port) + "/insertSet"
        response = requests.get(createUrl.format("db0", "set1"))
        insertParams = {
            "dbName": "db0",
            "setName": "set1",
            "setValue": "1"
        }
        response = requests.post(insertUrl, json=insertParams)
        removeParams = {
            "dbName":"db0",
            "setName":"set1",
            "setValue":"1"
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
        removeParams["setName"] = "set123"
        response = requests.post(url, json=removeParams)
        self.writeLog(url, json.dumps(removeParams), response.content.decode())

        # case5 error database name type
        removeParams["dbName"] = [1,2,3]
        removeParams["setName"] = "set1"
        response = requests.post(url, json=removeParams)
        self.writeLog(url, json.dumps(removeParams), response.content.decode())

        # case6 error set name type
        removeParams["dbName"] = "db0"
        removeParams["setName"] = [1,2,3]
        response = requests.post(url, json=removeParams)
        self.writeLog(url, json.dumps(removeParams), response.content.decode())

        # case7 non-existed set value
        removeParams["dbName"] = "db0"
        removeParams["setName"] = "set1"
        removeParams["setValue"] = "hello"
        response = requests.post(url, json=removeParams)
        self.writeLog(url, json.dumps(removeParams), response.content.decode())

        # case8 error url
        errorUrl = "http://" + self.host + ":" + str(self.port) + "/rmfromset"
        response = requests.post(errorUrl, json=removeParams)
        self.writeLog(errorUrl, json.dumps(removeParams), response.content.decode())

    def clearSetTest(self):
        url = "http://" + self.host + ":" + str(self.port) + "/clearSet/{}/{}"

        # case1 create, insert, then clear
        createUrl = "http://" + self.host + ":" + str(self.port) + "/makeSet/{}/{}"
        insertUrl = "http://" + self.host + ":" + str(self.port) + "/insertSet"
        response = requests.get(createUrl.format("db0", "set1"))
        insertParams = {
            "dbName": "db0",
            "setName": "set1",
            "setValue": "1"
        }
        response = requests.post(insertUrl, json=insertParams)
        response = requests.get(url.format("db0","set1"))
        self.writeLog(url.format("db0","set1"), "", response.content.decode())

        # case2 clear an empty set
        response = requests.get(url.format("db0","set1"))
        self.writeLog(url.format("db0","set1"), "", response.content.decode())

        # case3 unknown database name
        response = requests.get(url.format("db123","set1"))
        self.writeLog(url.format("db123","set1"), "", response.content.decode())

        # case4 unknown set name
        response = requests.get(url.format("db0","set123"))
        self.writeLog(url.format("db0","set123"), "", response.content.decode())

        # case5 error url
        errorUrl = "http://" + self.host + ":" + str(self.port) + "/clearset/{}/{}"
        response = requests.get(errorUrl.format("db0","set1"))
        self.writeLog(errorUrl.format("db0","set1"), "", response.content.decode())

    def deleteSetTest(self):
        url = "http://" + self.host + ":" + str(self.port) + "/deleteSet/{}/{}"

        # case1 create, then delete a set
        createUrl = "http://" + self.host + ":" + str(self.port) + "/makeSet/{}/{}"
        response = requests.get(createUrl.format("db0", "set1"))
        response = requests.get(url.format("db0","set1"))
        self.writeLog(url.format("db0","set1"), "", response.content.decode())

        # case2 delete a non-existed set
        response = requests.get(url.format("db0","set1"))
        self.writeLog(url.format("db0","set1"), "", response.content.decode())

        # case3 unknown database name
        response = requests.get(url.format("db789","set1"))
        self.writeLog(url.format("db789","set1"), "", response.content.decode())

        # case4 unknown set name
        response = requests.get(url.format("db0","set123"))
        self.writeLog(url.format("db0","set123"), "", response.content.decode())

        # case5 error url
        errorUrl = "http://" + self.host + ":" + str(self.port) + "/deleteset/{}/{}"
        response = requests.get(errorUrl.format("db0","set123"))
        self.writeLog(errorUrl.format("db0","set123"), "", response.content.decode())

    def searchSetTest(self):
        url = "http://" + self.host + ":" + str(self.port) + "/searchSet/{}/{}"

        # case1 create several sets, then search
        createUrl = "http://" + self.host + ":" + str(self.port) + "/makeSet/{}/{}"
        response = requests.get(createUrl.format("db0", "aabbset"))
        response = requests.get(createUrl.format("db0", "bcset"))
        response = requests.get(createUrl.format("db0", "setabset"))
        response = requests.get(createUrl.format("db0", "qwert1"))
        response = requests.get(createUrl.format("db0", "qwset"))
        response = requests.get(url.format("db0","a*"))
        self.writeLog(url.format("db0","a*"), "", response.content.decode())

        # case2 unknown database name
        response = requests.get(url.format("db999","a*"))
        self.writeLog(url.format("db999","a*"), "", response.content.decode())

        # case3 universal regular expression
        response = requests.get(url.format("db0","*"))
        self.writeLog(url.format("db0","*"), "", response.content.decode())

        # case4 error url
        errorUrl = "http://" + self.host + ":" + str(self.port) + "/searchset/{}/{}"
        response = requests.get(errorUrl.format("db0","*"))
        self.writeLog(errorUrl.format("db0","*"), "", response.content.decode())

    def unionSetTest(self):
        url = "http://" + self.host + ":" + str(self.port) + "/unionSet"

        # case1 create two empty set, then union
        createUrl = "http://" + self.host + ":" + str(self.port) + "/makeSet/{}/{}"
        response = requests.get(createUrl.format("db0", "set1"))
        response = requests.get(createUrl.format("db0", "set2"))
        unionParams = {
            "dbName":"db0",
            "setName1":"set1",
            "setName2":"set2"
        }
        response = requests.post(url,json=unionParams)
        self.writeLog(url,json.dumps(unionParams),response.content.decode())

        # case2 insert common values into two sets, then union
        insertUrl = "http://" + self.host + ":" + str(self.port) + "/insertSet"
        insertParams = {
            "dbName": "db0",
            "setName": "set1",
            "setValue": 1
        }
        response = requests.post(insertUrl,json=insertParams)
        insertParams = {
            "dbName": "db0",
            "setName": "set2",
            "setValue": 1
        }
        response = requests.post(url,json=unionParams)
        self.writeLog(url,json.dumps(unionParams),response.content.decode())

        # case3 unknown database name
        unionParams["dbName"] = "db999"
        response = requests.post(url, json=unionParams)
        self.writeLog(url, json.dumps(unionParams), response.content.decode())

        # case4 error database name type
        unionParams["dbName"] = [1,2,3]
        response = requests.post(url, json=unionParams)
        self.writeLog(url, json.dumps(unionParams), response.content.decode())

        # case5 unknown set name
        unionParams = {
            "dbName": "db0",
            "setName1": "set123",
            "setName2": "set2"
        }
        response = requests.post(url, json=unionParams)
        self.writeLog(url, json.dumps(unionParams), response.content.decode())

        # case6 error set name type
        unionParams = {
            "dbName": "db0",
            "setName1": ["hello","world"],
            "setName2": "set2"
        }
        response = requests.post(url, json=unionParams)
        self.writeLog(url, json.dumps(unionParams), response.content.decode())

        # error url
        errorUrl = "http://" + self.host + ":" + str(self.port) + "/unionset"
        response = requests.post(errorUrl, json=unionParams)
        self.writeLog(errorUrl, json.dumps(unionParams), response.content.decode())

    def intersectSetTest(self):
        url = "http://" + self.host + ":" + str(self.port) + "/intersectSet"

        # case1 create two empty set, then intersect
        createUrl = "http://" + self.host + ":" + str(self.port) + "/makeSet/{}/{}"
        response = requests.get(createUrl.format("db0", "set1"))
        response = requests.get(createUrl.format("db0", "set2"))
        intersectParams = {
            "dbName": "db0",
            "setName1": "set1",
            "setName2": "set2"
        }
        response = requests.post(url, json=intersectParams)
        self.writeLog(url, json.dumps(intersectParams), response.content.decode())

        # case2 insert common values into two sets, then union
        insertUrl = "http://" + self.host + ":" + str(self.port) + "/insertSet"
        insertParams = {
            "dbName": "db0",
            "setName": "set1",
            "setValue": 1
        }
        response = requests.post(insertUrl, json=insertParams)
        insertParams = {
            "dbName": "db0",
            "setName": "set2",
            "setValue": 1
        }
        response = requests.post(insertUrl, json=insertParams)
        response = requests.post(url, json=intersectParams)
        self.writeLog(url, json.dumps(intersectParams), response.content.decode())

        # case3 unknown database name
        intersectParams["dbName"] = "db999"
        response = requests.post(url, json=intersectParams)
        self.writeLog(url, json.dumps(intersectParams), response.content.decode())

        # case4 error database name type
        intersectParams["dbName"] = [1, 2, 3]
        response = requests.post(url, json=intersectParams)
        self.writeLog(url, json.dumps(intersectParams), response.content.decode())

        # case5 unknown set name
        intersectParams = {
            "dbName": "db0",
            "setName1": "set123",
            "setName2": "set2"
        }
        response = requests.post(url, json=intersectParams)
        self.writeLog(url, json.dumps(intersectParams), response.content.decode())

        # case6 error set name type
        intersectParams = {
            "dbName": "db0",
            "setName1": ["hello", "world"],
            "setName2": "set2"
        }
        response = requests.post(url, json=intersectParams)
        self.writeLog(url, json.dumps(intersectParams), response.content.decode())

        # error url
        errorUrl = "http://" + self.host + ":" + str(self.port) + "/intersectset"
        response = requests.post(errorUrl, json=intersectParams)
        self.writeLog(errorUrl, json.dumps(intersectParams), response.content.decode())


if __name__ == "__main__":
    test = hashTest()

    # testing make set function
    #test.makeSetTest()

    # testing get set function
    #test.getSetTest()

    # testing insert set function
    #test.insertSetTest()

    # testing remove from set function
    #test.rmFromSetTest()

    # testing clear set function
    #test.clearSetTest()

    # testing delete set function
    #test.deleteSetTest()

    # testing search set function
    #test.searchSetTest()

    # testing union set function
    #test.unionSetTest()

    # testing intersect set function
    test.intersectSetTest()