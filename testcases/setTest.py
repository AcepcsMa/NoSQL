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


if __name__ == "__main__":
    test = hashTest()

    # testing make set function
    #test.makeSetTest()

    # testing get set function
    #test.getSetTest()

    # testing insert set function
    #test.insertSetTest()

    # testing remove from set function
    test.rmFromSetTest()