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



if __name__ == "__main__":
    test = hashTest()

    # testing make set function
    #test.makeSetTest()

    # testing get set function
    test.getSetTest()