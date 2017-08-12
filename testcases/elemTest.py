__author__ = 'Ma Haoxiang'

import requests
import json
import time

class elemTest:
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

    # test create element function
    def createElemTest(self):
        url = "http://" + self.host + ":" + str(self.port) + "/makeElem"

        # case1 normally create new ELEMENT
        params = {
            "dbName" : "db0",
            "elemName" : "elem1",
            "elemValue" : 1
        }

        response = requests.post(url, json=params)
        self.writeLog(url, json.dumps(params), response.content.decode())

        # case2 create a new ELEMENT repeatedly
        response = requests.post(url, json=params)
        self.writeLog(url, json.dumps(params), response.content.decode())

        # case3 unknown db name
        params = {
            "dbName": "db10",
            "elemName": "elem1",
            "elemValue": 1
        }
        response = requests.post(url, json=params)
        self.writeLog(url, json.dumps(params), response.content.decode())

        # case4 error db name type
        params = {
            "dbName": [1,2,3],
            "elemName": "elem1",
            "elemValue": 1
        }
        response = requests.post(url, json=params)
        self.writeLog(url, json.dumps(params), response.content.decode())

        # case5 error elem name type
        params = {
            "dbName": "db0",
            "elemName": [1,2,3],
            "elemValue": 1
        }
        response = requests.post(url, json=params)
        self.writeLog(url, json.dumps(params), response.content.decode())

        # case6 error elem value type
        params = {
            "dbName": "db0",
            "elemName": "elem2",
            "elemValue": [1,2,3]
        }
        response = requests.post(url, json=params)
        self.writeLog(url, json.dumps(params), response.content.decode())

    # test get element function
    def getElemTest(self):
        url = "http://" + self.host + ":" + str(self.port) + "/getElem/{0}/{1}"

        # case1 get an ELEMENT normally
        response = requests.get(url.format("db0","elem1"))
        self.writeLog(url.format("db0","elem1"), "", response.content.decode())

        # case2 unknown db name
        response = requests.get(url.format("db100","elem1"))
        self.writeLog(url.format("db100","elem1"), "", response.content.decode())

        # case3 unknown elem name
        response = requests.get(url.format("db0","elem999"))
        self.writeLog(url.format("db0","elem999"), "", response.content.decode())

        # case4 error url
        errorUrl = "http://" + self.host + ":" + str(self.port) + "/getelem/db0/elem1"
        response = requests.get(errorUrl)
        self.writeLog(errorUrl, "", response.content.decode())


if __name__ == "__main__":
    test = elemTest()
    test.createElemTest()
    test.getElemTest()
