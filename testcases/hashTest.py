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

    def createHashTest(self):
        url = "http://" + self.host + ":" + str(self.port) + "/makeHash"

        # case1 create a hash
        params = {
            "dbName":"db0",
            "hashName":"hash1"
        }
        response = requests.post(url,json=params)
        self.writeLog(url,json.dumps(params),response.content.decode())

        # case2 create a hash repeatedly
        response = requests.post(url, json=params)
        self.writeLog(url, json.dumps(params), response.content.decode())

        # case3 unknown database name
        params = {
            "dbName": "db999",
            "hashName": "hash1"
        }
        response = requests.post(url, json=params)
        self.writeLog(url, json.dumps(params), response.content.decode())

        # case4 error database name type
        params = {
            "dbName": [1,2,3],
            "hashName": "hash1"
        }
        response = requests.post(url, json=params)
        self.writeLog(url, json.dumps(params), response.content.decode())

        # case5 error hash name type
        params = {
            "dbName": "db0",
            "hashName": [4,5,6]
        }
        response = requests.post(url, json=params)
        self.writeLog(url, json.dumps(params), response.content.decode())

        # case6 error url
        errorUrl = "http://" + self.host + ":" + str(self.port) + "/makehash"
        params = {
            "dbName": "db0",
            "hashName": "hash1"
        }
        response = requests.post(errorUrl, json=params)
        self.writeLog(errorUrl, json.dumps(params), response.content.decode())

    def getHashTest(self):
        url = "http://" + self.host + ":" + str(self.port) + "/getHash/{0}/{1}"

        # case1 create a hash and then get it
        createUrl = "http://" + self.host + ":" + str(self.port) + "/makeHash"
        params = {
            "dbName": "db0",
            "hashName": "hash1"
        }
        response = requests.post(createUrl, json=params)
        response = requests.get(url.format("db0","hash1"))
        self.writeLog(url.format("db0","hash1"),"",response.content.decode())

        # case2 unknown database name
        response = requests.get(url.format("db123","hash1"))
        self.writeLog(url.format("db123","hash1"),"",response.content.decode())

        # case3 unknown hash name
        response = requests.get(url.format("db0","hash999"))
        self.writeLog(url.format("db0","hash999"),"",response.content.decode())

        # case4 error url
        errorUrl = "http://" + self.host + ":" + str(self.port) + "/gethash/{0}/{1}"
        response = requests.get(errorUrl.format("db0","hash1"))
        self.writeLog(errorUrl.format("db0","hash1"),"",response.content.decode())

    def insertHashTest(self):
        url = "http://" + self.host + ":" + str(self.port) + "/insertHash"

        # case1 create a hash and then insert a value
        createUrl = "http://" + self.host + ":" + str(self.port) + "/makeHash"
        params = {
            "dbName": "db0",
            "hashName": "hash1"
        }
        response = requests.post(createUrl, json=params)
        insertParams = {
            "dbName":"db0",
            "hashName":"hash1",
            "keyName":"key1",
            "value":123
        }
        response = requests.post(url,json=insertParams)
        self.writeLog(url,json.dumps(insertParams),response.content.decode())

        # case2 unknown database name
        insertParams = {
            "dbName":"db999",
            "hashName":"hash1",
            "keyName":"key1",
            "value":123
        }
        response = requests.post(url,json=insertParams)
        self.writeLog(url,json.dumps(insertParams),response.content.decode())

        # case3 unknown hash name
        insertParams = {
            "dbName":"db0",
            "hashName":"hash999",
            "keyName":"key1",
            "value":123
        }
        response = requests.post(url,json=insertParams)
        self.writeLog(url,json.dumps(insertParams),response.content.decode())

        # case4 error database name type
        insertParams = {
            "dbName":[1,2,3],
            "hashName":"hash1",
            "keyName":"key1",
            "value":123
        }
        response = requests.post(url,json=insertParams)
        self.writeLog(url,json.dumps(insertParams),response.content.decode())

        # case5 error hash name type
        insertParams = {
            "dbName":"db0",
            "hashName":[4,5,6],
            "keyName":"key1",
            "value":123
        }
        response = requests.post(url,json=insertParams)
        self.writeLog(url,json.dumps(insertParams),response.content.decode())

        # case6 insert an existed key
        insertParams = {
            "dbName":"db0",
            "hashName":"hash1",
            "keyName":"key1",
            "value":789
        }
        response = requests.post(url,json=insertParams)
        self.writeLog(url,json.dumps(insertParams),response.content.decode())

        # case7 error key name type
        insertParams = {
            "dbName":"db0",
            "hashName":"hash1",
            "keyName":[7,8,9],
            "value":123
        }
        response = requests.post(url,json=insertParams)
        self.writeLog(url,json.dumps(insertParams),response.content.decode())

        # case8 error url
        errorUrl = "http://" + self.host + ":" + str(self.port) + "/inserthash"
        insertParams = {
            "dbName":"db0",
            "hashName":"hash1",
            "keyName":"key1",
            "value":123
        }
        response = requests.post(errorUrl,json=insertParams)
        self.writeLog(errorUrl,json.dumps(insertParams),response.content.decode())

    def deleteHashTest(self):
        url = "http://" + self.host + ":" + str(self.port) + "/deleteHash/{0}/{1}"

        # case1 create a hash and then delete it
        createUrl = "http://" + self.host + ":" + str(self.port) + "/makeHash"
        params = {
            "dbName": "db0",
            "hashName": "hash1"
        }
        response = requests.post(createUrl, json=params)
        response = requests.get(url.format("db0","hash1"))
        self.writeLog(url.format("db0","hash1"),"",response.content.decode())

        # case2 delete a hash repeatedly
        response = requests.get(url.format("db0","hash1"))
        self.writeLog(url.format("db0","hash1"),"",response.content.decode())

        # case3 unknown database name
        response = requests.get(url.format("db123","hash1"))
        self.writeLog(url.format("db123","hash1"),"",response.content.decode())

        # case4 unknown hash name
        response = requests.get(url.format("db0","hash999"))
        self.writeLog(url.format("db0","hash999"),"",response.content.decode())

        # case5 error url
        errorUrl = "http://" + self.host + ":" + str(self.port) + "/deletehash/{0}/{1}"
        response = requests.get(errorUrl.format("db0","hash999"))
        self.writeLog(errorUrl.format("db0","hash999"),"",response.content.decode())

    def rmFromHashTest(self):
        url = "http://" + self.host + ":" + str(self.port) + "/rmFromHash"

        # case1 create and insert, then remove
        createUrl = "http://" + self.host + ":" + str(self.port) + "/makeHash"
        insertUrl = "http://" + self.host + ":" + str(self.port) + "/insertHash"
        params = {
            "dbName": "db0",
            "hashName": "hash1"
        }
        insertParams = {
            "dbName": "db0",
            "hashName": "hash1",
            "keyName": "key1",
            "value": 123
        }
        removeParams = {
            "dbName":"db0",
            "hashName":"hash1",
            "keyName":"key1"
        }
        response = requests.post(createUrl, json=params)
        response = requests.post(insertUrl, json=insertParams)
        response = requests.post(url, json=removeParams)
        self.writeLog(url,json.dumps(removeParams),response.content.decode())

        # case2 remove the same key repeatedly
        removeParams = {
            "dbName": "db0",
            "hashName": "hash1",
            "keyName": "key1"
        }
        response = requests.post(url, json=removeParams)
        self.writeLog(url,json.dumps(removeParams),response.content.decode())

        # case3 unknown database name
        removeParams = {
            "dbName": "db999",
            "hashName": "hash1",
            "keyName": "key1"
        }
        response = requests.post(url, json=removeParams)
        self.writeLog(url,json.dumps(removeParams),response.content.decode())

        # case4 unknown hash name
        removeParams = {
            "dbName": "db0",
            "hashName": "hash123",
            "keyName": "key1"
        }
        response = requests.post(url, json=removeParams)
        self.writeLog(url,json.dumps(removeParams),response.content.decode())

        # case5 unknown key name
        removeParams = {
            "dbName": "db0",
            "hashName": "hash1",
            "keyName": "key2"
        }
        response = requests.post(url, json=removeParams)
        self.writeLog(url,json.dumps(removeParams),response.content.decode())

        # case6 error database name type
        removeParams = {
            "dbName": [1,2,3],
            "hashName": "hash1",
            "keyName": "key1"
        }
        response = requests.post(url, json=removeParams)
        self.writeLog(url,json.dumps(removeParams),response.content.decode())

        # case7 error hash name type
        removeParams = {
            "dbName": "db0",
            "hashName": [4,5,6],
            "keyName": "key1"
        }
        response = requests.post(url, json=removeParams)
        self.writeLog(url,json.dumps(removeParams),response.content.decode())

        # case8 error key name type
        removeParams = {
            "dbName": "db0",
            "hashName": "hash1",
            "keyName": [7,8,9]
        }
        response = requests.post(url, json=removeParams)
        self.writeLog(url,json.dumps(removeParams),response.content.decode())

        # case9 error url
        errorUrl = "http://" + self.host + ":" + str(self.port) + "/rmfromhash"
        removeParams = {
            "dbName": "db0",
            "hashName": "hash1",
            "keyName": "key1"
        }
        response = requests.post(errorUrl, json=removeParams)
        self.writeLog(errorUrl,json.dumps(removeParams),response.content.decode())



if __name__ == "__main__":
    test = hashTest()

    # testing create hash function
    #test.createHashTest()

    # testing get hash function
    #test.getHashTest()

    # testing insert hash function
    #test.insertHashTest()

    # testing delete hash function
    #test.deleteHashTest()

    # testing remove from hash function
    #test.rmFromHashTest()