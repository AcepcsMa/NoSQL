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
        response = requests.put(url,json=insertParams)
        self.writeLog(url, json.dumps(insertParams), response.content.decode())

        # case2 unknown database name
        insertParams = {
            "dbName":"db999",
            "hashName":"hash1",
            "keyName":"key1",
            "value":123
        }
        response = requests.put(url, json=insertParams)
        self.writeLog(url, json.dumps(insertParams), response.content.decode())

        # case3 unknown hash name
        insertParams = {
            "dbName":"db0",
            "hashName":"hash999",
            "keyName":"key1",
            "value":123
        }
        response = requests.put(url, json=insertParams)
        self.writeLog(url, json.dumps(insertParams), response.content.decode())

        # case4 error database name type
        insertParams = {
            "dbName":[1,2,3],
            "hashName":"hash1",
            "keyName":"key1",
            "value":123
        }
        response = requests.put(url, json=insertParams)
        self.writeLog(url, json.dumps(insertParams), response.content.decode())

        # case5 error hash name type
        insertParams = {
            "dbName":"db0",
            "hashName":[4,5,6],
            "keyName":"key1",
            "value":123
        }
        response = requests.put(url,json=insertParams)
        self.writeLog(url, json.dumps(insertParams), response.content.decode())

        # case6 insert an existed key
        insertParams = {
            "dbName":"db0",
            "hashName":"hash1",
            "keyName":"key1",
            "value":789
        }
        response = requests.put(url,json=insertParams)
        self.writeLog(url, json.dumps(insertParams), response.content.decode())

        # case7 error key name type
        insertParams = {
            "dbName":"db0",
            "hashName":"hash1",
            "keyName":[7,8,9],
            "value":123
        }
        response = requests.put(url,json=insertParams)
        self.writeLog(url, json.dumps(insertParams), response.content.decode())

        # case8 error url
        errorUrl = "http://" + self.host + ":" + str(self.port) + "/inserthash"
        insertParams = {
            "dbName":"db0",
            "hashName":"hash1",
            "keyName":"key1",
            "value":123
        }
        response = requests.put(errorUrl,json=insertParams)
        self.writeLog(errorUrl, json.dumps(insertParams), response.content.decode())

    def deleteHashTest(self):
        url = "http://" + self.host + ":" + str(self.port) + "/deleteHash/{0}/{1}"

        # case1 create a hash and then delete it
        createUrl = "http://" + self.host + ":" + str(self.port) + "/makeHash"
        params = {
            "dbName": "db0",
            "hashName": "hash1"
        }
        response = requests.post(createUrl, json=params)
        response = requests.delete(url.format("db0", "hash1"))
        self.writeLog(url.format("db0", "hash1"), "", response.content.decode())

        # case2 delete a hash repeatedly
        response = requests.delete(url.format("db0", "hash1"))
        self.writeLog(url.format("db0", "hash1"), "", response.content.decode())

        # case3 unknown database name
        response = requests.delete(url.format("db123", "hash1"))
        self.writeLog(url.format("db123", "hash1"), "", response.content.decode())

        # case4 unknown hash name
        response = requests.delete(url.format("db0", "hash999"))
        self.writeLog(url.format("db0","hash999"), "", response.content.decode())

        # case5 error url
        errorUrl = "http://" + self.host + ":" + str(self.port) + "/deletehash/{0}/{1}"
        response = requests.get(errorUrl.format("db0", "hash999"))
        self.writeLog(errorUrl.format("db0", "hash999"), "", response.content.decode())

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
        response = requests.put(insertUrl, json=insertParams)
        response = requests.put(url, json=removeParams)
        self.writeLog(url, json.dumps(removeParams), response.content.decode())

        # case2 remove the same key repeatedly
        removeParams = {
            "dbName": "db0",
            "hashName": "hash1",
            "keyName": "key1"
        }
        response = requests.put(url, json=removeParams)
        self.writeLog(url, json.dumps(removeParams), response.content.decode())

        # case3 unknown database name
        removeParams = {
            "dbName": "db999",
            "hashName": "hash1",
            "keyName": "key1"
        }
        response = requests.put(url, json=removeParams)
        self.writeLog(url, json.dumps(removeParams), response.content.decode())

        # case4 unknown hash name
        removeParams = {
            "dbName": "db0",
            "hashName": "hash123",
            "keyName": "key1"
        }
        response = requests.put(url, json=removeParams)
        self.writeLog(url, json.dumps(removeParams), response.content.decode())

        # case5 unknown key name
        removeParams = {
            "dbName": "db0",
            "hashName": "hash1",
            "keyName": "key2"
        }
        response = requests.put(url, json=removeParams)
        self.writeLog(url, json.dumps(removeParams), response.content.decode())

        # case6 error database name type
        removeParams = {
            "dbName": [1,2,3],
            "hashName": "hash1",
            "keyName": "key1"
        }
        response = requests.put(url, json=removeParams)
        self.writeLog(url, json.dumps(removeParams), response.content.decode())

        # case7 error hash name type
        removeParams = {
            "dbName": "db0",
            "hashName": [4,5,6],
            "keyName": "key1"
        }
        response = requests.put(url, json=removeParams)
        self.writeLog(url, json.dumps(removeParams), response.content.decode())

        # case8 error key name type
        removeParams = {
            "dbName": "db0",
            "hashName": "hash1",
            "keyName": [7,8,9]
        }
        response = requests.put(url, json=removeParams)
        self.writeLog(url, json.dumps(removeParams), response.content.decode())

        # case9 error url
        errorUrl = "http://" + self.host + ":" + str(self.port) + "/rmfromhash"
        removeParams = {
            "dbName": "db0",
            "hashName": "hash1",
            "keyName": "key1"
        }
        response = requests.put(errorUrl, json=removeParams)
        self.writeLog(errorUrl, json.dumps(removeParams), response.content.decode())

    def clearHashTest(self):
        url = "http://" + self.host + ":" + str(self.port) + "/clearHash/{}/{}"

        # case1 create a hash, insert some values and clear it
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
        response = requests.post(createUrl, json=params)
        response = requests.put(insertUrl, json=insertParams)
        response = requests.put(url.format("db0", "hash1"))
        self.writeLog(url.format("db0","hash1"), "", response.content.decode())

        # case2 clear an empty hash
        response = requests.put(url.format("db0", "hash1"))
        self.writeLog(url.format("db0", "hash1"), "", response.content.decode())

        # case3 unknown database name
        response = requests.put(url.format("db999", "hash1"))
        self.writeLog(url.format("db999", "hash1"), "", response.content.decode())

        # case4 unknown hash name
        response = requests.put(url.format("db0", "hash123"))
        self.writeLog(url.format("db0", "hash123"), "", response.content.decode())

        # case5 error url
        errorUrl = "http://" + self.host + ":" + str(self.port) + "/clearhash/{}/{}"
        response = requests.put(errorUrl.format("db0","hash1"))
        self.writeLog(errorUrl.format("db0","hash1"), "", response.content.decode())

    def replaceHashTest(self):
        url = "http://" + self.host + ":" + str(self.port) + "/replaceHash"

        # case1 create a hash, then replace it
        createUrl = "http://" + self.host + ":" + str(self.port) + "/makeHash"
        params = {
            "dbName": "db0",
            "hashName": "hash1"
        }
        response = requests.post(createUrl, json=params)
        replaceParams = {
            "dbName":"db0",
            "hashName":"hash1",
            "hashValue":{
                "key1":1,
                "key2":2,
                "key3":3
            }
        }
        response = requests.put(url,json=replaceParams)
        self.writeLog(url, json.dumps(replaceParams), response.content.decode())

        # case2 error element type
        replaceParams = {
            "dbName": "db0",
            "hashName": "hash1",
            "hashValue": [1,2,3]
        }
        response = requests.put(url, json=replaceParams)
        self.writeLog(url, json.dumps(replaceParams), response.content.decode())

        # case3 unknown database name
        replaceParams = {
            "dbName": "db999",
            "hashName": "hash1",
            "hashValue": {
                "key1": 1,
                "key2": 2,
                "key3": 3
            }
        }
        response = requests.put(url, json=replaceParams)
        self.writeLog(url, json.dumps(replaceParams), response.content.decode())

        # case4 unknown hash name
        replaceParams = {
            "dbName": "db0",
            "hashName": "hash999",
            "hashValue": {
                "key1": 1,
                "key2": 2,
                "key3": 3
            }
        }
        response = requests.put(url, json=replaceParams)
        self.writeLog(url, json.dumps(replaceParams), response.content.decode())

        # case5 error url
        errorUrl = "http://" + self.host + ":" + str(self.port) + "/replacehash"
        response = requests.put(errorUrl, json=replaceParams)
        self.writeLog(errorUrl, json.dumps(replaceParams), response.content.decode())

    def mergeHashTest(self):
        url = "http://" + self.host + ":" + str(self.port) + "/mergeHashs"

        # case1 create two hashs and then merge into a third hash
        createUrl = "http://" + self.host + ":" + str(self.port) + "/makeHash"
        insertUrl = "http://" + self.host + ":" + str(self.port) + "/insertHash"

        params = {
            "dbName": "db0",
            "hashName": "hash1"
        }
        response = requests.post(createUrl, json=params)
        params["hashName"] = "hash2"
        response = requests.post(createUrl, json=params)

        insertParmas = {
            "dbName": "db0",
            "hashName": "hash1",
            "keyName": "key1",
            "value":1
        }
        response = requests.post(insertUrl, json=params)
        params["hashName"] = "hash2"
        params["keyName"] = "key2"
        response = requests.post(insertUrl, json=params)

        mergeParams = {
            "dbName":"db0",
            "hash1":"hash1",
            "hash2":"hash2",
            "resultHash":"mergeResult",
            "mode":0
        }
        response = requests.post(url,json=mergeParams)
        self.writeLog(url,json.dumps(mergeParams),response.content.decode())

        # case2 merge result hash already exists
        response = requests.post(url,json=mergeParams)
        self.writeLog(url,json.dumps(mergeParams),response.content.decode())

        # case3 merge into the first hash
        mergeParams = {
            "dbName": "db0",
            "hash1": "hash1",
            "hash2": "hash2",
            "resultHash": "",
            "mode": 0
        }
        response = requests.post(url,json=mergeParams)
        self.writeLog(url,json.dumps(mergeParams),response.content.decode())

        # case4 unknown database name
        mergeParams = {
            "dbName": "db999",
            "hash1": "hash1",
            "hash2": "hash2",
            "resultHash": "mergeResult",
            "mode": 0
        }
        response = requests.post(url,json=mergeParams)
        self.writeLog(url,json.dumps(mergeParams),response.content.decode())

        # case5 unknown hash name
        mergeParams = {
            "dbName": "db999",
            "hash1": "hash123",
            "hash2": "hash456",
            "resultHash": "mergeResult",
            "mode": 0
        }
        response = requests.post(url,json=mergeParams)
        self.writeLog(url,json.dumps(mergeParams),response.content.decode())

        # case6 error database name type
        mergeParams = {
            "dbName": [1,2,3],
            "hash1": "hash1",
            "hash2": "hash2",
            "resultHash": "mergeResult",
            "mode": 0
        }
        response = requests.post(url,json=mergeParams)
        self.writeLog(url,json.dumps(mergeParams),response.content.decode())

        # case7 error hash name type
        mergeParams = {
            "dbName": "db999",
            "hash1": [1,2,3],
            "hash2": [4,5,6],
            "resultHash": "mergeResult",
            "mode": 0
        }
        response = requests.post(url,json=mergeParams)
        self.writeLog(url,json.dumps(mergeParams),response.content.decode())

        # case8 error url
        errorUrl = "http://" + self.host + ":" + str(self.port) + "/mergehashs"
        response = requests.post(errorUrl,json=mergeParams)
        self.writeLog(errorUrl,json.dumps(mergeParams),response.content.decode())

    def searchHashTest(self):
        url = "http://" + self.host + ":" + str(self.port) + "/searchHash/{}/{}"

        # case1 create several hashs and search
        createUrl = "http://" + self.host + ":" + str(self.port) + "/makeHash"
        params = {
            "dbName": "db0",
            "hashName": "hash1"
        }
        response = requests.post(createUrl, json=params)
        params["hashName"] = "abcd"
        response = requests.post(createUrl, json=params)
        params["hashName"] = "bcda1"
        response = requests.post(createUrl, json=params)
        params["hashName"] = "bonjour"
        response = requests.post(createUrl, json=params)
        params["hashName"] = "a*"
        response = requests.post(createUrl, json=params)

        response = requests.get(url.format("db0","a*"))
        self.writeLog(url.format("db0","a*"), "", response.content.decode())

        # case2 unknown database name
        response = requests.get(url.format("db123", "a*"))
        self.writeLog(url.format("db123", "a*"), "", response.content.decode())

        # case3 universal regular expression
        response = requests.get(url.format("db0", "*"))
        self.writeLog(url.format("db0", "*"), "", response.content.decode())

        # case4 error url
        errorUrl = "http://" + self.host + ":" + str(self.port) + "/searchhash/{}/{}"
        response = requests.get(errorUrl.format("db0", "*"))
        self.writeLog(errorUrl.format("db0", "*"), "", response.content.decode())

    def setTTLTest(self):
        url = "http://" + self.host + ":" + str(self.port) + "/setHashTTL/{}/{}/{}"

        # case1 create a hash and set TTL
        createUrl = "http://" + self.host + ":" + str(self.port) + "/makeHash"
        params = {
            "dbName": "db0",
            "hashName": "hash1"
        }
        response = requests.post(createUrl, json=params)
        response = requests.get(url.format("db0", "hash1", 30))
        self.writeLog(url.format("db0", "hash1", 30), "", response.content.decode())

        # case2 unknown database name
        response = requests.get(url.format("db999", "hash1", 15))
        self.writeLog(url.format("db999", "hash1", 15), "", response.content.decode())

        # case3 unknown hash name
        response = requests.get(url.format("db0", "hash456", 20))
        self.writeLog(url.format("db0", "hash456", 20), "", response.content.decode())

        # case4 TTL is not Int type
        response = requests.get(url.format("db0", "hash1", "hello world"))
        self.writeLog(url.format("db0", "hash1", "hello world"), "", response.content.decode())

        # case5 error url
        errorUrl = "http://" + self.host + ":" + str(self.port) + "/sethashttl/{0}/{1}/{2}"
        response = requests.get(errorUrl.format("db0", "hash1", 15))
        self.writeLog(errorUrl.format("db0", "hash1", 15), "", response.content.decode())

    def clearTTLTest(self):
        url = "http://" + self.host + ":" + str(self.port) + "/clearHashTTL/{}/{}"

        # case1 set a TTL and then clear it
        createUrl = "http://" + self.host + ":" + str(self.port) + "/makeHash"
        params = {
            "dbName": "db0",
            "hashName": "hash1"
        }
        response = requests.post(createUrl, json=params)
        setUrl = "http://" + self.host + ":" + str(self.port) + "/setHashTTL/{}/{}/{}"
        response = requests.get(setUrl.format("db0", "hash1", 20))
        response = requests.get(url.format("db0", "hash1"))
        self.writeLog(url.format("db0", "hash1"), "", response.content.decode())

        # case2 clear TTL repeatedly
        response = requests.get(url.format("db0", "hash1"))
        self.writeLog(url.format("db0", "hash1"), "", response.content.decode())

        # case3 clear non-existed TTL
        params["hashName"] = "hash2"
        response = requests.post(createUrl, json=params)
        response = requests.get(url.format("db0", "hash2"))
        self.writeLog(url.format("db0", "hash2"), "", response.content.decode())

        # case4 unknown database name
        response = requests.get(url.format("db999", "hash1"))
        self.writeLog(url.format("db999", "hash1"), "", response.content.decode())

        # case5 unknown element name
        response = requests.get(url.format("db0", "abcde"))
        self.writeLog(url.format("db0", "abcde"), "", response.content.decode())

        # case6 error url
        errorUrl = "http://" + self.host + ":" + str(self.port) + "/clearhashttl/{0}/{1}"
        response = requests.get(errorUrl.format("db0", "hash1"))
        self.writeLog(errorUrl.format("db0", "hash1"), "", response.content.decode())

    def getSizeTest(self):
        url = "http://" + self.host + ":" + str(self.port) + "/getHashSize/{}/{}"

        # case1 create a hash, insert, and get its size
        createUrl = "http://" + self.host + ":" + str(self.port) + "/makeHash"
        insertUrl = "http://" + self.host + ":" + str(self.port) + "/insertHash"
        params = {
            "dbName": "db0",
            "hashName": "hash1"
        }
        response = requests.post(createUrl, json=params)
        insertParmas = {
            "dbName": "db0",
            "hashName": "hash1",
            "keyName": "key1",
            "value": 1
        }
        response = requests.post(insertUrl, json=insertParmas)
        response = requests.get(url.format("db0", "hash1"))
        self.writeLog(url.format("db0", "hash1"), "", response.content.decode())

        # case2 get size of an empty hash
        params = {
            "dbName": "db0",
            "hashName": "hash2"
        }
        response = requests.post(createUrl, json=params)
        response = requests.get(url.format("db0", "hash2"))
        self.writeLog(url.format("db0", "hash2"), "", response.content.decode())

        # case3 unknown database name
        response = requests.get(url.format("db123", "hash1"))
        self.writeLog(url.format("db123", "hash1"), "", response.content.decode())

        # case4 unknown hash name
        response = requests.get(url.format("db0", "hash123"))
        self.writeLog(url.format("db0", "hash123"), "", response.content.decode())

        #case5 error url
        errorUrl = "http://" + self.host + ":" + str(self.port) + "/gethashsize/{}/{}"
        response = requests.get(errorUrl.format("db0", "hash1"))
        self.writeLog(errorUrl.format("db0", "hash1"), "", response.content.decode())

    def increaseTest(self):
        url = "http://" + self.host + ":" + str(self.port) + "/increaseHash/{}/{}/{}"

        # case1 create a hash, insert, and increase
        createUrl = "http://" + self.host + ":" + str(self.port) + "/makeHash"
        insertUrl = "http://" + self.host + ":" + str(self.port) + "/insertHash"
        params = {
            "dbName": "db0",
            "hashName": "hash1"
        }
        response = requests.post(createUrl, json=params)
        insertParmas = {
            "dbName": "db0",
            "hashName": "hash1",
            "keyName": "key1",
            "value": 1
        }
        response = requests.post(insertUrl, json=insertParmas)
        response = requests.get(url.format("db0", "hash1", "key1"))
        self.writeLog(url.format("db0", "hash1", "key1"), "", response.content.decode())

        # case2 increase non-existed key
        response = requests.get(url.format("db0", "hash1", "key2"))
        self.writeLog(url.format("db0", "hash1", "key2"), "", response.content.decode())

        # case3 increase non-int key
        insertParmas = {
            "dbName": "db0",
            "hashName": "hash1",
            "keyName": "key3",
            "value": "1"
        }
        response = requests.post(insertUrl, json=insertParmas)
        response = requests.get(url.format("db0", "hash1", "key3"))
        self.writeLog(url.format("db0", "hash1", "key3"), "", response.content.decode())

        # case4 unknown database name
        response = requests.get(url.format("db999", "hash1", "key2"))
        self.writeLog(url.format("db0", "hash1", "key2"), "", response.content.decode())

        # case5 unknown hash name
        response = requests.get(url.format("db0", "hash2", "key2"))
        self.writeLog(url.format("db0", "hash2", "key2"), "", response.content.decode())

        # error url
        errorUrl = "http://" + self.host + ":" + str(self.port) + "/increasehash/{}/{}/{}"
        response = requests.get(errorUrl.format("db0", "hash1", "key1"))
        self.writeLog(errorUrl.format("db0", "hash1", "key1"), "", response.content.decode())

    def decreaseTest(self):
        url = "http://" + self.host + ":" + str(self.port) + "/decreaseHash/{}/{}/{}"

        # case1 create a hash, insert, and decrease
        createUrl = "http://" + self.host + ":" + str(self.port) + "/makeHash"
        insertUrl = "http://" + self.host + ":" + str(self.port) + "/insertHash"
        params = {
            "dbName": "db0",
            "hashName": "hash1"
        }
        response = requests.post(createUrl, json=params)
        insertParmas = {
            "dbName": "db0",
            "hashName": "hash1",
            "keyName": "key1",
            "value": 1
        }
        response = requests.post(insertUrl, json=insertParmas)
        response = requests.get(url.format("db0", "hash1", "key1"))
        self.writeLog(url.format("db0", "hash1", "key1"), "", response.content.decode())

        # case2 decrease non-existed key
        response = requests.get(url.format("db0", "hash1", "key2"))
        self.writeLog(url.format("db0", "hash1", "key2"), "", response.content.decode())

        # case3 decrease non-int key
        insertParmas = {
            "dbName": "db0",
            "hashName": "hash1",
            "keyName": "key3",
            "value": "1"
        }
        response = requests.post(insertUrl, json=insertParmas)
        response = requests.get(url.format("db0", "hash1", "key3"))
        self.writeLog(url.format("db0", "hash1", "key3"), "", response.content.decode())

        # case4 unknown database name
        response = requests.get(url.format("db999", "hash1", "key2"))
        self.writeLog(url.format("db0", "hash1", "key2"), "", response.content.decode())

        # case5 unknown hash name
        response = requests.get(url.format("db0", "hash2", "key2"))
        self.writeLog(url.format("db0", "hash2", "key2"), "", response.content.decode())

        # error url
        errorUrl = "http://" + self.host + ":" + str(self.port) + "/decreasehash/{}/{}/{}"
        response = requests.get(errorUrl.format("db0", "hash1", "key1"))
        self.writeLog(errorUrl.format("db0", "hash1", "key1"), "", response.content.decode())

    def getKeySetTest(self):
        url = "http://" + self.host + ":" + str(self.port) + "/getHashKeySet/{}/{}"

        # case1 create, get empty key set
        createUrl = "http://" + self.host + ":" + str(self.port) + "/makeHash"
        params = {
            "dbName": "db0",
            "hashName": "hash1"
        }
        response = requests.post(createUrl, json=params)

        response = requests.get(url.format("db0", "hash1"))
        self.writeLog(url.format("db0", "hash1"), "", response.content.decode())

        # case2 insert, get key set
        insertUrl = "http://" + self.host + ":" + str(self.port) + "/insertHash"
        insertParmas = {
            "dbName": "db0",
            "hashName": "hash1",
            "keyName": "key1",
            "value": 1
        }
        response = requests.post(insertUrl, json=insertParmas)
        response = requests.get(url.format("db0", "hash1"))
        self.writeLog(url.format("db0", "hash1"), "", response.content.decode())

        # case3 unknown database name
        response = requests.get(url.format("db999", "hash1"))
        self.writeLog(url.format("db999", "hash1"), "", response.content.decode())

        # case4 unknown hash name
        response = requests.get(url.format("db0", "hash999"))
        self.writeLog(url.format("db0", "hash999"), "", response.content.decode())

        # case5 error url
        errorUrl = "http://" + self.host + ":" + str(self.port) + "/gethashkeyset/{}/{}"
        response = requests.get(errorUrl.format("db0", "hash1"))
        self.writeLog(errorUrl.format("db0", "hash1"), "", response.content.decode())

    def getValuesTest(self):
        url = "http://" + self.host + ":" + str(self.port) + "/getHashValues/{}/{}"

        # case1 create, get empty hash values
        createUrl = "http://" + self.host + ":" + str(self.port) + "/makeHash"
        params = {
            "dbName": "db0",
            "hashName": "hash1"
        }
        response = requests.post(createUrl, json=params)

        response = requests.get(url.format("db0", "hash1"))
        self.writeLog(url.format("db0", "hash1"), "", response.content.decode())

        # case2 insert, get hash values
        insertUrl = "http://" + self.host + ":" + str(self.port) + "/insertHash"
        insertParmas = {
            "dbName": "db0",
            "hashName": "hash1",
            "keyName": "key1",
            "value": 1
        }
        response = requests.post(insertUrl, json=insertParmas)
        response = requests.get(url.format("db0", "hash1"))
        self.writeLog(url.format("db0", "hash1"), "", response.content.decode())

        # case3 unknown database name
        response = requests.get(url.format("db999", "hash1"))
        self.writeLog(url.format("db999", "hash1"), "", response.content.decode())

        # case4 unknown hash name
        response = requests.get(url.format("db0", "hash999"))
        self.writeLog(url.format("db0", "hash999"), "", response.content.decode())

        # case5 error url
        errorUrl = "http://" + self.host + ":" + str(self.port) + "/gethashvalues/{}/{}"
        response = requests.get(errorUrl.format("db0", "hash1"))
        self.writeLog(errorUrl.format("db0", "hash1"), "", response.content.decode())

if __name__ == "__main__":
    test = hashTest()

    # testing create hash function
    #test.createHashTest()

    # testing get hash function
    #test.getHashTest()

    # testing insert hash function
    # test.insertHashTest()

    # testing delete hash function
    # test.deleteHashTest()

    # testing remove from hash function
    # test.rmFromHashTest()

    # testing clear hash function
    # test.clearHashTest()

    # testing replace hash function
    test.replaceHashTest()

    # testing merge hashs function
    #test.mergeHashTest()

    # testing search hash function
    #test.searchHashTest()

    # testing set ttl function
    #test.setTTLTest()

    # testing clear ttl function
    #test.clearTTLTest()

    # testing get size function
    #test.getSizeTest()

    # testing increase hash function
    #test.increaseTest()

    # testing decrease hash function
    #test.decreaseTest()

    # testing get key set function
    #test.getKeySetTest()

    # testing get hash values function
    #test.getValuesTest()