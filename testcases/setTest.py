__author__ = 'Ma Haoxiang'

import requests
import json
import time

class setTest:
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
        url = "http://" + self.host + ":" + str(self.port) + "/makeSet"

        # case1 make a new set
        params = {
            "dbName": "db0",
            "setName": "set1"
        }
        response = requests.post(url=url, json=params)
        self.writeLog(url, json.dumps(params), response.content.decode())

        # case2 make a set repeatedly
        response = requests.post(url=url, json=params)
        self.writeLog(url, json.dumps(params), response.content.decode())

        # case3 unknown database name
        params["dbName"] = "db999"
        response = requests.post(url=url, json=params)
        self.writeLog(url, json.dumps(params), response.content.decode())

        # case4 invalid set name
        params["dbName"] = "db0"
        params["setName"] = "!@abc"
        response = requests.post(url=url, json=params)
        self.writeLog(url, json.dumps(params), response.content.decode())

        # case5 error url
        errorUrl = "http://" + self.host + ":" + str(self.port) + "/makeset"
        response = requests.post(url=errorUrl, json=params)
        self.writeLog(url, json.dumps(params), response.content.decode())

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
        createUrl = "http://" + self.host + ":" + str(self.port) + "/makeSet"
        params = {
            "dbName": "db0",
            "setName": "set1"
        }
        response = requests.post(url=createUrl, json=params)
        insertParams = {
            "dbName":"db0",
            "setName":"set1",
            "setValue":"1"
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
        insertParams["setName"] = "set123"
        response = requests.put(url, json=insertParams)
        self.writeLog(url, json.dumps(insertParams), response.content.decode())

        # case5 error database name type
        insertParams["dbName"] = [7,8,9]
        response = requests.put(url, json=insertParams)
        self.writeLog(url, json.dumps(insertParams), response.content.decode())

        # case6 error set name type
        insertParams["dbName"] = "db0"
        insertParams["setName"] = [4,5,6]
        response = requests.put(url, json=insertParams)
        self.writeLog(url, json.dumps(insertParams), response.content.decode())

        # case7 error url
        errorUrl = "http://" + self.host + ":" + str(self.port) + "/insertset"
        insertParams = {
            "dbName": "db0",
            "setName": "set1",
            "setValue": "1"
        }
        response = requests.put(errorUrl, json=insertParams)
        self.writeLog(errorUrl, json.dumps(insertParams), response.content.decode())

    def rmFromSetTest(self):
        url = "http://" + self.host + ":" + str(self.port) + "/rmFromSet"

        # case1 create, insert, then remove
        createUrl = "http://" + self.host + ":" + str(self.port) + "/makeSet"
        params = {
            "dbName": "db0",
            "setName": "set1"
        }
        response = requests.post(url=createUrl, json=params)

        insertUrl = "http://" + self.host + ":" + str(self.port) + "/insertSet"
        insertParams = {
            "dbName": "db0",
            "setName": "set1",
            "setValue": "1"
        }
        response = requests.put(insertUrl, json=insertParams)
        removeParams = {
            "dbName":"db0",
            "setName":"set1",
            "setValue":"1"
        }
        response = requests.put(url, json=removeParams)
        self.writeLog(url, json.dumps(removeParams), response.content.decode())

        # case2 remove from an empty set
        response = requests.put(url, json=removeParams)
        self.writeLog(url, json.dumps(removeParams), response.content.decode())

        # case3 unknown database name
        removeParams["dbName"] = "db999"
        response = requests.put(url, json=removeParams)
        self.writeLog(url, json.dumps(removeParams), response.content.decode())

        # case4 unknown set name
        removeParams["dbName"] = "db0"
        removeParams["setName"] = "set123"
        response = requests.put(url, json=removeParams)
        self.writeLog(url, json.dumps(removeParams), response.content.decode())

        # case5 error database name type
        removeParams["dbName"] = [1,2,3]
        removeParams["setName"] = "set1"
        response = requests.put(url, json=removeParams)
        self.writeLog(url, json.dumps(removeParams), response.content.decode())

        # case6 error set name type
        removeParams["dbName"] = "db0"
        removeParams["setName"] = [1,2,3]
        response = requests.put(url, json=removeParams)
        self.writeLog(url, json.dumps(removeParams), response.content.decode())

        # case7 non-existed set value
        removeParams["dbName"] = "db0"
        removeParams["setName"] = "set1"
        removeParams["setValue"] = "hello"
        response = requests.put(url, json=removeParams)
        self.writeLog(url, json.dumps(removeParams), response.content.decode())

        # case8 error url
        errorUrl = "http://" + self.host + ":" + str(self.port) + "/rmfromset"
        response = requests.put(errorUrl, json=removeParams)
        self.writeLog(errorUrl, json.dumps(removeParams), response.content.decode())

    def clearSetTest(self):
        url = "http://" + self.host + ":" + str(self.port) + "/clearSet/{}/{}"

        # case1 create, insert, then clear
        createUrl = "http://" + self.host + ":" + str(self.port) + "/makeSet"
        params = {
            "dbName": "db0",
            "setName": "set1"
        }
        response = requests.post(url=createUrl, json=params)

        insertUrl = "http://" + self.host + ":" + str(self.port) + "/insertSet"
        insertParams = {
            "dbName": "db0",
            "setName": "set1",
            "setValue": "1"
        }
        response = requests.put(insertUrl, json=insertParams)
        response = requests.put(url.format("db0","set1"))
        self.writeLog(url.format("db0","set1"), "", response.content.decode())

        # case2 clear an empty set
        response = requests.put(url.format("db0","set1"))
        self.writeLog(url.format("db0","set1"), "", response.content.decode())

        # case3 unknown database name
        response = requests.put(url.format("db123","set1"))
        self.writeLog(url.format("db123","set1"), "", response.content.decode())

        # case4 unknown set name
        response = requests.put(url.format("db0","set123"))
        self.writeLog(url.format("db0","set123"), "", response.content.decode())

        # case5 error url
        errorUrl = "http://" + self.host + ":" + str(self.port) + "/clearset/{}/{}"
        response = requests.put(errorUrl.format("db0","set1"))
        self.writeLog(errorUrl.format("db0","set1"), "", response.content.decode())

    def deleteSetTest(self):
        url = "http://" + self.host + ":" + str(self.port) + "/deleteSet/{}/{}"

        # case1 create, then delete a set
        createUrl = "http://" + self.host + ":" + str(self.port) + "/makeSet"
        params = {
            "dbName": "db0",
            "setName": "set1"
        }
        response = requests.post(url=createUrl, json=params)
        response = requests.delete(url.format("db0", "set1"))
        self.writeLog(url.format("db0","set1"), "", response.content.decode())

        # case2 delete a non-existed set
        response = requests.delete(url.format("db0", "set1"))
        self.writeLog(url.format("db0","set1"), "", response.content.decode())

        # case3 unknown database name
        response = requests.delete(url.format("db789", "set1"))
        self.writeLog(url.format("db789","set1"), "", response.content.decode())

        # case4 unknown set name
        response = requests.delete(url.format("db0", "set123"))
        self.writeLog(url.format("db0","set123"), "", response.content.decode())

        # case5 error url
        errorUrl = "http://" + self.host + ":" + str(self.port) + "/deleteset/{}/{}"
        response = requests.delete(errorUrl.format("db0", "set123"))
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
        createUrl = "http://" + self.host + ":" + str(self.port) + "/makeSet"
        params = {
            "dbName": "db0",
            "setName": "set1"
        }
        response = requests.post(url=createUrl, json=params)
        params["setName"] = "set2"
        response = requests.post(url=createUrl, json=params)

        unionParams = {
            "dbName":"db0",
            "setName1":"set1",
            "setName2":"set2"
        }
        response = requests.put(url, json=unionParams)
        self.writeLog(url,json.dumps(unionParams),response.content.decode())

        # case2 insert common values into two sets, then union
        insertUrl = "http://" + self.host + ":" + str(self.port) + "/insertSet"
        insertParams = {
            "dbName": "db0",
            "setName": "set1",
            "setValue": 1
        }
        response = requests.put(insertUrl, json=insertParams)
        insertParams = {
            "dbName": "db0",
            "setName": "set2",
            "setValue": 1
        }
        response = requests.put(url, json=unionParams)
        self.writeLog(url, json.dumps(unionParams), response.content.decode())

        # case3 unknown database name
        unionParams["dbName"] = "db999"
        response = requests.put(url, json=unionParams)
        self.writeLog(url, json.dumps(unionParams), response.content.decode())

        # case4 error database name type
        unionParams["dbName"] = [1,2,3]
        response = requests.put(url, json=unionParams)
        self.writeLog(url, json.dumps(unionParams), response.content.decode())

        # case5 unknown set name
        unionParams = {
            "dbName": "db0",
            "setName1": "set123",
            "setName2": "set2"
        }
        response = requests.put(url, json=unionParams)
        self.writeLog(url, json.dumps(unionParams), response.content.decode())

        # case6 error set name type
        unionParams = {
            "dbName": "db0",
            "setName1": ["hello","world"],
            "setName2": "set2"
        }
        response = requests.put(url, json=unionParams)
        self.writeLog(url, json.dumps(unionParams), response.content.decode())

        # error url
        errorUrl = "http://" + self.host + ":" + str(self.port) + "/unionset"
        response = requests.put(errorUrl, json=unionParams)
        self.writeLog(errorUrl, json.dumps(unionParams), response.content.decode())

    def intersectSetTest(self):
        url = "http://" + self.host + ":" + str(self.port) + "/intersectSet"

        # case1 create two empty set, then intersect
        createUrl = "http://" + self.host + ":" + str(self.port) + "/makeSet"
        params = {
            "dbName": "db0",
            "setName": "set1"
        }
        response = requests.post(url=createUrl, json=params)
        params["setName"] = "set2"
        response = requests.post(url=createUrl, json=params)
        intersectParams = {
            "dbName": "db0",
            "setName1": "set1",
            "setName2": "set2"
        }
        response = requests.put(url, json=intersectParams)
        self.writeLog(url, json.dumps(intersectParams), response.content.decode())

        # case2 insert common values into two sets, then intersect
        insertUrl = "http://" + self.host + ":" + str(self.port) + "/insertSet"
        insertParams = {
            "dbName": "db0",
            "setName": "set1",
            "setValue": 1
        }
        response = requests.put(insertUrl, json=insertParams)
        insertParams = {
            "dbName": "db0",
            "setName": "set2",
            "setValue": 1
        }
        response = requests.put(insertUrl, json=insertParams)
        response = requests.put(url, json=intersectParams)
        self.writeLog(url, json.dumps(intersectParams), response.content.decode())

        # case3 unknown database name
        intersectParams["dbName"] = "db999"
        response = requests.put(url, json=intersectParams)
        self.writeLog(url, json.dumps(intersectParams), response.content.decode())

        # case4 error database name type
        intersectParams["dbName"] = [1, 2, 3]
        response = requests.put(url, json=intersectParams)
        self.writeLog(url, json.dumps(intersectParams), response.content.decode())

        # case5 unknown set name
        intersectParams = {
            "dbName": "db0",
            "setName1": "set123",
            "setName2": "set2"
        }
        response = requests.put(url, json=intersectParams)
        self.writeLog(url, json.dumps(intersectParams), response.content.decode())

        # case6 error set name type
        intersectParams = {
            "dbName": "db0",
            "setName1": ["hello", "world"],
            "setName2": "set2"
        }
        response = requests.put(url, json=intersectParams)
        self.writeLog(url, json.dumps(intersectParams), response.content.decode())

        # error url
        errorUrl = "http://" + self.host + ":" + str(self.port) + "/intersectset"
        response = requests.put(errorUrl, json=intersectParams)
        self.writeLog(errorUrl, json.dumps(intersectParams), response.content.decode())

    def diffSetTest(self):
        url = "http://" + self.host + ":" + str(self.port) + "/diffSet"

        # case1 create two empty set, then diff
        createUrl = "http://" + self.host + ":" + str(self.port) + "/makeSet"
        params = {
            "dbName": "db0",
            "setName": "set1"
        }
        response = requests.post(url=createUrl, json=params)
        params["setName"] = "set2"
        response = requests.post(url=createUrl, json=params)
        diffParams = {
            "dbName": "db0",
            "setName1": "set1",
            "setName2": "set2"
        }
        response = requests.put(url, json=diffParams)
        self.writeLog(url, json.dumps(diffParams), response.content.decode())

        # case2 insert common values into two sets, then diff
        insertUrl = "http://" + self.host + ":" + str(self.port) + "/insertSet"
        insertParams = {
            "dbName": "db0",
            "setName": "set1",
            "setValue": 1
        }
        response = requests.put(insertUrl, json=insertParams)
        insertParams = {
            "dbName": "db0",
            "setName": "set2",
            "setValue": 1
        }
        response = requests.put(insertUrl, json=insertParams)
        response = requests.put(url, json=diffParams)
        self.writeLog(url, json.dumps(diffParams), response.content.decode())

        # case3 unknown database name
        diffParams["dbName"] = "db999"
        response = requests.put(url, json=diffParams)
        self.writeLog(url, json.dumps(diffParams), response.content.decode())

        # case4 error database name type
        diffParams["dbName"] = [1, 2, 3]
        response = requests.put(url, json=diffParams)
        self.writeLog(url, json.dumps(diffParams), response.content.decode())

        # case5 unknown set name
        diffParams = {
            "dbName": "db0",
            "setName1": "set123",
            "setName2": "set2"
        }
        response = requests.put(url, json=diffParams)
        self.writeLog(url, json.dumps(diffParams), response.content.decode())

        # case6 error set name type
        diffParams = {
            "dbName": "db0",
            "setName1": ["hello", "world"],
            "setName2": "set2"
        }
        response = requests.put(url, json=diffParams)
        self.writeLog(url, json.dumps(diffParams), response.content.decode())

        # error url
        errorUrl = "http://" + self.host + ":" + str(self.port) + "/diffset"
        response = requests.put(errorUrl, json=diffParams)
        self.writeLog(errorUrl, json.dumps(diffParams), response.content.decode())

    def replaceSetTest(self):
        url = "http://" + self.host + ":" + str(self.port) + "/replaceSet"

        # case1 create a set, replace it with new value
        createUrl = "http://" + self.host + ":" + str(self.port) + "/makeSet"
        params = {
            "dbName": "db0",
            "setName": "set1"
        }
        response = requests.post(url=createUrl, json=params)
        insertUrl = "http://" + self.host + ":" + str(self.port) + "/insertSet"
        insertParams = {
            "dbName": "db0",
            "setName": "set1",
            "setValue": 1
        }
        response = requests.put(insertUrl, json=insertParams)
        replaceParams = {
            "dbName":"db0",
            "setName":"set1",
            "setValue":["hello","world"]
        }
        response = requests.put(url, json=replaceParams)
        self.writeLog(url, json.dumps(replaceParams), response.content.decode())

        # case2 replace the set with empty value
        replaceParams["setValue"] = []
        response = requests.put(url, json=replaceParams)
        self.writeLog(url, json.dumps(replaceParams), response.content.decode())

        # case3 unknown database name
        replaceParams["dbName"] = "db999"
        response = requests.put(url, json=replaceParams)
        self.writeLog(url, json.dumps(replaceParams), response.content.decode())

        # case4 unknown set name
        replaceParams = {
            "dbName": "db0",
            "setName": "set123",
            "setValue": "1"
        }
        response = requests.put(url, json=replaceParams)
        self.writeLog(url, json.dumps(replaceParams), response.content.decode())

        # case5 error url
        errorUrl = "http://" + self.host + ":" + str(self.port) + "/replaceset"
        replaceParams = {
            "dbName": "db0",
            "setName": "set1",
            "setValue": "1"
        }
        response = requests.put(errorUrl, json=replaceParams)
        self.writeLog(errorUrl, json.dumps(replaceParams), response.content.decode())

    def setTTLTest(self):
        url = "http://" + self.host + ":" + str(self.port) + "/setTTL"

        # case1 create a set, set ttl
        createUrl = "http://" + self.host + ":" + str(self.port) + "/makeSet"
        params = {
            "dbName": "db0",
            "setName": "set1"
        }
        response = requests.post(url=createUrl, json=params)
        ttlParams = {
            "dataType": "SET",
            "dbName": "db0",
            "keyName": "set1",
            "ttl": 20
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
        ttlParams["keyName"] = "set123"
        response = requests.post(url=url, json=ttlParams)
        self.writeLog(url, json.dumps(ttlParams), response.content.decode())

        # case5 ttl is not INT type
        ttlParams["keyName"] = "set1"
        ttlParams["ttl"] = "hello"
        response = requests.post(url=url, json=ttlParams)
        self.writeLog(url, json.dumps(ttlParams), response.content.decode())

        # error url
        errorUrl = "http://" + self.host + ":" + str(self.port) + "/setttl/{}/{}/{}"
        response = requests.post(url=errorUrl, json=ttlParams)
        self.writeLog(errorUrl, json.dumps(ttlParams), response.content.decode())

    def clearTTLTest(self):
        url = "http://" + self.host + ":" + str(self.port) + "/clearTTL"

        # case1 set a TTL and then clear it
        createUrl = "http://" + self.host + ":" + str(self.port) + "/makeSet"
        params = {
            "dbName": "db0",
            "setName": "set1"
        }
        response = requests.post(url=createUrl, json=params)
        setUrl = "http://" + self.host + ":" + str(self.port) + "/setTTL"
        ttlParams = {
            "dataType": "SET",
            "dbName": "db0",
            "keyName": "set1",
            "ttl": 90
        }
        response = requests.post(url=setUrl, json=ttlParams)
        clearParams = {
            "dataType": "SET",
            "dbName": "db0",
            "keyName": "set1"
        }
        response = requests.post(url=url, json=clearParams)
        self.writeLog(url, json.dumps(clearParams), response.content.decode())

        # case2 clear TTL repeatedly
        response = requests.post(url=url, json=clearParams)
        self.writeLog(url, json.dumps(clearParams), response.content.decode())

        # case3 clear non-existed TTL
        params = {
            "dbName": "db0",
            "setName": "set2"
        }
        response = requests.post(url=createUrl, json=params)
        clearParams["keyName"] = "set2"
        response = requests.post(url=url, json=clearParams)
        self.writeLog(url, json.dumps(clearParams), response.content.decode())

        # case4 unknown database name
        clearParams["dbName"] = "db999"
        response = requests.post(url=url, json=clearParams)
        self.writeLog(url, json.dumps(clearParams), response.content.decode())

        # case5 unknown element name
        clearParams["keyName"] = "set999"
        clearParams["dbName"] = "db0"
        response = requests.post(url=url, json=clearParams)
        self.writeLog(url, json.dumps(clearParams), response.content.decode())

        # case6 error url
        errorUrl = "http://" + self.host + ":" + str(self.port) + "/clearttl/{0}/{1}"
        response = requests.post(url=errorUrl, json=clearParams)
        self.writeLog(errorUrl, json.dumps(clearParams), response.content.decode())

    def getSizeTest(self):
        url = "http://" + self.host + ":" + str(self.port) + "/getSetSize/{}/{}"

        # case1 create a set, insert, and get its size
        createUrl = "http://" + self.host + ":" + str(self.port) + "/makeSet/{}/{}"
        insertUrl = "http://" + self.host + ":" + str(self.port) + "/insertSet"
        insertParams = {
            "dbName": "db0",
            "setName": "set1",
            "setValue": 1
        }
        response = requests.get(createUrl.format("db0", "set1"))
        response = requests.post(insertUrl, json=insertParams)
        response = requests.get(url.format("db0", "set1"))
        self.writeLog(url.format("db0", "set1"), "", response.content.decode())

        # case2 get size of an empty set
        response = requests.get(createUrl.format("db0", "set2"))
        response = requests.get(url.format("db0", "set2"))
        self.writeLog(url.format("db0", "set2"), "", response.content.decode())

        # case3 unknown database name
        response = requests.get(url.format("db123", "set1"))
        self.writeLog(url.format("db123", "set1"), "", response.content.decode())

        # case4 unknown hash name
        response = requests.get(url.format("db0", "set123"))
        self.writeLog(url.format("db0", "set123"), "", response.content.decode())

        #case5 error url
        errorUrl = "http://" + self.host + ":" + str(self.port) + "/getsetsize/{}/{}"
        response = requests.get(errorUrl.format("db0", "set1"))
        self.writeLog(errorUrl.format("db0", "set1"), "", response.content.decode())

if __name__ == "__main__":
    test = setTest()

    # testing make set function
    # test.makeSetTest()

    # testing get set function
    #test.getSetTest()

    # testing insert set function
    # test.insertSetTest()

    # testing remove from set function
    # test.rmFromSetTest()

    # testing clear set function
    # test.clearSetTest()

    # testing delete set function
    # test.deleteSetTest()

    # testing search set function
    #test.searchSetTest()

    # testing union set function
    # test.unionSetTest()

    # testing intersect set function
    # test.intersectSetTest()

    # testing diff set function
    # test.diffSetTest()

    # testing replace set function
    # test.replaceSetTest()

    # testing set ttl function
    # test.setTTLTest()

    # testing clear ttl function
    # test.clearTTLTest()

    # testing get size function
    # test.getSizeTest()