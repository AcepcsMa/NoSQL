
# Introduction
This is a NoSQL Database implemented in Python (running in memory like Redis).

Multiple data structures are implemented and the corresponding APIs are well developed.

# Development Tool
1. ```Python 3.6.0```
2. ```Flask```

# Project Goal
1. Build 5(or more) data structures: 

	+ ```ELEMENT``` => The most common variable which can be Integer / String.
	+ ```LIST``` => A Sequential container of flexible length.
	+ ```HASH``` => A Key-Value mapping.
	+ ```SET``` => A container composed of unique data.
	+ ```ZSET``` => A **sorted** container composed of unique data.

2. Implement CRUD operations for each data structure mentioned above.

3. Implement DB-level operations like ***TTL***, ***Lock***, ***Authentification***.

3. Develop a client program and server program for use.

# User Manual
1. ***ELEMENT***
	+ ```HTTP POST```: *makeElem (dbName, elemName, value)*
	+ ```HTTP GET```: *getElem (dbName, elemName)*
	+ ```HTTP PUT```: *updateElem (dbName, elemName, value)*
	+ ```HTTP GET```: *searchElem (dbName, regex)*
	+ ```HTTP GET```: *searchAllElem (dbName)*
	+ ```HTTP PUT```: *increaseElem (dbName, elemName)*
	+ ```HTTP PUT```: *decreaseElem (dbName, elemName)*
	+ ```HTTP DELETE```: *deleteElem (dbName, elemName)*
	
2. ***LIST***
	+ ```HTTP POST```: *makeList (dbName, listName)*
	+ ```HTTP GET```: *getList (dbName, listName)*
	+ ```HTTP GET```: *leftGetList (dbName, listName, count)*
	+ ```HTTP GET```: *rightGetList (dbName, listName, count)*
	+ ```HTTP GET```: *getByRange (dbName, listName, start, end)*
	+ ```HTTP GET```: *getRandom (dbName, listName, count)*
	+ ```HTTP PUT```: *insertList (dbName, listName, value)*
	+ ```HTTP PUT```: *leftInsertList (dbName, listName, value)*
	+ ```HTTP DELETE```: *deleteList (dbName, listName)*
	+ ```HTTP PUT```: *rmFromList (dbName, listName, value)*
	+ ```HTTP PUT```: *clearList (dbName, listName)*
	+ ```HTTP PUT```: *mergeLists (dbName, listName1, listName2, resultListName)*
	+ ```HTTP GET```: *searchList (dbName, regex)*
	+ ```HTTP GET```: *searchAllList (dbName)*
	+ ```HTTP GET```: *getListSize (dbName, listName)*

3. ***HASH***
	+ *makeHash (dbName, hashName)*
	+ *getHash (dbName, hashName)*
	+ *getHashKeySet (dbName, hashName)*
	+ *getHashValues (dbName, hashName)*
	+ *getMultipleHashValues (dbName, hashName, keyNames)*
	+ *insertHash (dbName, hashName, keyName, value)*
	+ *isHashKeyExist (dbName, hashName, keyName)*
	+ *deleteHash (dbName, hashName)*
	+ *rmFromHash (dbName, hashName, keyName)*
	+ *clearHash (dbName, hashName)*
	+ *replaceHash (dbName, hashName, hashValue)*
	+ *mergeHashs (dbName, hashName1, hashName2, resultHashName, mode)*
	+ *searchHash (dbName, regex)*
	+ *searchAllHash (dbName)*
	+ *getHashSize (dbName, hashName)*
	+ *increaseHash (dbName, hashName, keyName)*
	+ *decreaseHash (dbName, hashName, keyName)*
	
4. ***SET***
	+ *makeSet (dbName, setName)*
	+ *getSet (dbName, setName)*
	+ *getSetRandom (dbName, setName, count)*
	+ *insertSet (dbName, setName, value)*
	+ *rmFromSet (dbName, setName, value)*
	+ *clearSet (dbName, setName)*
	+ *deleteSet (dbName, setName)*
	+ *searchSet (dbName, regex)*
	+ *searchAllSet (dbName)*
	+ *unionSet (dbName, setName1, setName2)*
	+ *intersectSet (dbName, setName1, setName2)*
	+ *diffSet (dbName, setName1, setName2)*
	+ *replaceSet (dbName, setName, setValue)*
	+ *getSetSize (dbName, setName)*

5. ***ZSET***
	+ *makeZSet (dbName, zsetName)*
	+ *getZSet (dbName, zsetName)*
	+ *insertZSet (dbName, zsetName, value, score)*
	+ *rmFromZSet (dbName, zsetName, value)*
	+ *clearZSet (dbName, zsetName)*
	+ *deleteZSet (dbName, zsetName)*
	+ *searchZSet (dbName, regex)*
	+ *searchAllZSet (dbName)*
	+ *findMinFromZSet (dbName, zsetName)*
	+ *findMaxFromZSet (dbName, zsetName)*
	+ *getScore (dbName, zsetName, value)*
	+ *getValuesByRange (dbName, zsetName, start, end)*
	+ *getZSetSize (dbName, zsetName)*
	+ *getRank (dbName, zsetName, value)*
	+ *rmFromZSetByScore (dbName, zsetName, start, end)*

6. ***COMMON OPERATIONS***
	+ *addDatabase (adminKey, dbName)*
	+ *getAllDatabase (adminKey)*
	+ *delDatabase (adminKey, dbName)*
	+ *saveDatabase ()*
	+ *changeSaveInterval (interval)*
	+ *setDbPassword (adminKey, dbName, password)*
	+ *changeDbPassword (adminKey, dbName, originalPwd, newPwd)*
	+ *removeDbPassword (adminKey, dbName)*
	+ *getType (dbName, keyName)*
	+ *setTTL (dataType, dbName, keyName, ttl)*
	+ *clearTTL (dataType, dbName, keyName)*





