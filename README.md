
# Introduction
This is a NoSQL Database implemented in Python (running in memory like Redis).

Multiple data structures are implemented and the corresponding APIs are well developed.

# Development Tool
1. Python 3.6.0
2. Flask

# Project Goal
1. Build 5(or more) data structures: 

	+ ***ELEMENT*** => The most common variable which can be Integer / String.
	+ ***LIST*** => A Sequential container of flexible length.
	+ ***HASH*** => A Key-Value mapping.
	+ ***SET*** => A container composed of unique data.
	+ ***ZSET*** => A **sorted** container composed of unique data.

2. Implement CRUD operations for each data structure mentioned above.

3. Implement DB-level operations like ***TTL***, ***Lock***, ***Authentification***.

3. Develop a client program and server program for use.

# User Manual
1. ***ELEMENT***
	+ *makeElem (dbName, elemName, value)*
	+ *getElem (dbName, elemName)*
	+ *updateElem (dbName, elemName, value)*
	+ *searchElem (dbName, regex)*
	+ *searchAllElem (dbName)*
	+ *increaseElem (dbName, elemName)*
	+ *decreaseElem (dbName, elemName)*
	+ *deleteElem (dbName, elemName)*
	
2. ***LIST***
	+ *makeList (dbName, listName)*
	+ *getList (dbName, listName)*
	+ *leftGetList (dbName, listName, count)*
	+ *rightGetList (dbName, listName, count)*
	+ *getByRange (dbName, listName, start, end)*
	+ *getRandom (dbName, listName, count)*
	+ *insertList (dbName, listName, value)*
	+ *leftInsertList (dbName, listName, value)*
	+ *deleteList (dbName, listName)*
	+ *rmFromList (dbName, listName, value)*
	+ *clearList (dbName, listName)*
	+ *mergeLists (dbName, listName1, listName2, resultListName)*
	+ *searchList (dbName, regex)*
	+ *searchAllList (dbName)*
	+ *getListSize (dbName, listName)*

	
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







