
# Introduction
This is a NoSQL Database implemented in Python (running in memory like Redis).

Multiple data structures are implemented and the corresponding APIs are well developed.

# Development Tool
1. Python 3.6.0
2. Flask

# Project Goal
1. Build 5(or more) data structures: 

	+ ***ELEMENT***
	+ ***LIST***
	+ ***HASH***
	+ ***SET***
	+ ***ZSET***

2. Implement CRUD operations for each data structure mentioned above.

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
4. ***SET***
5. ***ZSET***
