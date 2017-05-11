#ifndef MAPREDUCESEARCH_H
#define MAPREDUCESEARCH_H

#include <vector>
#include "MapReduceClient.h"

//input key and value.
//the key, value for the map function and the MapReduceFramework
class k1BaseSearch: public k1Base {
public:
	char *pathName;
	k1BaseSearch(char* path);
	~k1BaseSearch(){}
	virtual bool operator<(const k1Base &other) const override;
	char* getPathName() const;
};

class v1BaseSearch: public v1Base {
public:
	char *stringToSearch;
	v1BaseSearch(char* inputString);
	~v1BaseSearch() {}
};

//intermediate key and value.
//the key, value for the Reduce function created by the Map function
class k2BaseSearch: public k2Base {
public:
	char* pathName;
	k2BaseSearch(char* pathNameContainsString);
	~k2BaseSearch(){}
	virtual bool operator<(const k2Base &other) const;
};

class v2BaseSearch: public v2Base {
public:
	int didAppear = 1;
	~v2BaseSearch(){}
};

//output key and value
//the key,value for the Reduce function created by the Map function
class k3BaseSearch :public k3Base{
public:
	char* pathName;
	k3BaseSearch(char* pathNameContainsString);
	~k3BaseSearch(){}
	virtual bool operator<(const k3Base &other) const;
};

class v3BaseSearch: public v3Base {
public:
	int counter;
	v3BaseSearch(int counterOfAppear);
	~v3BaseSearch() {}
};

typedef std::vector<v2Base *> value2_vec;

class MapReduceBaseSearch: public MapReduceBase {
public:
	virtual void Map(const k1Base *const key, const v1Base *const val) const override ;
	virtual void Reduce(const k2Base *const key, const value2_vec &vals) const override ;
};


#endif //MAPREDUCESEARCH_H
