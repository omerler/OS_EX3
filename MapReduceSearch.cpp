#include "MapReduceSearch.h"
#include <cstring>
#include <sys/stat.h>
#include <dirent.h>
#include <libltdl/lt_system.h>
#include <cstdio>
#include "ReduceFrameworkException.h"
#include "MapReduceFramework.h"


//input key and value.
//the key, value for the map function and the MapReduceFramework

//k1 implementation
k1BaseSearch::k1BaseSearch(char *path) {//The ~ operator is bitwise NOT, it inverts the bits in a binary number:
	pathName = path;
}

bool k1BaseSearch::operator<(const k1Base &other) const { //operator to compare to path names
	return ((strcmp(pathName, ((k1BaseSearch&)other).getPathName())) <= 0); //returns true if the
	// path name of the k1Base is lexicographic smaller then the other path name
};

char * k1BaseSearch::getPathName() const{
	return pathName;
}


//v1 implementation
v1BaseSearch::v1BaseSearch(char *inputString) {
	stringToSearch = inputString;
}


//intermediate key and value.
//the key, value for the Reduce function created by the Map function

//k2 implementation

k2BaseSearch::k2BaseSearch(char *pathNameContainsString) {
	pathName = pathNameContainsString;
}
bool k2BaseSearch::operator<(const k2Base &other) const {
	return strcmp(pathName, ((k2BaseSearch&)other).pathName) <= 0; //returns true if the path
	// name of the k1Base is lexicographic
	//smaller then the other path name
};

//output key and value
//the key,value for the Reduce function created by the Map function

//k3 implementation

k3BaseSearch::k3BaseSearch(char *pathNameContainsString) {
	pathName = pathNameContainsString;
}

bool k3BaseSearch::operator<(const k3Base &other) const{
	return (strcmp(pathName, ((k3BaseSearch&)other).pathName) <= 0); //returns true if the path
	// name of the k1Base is lexicographic smaller then the other path name
};


//v3 implementation
v3BaseSearch::v3BaseSearch(int counterOfAppear) {
	counter = counterOfAppear;
}


typedef std::vector<v2Base *> value2_vec;

/*
 * Assisting function to Map- the function checks if the given stringToSearch is substring of the given fileName,
 * and if so- returns 0.
 */
int mapHelper(char* fileName, char* stringToSearch) {
	if(strstr(fileName, stringToSearch) != NULL) {
		return 0;
	}
	return -1;
}

//Map and reduce implementation

void MapReduceBaseSearch::Map(const k1Base *const key, const v1Base *const val) const { //todo-
// can we get a file which is not a directory, and search for our str in it?
	k1BaseSearch* k1Base1 = (k1BaseSearch*) key;
	v1BaseSearch* v1Base1 = (v1BaseSearch*) val;
	
	struct stat path_stat; //check if the k1 is directory or regular file
	stat(k1Base1->pathName, &path_stat);
	if (S_ISDIR(path_stat.st_mode)){ //if k1 is directory
		DIR *dir;
		struct dirent *ent;
		if ((dir = opendir (k1Base1->pathName)) != NULL) {
			/* iterate all the files and directories within directory */
			while ((ent = readdir (dir)) != NULL) {
				//check if the file name contains the needed string
				int res = mapHelper(ent->d_name, v1Base1->stringToSearch);
				if (res == 0){
					k2BaseSearch* k2Base1 = new k2BaseSearch(k1Base1->pathName);
					v2BaseSearch* v2Base1 = new v2BaseSearch();
					
					Emit2(k2Base1, (v2Base*)v2Base1);
				}
			}
			closedir (dir);
		} else {
			/* could not open directory or empty directory */
			throw ReduceFrameworkException ("openDir");
		}
	}
}

void MapReduceBaseSearch::Reduce(const k2Base *const key, const value2_vec &vals) const {
	if (&key != NULL && vals.size()!=0){
		k2BaseSearch* k2Base1 = (k2BaseSearch*) key;
		k3BaseSearch *k3BaseP = new k3BaseSearch((k2Base1->pathName));
		int v2VecLength = (int)vals.size();
		v3BaseSearch *v3Base1P = new v3BaseSearch(v2VecLength);
		Emit3(k3BaseP,v3Base1P);
	}
	//todo think if here should i delete (dealloc) the V2 CONTAINERS (indicated by autoDeleteV2K2)
}