#ifndef MAPREDUCECLIENT_H
#define MAPREDUCECLIENT_H

#include <vector>
#include <cstring>
#include <bits/stat.h>
#include <sys/stat.h>
#include <dirent.h>
#include <libltdl/lt_system.h>
#include <cstdio>
#include <stdlib.h>
#include "ReduceFrameworkException.h"
#include "MapReduceFramework.h"


//input key and value.
//the key, value for the map function and the MapReduceFramework
class k1Base {
public:
    char *pathName;
    k1Base(char* path){//The ~ operator is bitwise NOT, it inverts the bits in a binary number:
        pathName = path;
    }
    bool operator<(const k1Base &other) const { //operator to compare to path names
        return (strcmp(pathName, (k1Base&)other.pathName) <= 0); //returns true if the path name of the k1Base is lexicographic
        //smaller then the other path name
    };
};

class v1Base {
public:
    char *stringToSearch;
    v1Base(char* inputString) {
        stringToSearch = inputString;
    }
};

//intermediate key and value.
//the key, value for the Reduce function created by the Map function
class k2Base {
public:
    char* pathName;
    k2Base(char* pathNameContainsString){
        pathName = pathNameContainsString;
    }
    bool operator<(const k2Base &other) const {
        return (strcmp(pathName, (k2Base&)other.pathName) <= 0); //returns true if the path name of the k1Base is lexicographic
        //smaller then the other path name
    };
};

class v2Base {
public:
    int didAppear = 1; //did appear =1 if the string appears in the fileName
    v2Base(){}
};

//output key and value
//the key,value for the Reduce function created by the Map function
class k3Base {
public:
    char* pathName;
    k3Base(char* pathNameContainsString)  {
        pathName = pathNameContainsString;
    }
    bool operator<(const k3Base &other) const{
        return (strcmp(pathName, (k3Base&)other.pathName) <= 0); //returns true if the path name of the k1Base is lexicographic
        //smaller then the other path name
    };
};

class v3Base {
public:
    int counter;
    v3Base(int counterOfAppear) {
        counter = counterOfAppear;
    }
};

typedef std::vector<v2Base *> V2_VEC;

class MapReduceBase {
public:

    /*
     * Assisting function to Map- the function checks if the given stringToSearch is substring of the given fileName,
     * and if so- returns 0.
     */
    int mapHelper(char* fileName, char* stringToSearch)const {
        if(strstr(fileName, stringToSearch) != NULL) {
            return 0;
        }
        return -1;
    }

    void Map(const k1Base *const key, const v1Base *const val) const{ //todo- can we get a file which is not a directory, and search for our str in it?
        k1Base* k1Base1 = (k1Base*) key;
        v1Base* v1Base1 = (v1Base*) val;

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
                        k2Base* k2Base1 = new k2Base(k1Base1->pathName);
                        v2Base* v2Base1 = new v2Base();

                        Emit2(k2Base1, v2Base1);
                    }
                }
                closedir (dir);
            } else {
                /* could not open directory or empty directory */
                throw ReduceFrameworkException ("openDir");
            }
        }
    };

    void Reduce(const k2Base *const key, const V2_VEC &vals) const{
        if (&key != NULL && vals.size()!=0){
            k2Base * k2Base1 = new k2Base(key->pathName);
            k3Base *k3BaseP = new k3Base((k2Base1->pathName));
            int v2VecLength = (int)vals.size();
            v3Base *v3Base1P = new v3Base(v2VecLength);
            Emit3(k3BaseP,v3Base1P);
        }
        //todo think if here should i delete (dealloc) the V2 CONTAINERS (indicated by autoDeleteV2K2)
    }};


#endif //MAPREDUCECLIENT_H
