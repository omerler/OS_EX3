#ifndef MAPREDUCECLIENT_H
#define MAPREDUCECLIENT_H

#include <vector>
#include <cstring>
#include <bits/stat.h>
#include <sys/stat.h>
#include <dirent.h>
#include <libltdl/lt_system.h>
#include <cstdio>
#include "ReduceFrameworkException.h"

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
    virtual ~v1Base(char* inputString) {
        stringToSearch = inputString;
    }
};

//intermediate key and value.
//the key, value for the Reduce function created by the Map function
class k2Base {
public:
    char* pathName;
    virtual ~k2Base(char* pathNameContainsString){
        pathName = pathNameContainsString;
    }
    virtual bool operator<(const k2Base &other) const {
        return (strcmp(pathName, (k2Base&)other.pathName) <= 0); //returns true if the path name of the k1Base is lexicographic
        //smaller then the other path name
    };
};

class v2Base {
public:
    int didAppear = 1; //did appear =1 if the string appears in the fileName
    virtual ~v2Base(){}
};

//output key and value
//the key,value for the Reduce function created by the Map function
class k3Base {
public:
    char* pathName;
    virtual ~k3Base(char* pathNameContainsString)  {
        pathName = pathNameContainsString;
    }
    virtual bool operator<(const k3Base &other) const{
        return (strcmp(pathName, (k3Base&)other.pathName) <= 0); //returns true if the path name of the k1Base is lexicographic
        //smaller then the other path name
    };
};

class v3Base {
public:
    int counter;
    virtual ~v3Base(int counterOfAppear) {
        counter = counterOfAppear;
    }
};

typedef std::vector<v2Base *> V2_VEC;

class MapReduceBase {
public:

    int mapHelper(char* fileName, char* stringToSearch)const {} //if the given string is Substring of the file name,
    //return 0

    void Map(const k1Base *const key, const v1Base *const val) const{
        k1Base* k1Base1 = (k1Base*) key;
        v1Base* v1Base1 = (v1Base*) val;

        struct stat path_stat; //check if the k1 is directory or regular file
        stat(k1Base1->pathName, &path_stat);

        int res;
        if (S_ISDIR(path_stat.st_mode)){
            DIR *dir;
            struct dirent *ent;
            if ((dir = opendir (k1Base1->pathName)) != NULL) {
                /* print all the files and directories within directory */
                while ((ent = readdir (dir)) != NULL) {
                    //check if the file name contains the needed string
                    res = mapHelper(ent->d_name, v1Base1->stringToSearch);
                }
                closedir (dir);
            } else {
                /* could not open directory */
                throw ReduceFrameworkException ("openDir");
                //return EXIT_FAILURE; //todo- check if needed to exit the program
            }
        } else {
            res = mapHelper(k1Base1->pathName, v1Base1->stringToSearch);
        }
        if (res == 0){
            k2Base* k2Base1 = new k2Base(k1Base1->pathName);
        }



    };
    void Reduce(const k2Base *const key, const V2_VEC &vals) const = 0;
};


#endif //MAPREDUCECLIENT_H
