
#include <vector>
#include <libltdl/lt_system.h>
#include "MapReduceClient.h"
#include <cstdio>
#include <stdlib.h>
#include <stdexcept>
#include <iostream>
#include <malloc.h>
#include "MapReduceFramework.h"
#include "ReduceFrameworkException.h"

#define INVALID_INPUT "Usage: <substring to search> <folders, separated by space>"
#define RESERVE "reserve"
#define MALLOC "malloc"
#define EXIT_FAILURE -1



char * stringToSearch;

typedef struct k1Base * k1BaseP;
typedef struct v1Base * v1BaseP;

struct k1v1{
    k1BaseP k1Basep;
    v1BaseP v1Basep;
};

typedef struct k1v1 * k1v1P;

// Create a vector containing integers


//// Add two more integers to vector
//v.push_back(25);
//v.push_back(13);
//
//// Iterate and print values of vector
//for(int n : v) {
//std::cout << n << '\n';
//}


void validateInput(int argc){
	//Check if input is valid.
	if (argc < 2){
		fprintf(stderr,"%s\n", INVALID_INPUT);
		exit(1);
		//no libraries to check, exit.
	}  else if (argc == 2){
		exit(0);
	}
	// else, valid input, return
}

int main(int argc, char* argv[]){
	validateInput(argc);
	int numOfLibrariesToSearch = argc - 2; //first is SEARCH, second is the string to search
	int multiThreadLevel = numOfLibrariesToSearch; // number of threads for the map level and
	
	//Create first container as a vector
	std::vector<k1v1P> container;
	try {
		container.reserve((unsigned long)multiThreadLevel);  // Allocate it's own space
	} catch (const std::length_error& le) { //allocation error
		throw ReduceFrameworkException(RESERVE);
	}
	stringToSearch =(char *)malloc(FILENAME_MAX * sizeof(char));
	if (stringToSearch == NULL){
		throw ReduceFrameworkException(MALLOC);
	}
	stringToSearch = argv[1]; //verify if 0 or 1
	
	
	int i = 0;
	for (i; i < numOfLibrariesToSearch; i++){
		k1v1 temp = {(k1BaseP)argv[i], (v1BaseP)stringToSearch};
		container.push_back(&temp);
	}
	


    //initialize K1 and V1 from the input
    k1Base newk1 = new()


    typedef MapReduceBase * MapReduceP;
    MapReduceP mapReduceP = new(); //how to initiate with pointer?
    mapReduceP->Map()

    MapReduceBase mapReduceBase2 = new()
    /**
     *
     *
     *
     */
}
