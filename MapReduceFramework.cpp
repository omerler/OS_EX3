#include "MapReduceFramework.h"
#include "logFileHandler.h"
#include <pthread.h>
#include <mutex>
#include <map>
#include <vector>
#include <iostream>
#include <stdlib.h>
#include <sys/time.h>
#include <unordered_map>
#include "FrameworkException.h"

#define NUM_OF_ITEMS_TO_HANDLE 10
#define BEGIN_INDEX 0
#define SECONDS_TO_NANO 1000000000
#define MICRO_TO_NANO 1000
#define NANOS_TO_WAIT 10000000
#define SECS_TO_WAIT 0

#define MAP_TYPE "ExecMap"
#define SHUFFLE_TYPE "Shuffle"
#define REDUCE_TYPE "ExecReduce"
#define MAP_SHUFFLE_PROCESS "Map and Shuffle"
#define REDUCE_PROCESS "Reduce"

#define CREATE_FAILURE "MapReduceFramework Failure: pthread_create failed.\n"
#define COND_WAIT_FAILURE "MapReduceFramework Failure: pthread_cond_timedwait failed.\n"
#define LOCK_FAILURE "MapReduceFramework Failure: pthread_mutex_lock failed.\n"
#define UNLOCK_FAILURE "MapReduceFramework Failure: pthread_mutex_unlock failed.\n"
#define NEW_FAILURE "MapReduceFramework Failure: new failed.\n"
#define PTHREAD_JOIN_FAILURE "MapReduceFramework Failure: pthread_join failed.\n"
#define SIGNAL_FAILURE "MapReduceFramework Failure: pthread_cond_signal failed.\n"
#define GETTIME_FAILURE "MapReduceFramework Failure: gettimeofday failed.\n"
#define MUTEX_DESTROY_FAILURE "MapReduceFramework Failure: pthread_mutex_destroy failed.\n"
#define MUTEX_INIT_FAILURE "MapReduceFramework Failure: pthread_mutex_init failed.\n"
#define COND_DESTROY_FAILURE "MapReduceFramework Failure: pthread_cond_destroy failed.\n"
#define COND_INIT_FAILURE "MapReduceFramework Failure: pthread_cond_init failed.\n"
#define TIME_FAILURE "MapReduceFramework Failure: time failed.\n"

static void terminateMutexes();
static void initializeMutex(pthread_mutex_t* mutex);

/*
 * threads comparator
 */
struct pthread_compare{
	bool operator() (const pthread_t& thread1, const pthread_t& thread2) const{
		return (thread1 < thread2);
	}
};

typedef struct ExecMapShuffleTools ExecMapShuffleTools;
typedef std::vector<IN_ITEM> IN_ITEMS_VECTOR;
typedef std::pair<k2Base*, v2Base*> MAP_OUT_ITEM;
typedef std::list<MAP_OUT_ITEM> MAP_OUT_ITEMS_LIST;
typedef std::map<pthread_t, ExecMapShuffleTools*, pthread_compare> EMIT2_OUT_MAP;
typedef std::pair<k2Base*, std::list<v2Base*>> SHUFFLE_OUT_ITEM;
typedef std::map<k2Base*, std::list<v2Base*>> SHUFFLE_OUT_MAP;
typedef std::vector<SHUFFLE_OUT_ITEM> SHUFFLE_OUT_VECTOR;
typedef std::map<pthread_t, OUT_ITEMS_LIST, pthread_compare> EMIT3_OUT_MAP;

struct ExecMapShuffleTools{
	MAP_OUT_ITEMS_LIST emit2OutItemsList;
	pthread_mutex_t mapShuffleMutex;
};

/*
 * holds the utils needed for the framework to run
 */
struct FrameworkUtils{
	//represents the next chunk of items to be processed
	unsigned int index = 0;
	//the index mutex
	pthread_mutex_t ixMutex;
	//the items the framework originally receives, as a vector
	IN_ITEMS_VECTOR inItemsVector;
	//the amount of items to process
	unsigned long inVectorLength = 0;
	//the users' class implementing the map and reduce functions
	MapReduceBase& clientMapReduce;
	//condition variable used in pthread_cond_timedwait of the shuffle thread
	pthread_cond_t shuffleCond;
	//mutex used in the condition variable of the shuffle
	pthread_mutex_t shuffleCondMutex;
	//the output of the emit2 function
	EMIT2_OUT_MAP emit2OutMap;
	//a mutex for protecting the emit2OutMap counter
	pthread_mutex_t emit2OutMapMutex;
	//the output of the shuffle function
	SHUFFLE_OUT_MAP shuffleOutMap;
	//the output of the shuffle function, as a vector
	SHUFFLE_OUT_VECTOR shuffleOutVector;
	//the output of the emit3 function
	EMIT3_OUT_MAP emit3OutMap;
	//a mutex for protecting the emit3OutMap counter
	pthread_mutex_t emit3OutMapMutex;
	//the final list to be returned by the framework
	OUT_ITEMS_LIST outItemsList;
	//a counter for living execMap threads
	int mapCounter = 0;
	//a mutex for protecting the mapThreads counter
	pthread_mutex_t counterMutex;
	//timer for the cond_timedwait
	struct timespec timeToWait = {0, 0};
	struct timeval now;


	//time measurement variables
	struct timeval start;
	struct timeval endMapShuffle;
	struct timeval end;

	FrameworkUtils(MapReduceBase &mapReduce, IN_ITEMS_LIST itemsList):
			clientMapReduce(mapReduce) {
		index = BEGIN_INDEX;
		inItemsVector = IN_ITEMS_VECTOR(itemsList.begin(), itemsList.end());
		inVectorLength = inItemsVector.size();
		initializeMutex(&ixMutex);
		initializeMutex(&shuffleCondMutex);
		initializeMutex(&emit2OutMapMutex);
		initializeMutex(&emit3OutMapMutex);
		initializeMutex(&counterMutex);
		int ret = pthread_cond_init(&shuffleCond, NULL);
		if(ret){
			std::cerr<<COND_INIT_FAILURE;
			exit(EXIT_FAILURE);
		}
		mapCounter = 0;
	}
};

FrameworkUtils * utils;


/*
 * helper function for terminating the utils
 */
static void terminateAll(){
	terminateMutexes();
	for(EMIT2_OUT_MAP::iterator it = utils->emit2OutMap.begin();
		it != utils->emit2OutMap.end(); it++){
		delete(it->second);
	}

	int ret = pthread_cond_destroy(&utils->shuffleCond);
	if(ret){
		std::cerr<<COND_DESTROY_FAILURE;
		exit(EXIT_FAILURE);
	}

	utils->emit2OutMap.clear();
	utils->emit3OutMap.clear();
	utils->inItemsVector.clear();
	utils->outItemsList.clear();
	utils->shuffleOutMap.clear();
	utils->shuffleOutVector.clear();

	closeLogFile();
	delete(utils);
}

/*
 * a helper method for destroying a mutex.
 */
static void initializeMutex(pthread_mutex_t* mutex){
	int ret = pthread_mutex_init(mutex, NULL);
	if (ret){
		throw FrameworkException(MUTEX_INIT_FAILURE);
	}
}


/*
 * a helper method for destroying a mutex.
 */
static void terminateMutex(pthread_mutex_t* mutex){
	int ret = pthread_mutex_destroy(mutex);
	if (ret){
		throw FrameworkException(MUTEX_DESTROY_FAILURE);
	}
}


/*
 * helper function for properly deleting the mutex pointers.
 */
static void terminateMutexes(){
	for(EMIT2_OUT_MAP::iterator it = utils->emit2OutMap.begin();
		it != utils->emit2OutMap.end(); it++){
		terminateMutex(&it->second->mapShuffleMutex);
	}

	terminateMutex(&utils->ixMutex);
	terminateMutex(&utils->shuffleCondMutex);
	terminateMutex(&utils->emit2OutMapMutex);
	terminateMutex(&utils->emit3OutMapMutex);
}


/**
 * a helper function for getting the next index the thread should work on.
 */
int getIndex(){
	int ret = pthread_mutex_lock(&utils->ixMutex);
	if(ret){
		//couldn't lock
		throw FrameworkException(LOCK_FAILURE);
	}

	int index = utils->index;
	utils->index += NUM_OF_ITEMS_TO_HANDLE;

	ret = pthread_mutex_unlock(&utils->ixMutex);
	if(ret){
		//couldn't unlock
		throw FrameworkException(UNLOCK_FAILURE);
	}

	return index;
}


/*
 * the function that ExecMap threads execute: takes a constant number of items
 * from the in items list, and calls the users' map function on each one.
 * afterwards it calls the pthread_cond_signal for the shuffle thread to handle
 * the new chunk of pairs entered.
 */
void* execMapFunction(void* arg){
	//initializing data structures
	ExecMapShuffleTools* newTools = new ExecMapShuffleTools;
	initializeMutex(&newTools->mapShuffleMutex);

	int ret = pthread_mutex_lock(&utils->emit2OutMapMutex);
	if(ret){
		throw FrameworkException(LOCK_FAILURE);
	}

	utils->emit2OutMap.emplace(pthread_self(), newTools);


	ret = pthread_mutex_unlock(&utils->emit2OutMapMutex);
	if(ret){
		throw FrameworkException(UNLOCK_FAILURE);
	}


	while(utils->index < utils->inVectorLength){
		//updating the index to read from the list for other threads.
		int index = getIndex();

		//main functionality of the thread - calling the users' map function
		for(unsigned int i = 0; i < NUM_OF_ITEMS_TO_HANDLE &&
				index + i < utils->inVectorLength; i++){
			//should work on list item number index+i
			IN_ITEM inItem = utils->inItemsVector[index+i];
			utils->clientMapReduce.Map(inItem.first, inItem.second);
		}

		//signaling the shuffle thread about the new chunk written
		ret = pthread_cond_signal(&utils->shuffleCond);
		if (ret){
			throw FrameworkException(SIGNAL_FAILURE);
		}
	}

	writeThreadTermination(MAP_TYPE);
	ret = pthread_mutex_lock(&utils->counterMutex);
	if(ret){
		//couldn't lock
		throw FrameworkException(LOCK_FAILURE);
	}

	utils->mapCounter --;

	ret = pthread_mutex_unlock(&utils->counterMutex);
	if(ret){
		//couldn't unlock
		throw FrameworkException(UNLOCK_FAILURE);
	}

	pthread_exit(NULL);
}

/*
 * the function that ExecReduce threads execute: takes a constant number of
 * items from the shuffle output, and calls the users' reduce function on each
 * one.
 */
void* execReduceFunction(void* arg){
	int ret = pthread_mutex_lock(&utils->emit3OutMapMutex);
	if(ret){
		throw FrameworkException(LOCK_FAILURE);
	}
	utils->emit3OutMap[pthread_self()];

	ret = pthread_mutex_unlock(&utils->emit3OutMapMutex);
	if(ret){
		throw FrameworkException(UNLOCK_FAILURE);
	}

	while(utils->index < utils->shuffleOutVector.size()){
		//updating the index to read from the list for other threads.
		int index = getIndex();

		//main functionality of the thread - calling the users' map function
		for(unsigned int i = 0; i < NUM_OF_ITEMS_TO_HANDLE &&
				index + i < utils->shuffleOutVector.size(); i++){
			//should work on list item number index+i
			SHUFFLE_OUT_ITEM item = utils->shuffleOutVector[index+i];
			utils->clientMapReduce.Reduce(item.first, item.second);
		}
	}

	writeThreadTermination(REDUCE_TYPE);
	pthread_exit(NULL);
}


/**
 * function that receives a couple of <k2Base*, v2Base*> and inserts it into
 * the frameworks data.
 */
void Emit2 (k2Base* key, v2Base* value){
	MAP_OUT_ITEM newPair (key, value);
	pthread_t current = pthread_self();
	int ret = pthread_mutex_lock(&utils->emit2OutMap[current]->mapShuffleMutex);
	if(ret){
		//couldn't lock
		throw FrameworkException(LOCK_FAILURE);
	}

	utils->emit2OutMap[pthread_self()]->emit2OutItemsList.push_back(newPair);

	ret = pthread_mutex_unlock(&utils->emit2OutMap[pthread_self()]->mapShuffleMutex);
	if(ret){
		//couldn't unlock
		throw FrameworkException(UNLOCK_FAILURE);
	}
}

/**
 * function that receives a couple of <k3Base*, v3Base*> and inserts it into the
 * frameworks data.
 */
void Emit3 (k3Base* key, v3Base* value){
	OUT_ITEM newPair (key, value);
	utils->emit3OutMap[pthread_self()].push_back(newPair);
}


/*
 * a helper function for figuring whether or not a certain key already exists
 * in the shuffleOutMap, using the operator<.
 */
static k2Base* findKey(k2Base* other){
	for(SHUFFLE_OUT_MAP::iterator it = utils->shuffleOutMap.begin();
			it != utils->shuffleOutMap.end(); ++it){
		if (!(*it->first < *other) && !(*other < *it->first)) {
			return it->first;
		}
	}
	return other;
}


/*
 * helper function for the shuffle execution
 */
static void shuffleLoop(){
	//collecting relevant items to shuffle
	MAP_OUT_ITEMS_LIST items;
	for (EMIT2_OUT_MAP::iterator it = utils->emit2OutMap.begin();
		 it != utils->emit2OutMap.end(); it++){
		if (!it->second->emit2OutItemsList.empty()){
			int ret = pthread_mutex_lock(&utils->emit2OutMap[it->first]->mapShuffleMutex);
			if(ret){
				//couldn't lock
				throw FrameworkException(LOCK_FAILURE);
			}

			items.merge(it->second->emit2OutItemsList);

			ret = pthread_mutex_unlock(&utils->emit2OutMap[it->first]->mapShuffleMutex);
			if(ret){
				//couldn't unlock
				throw FrameworkException(UNLOCK_FAILURE);
			}
		}
	}

	for (MAP_OUT_ITEMS_LIST::iterator item = items.begin(); item != items.end();
		 item++){
		k2Base* key = findKey(item->first);
		utils->shuffleOutMap[key].push_back(item->second);
	}
}

/*
 * the function that Shuffle thread executes.
 */
void* shuffleFunction(void* arg){
	int ret = pthread_mutex_lock(&utils->shuffleCondMutex);
	if(ret){
		throw FrameworkException(LOCK_FAILURE);
	}

	ret = pthread_mutex_lock(&utils->counterMutex);
	if(ret){
		throw FrameworkException(LOCK_FAILURE);
	}

	int counter = utils->mapCounter;

	ret = pthread_mutex_unlock(&utils->counterMutex);
	if(ret){
		throw FrameworkException(UNLOCK_FAILURE);
	}

	while (counter > 0){
		ret = gettimeofday(&utils->now, nullptr);
		if (ret){
			throw FrameworkException(GETTIME_FAILURE);
		}
		long nano = utils->now.tv_usec * MICRO_TO_NANO + NANOS_TO_WAIT;
		long sec = (nano) / SECONDS_TO_NANO;
		utils->timeToWait.tv_sec = utils->now.tv_sec + SECS_TO_WAIT + sec;
		utils->timeToWait.tv_nsec = nano % SECONDS_TO_NANO;

		ret = pthread_cond_timedwait(&(utils->shuffleCond),
									 &(utils->shuffleCondMutex),
									 &(utils->timeToWait));

		if (ret != EXIT_SUCCESS && ret != ETIMEDOUT){
			throw FrameworkException(COND_WAIT_FAILURE);
		}
		shuffleLoop();

		ret = pthread_mutex_lock(&utils->counterMutex);
		if(ret){
			throw FrameworkException(LOCK_FAILURE);
		}

		counter = utils->mapCounter;

		ret = pthread_mutex_unlock(&utils->counterMutex);
		if(ret){
			throw FrameworkException(UNLOCK_FAILURE);
		}
	}
	shuffleLoop();

	ret = pthread_mutex_unlock(&utils->shuffleCondMutex);
	if(ret){
		throw FrameworkException(UNLOCK_FAILURE);
	}
	writeThreadTermination(SHUFFLE_TYPE);
	pthread_exit(NULL);
}

/**
 * handles the entire map procedure: creates multiple execMap threads, each one of them executes
 * the map process.
 */
int execMap(pthread_t execMapThreads[], int multiThreadLevel){

	for(int i = 0; i < multiThreadLevel; i++){
		int create = pthread_create(&execMapThreads[i], NULL, execMapFunction, NULL);
		if (create)
		{
			//couldn't create the new thread
			throw FrameworkException(CREATE_FAILURE);
		}

		int ret = pthread_mutex_lock(&utils->counterMutex);
		if(ret){
			//couldn't lock
			throw FrameworkException(LOCK_FAILURE);
		}

		utils->mapCounter++;

		ret = pthread_mutex_unlock(&utils->counterMutex);
		if(ret){
			//couldn't unlock
			throw FrameworkException(UNLOCK_FAILURE);
		}

		writeThreadCreation(MAP_TYPE);
	}

	return EXIT_SUCCESS;
}


/**
 * handles the entire reduce procedure: creates multiple execReduce threads, each one of them
 * executes the reduce process.
 */
int execReduce(int multiThreadLevel) {
	utils->index = BEGIN_INDEX;

	utils->shuffleOutVector = SHUFFLE_OUT_VECTOR(utils->shuffleOutMap.begin(),
												 utils->shuffleOutMap.end());

	pthread_t execReduceThreads[multiThreadLevel];
	for(int i = 0; i < multiThreadLevel; i++){
		int create = pthread_create(&execReduceThreads[i], NULL, execReduceFunction, NULL);
		if (create)
		{
			//couldn't create the new thread
			throw FrameworkException(CREATE_FAILURE);
		}

		writeThreadCreation(REDUCE_TYPE);
	}

	for(int i = 0; i < multiThreadLevel; i++){
		int ret = pthread_join(execReduceThreads[i], NULL);
		if (ret){
			throw FrameworkException(PTHREAD_JOIN_FAILURE);
		}
	}

	return EXIT_SUCCESS;
}


/*
 * comparator for sorting the final list
 */
bool comp(const OUT_ITEM& item1, const OUT_ITEM& item2){
	return (*item1.first < *item2.first);
}


/*
 * a helper function for calculating time differences in nanoseconds, recieves
 * start time and end time and returns the difference.
 */
static long calculate_time_diff(const struct timeval *start,
								const struct timeval *end)
{
	long diff = (end->tv_sec * SECONDS_TO_NANO + end->tv_usec * MICRO_TO_NANO)
				- (start->tv_sec * SECONDS_TO_NANO +
				   start->tv_usec * MICRO_TO_NANO);
	return diff;
}



/**
 * the main library function: runs the entire flow of the framework
 */
OUT_ITEMS_LIST runMapReduceFramework(MapReduceBase& mapReduce,
									 IN_ITEMS_LIST& itemsList,
									 int multiThreadLevel){
	OUT_ITEMS_LIST finalList;
	try{
		openLogFile();
		writeBeginning(multiThreadLevel);
		utils = new FrameworkUtils(mapReduce, itemsList);
		if (gettimeofday(&utils->start, NULL) == EXIT_FAILURE){
			throw FrameworkException(GETTIME_FAILURE);
		}
		pthread_t execMapThreads[multiThreadLevel];
		execMap(execMapThreads, multiThreadLevel);

		pthread_t shuffleThread;
		int create = pthread_create(&shuffleThread, NULL, shuffleFunction, NULL);
		if (create)
		{
			//couldn't create the new thread
			throw FrameworkException(CREATE_FAILURE);
		}
		writeThreadCreation(SHUFFLE_TYPE);

		//main thread will wait for execMaps to terminate
		for(int i = 0; i < multiThreadLevel; i++){
			int ret = pthread_join(execMapThreads[i], NULL);
			if (ret){
				throw FrameworkException(PTHREAD_JOIN_FAILURE);
			}
		}

		//main thread will wait for shuffle to terminate
		int ret = pthread_join(shuffleThread, NULL);
		if (ret){
			throw FrameworkException(PTHREAD_JOIN_FAILURE);
		}

		//calculating map-shuffle time
		if (gettimeofday(&utils->endMapShuffle, NULL) == EXIT_FAILURE){
			throw FrameworkException(GETTIME_FAILURE);
		}

		long elapsedNano = calculate_time_diff(&utils->start,
											   &utils->endMapShuffle);

		execReduce(multiThreadLevel);

		//calculating map-shuffle time
		if (gettimeofday(&utils->end, NULL) == EXIT_FAILURE){
			throw FrameworkException(GETTIME_FAILURE);
		}

		//printing map-shuffle time
		writeElapsedTime(MAP_SHUFFLE_PROCESS, elapsedNano);

		elapsedNano = calculate_time_diff(&utils->endMapShuffle, &utils->end);

		//printing reduce time
		writeElapsedTime(REDUCE_PROCESS, elapsedNano);

		for(EMIT3_OUT_MAP::iterator iterator = utils->emit3OutMap.begin();
			iterator != utils->emit3OutMap.end(); iterator++){
			utils->outItemsList.merge(iterator->second);
		}
		utils->outItemsList.sort(comp);

		writeEndning();
		finalList = utils->outItemsList;

		terminateAll();
		return finalList;
	}

	catch (std::bad_alloc e){
		terminateAll();
		std::cerr<<NEW_FAILURE;
		exit(EXIT_FAILURE);
	}
	catch (FrameworkException e){
		terminateAll();
		std::cerr<<e.getMsg();
		exit(EXIT_FAILURE);
	}
	return finalList;
}