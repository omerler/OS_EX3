#include <map>
#include "MapReduceFramework.h"
#include "MapReduceClient.h"
#include <stdlib.h>
#include <libltdl/lt_system.h>
#include ReduceFrameworkException.h
#include pthread.h
#include <mutex>
#include <semaphore.h>

/*
 * constants
 */
#define CHUNK 10 //constant that represents the number of couples in a thread


// data structures:
// pThread shuffle - read values from the queue and insert them tham to the map:
//      if exist -> list[len]=1;
//      else new <key2, value2[]>

//shared data structures that need to be protected with mutex:
// pThread_mutex - prevents pThread from running
//  Queue [<key2,value2>]- execMap pushes to it the output of the map part, and the shuffle executing from it.
// Another map of pthread to mutex
// hold a list of mutex for each thread
// The shuffle finds non-empty container, checks if the mutex is blocked and if not- takes it.

/*
 * Data structures
 */

//  int numOfItemsInTheInputVector - the next index in the vector to take chunk from.
unsigned long numOfItemsInTheInputVector;
//IN_ITEMS_VEC& itemsVec- contains all the couple of <k1,v1>
IN_ITEMS_VEC inputCouplesVec;
// couples representing <k2,v2> (output of Map)
typedef std::pair<k2Base*, v2Base*> pair2;
// array of pair2, which is the output of Map
typedef pair2 * k2v2Container; // todo need to be resize to [CHUNK]
// pointer to k2v2container
typedef  k2v2Container* k2v2ContainerP; //todo talk with carmel
//struct representing container of a thread and a mutex for it
struct MutexAndContainerP2{
	std::mutex mtx;           // mutex for critical section
	k2v2ContainerP k2v2ContainerP1;
	int counter = 0;
};
//  pthreadToContainer - Map that maps pthread_t to its container and mutex
std::map<pthread_t ,MutexAndContainerP2> pthreadToContainerMap;
//pThread execMap_i - receive "chunk" of couples of <key,value>, and Map, and Reduce X
pthread_t * pthreadArray;
//Thread for shuffle
pthread_t shuffleThread;
//Semaphore that represents the number of threads that finished Map
sem_t * semaphore;
//vector that represents the container of the values, after shuffle
typedef std::vector<v2Base *> value2_vec;
//Map that represents the values after shuffle: keys: k2, values: v2 vector
std::map<k2Base* ,value2_vec> map2afterShuffle;



//####################### Omer Variables ##############################################
void * executeWholeMap();
void * ExecMap(void *arg);
void * deallocAll();
void * allocAll();
OUT_ITEMS_VEC sort(OUT_ITEMS_VEC unsortedVec);
MapReduceBase innerMapReduce;
OUT_ITEMS_VEC outItemsVec;
std::mutex pthreadToContainerMutex;
std::mutex inputVectorCurIndexMutex;
#include "ReduceFrameworkException.h"
#include <iostream>
#include <algorithm>
#include <vector>
#define PTHREAD_CREATE "pthread_create"
#define AT "at"
#define ALLOC "Alloc"
int numOfThreads;



/*
 *
 */
void * executeWholeMap() {
	for (int i = 0; i < numOfThreads; i++) {
		int result = pthread_create(&pthreadArray[i], NULL, &ExecMap, NULL); //include allocation
		// inside the function.
		if (result) {
			throw ReduceFrameworkException(PTHREAD_CREATE);
		} //else, creation succeeded
		MutexAndContainerP2 mutexAndContainerP2;
		mutexAndContainerP2.counter = 0;
		try{
			mutexAndContainerP2.k2v2ContainerP1; //todo validate that alloc is good
		} catch (std::bad_alloc& exc) {
			throw ReduceFrameworkException(ALLOC);
		}
		pthreadToContainerMap.insert(std::pair<pthread_t, MutexAndContainerP2>(pthreadArray[i],
																			  mutexAndContainerP2));
		pthread_join(pthreadArray[i], NULL);
	}
}

/*
 * The thread's "body"
 * recive CHUNK couples and call map on each pair
 * lock and then unlock the pthreadToContainerMutex
 */
void * ExecMap(void *arg) {
	pthreadToContainerMutex.try_lock(); //todo- understand why...
	pthreadToContainerMutex.unlock();
	while (true){
		if (!inputVectorCurIndexMutex.try_lock()){ //was already locked by other thread
			continue;
		} else {
			unsigned long curIndex = numOfItemsInTheInputVector;
			numOfItemsInTheInputVector+=CHUNK;
			inputVectorCurIndexMutex.unlock();
			if (curIndex > numOfItemsInTheInputVector){
				break;
			} else {
				IN_ITEMS_VEC in_items_vec;
				in_items_vec.reserve(CHUNK);
				unsigned long j;
				for (j = curIndex;(j < curIndex + CHUNK) && (j < numOfItemsInTheInputVector); j++) {
					try {
						in_items_vec.push_back(inputCouplesVec.at(j));
					} catch(std::out_of_range o){
							throw ReduceFrameworkException (AT);
					}
				}
				unsigned long i = 0;
				for (i; i < j; i++) {
					innerMapReduce.Map(in_items_vec.at(i).first, //key1
									   in_items_vec.at(i).second); //value1
				}
			}
		}
	}
}


/*added!*/
//typedef std::vector<v2Base *> value2_vec;
//std::map<k2Base* ,value2_vec> map2afterShuffle;


/*
 * execute the actual shuffle- Go over the map2afterShuffle, and check if there is a k2 which equals to the received k2.
 * if so- we will add the value to the values vector and return.
 * If not, we will create new key to the map of k2, and add the value to a new vector
 */
void shuffleOperation(k2Base* k2BaseInput, v2Base* v2BaseInput){

    for(auto const k2Base : map2afterShuffle) {
        if (k2Base == k2BaseInput){
            k2Base.second.insert(value2_vec.begin(), v2BaseInput);
            return;
        }
    }
    std::vector<v2Base *> vectorValues2;
    value2_vec.insert(value2_vec.begin(), v2BaseInput);
    map2afterShuffle.insert(std::pair<k2Base*,std::vector<v2Base *>>(k2BaseInput, vectorValues2));

}

/*
 * ShuffleFunction will be awaked by the semaphore when it is greater then 0.
 * The function will iterate all the threads' containers and check if they are empty.
 *  if they are: continue to next container in the map.
 *  else: unlock the current container -> pull all the data -> lock -> shuffle -> decrease semaphore.
 *  if none of the threads containers have data (it means that the main thread increase the semaphore as a sign that
 *  map mission is done for all of the vector) ->return
 *  todo delete: important- once the semaphore reaches to 0 or below, the thread that called it is blocked (it can be only shuffle) The thread will be unblocked, when the semaphore is 1
 */
void ShuffleFunction(){
    bool anyContainerWasFull = false;
    for(auto const &key : pthreadToContainerMap) { //iterate the map
        for (int i=0; i<key.second.counter;i++) {
            if (key.second.k2v2ContainerP1[i] == NULL) { //check if the container of the thread is empty
                continue;
            } else {
                key.second.mtx.lock(); //lock mutex
                k2v2ContainerP k2v2Containerp = key.second.k2v2ContainerP1; //pull all the data in the container
                key.second.mtx.unlock(); //unlock mutex

                shuffleOperation(k2v2Containerp[i]->first, k2v2Containerp[i]->second);
                anyContainerWasFull = true;

                key.second.mtx.lock(); //lock mutex
                sem_wait(semaphore); // decrease the semaphore
                key.second.mtx.unlock(); //unlock mutex
            }
        }
    }
    if (!anyContainerWasFull){ //in this case it means that the main function called the shuffle and map is done
        return;
    } //todo- is there need in it? because when the semaphore is 0, the shuffle thread will not called

    return;

}


/*
 * The function is called for each map action on a single couple.
 * it will use pthread_self() to recognize with pthreadContainer is belong (in pthreadToContainer) ->
 * if it's container is locked (it means that the shuffle was in the middle of pooling the data)
 * 		so the thread will finish it's quantum (in order to let the suffle finish and unlock the
 * 		container).
 * else: it lock the container ->
 *     push(<k2,v2> into the container of the p.thread ->
 *     increase the semaphore ->
 *     unlock current container
 */


void Emit2 (k2Base* k2, v2Base* v2) {
	pthread_t curThread = pthread_self();
	if (!pthreadToContainerMap[curThread].mtx.try_lock()){//was locked
		pthread_join(shuffleThread, NULL); //run EMIT2 only after shuffle runs. todo validate if it's says that
	} else {
		//was not locked
		int size = pthreadToContainerMap[curThread].counter;
		pair2 *pair = new pair2(k2, v2);
		pthreadToContainerMap[curThread].k2v2ContainerP1[size] = pair;
		sem_post(semaphore);
		pthreadToContainerMap[curThread].mtx.unlock();
	}
}

/*
 *
 */
struct less_than_key
{
	inline bool operator() (const k3Base& struct1, const k3Base& struct2)
	{
		return (struct1 < struct2);
	}
};

OUT_ITEMS_VEC sort(OUT_ITEMS_VEC unsortedVec){
	std::sort(unsortedVec.begin(), unsortedVec.end(), less_than_key());
	return unsortedVec;
}

/*
 * The method allocates memory
 */
void * allocAll(){
	try{
		pthreadArray = new pthread_t[numOfThreads];
		inputCouplesVec.reserve(numOfItemsInTheInputVector);
		//pthreadToContainerMap is allocated with each couple
	}
	catch(std::bad_alloc& exc)
	{
		throw ReduceFrameworkException(ALLOC);
	}
}

/*
 * The method deallocate all the memory
 */
void * deallocAll(){
	// The threads are deleted inside the "terminated" caused from the thread code end
	delete(pthreadArray);
	delete(inputCouplesVec);
	delete(pthreadToContainerMap);
	delete(semaphore);
	delete(pthreadToContainerMutex);
	delete(inputVectorCurIndexMutex);
}



/*
 * will perform as the main thread.
 * lock pthreadToContainerMutex //todo- why it should be protected by mutex
 * create ExecMap threads -> (block immediately after initialization of each thread
 * create pthreadToContainer
 * unblock pthreadToContainerMutex
 *
 * Create pthread by executing chunk of couples from the vector
 * create the container protected by mutex of <k2,v2>
 * give each thread ten couples of <key,value>
 * check if mutex integer = vector.size().
 *  if done -> call shuffle ->
 */
OUT_ITEMS_VEC RunMapReduceFramework(MapReduceBase& mapReduce, IN_ITEMS_VEC& itemsVec,
                                    int multiThreadLevel, bool autoDeleteV2K2){
    //todo init pthreaArray and alloc;
    numOfItemsInTheInputVector = itemsVec.size();
    numOfThreads = multiThreadLevel;
    allocAll();
    inputCouplesVec = itemsVec;
    innerMapReduce = mapReduce;
    executeWholeMap();
    //todo ExecReduce into "output"
    outItemsVec = sort(outItemsVec);
    deallocAll();
    return outItemsVec;
}
