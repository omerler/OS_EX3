#include <mutex>
#include <map>
#include "MapReduceFramework.h"
#include "pthread.h"
#include <semaphore.h>


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

#define CHUNK 10
//  int itemsVecNext - the next index in the vector to take chunk from.
int itemsVecNext;

//IN_ITEMS_VEC& itemsVec- contains all the couple of <k1,v1>
IN_ITEMS_VEC itemsVec;

typedef std::pair<k2Base*, v2Base*> pair2; // todo think if need to insert default values,
typedef pair2 * k2v2Container;
typedef  k2v2Container* k2v2ContainerP;

struct mutexAndContainerP2{
    std::mutex mtx;           // mutex for critical section- 0 is open and 1 is locked
    k2v2ContainerP k2v2Containerp;
    int counter = 0;
};

//  pthreadToContainer - Map that maps pthread_t to its container
std::map<pthread_t ,mutexAndContainerP2> pthreadToContainerMap;

//pThread execMap_i - recives "chunk" of couples of <key,value>, and Map, and Reduce X multiThreadLevel
pthread_t * pthreadArray;

pthread_t shuffleThread;


sem_t *semaphore;

/*added!*/
typedef std::vector<v2Base *> value2_vec;
std::map<k2Base* ,value2_vec> map2afterShuffle;



/*
// initiate MultylevelThreads memed execMap;
 *
 */

/*
 * will perform as the main thread.
 * lock pthreadToContainer_mutex //todo- why it should be protected by mutex
 * create ExecMap threads -> (block immediately after initialization of each thread
 * create pthreadToContainer
 * unblock pthreadToContainer_mutex
 *
 * Create pthread by executing chunk of couples from the vector
 * create the container protected by mutex of <k2,v2>
 *give each thread ten couples of <key,value>
 * check if mutex integer = vector.size().
 *  if done -> call shuffle ->
 */

OUT_ITEMS_VEC RunMapReduceFramework(MapReduceBase& mapReduce, IN_ITEMS_VEC& itemsVec,
                                    int multiThreadLevel, bool autoDeleteV2K2){
    sem_init(semaphore, 0, 1);

}


/*
 * The thread's "init"
 * lock and then unlock the pthreadToContainer_mutex
 */
void ExecMap(){}




/*added!*/
//typedef std::vector<v2Base *> value2_vec;
//std::map<k2Base* ,value2_vec> map2afterShuffle;


/*
 * execute the actual shuffle
 */
void shuffleOperation(k2Base* k2BaseInput, v2Base v2BaseInput){

    for(auto const k2Base : map2afterShuffle) {
        if (k2Base == k2BaseInput){
            k2Base.second.insert(value2_vec.begin(), &v2BaseInput);
            return;
        }
    }

    std::vector<v2Base *> vectorValues2;
    value2_vec.insert(value2_vec.begin(), &v2BaseInput);
    map2afterShuffle.insert(std::pair<k2Base*,std::vector<v2Base *>>(k2BaseInput, vectorValues2));

    }

/*
 * ShuffleFunction() will be awaked by the semaphore (when it is greater then 0)
 * it iterate all the threads' containers and check if they are empty:
 *  if they are: continue to next container in the map.
 *  else: unlock the current container -> pull all the data -> lock -> shuffle -> decrease semaphore.
 *  if none of the threads containers have data (it means that the main thread increase the semaphore as a sign that
 *  map mission is done for all of the vector) ->return
 *  important- once the semaphore reaches to 0 or below, the thread that called it is blocked (it can be only shuffle)
 *  The thread will be unblocked, when the semaphore is 1
 *
 */
void ShuffleFunction(){

    std::map<pthread_t ,mutexAndContainerP2> pthreadToContainerMap;

//    std::map<std::string, std::map<std::string, std::string>> mymap;

    for(auto const &key : pthreadToContainerMap) {
        // ent1.first is the first key
        if (key.second.k2v2Containerp[0] == NULL){ //todo- is there more elegant way to check if array is empty?
            continue;
        } else{
            key.second.mtx.lock(); //lock mutex
            k2v2ContainerP k2v2Containerp = key.second.k2v2Containerp; //pull all the data in the container
            key.second.mtx.unlock(); //unlock mutex


            shuffleOperation(&key.second.k2v2Containerp, k2v2Containerp);

            key.second.mtx.lock(); //lock mutex
            sem_wait(semaphore); // decrease the semaphore
            key.second.mtx.unlock(); //unlock mutex

        }

    }

    return;

}



/*
 * The function is called for each map action on a a single couple.
 * it will use p_thredSelf() to recognize with pthreadContainer is belong (in pthreadToContainer) ->
 * unlock to the container ->
 * push(<k2,v2> into the container of the p.thread ->
 * increase the semaphore ->
 * lock current container
 */
void Emit2 (k2Base*, v2Base*);







