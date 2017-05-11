#include "MapReduceFramework.h"
#include <pthread.h>
#include <map>
#include <iostream>
#include <stdlib.h>
#include <fstream>
#include "semaphore.h"

#define CHUNK_SIZE 10

typedef std::vector<std::pair<k2Base*, v2Base*>> MAP_VEC;

typedef std::vector<std::pair<pthread_t,MAP_VEC>> PTC;

typedef std::vector<std::pair<k3Base*, v3Base*>> REDUCE_VEC;

typedef std::vector<std::pair<pthread_t,REDUCE_VEC>> REDUCED_CONTAINERS;

typedef std::pair<k3Base*, v3Base*> OUT_ITEM;

typedef std::vector<OUT_ITEM> OUT_ITEMS_VEC;

/** vector of pairs of thread and his matching after-mapping vector*/
PTC execMapVector;

/** the shuffle thread*/
pthread_t shuffleThread;

/** vector of pairs of thread and his matching after-reducing vector*/
REDUCED_CONTAINERS execReduceVector;

/** the input vector that was given in the runMapReduceFramework*/
MapReduceBase* mapReduce;

/**
 * a semaphore which control the shuffle progressing by up/down the semaphore
 * any time a pair is been added/deleted form the ExecMap containers
 */
sem_t *shuffleSemaphore;

/** a vector which contain pointers to the mutex of execMap containers*/
std::vector<pthread_mutex_t> mutexVector;

/** the output vector of the shuffle process*/
std::map<k2Base*,std::vector<v2Base*>> shuffledMap;

/** final output vector*/
OUT_ITEMS_VEC outputVector;

/** mutex for the final output vector */
pthread_mutex_t outputVectorMutex;

std::ofstream myLogFile;

pthread_mutex_t logMutex;


/**
 * a struct of resources for the ExecMap objects
 */
struct MapResources{

    /** a mutex on the pthreadToCotnainer*/
    pthread_mutex_t pthreadToContainerMutex;

    /** a mutex on the inputVectorIndexMutex*/
    pthread_mutex_t inputVectorIndexMutex;

    /** the input vector that was given in the runMapReduceFramework*/
    IN_ITEMS_VEC inputVector;

    /** the index of current location in the input vector*/
    unsigned int inputVectorIndex=0;

}MapResources;

struct ShuffleResources{

	/** the number of pair that need to be shuffled*/
	unsigned long numOfPairs;

	/** a vector of indexes that specify where the shuffle is in the passing through the container*/
	std::vector<unsigned int> mapContainerIndex;

}ShuffleResources;

/**
 * a struct of resources for the ExecMap objects
 */
struct ReduceResources{

	/** a mutex on the inputVectorIndexMutex*/
	pthread_mutex_t shuffledVectorIndexMutex;

	/** the index of current location in the input vector*/
	unsigned int shuffledVectorIndex=0;

	/** an object of mapReduce which contain the map function*/
	MapReduceBase* mapReduce;

}ReduceResources;

void* mapAll(void*);

void* shuffleAll(void*);

void* reduceAll(void*);

/**
 * create a non specific thread
 * @param thread a reference to a thread to be created
 * @param function the function the thread is supposed to execute
 */
void createThread(pthread_t &thread, void * function(void*)){
	int error= pthread_create(&thread, NULL, function, NULL);
	if(error!=0){
		std::cerr<<"mapReduceFramework failure: pthread_create failed"<<std::endl;
		exit(1);
	}
}

/**
 * create a non specific mutex
 * @param mutex a mutex
 */
void createMutex(pthread_mutex_t &mutex){
	if(pthread_mutex_init(&mutex,NULL)!=0){
		std::cerr<<"mapReduceFramework failure: pthread_mutex_init failed"<<std::endl;
		exit(1);
	}
}

/**
 * lock the mutex
 * @param mutex a mutex
 */
void lockMutex(pthread_mutex_t &mutex){
	if(pthread_mutex_lock(&mutex)!=0){
		std::cerr<<"mapReduceFramework failure: pthread_mutex_lock failed"<<std::endl;
		exit(1);
	}
}

/**
 * unlock the mutex
 * @param mutex a mutex
 */
void unlockMutex(pthread_mutex_t &mutex){
	if(pthread_mutex_unlock(&mutex)!=0){
		std::cerr<<"mapReduceFramework failure: pthread_mutex_unlock failed"<<std::endl;
		exit(1);
	}
}

/**
 * initiating the map threads and the vector which contains the
 * map container the a mutex for each map thread
 * @param numThread
 */
void mappingThreadsInit(int numThread){
    //spawn the new threads and initiate the vector pthreadToContainer
    for(int i=0;i<numThread;i++){
        // create vector of mapping threads
        pthread_t newExecMapThread;
        MAP_VEC newMappingVector;
        createThread(newExecMapThread,mapAll);

        execMapVector.push_back(std::make_pair(newExecMapThread, newMappingVector));
        pthread_mutex_t mapContainerMutex;
	    createMutex(mapContainerMutex);

        mutexVector.push_back(mapContainerMutex);

    }
}

/**
 * initiating the shuffle thread and the vector that contains the indexes
 * that point where the shuffle at the passage of the map conainter
 * @param numOfThreads the number of threads
 * @param numOfPairs the nufm of pairs to be shuffled
 */
void shuffleThreadInit(int numOfThreads, unsigned long numOfPairs) {
	for (int i = 0; i < numOfThreads; i++) {
		ShuffleResources.mapContainerIndex.push_back(0);
	}
	ShuffleResources.numOfPairs = numOfPairs;
	createThread(shuffleThread, shuffleAll);
}

/**
 * initiaing the reduce threads and the vector that contains the thread and
 * his contained
 * @param numThread the number of reduce threads to be created
 */
void reducingThreadsInit(int numThread){
    //spawn the new threads and initiate the vector execReduceVector
    for(int i=0;i<numThread;i++){
        // create vector of mapping threads
        pthread_t newExecReduceThread;
        REDUCE_VEC newReducingVector;
        createThread(newExecReduceThread,reduceAll);
        execReduceVector.push_back(std::make_pair(newExecReduceThread, newReducingVector));

    }
}

/**
 * creating the log fle
 */
void createLogFile(int numThread){

	myLogFile.exceptions(std::ofstream::failbit|std::ofstream::badbit);
	try{
		myLogFile.open(".temp");
		myLogFile<<"RunMapReduceFramework started with " <<numThread<<" threads\n";
	}
	catch(std::ofstream::failure e){
		std::cerr<<"mapReduceFramework failure: open failed"<<std::endl;
		exit(1);
	}
}

/**
 * initiate the threads of the mapping and shuffling, and also initiate the
 * pthreadToContainer
 * @param numThread the number of threads to be create while mapping
 * @param mapReduce an object that contain the map function
 */
void init(int numThread,MapReduceBase& mapReduceBase,IN_ITEMS_VEC& itemsVec){
	//creating the log file
	createLogFile(numThread);

	//initiate the semaphore
	if(sem_init(shuffleSemaphore,0,1)==-1){
		std::cerr<<"mapReduceFramework failure: sem_init failed"<<std::endl;
		exit(1);
	}

	//update  map resources
	createMutex(MapResources.inputVectorIndexMutex);
	createMutex(MapResources.pthreadToContainerMutex);
    MapResources.inputVector = itemsVec;
    mapReduce = &mapReduceBase;

	createMutex(outputVectorMutex);
	createMutex(logMutex);

	//lock the pthreadToContainer
	lockMutex(MapResources.pthreadToContainerMutex);

    mappingThreadsInit(numThread);
	//
	shuffleThreadInit(numThread, itemsVec.size());

	//unlock the pthreadToContainer
    unlockMutex(MapResources.pthreadToContainerMutex);
}

void clearK2V2Vector(int numThreads){
    for (unsigned int threadIndex = 0; threadIndex < numThreads; ++threadIndex){
        MAP_VEC currentVector = execMapVector.at(threadIndex).second;
        for (auto itemsIter = currentVector.begin(); itemsIter != currentVector.end() ; ++itemsIter)
        {
            delete (*itemsIter).first;
            delete (*itemsIter).second;
        }
    }
}

/**
 * release all the resourses that allocated during the running of RunMapReduceFramework.
 * @param autoDeleteV2K2 if true, release also V2K2 pairs
 */
void finalizer(bool autoDeleteV2K2, int numThreads){
    if (autoDeleteV2K2){
        clearK2V2Vector(numThreads);
    }
    // clear mutex and semaphore
}

OUT_ITEMS_VEC RunMapReduceFramework(MapReduceBase& mapReduce, IN_ITEMS_VEC& itemsVec,
                                    int multiThreadLevel, bool autoDeleteV2K2){

	init(multiThreadLevel,mapReduce,itemsVec);

    if (pthread_join(shuffleThread, NULL) != 0){
        std::cerr << "MapReduceFramework Failure: pthread_join failed." << std::endl;
        exit(1);
    }
    reducingThreadsInit(multiThreadLevel);
    finalizer(autoDeleteV2K2, multiThreadLevel);
    return outputVector;
}


void Emit2 (k2Base* key, v2Base* value)
{
	pthread_t currentThreadId = pthread_self();
	for (unsigned int i = 0; i < execMapVector.size(); ++i)
	{
		if (pthread_equal(execMapVector.at(i).first,currentThreadId))
		{
			lockMutex(mutexVector.at(i));
			execMapVector.at(i).second.push_back(std::make_pair(key, value));
			unlockMutex(mutexVector.at(i));
			break;
		}
	}
	if(sem_post(shuffleSemaphore)!=0){
		std::cerr<<"mapReduceFramework failure: sem_post failed"<<std::endl;
		exit(1);
	}
}

void Emit3 (k3Base* key, v3Base* value){
	pthread_t currentThreadId = pthread_self();
	for (unsigned int i = 0; i < execReduceVector.size(); ++i)
	{
		if (pthread_equal(execReduceVector.at(i).first,currentThreadId))
		{
			auto pair = std::make_pair(key,value);

			lockMutex(outputVectorMutex);
			outputVector.push_back(pair);
			unlockMutex(outputVectorMutex);
			break;
		}
	}
}

void mapCurrentChunk(unsigned int chunkStartingIndex) {
    IN_ITEM currentItem;
    // take the minimum so we don't get out of bounds from input vector
    unsigned int numberOfIterations = std::min(chunkStartingIndex + CHUNK_SIZE,
                                  (unsigned int)MapResources.inputVector.size());
    for (unsigned int i = chunkStartingIndex; i < numberOfIterations; ++i)
    {
        // map the current item from input vector
        currentItem = MapResources.inputVector.at(i);
        mapReduce->Map(currentItem.first, currentItem.second);
    }
}


/**
 * mapping function that the thread actually runs, gets chuncks of pairs from input vector and
 * mapping them according to the given map function
 * @return
 */
void* mapAll(void*)
{
    // lock and unlock the pTC mutex
	lockMutex(MapResources.pthreadToContainerMutex);
	unlockMutex(MapResources.pthreadToContainerMutex);

    unsigned int chunkStartingIndex = 0;
    // loop until there are no more pairs to take from input vector
    while (chunkStartingIndex < MapResources.inputVector.size()){
        // lock inputVectorIndex to get the starting index for next chunk to map
	    lockMutex(MapResources.inputVectorIndexMutex);
        chunkStartingIndex = MapResources.inputVectorIndex;
        MapResources.inputVectorIndex += CHUNK_SIZE;
        unlockMutex(MapResources.inputVectorIndexMutex);

        mapCurrentChunk(chunkStartingIndex);
    }

	return 0;
}


/**
 * search the key, if it is in the map append the value to the vector,
 * else add a new pair to the map (key,value)
 * @param key the key on which to search
 * @param value the data that need to append
 */
void searchingAndInsertingData(k2Base* key, v2Base* value,unsigned int &pairsShuffled){
	//search the key
	auto search = shuffledMap.find(key);
	//if the key has been found ,append only the value
	if(search != shuffledMap.end()) {
		search->second.push_back(value);
	}
	else{
		// add a new pair
		auto valueVector= std::vector<v2Base*>{value} ;
		shuffledMap.insert(std::make_pair(key, valueVector));

	}
	//increasing the count of the pairs that had been shuffled
	pairsShuffled++;
	if(sem_wait(shuffleSemaphore)!=0){
		std::cerr<<"mapReduceFramework failure: sem_wait failed"<<std::endl;
		exit(1);
	}
}

/**
 * shuffle data from a container
 * @param i the index of the execMap container
 * @param pairsShuffled the number of the pairs that had been shuffled
 */
void shufflingDataFromAContainer(unsigned int i, unsigned int &pairsShuffled){

	//going through the execMapVector
	while(ShuffleResources.mapContainerIndex.at(i) > execMapVector.at(i).second.size()) {
		unsigned int index= ShuffleResources.mapContainerIndex.at(i);

		//lock the mutex of the container
		lockMutex(mutexVector.at(i));

		//get the value from the container
		k2Base *key = execMapVector.at(i).second.at(index).first;
		v2Base *value = execMapVector.at(i).second.at(index).second;

		//unlock the mutex of the container
		unlockMutex(mutexVector.at(i));

		//increase the index value of the specific map container
		ShuffleResources.mapContainerIndex.at(i)+=1;

		// insert the pair into the shuffle container
		searchingAndInsertingData(key,value,pairsShuffled);

	}
}

/**
 * the function performs the shuffle process
 * this process take every pair from the map containers and insert to
 * the shuffled container which each cell in it contain a key
 * and vector of value that correspond
 * @return do not return anything
 */
void* shuffleAll(void*){

	unsigned int pairsShuffled = 0;
	//wait until one of the containers is not empty
	if(sem_wait(shuffleSemaphore)!=0){
		std::cerr<<"mapReduceFramework failure: sem_wait failed"<<std::endl;
		exit(1);
	}

	while(pairsShuffled != ShuffleResources.numOfPairs){
		for(unsigned int i=0;i< execMapVector.size();i++){

			// shuffling Data From A specific Container
			shufflingDataFromAContainer(i,pairsShuffled);
		}
	}

	return 0;
}

void reduceCurrentChunck(unsigned int chunkStartingIndex){
    auto iteratingIndex = shuffledMap.begin();
    std::advance(iteratingIndex, chunkStartingIndex);
	// take the minimum so we don't get out of bounds from shuffled vector
	unsigned int numberOfIterations = std::min(chunkStartingIndex + CHUNK_SIZE,
								  (unsigned int)shuffledMap.size());
	for (unsigned int i = 0; i < numberOfIterations; ++i)
	{
		ReduceResources.mapReduce->Reduce(iteratingIndex->first, iteratingIndex->second);
        ++iteratingIndex;
	}
}

void* reduceAll(void *)
{
	unsigned int chunkStartingIndex = 0;

	// loop until there are no more pairs to take from input vector
	while (chunkStartingIndex < shuffledMap.size()){
		// lock shuffledVectorIndex to get the starting index for next chunk to map
		lockMutex(ReduceResources.shuffledVectorIndexMutex);
		chunkStartingIndex = ReduceResources.shuffledVectorIndex;
		ReduceResources.shuffledVectorIndex += CHUNK_SIZE;
		unlockMutex(ReduceResources.shuffledVectorIndexMutex);

		reduceCurrentChunck(chunkStartingIndex);
	}

	return 0;
}