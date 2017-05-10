#include "MapReduceFramework.h"
#include <pthread.h>
#include <map>
#include "semaphore.h"

#define CHUNK_SIZE 10

typedef std::vector<std::pair<k2Base*, v2Base*>> MAP_VEC;

typedef std::vector<std::pair<pthread_t,MAP_VEC>> PTC;

typedef std::pair<k2Base*, std::vector<v2Base*>> SHUFFLED_ITEM;

typedef std::vector<SHUFFLED_ITEM> SHUFFLED_VEC;

typedef std::vector<std::pair<k3Base*, v3Base*>> REDUCE_VEC;

typedef std::vector<std::pair<pthread_t,REDUCE_VEC>> REDUCED_CONTAINERS;

typedef std::pair<k3Base*, v3Base*> OUT_ITEM;

typedef std::vector<OUT_ITEM> OUT_ITEMS_VEC;


OUT_ITEMS_VEC outputVector;

PTC execMapVector;

/** the shuffle thread*/
pthread_t shuffleThread;

REDUCED_CONTAINERS execReduceVector;

pthread_mutex_t *outputVectorMutex;

/** the input vector that was given in the runMapReduceFramework*/
MapReduceBase* mapReduce;

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

}mapResources;

struct ShuffleResources{

	/**
	 * a semaphore which control the shuffle progressing by up/down the semaphore
	 * any time a pair is been added/deleted form the ExecMap containers
	 */
	sem_t *shuffleSemaphore;

	/** a vector which contain pointers to the mutex of execMap containers*/
	std::vector<pthread_mutex_t*> mutexVector;

	/** the output vector of the shuffle process*/
	std::map<k2Base*,std::vector<v2Base*>> shuffledMap;

	/** the number of pair that need to be shuffled*/
	unsigned long numOfPairs;

	/** a vector of indexes that specify where the shuffle is in the passing through the container*/
	std::vector<unsigned int> mapContainerIndex;

}shuffleResources;

/**
 * a struct of resources for the ExecMap objects
 */
struct ReduceResources{

	/** a mutex on the inputVectorIndexMutex*/
	pthread_mutex_t shuffledVectorIndexMutex;

	/** the input vector that was given in the runMapReduceFramework*/
	SHUFFLED_VEC shuffledVector;

	/** the index of current location in the input vector*/
	int shuffledVectorIndex=0;

	/** an object of mapReduce which contain the map function*/
	MapReduceBase* mapReduce;

}reduceResources;

void* mapAll(void*);

void* shuffleAll(void*);

void* reduceAll(void*);

void mappingThreadsInit(int numThread){
    //spawn the new threads and initiate the vector pthreadToContainer
    for(int i=0;i<numThread;i++){
        // create vector of mapping threads
        pthread_t newExecMapThread;
        MAP_VEC newMappingVector;
        pthread_create(&newExecMapThread, NULL, mapAll, NULL);
        execMapVector.push_back(std::make_pair(newExecMapThread, newMappingVector));
        pthread_mutex_t mapContainerMutex;
        pthread_mutex_init(&mapContainerMutex,NULL); //todo check if there are errors of the functions
        shuffleResources.mutexVector.push_back(&mapContainerMutex);

    }
}

void shuffleThreadInit(int numOfThreads, unsigned long numOfPairs){
	for (int i = 0 ; i < numOfThreads;i++){
		shuffleResources.mapContainerIndex.push_back(0);
	}
	shuffleResources.numOfPairs = numOfPairs;
	int error = pthread_create(&shuffleThread, NULL, shuffleAll, NULL); //todo error?
}

void reducingThreadsInit(int numThread){
    //spawn the new threads and initiate the vector execReduceVector
    for(int i=0;i<numThread;i++){
        // create vector of mapping threads
        pthread_t newExecReduceThread;
        REDUCE_VEC newReducingVector;
        pthread_create(&newExecReduceThread, NULL, reduceAll, NULL);
        execReduceVector.push_back(std::make_pair(newExecReduceThread, newReducingVector));

    }
}

/**
 * initiate the threads of the mapping and shuffling, and also initiate the
 * pthreadToContainer
 * @param numThread the number of threads to be create while mapping
 * @param mapReduce an object that contain the map function
 */
void init(int numThread,MapReduceBase& mapReduceBase,IN_ITEMS_VEC& itemsVec){
	//initiate the semaphore
	int error = sem_init(shuffleResources.shuffleSemaphore,0,1); //todo error

	//update  map resources
	pthread_mutex_init(&mapResources.inputVectorIndexMutex,NULL);
	pthread_mutex_init(&mapResources.pthreadToContainerMutex,NULL);
    mapResources.inputVector = itemsVec;
    mapReduce = &mapReduceBase;

	pthread_mutex_init(outputVectorMutex,NULL);

	//lock the pthreadToContainer
	pthread_mutex_lock(&mapResources.pthreadToContainerMutex);

    mappingThreadsInit(numThread);
	//
	shuffleThreadInit(numThread, itemsVec.size());

	//unlock the pthreadToContainer
	pthread_mutex_unlock(&mapResources.pthreadToContainerMutex);

    reducingThreadsInit(numThread);


}

OUT_ITEMS_VEC RunMapReduceFramework(MapReduceBase& mapReduce, IN_ITEMS_VEC& itemsVec,
                                    int multiThreadLevel, bool autoDeleteV2K2){

	init(multiThreadLevel,mapReduce,itemsVec);
}


void Emit2 (k2Base* key, v2Base* value)
{
	pthread_t currentThreadId = pthread_self();
	for (unsigned int i = 0; i < execMapVector.size(); ++i)
	{
		if (pthread_equal(execMapVector.at(i).first,currentThreadId))
		{
			pthread_mutex_lock(shuffleResources.mutexVector.at(i));
			execMapVector.at(i).second.push_back(std::make_pair(key, value));
			pthread_mutex_unlock(shuffleResources.mutexVector.at(i));
			break;
		}
	}
	sem_post(shuffleResources.shuffleSemaphore);
}

void Emit3 (k3Base* key, v3Base* value){
	pthread_t currentThreadId = pthread_self();
	for (unsigned int i = 0; i < execReduceVector.size(); ++i)
	{
		if (pthread_equal(execReduceVector.at(i).first,currentThreadId))
		{
			auto pair = std::make_pair(key,value);

			pthread_mutex_lock(outputVectorMutex);
			outputVector.push_back(pair);
			pthread_mutex_unlock(outputVectorMutex);
			break;
		}
	}
}

void mapCurrentChunk(unsigned int chunkStartingIndex) {
    IN_ITEM currentItem;
    // take the minimum so we don't get out of bounds from input vector
    unsigned int numberOfIterations = std::min(chunkStartingIndex + CHUNK_SIZE,
                                  (unsigned int)mapResources.inputVector.size());
    for (unsigned int i = chunkStartingIndex; i < numberOfIterations; ++i)
    {
        // map the current item from input vector
        currentItem = mapResources.inputVector.at(i);
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
    pthread_mutex_lock(&mapResources.pthreadToContainerMutex);
    pthread_mutex_unlock(&mapResources.pthreadToContainerMutex);

    unsigned int chunkStartingIndex = 0;
    // loop until there are no more pairs to take from input vector
    while (chunkStartingIndex < mapResources.inputVector.size()){
        // lock inputVectorIndex to get the starting index for next chunk to map
        pthread_mutex_lock(&mapResources.inputVectorIndexMutex);
        chunkStartingIndex = mapResources.inputVectorIndex;
        mapResources.inputVectorIndex += CHUNK_SIZE;
        pthread_mutex_unlock(&mapResources.inputVectorIndexMutex);

        mapCurrentChunk(chunkStartingIndex);
    }

}


/**
 * search the key, if it is in the map append the value to the vector,
 * else add a new pair to the map (key,value)
 * @param key the key on which to search
 * @param value the data that need to append
 */
void searchingAndInsertingData(k2Base* key, v2Base* value,unsigned int &pairsShuffled){
	//search the key
	auto search = shuffleResources.shuffledMap.find(key);
	//if the key has been found ,append only the value
	if(search!=shuffleResources.shuffledMap.end()) {
		search->second.push_back(value);
	}
	else{
		// add a new pair
		auto *valueVector= new std::vector<v2Base*>{value} ;
		shuffleResources.shuffledMap.insert(std::make_pair(key, *valueVector));
		//todo do i need to delete the valueVector?

	}
	//increasing the count of the pairs that had been shuffled
	pairsShuffled++;
	sem_wait(shuffleResources.shuffleSemaphore);
}

/**
 * shuffle data from a container
 * @param i the index of the execMap container
 * @param pairsShuffled the number of the pairs that had been shuffled
 */
void shufflingDataFromAContainer(unsigned int i, unsigned int &pairsShuffled){

	//going through the execMapVector
	while(shuffleResources.mapContainerIndex.at(i) > execMapVector.at(i).second.size()) {
		unsigned int index= shuffleResources.mapContainerIndex.at(i);

		//lock the mutex of the container
		pthread_mutex_lock(shuffleResources.mutexVector.at(i));

		//get the value from the container
		k2Base *key = execMapVector.at(i).second.at(index).first;
		v2Base *value = execMapVector.at(i).second.at(index).second;

		//unlock the mutex of the container
		pthread_mutex_unlock(shuffleResources.mutexVector.at(i));

		//increase the index value of the specific map container
		shuffleResources.mapContainerIndex.at(i)+=1;

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
	sem_wait(shuffleResources.shuffleSemaphore);

	while(pairsShuffled != shuffleResources.numOfPairs){
		for(unsigned int i=0;i< execMapVector.size();i++){

			// shuffling Data From A specific Container
			shufflingDataFromAContainer(i,pairsShuffled);
		}
	}
}

void reduceCurrentChunck(unsigned int chunkStartingIndex){
	SHUFFLED_ITEM currentItem;

	// take the minimum so we don't get out of bounds from shuffled vector
	unsigned int numberOfIterations = std::min(chunkStartingIndex + CHUNK_SIZE,
								  (unsigned int)reduceResources.shuffledVector.size());
	for (unsigned int i = chunkStartingIndex; i < numberOfIterations; ++i)
	{
		// map the current item from input vector
		currentItem = reduceResources.shuffledVector.at(i);
		reduceResources.mapReduce->Reduce(currentItem.first, currentItem.second);
	}
}

void* reduceAll(void *)
{
	unsigned int chunkStartingIndex = 0;

    int error = pthread_join(shuffleThread, NULL);
	// loop until there are no more pairs to take from input vector
	while (chunkStartingIndex < reduceResources.shuffledVector.size()){
		// lock shuffledVectorIndex to get the starting index for next chunk to map
		pthread_mutex_lock(&reduceResources.shuffledVectorIndexMutex);
		chunkStartingIndex = reduceResources.shuffledVectorIndex;
		reduceResources.shuffledVectorIndex += CHUNK_SIZE;
		pthread_mutex_unlock(&reduceResources.shuffledVectorIndexMutex);


	}
}