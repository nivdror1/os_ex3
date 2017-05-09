

#include "Shuffle.h"
#include "ExecMap.h"




/**
 * c-tor
 * */
Shuffle::Shuffle(unsigned int pairsNum, int numOfThreads ): numOfPairs(pairsNum)
{
    for (int i = 0 ; i < numOfThreads;i++){
        this->mapContainerIndex.push_back(0);
    }
	int error = pthread_create(&_thread, NULL, shuffleAll, NULL); //todo error?
}

/**
 * search the key, if it is in the map append the value to the vector,
 * else add a new pair to the map (key,value)
 * @param key the key on which to search
 * @param value the data that need to append
 */
 void Shuffle::searchingAndInsertingData(k2Base* key, v2Base* value,unsigned int &pairsShuffled){
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
 void Shuffle::shufflingDataFromAContainer(unsigned int i, unsigned int &pairsShuffled){

    while(mapContainerIndex[i] > shuffleResources.execMapVector.at(i)->getVectorSize()) {
	    unsigned int index= mapContainerIndex[i];

	    pthread_mutex_lock(shuffleResources.mutexVector[i]);

	    k2Base *key = shuffleResources.execMapVector.at(i)->getPastMapVector()->at(index).first;
        v2Base *value = shuffleResources.execMapVector.at(i)->getPastMapVector()->at(index).second;

	    pthread_mutex_unlock(shuffleResources.mutexVector[i]);

	    this->mapContainerIndex[i]+=1;

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
void* Shuffle::shuffleAll(void*){

	unsigned int pairsShuffled = 0;
	//wait until one of the containers is not empty
	sem_wait(shuffleResources.shuffleSemaphore);

	while(pairsShuffled != numOfPairs){
		for(unsigned int i=0;i<shuffleResources.execMapVector.size();i++){

                // shuffling Data From A specific Container
				shufflingDataFromAContainer(i,pairsShuffled);
		}
	}
}