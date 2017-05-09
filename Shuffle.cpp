

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
static void searchingAndInsertingData(k2Base* key, v2Base* value){
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
}

/**
 * shuffle data from a container
 * @param i the index of the execMap container
 * @param pairsShuffled the number of the pairs that had been shuffled
 */
static void shufflingDataFromAContainer(unsigned int i, int pairsShuffled){

    for(unsigned int j= 0 ;j<shuffleResources.execMapVector.at(i)->getVectorSize();j++) {

        k2Base *key = shuffleResources.execMapVector.at(i)->getPastMapVector()->at(j).first;
        v2Base *value = shuffleResources.execMapVector.at(i)->getPastMapVector()->at(j).second;
        searchingAndInsertingData(key,value);
        //increasing the count of the pairs that had been shuffled
        pairsShuffled++;
    }
}

void* Shuffle::shuffleAll(void*){

	unsigned int pairsShuffled = 0;
	while(pairsShuffled != numOfPairs){
		//wait until one of the containers is not empty
		sem_wait(shuffleResources.shuffleSemaphore);
		for(unsigned int i=0;i<shuffleResources.execMapVector.size();i++){

			if(!shuffleResources.execMapVector.at(i)->getPastMapVector()->empty()) {

				pthread_mutex_lock(shuffleResources.mutexVector[i]); //todo check if we can do the mutex in the loop
                // shuffling Data From A specific Container
				shufflingDataFromAContainer(i,pairsShuffled);
                sem_wait(shuffleResources.shuffleSemaphore);

                //todo ido notice that updating the semaphore every time we take a pair(by the emit2 or the shuffle)
                // todo is pointless because either of them we be stuck until the release of the mutex

				shuffleResources.execMapVector.at(i)->getPastMapVector()->clear();
				pthread_mutex_unlock(shuffleResources.mutexVector[i]);
			}
		}
	}
}