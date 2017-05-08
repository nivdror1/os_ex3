//
// Created by nivdror1 on 5/5/17.
//

#include "Shuffle.h"
#include "ExecMap.h"

std::vector<pthread_mutex_t> mutexVector;

/**
 * c-tor
 * */
Shuffle::Shuffle(sem_t& semShuffle,std::vector<ExecMap*> &execMapVector,
                 std::vector<pthread_mutex_t> mutexVector, unsigned int numInput ):
		shuffleSemaphore(&semShuffle), execMapVector(execMapVector),
		mutexVector(mutexVector),numOfPairs(numInput){

	int error = pthread_create(&_thread, NULL, shuffleAll, NULL); //todo error?
}


void* Shuffle::shuffleAll(void*){

	unsigned int pairsShuffled = 0;
	while(pairsShuffled != numOfPairs){
		//wait until one of the containers is not empty
		sem_wait(this->shuffleSemaphore);
		for(unsigned int i=0;i<execMapVector.size();i++){

			if(!execMapVector.at(i)->getPastMapVector()->empty()) {

				pthread_mutex_lock(mutexVector[i]); //todo check if we can do the mutex in the loop
				for(unsigned int j;j<execMapVector.at(i)->getVectorSize();j++) {


					k2Base *fileName = execMapVector.at(i)->getPastMapVector()->at(j).first;
					v2Base *fileNum = execMapVector.at(i)->getPastMapVector()->at(j).second;
					SHUFFLED_ITEM newPair = std::make_pair(fileName,fileNum);
					shuffledVector.push_back(newPair); // todo could map work better? and to insert it properly and to clear one by one
					sem_wait(this->shuffleSemaphore);

					pairsShuffled++;
				}
				execMapVector.at(i)->getPastMapVector()->clear();
				pthread_mutex_unlock(mutexVector[i]);

			}
		}
	}
}