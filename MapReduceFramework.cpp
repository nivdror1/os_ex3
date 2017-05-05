#include "MapReduceFramework.h"
#include <pthread.h>
#include <ExecMap.h>
#include <ExecReduce.h>
#include "Shuffle.h"
#include "ExecMap.h"


typedef std::vector<Map_Vec*> PTC;

std::vector<ExecMap*> execMapVector;
Shuffle * shuffle;
PTC pthreadToContainer;

/**
 * initate the threads of the mapping and shuffling, and also initate the
 * pthreadToContainer
 * @param numThread the number of threads to be create while mapping
 * @param mapReduce an object
 * @param itemsVec
 */
void init(int numThread,MapReduceBase& mapReduce,IN_ITEMS_VEC& itemsVec){

	pthread_mutex_t pthreadToContainerMutex;
	pthread_mutex_lock(&pthreadToContainerMutex);
	for(int i=0;i<numThread;i++){
		execMapVector[i] =  new ExecMap(i,mapReduce.Map);
 	}

	shuffle= new Shuffle();

	for(int j=0;j<numThread;j++){
		pthreadToContainer[j]=execMapVector[j]->getPastMapVector();
	}

	pthread_mutex_unlock(&pthreadToContainerMutex);


}

OUT_ITEMS_VEC RunMapReduceFramework(MapReduceBase& mapReduce, IN_ITEMS_VEC& itemsVec,
                                    int multiThreadLevel, bool autoDeleteV2K2){

}
