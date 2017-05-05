
#include <iostream>
#include <sys/stat.h>
#include "MapReduceDerived.h"
#include <algorithm>


void printResults(OUT_ITEMS_VEC &outputVector){
	for(int j=0;j<outputVector.size();j++){
		StringContainers *result= (StringContainers*)outputVector.at(j).first;
		IntegerContainers *times= (IntegerContainers*)outputVector.at(j).second;

		for (int k=0;k<times->getValue();k++){
			if(j==outputVector.size()-1 && k==times->getValue()-1){
				std::cout<<result->getData();
				break;
			}
			std::cout<<result->getData() <<" ";
		}
	}
}

void deleteResources(IN_ITEMS_VEC &inputVector,
                     OUT_ITEMS_VEC &outputVector,MapReduceDerived *worker,StringContainers *keyWord){
	//delete v3Base and k3Base
	for(int l = 0;l<outputVector.size();l++){
		delete outputVector.at(l).first;
		delete outputVector.at(l).second;
	}
	//delete v2Base
	for(int j=0;j<inputVector.size();j++){
		delete inputVector.at(j).second;
	}
	//delete k1Base
	delete keyWord;
	delete worker;
}

int main(int argc,char* argv[]){
	struct stat st;

	if(argc==1){
		std::cerr<<"Usage: <substring to search> <folders, separated by space>"<<std::endl;
		exit(1);
	}
	MapReduceDerived *worker = new MapReduceDerived();
	IN_ITEMS_VEC inputVector;
	StringContainers *keyWord= new StringContainers(argv[1]);
	for(int i = 2; i < argc ; i++){
		if(stat(argv[i],&st)==0){
			if(st.st_mode & S_IFDIR!=0){ //check if it a directory
				inputVector.push_back(IN_ITEM(keyWord,
				                              new StringContainers(argv[i])));
			}
		}
	}
	OUT_ITEMS_VEC outputVector = RunMapReduceFramework(*(worker),inputVector,inputVector.size(),true);

	std::sort(outputVector.begin(),outputVector.end());

	printResults(outputVector);

	deleteResources(inputVector,outputVector,worker,keyWord);
	return 0;
}

