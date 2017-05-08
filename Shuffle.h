//
// Created by nivdror1 on 5/5/17.
//

#ifndef OS_EX3_SHUFFLE_H
#define OS_EX3_SHUFFLE_H


class Shuffle {

private:
	/** the thread*/
	pthread_t _thread;

	/** the output vector of the shuffle process*/
	std::vector<k2Base*,std::vector<v2Base*>> shuffledVector;

public:
	/**
	 * c-tor
	 * */
	Shuffle();


	void shuffleAll(void*);


};


#endif //OS_EX3_SHUFFLE_H
