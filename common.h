

#ifndef COMMON_H
#define COMMON_H

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <pthread.h>
#include <netdb.h>
#include <queue>
#include <map>
#include <string>
#include <errno.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <iostream>
#include <sstream>
#include <queue>
#include <sys/time.h>

using namespace std;

#define NUM_OF_SERVERS 3
#define NUM_OF_CLIENTS 7
#define NUM_OF_WRITES 40
#define SLEEP_TIME_MS 15

struct msgPkt{
    char cmd[12];
    char fname[16];
    char data[1024];
    int master_server;    
    int optParam;
    char context[2];
};

enum msgType
{
	REQUEST = 0,
	GRANT,
	RELEASE,
	FAILED,
	INQUIRE,
	YIELD,
	LOCAL_REQUEST,
	TERMINATE,
	DO_NOTHING
};

struct message
{	
	int seqNum;
	int id;
	msgType type;
	int sockId;	
};

struct Comp{
    bool operator()(const message& a, const message& b){
        if(a.seqNum == b.seqNum)
		{
			return a.id > b.id;
		}
		else
		{
			return a.seqNum > b.seqNum;
		}
    }
};

struct sockInfo{
	int messageSock[NUM_OF_CLIENTS];
	struct sockaddr_in server[NUM_OF_CLIENTS];
	int context;
};


void *connectToQuorums(void *messageSock);
void *reqGranter(void *serverSock);
void *receiveHandler(void *servSock);

#endif
