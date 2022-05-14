
#include "common.h"

//Global variables
map<string, string> serverList;
map<int, string> clientList;
map<int, int> clientPort;
vector<int> reqSet[NUM_OF_CLIENTS];
vector<int> grantSet[NUM_OF_CLIENTS];

//struct message m_data = {0};
struct message m_grant = {0};
bool replySent = false;
bool failedMsgReceived = false;
int yieldSent = 0;
bool performWrite = false;
unsigned int context;
unsigned int seqNumber = 0;
unsigned int grantCount = 0;
bool localGrant = false;
bool inquireSent = false;

bool done = false;

pthread_mutex_t pktLock;
pthread_mutex_t seqNumLock;
pthread_mutex_t grantCountLock;
pthread_mutex_t yieldSentLock;
pthread_mutex_t failedMsgReceivedLock;
pthread_mutex_t localGrantLock;
pthread_mutex_t replySentLock;
pthread_mutex_t performWriteLock;
pthread_mutex_t queueLock;
pthread_mutex_t grantMsgLock;
pthread_mutex_t inquireRecSockQLock;
pthread_mutex_t inquireSentLock;
pthread_mutex_t doneLock;

priority_queue<message, vector<message>, Comp> reqQ;
queue<int> inquireRecSockQ;

int requestCounter = 0;
int replyCounter = 0;
int releaseCounter = 0;
int inquireCounter = 0;
int failedCounter = 0;
int yieldCounter = 0;

pthread_mutex_t requestCounterLock;
pthread_mutex_t replyCounterLock;
pthread_mutex_t releaseCounterLock;
pthread_mutex_t inquireCounterLock;
pthread_mutex_t failedCounterLock;
pthread_mutex_t yieldCounterLock;

FILE *fp;

void init_client()
{
	struct sockaddr_in init_server, init_client;
	int initSock, siz;
	
	//init Socket
	initSock = socket(AF_INET , SOCK_STREAM , 0);
	if (initSock == -1)
	{
		printf("Could not create socket");
		exit(1);
	}

	//Prepare the sockaddr_in structure
	init_server.sin_family = AF_INET;
	init_server.sin_addr.s_addr = INADDR_ANY;
	init_server.sin_port = htons(9000 + context);    

	int m_true = 1;
	setsockopt(initSock,SOL_SOCKET,SO_REUSEADDR,&m_true,sizeof(int));

	//Bind
	if( bind(initSock,(struct sockaddr *)&init_server , sizeof(init_server)) < 0)
	{
		puts("Bind failed. Error");
		exit(1);
	}
    siz = sizeof(struct sockaddr_in);

	//Listen
	listen(initSock , 1);
	
	if((accept(initSock,(struct sockaddr *)&init_client, (socklen_t*)&siz)) < 0)
	{
		printf("Initiator accept failed Error %d\n", errno);
		exit(1);
	}
}

int main(int argc, char *argv[])
{
    if(argc < 4)
    {
        printf("Usage %s server_name server_port context\n", argv[0]);
        exit(1);
    }
    
    context = atoi(argv[3]);        
     
    serverList["s1"] = "10.176.66.73";
    serverList["s2"] = "10.176.66.74";
    serverList["s3"] = "10.176.66.75";
	
	clientList[0] = "dc30.utdallas.edu";
    clientList[1] = "dc31.utdallas.edu";
    clientList[2] = "dc32.utdallas.edu";
    clientList[3] = "dc33.utdallas.edu";
    clientList[4] = "dc34.utdallas.edu";
    clientList[5] = "dc35.utdallas.edu";
    clientList[6] = "dc36.utdallas.edu";    
    
    clientPort[0] = 8050;
    clientPort[1] = 8051;
    clientPort[2] = 8052;
    clientPort[3] = 8053;
    clientPort[4] = 8054;
    clientPort[5] = 8055;
    clientPort[6] = 8056;

    reqSet[0].push_back(4);
    reqSet[0].push_back(2);

    reqSet[1].push_back(7);
    reqSet[1].push_back(3);
    reqSet[1].push_back(4);

    reqSet[2].push_back(1);
    reqSet[2].push_back(7);

    reqSet[3].push_back(5);
    reqSet[3].push_back(6);
    reqSet[3].push_back(7);

    reqSet[4].push_back(2);
    reqSet[4].push_back(3);
    reqSet[4].push_back(6);

    reqSet[5].push_back(1);
    reqSet[5].push_back(3);

    reqSet[6].push_back(3);
    reqSet[6].push_back(4);
    reqSet[6].push_back(5);

    grantSet[0].push_back(3);
    grantSet[0].push_back(6);

    grantSet[1].push_back(1);
    grantSet[1].push_back(5);

    grantSet[2].push_back(2);
    grantSet[2].push_back(5);
    grantSet[2].push_back(6);
    grantSet[2].push_back(7);

    grantSet[3].push_back(1);
    grantSet[3].push_back(2);
    grantSet[3].push_back(7);

    grantSet[4].push_back(4);
    grantSet[4].push_back(7);

    grantSet[5].push_back(4);
    grantSet[5].push_back(5);

    grantSet[6].push_back(2);
    grantSet[6].push_back(3);
    grantSet[6].push_back(4);
    
    char fname[100];
	sprintf(fname, "Node_%d.log", context);
	fp = fopen(fname, "w");
    

	struct sockaddr_in my_addr;
    struct hostent *serv;
    //serv = gethostbyname(serverList.find(argv[1])->second.c_str());
	int hsock;
	int * p_int;
    int err;    

    //client socket
	hsock = socket(AF_INET, SOCK_STREAM, 0);
	if(hsock == -1)
	{
		printf("Error initializing socket %d\n",errno);
		exit(1);
	}
	
    //temporary int pointer for setsockopt call
	p_int = (int*)malloc(sizeof(int));
	*p_int = 1;
		
    //set client socket opt    
	if( (setsockopt(hsock, SOL_SOCKET, SO_REUSEADDR, (char*)p_int, sizeof(int)) == -1 )||
		(setsockopt(hsock, SOL_SOCKET, SO_KEEPALIVE, (char*)p_int, sizeof(int)) == -1 ) )
	{
		printf("Error setting options %d\n",errno);
		free(p_int);
		exit(1);
	}
	free(p_int);

    //server socket setup details.
	my_addr.sin_family = AF_INET ;
    my_addr.sin_port = htons(atoi(argv[2]));
    memset((void *)&(my_addr.sin_zero), 0, 8);
    //bcopy((char *)serv->h_addr, (char *)&my_addr.sin_addr.s_addr, serv->h_length);
	my_addr.sin_addr.s_addr = inet_addr(serverList.find(argv[1])->second.c_str());
    
    //establish socket connection from client to server
	if( connect( hsock, (struct sockaddr*)&my_addr, sizeof(my_addr)) == -1 )
	{
		if((err = errno) != EINPROGRESS)
		{
			fprintf(stderr, "Error connecting socket %d\n", errno);
			exit(1);
		}
	}
	
	/* Maekawa's Mutex Algorithm Handling */	
    init_client();    
    
    struct sockInfo mSockInfo;    
    struct hostent *servDetails;    
    struct sockaddr_in server , client;
    int serverSock , clientSock , *tempSock, siz;
    int connectionCount = 0;
    int maxConnection = grantSet[context].size();
    pthread_t thread_id, serverThread;
    
    memset((void *)&mSockInfo, 0, sizeof(mSockInfo));
    
    // Populate mSockInfo for all the clients
	for(int i=0;i<NUM_OF_CLIENTS;i++)
	{
		//memset((void *)serv, 0, sizeof(serv));		
		mSockInfo.server[i].sin_family 	= AF_INET;
		mSockInfo.server[i].sin_port 	= htons(clientPort.find(i)->second);
		memset((void *)&(mSockInfo.server[i].sin_zero), 0, 8);
		servDetails = gethostbyname(clientList.find(i)->second.c_str());
		bcopy((char *)servDetails->h_addr, (char *)&mSockInfo.server[i].sin_addr.s_addr, servDetails->h_length);
	}
	mSockInfo.context = context;
	
	//Create Socket
	serverSock = socket(AF_INET , SOCK_STREAM , 0);
	if (serverSock == -1)
	{
		printf("Could not create socket");
		exit(1);
	}

	//Prepare the sockaddr_in structure
	server.sin_family = AF_INET;
	server.sin_addr.s_addr = INADDR_ANY;
	server.sin_port = htons(clientPort.find(context)->second);

	int m_true = 1;
	setsockopt(serverSock,SOL_SOCKET,SO_REUSEADDR,&m_true,sizeof(int));

	//Bind
	if( bind(serverSock,(struct sockaddr *)&server , sizeof(server)) < 0)
	{
		puts("Bind failed. Error");
		exit(1);
	}

	//Listen
	listen(serverSock , 25);

    //wait to make sure that all clients are started from initiator.
	sleep(1);
	
	//Accept and incoming connection
	puts("Waiting for incoming connections...");
	siz = sizeof(struct sockaddr_in);	

	// Create a thread to initiate connections to ALL the servers and quit.
	// It will populate messageSock[10], to be used later to send messages
	if(pthread_create(&serverThread, NULL, connectToQuorums, (void *)&mSockInfo) < 0)
	{
		puts("Failed to create the Server Thread");
		exit(1);
	}

	while( (connectionCount < maxConnection) && (clientSock = accept(serverSock, (struct sockaddr *)&client, (socklen_t*)&siz)) )
	{
		tempSock = (int *)malloc(sizeof(int));
		*tempSock = clientSock;
		connectionCount++;				

		if( pthread_create( &thread_id , NULL ,  reqGranter , (void*)tempSock) < 0)
		{
			puts("Failed to create the Client Handler Thread");
			exit(1);
		}
	}	

    msgPkt m_pkt;    
    char result[1024];
	char entry[100];
	
	while(1)
	{		
		if(performWrite)
		{
			memset((void *)&m_pkt, 0, sizeof(m_pkt));
			memset((void *)&entry, 0, sizeof(entry));
			strcpy(m_pkt.cmd, "write");
			strcpy(m_pkt.fname,"client.txt");			
			sprintf(entry, "<Id :%d, Seq No : %d, Hostname : %s>\n", context, seqNumber, clientList.find(context)->second.c_str());
			strcpy(m_pkt.data, entry);
			m_pkt.master_server = 1;
			m_pkt.optParam = 1;
			strcpy(m_pkt.context, argv[1]);
			
            if(send(hsock, (void *)&m_pkt, sizeof(m_pkt), 0) == -1)
            {
                fprintf(stderr, "Error sending data %d\n", errno);
                exit(1);
            }

            if(recv(hsock,result, 1024, 0) == -1)
            {
                fprintf(stderr, "Error receiving data %d\n", errno);
                exit(1);                 
            }
            pthread_mutex_lock(&performWriteLock);
				performWrite = false;
			pthread_mutex_unlock(&performWriteLock); 
            printf("%s\n", result);            
		}
		
		if(done)
		{
			char res[100] = {};
			memset((void *)&m_pkt, 0, sizeof(m_pkt));
			strcpy(m_pkt.cmd, "Done");
			m_pkt.master_server = 1;
			if(send(hsock, (void *)&m_pkt, sizeof(m_pkt), 0) == -1)
            {
                fprintf(stderr, "Error sending data %d\n", errno);
                exit(1);
            }
            if(recv(hsock,res, 100, 0) == -1)
            {
                fprintf(stderr, "Error receiving data %d\n", errno);
                exit(1);                 
            }
            if(strcmp(res, "terminate") == 0)
            {
				printf("*********  TERMINATING ***********\n");
				exit(0);
			}
		}		      
    }
    return 0;
}

void *connectToQuorums(void *messageSock)
{
	struct sockInfo *sock = (struct sockInfo *)messageSock;	
	int err;
	pthread_t receiverThread;

    // connect to quorums 
	for(int i=0;i<reqSet[context].size();i++)
	{
		int index = reqSet[context][i] - 1;			
		// Create a socket		
		sock->messageSock[index] = socket(AF_INET, SOCK_STREAM, 0);		
		
		// Connect to the quorums using the Socket
		if( connect(sock->messageSock[index], (struct sockaddr *)&(sock->server[index]), sizeof(sock->server[index])) < 0) 
		{
			if((err = errno) != EINPROGRESS)
			{
				fprintf(stderr, "Error connecting socket %d\n", errno);
				exit(1);
			}			
		}
		
		//create receiver handler thread for each request messages
		if(pthread_create(&receiverThread, NULL, receiveHandler, (void *)&sock->messageSock[index]) < 0)
		{
			puts("Failed to create the Server Thread");
			exit(1);
		}		
	}		
	
	//40 writes by each client
	for(int i=0; i< NUM_OF_WRITES; ++i)
	{
		struct timeval t1, t2;
		double elapsedTime;
		
		gettimeofday(&t1, NULL);
		pthread_mutex_lock(&seqNumLock);
			++seqNumber;
		pthread_mutex_unlock(&seqNumLock);
		
		//local machine should get grant from itself to enter critical section
		if(reqQ.empty() && !replySent)
		{
			pthread_mutex_lock(&localGrantLock);
				localGrant = true;
			pthread_mutex_unlock(&localGrantLock);
			
			pthread_mutex_lock(&replySentLock);
				replySent = false;
			pthread_mutex_unlock(&replySentLock);
			
			pthread_mutex_lock(&grantMsgLock);
					memset((void *)&m_grant, 0, sizeof(m_grant));					
					m_grant.id = context;
					m_grant.seqNum = seqNumber;
					m_grant.type = GRANT;
					m_grant.sockId = 0;
			pthread_mutex_unlock(&grantMsgLock);
			printf("Local granted\n");						
		}
		else
		{
            /*if new request has lower priority than the already granted
            then set the failed message received to true*/                  
            if(m_grant.seqNum < seqNumber ||
                ((m_grant.seqNum == seqNumber) && (m_grant.id < context)))
            {                                                                       
                failedMsgReceived = true;
            }                        
            else
            {
                /* send the inquire message */
                if(!inquireSent)
                {
                    message temp;
                    memset((void *)&temp, 0, sizeof(temp));                                             
                    temp.type = INQUIRE;
                    temp.id = context;  
                    send(m_grant.sockId, (void *)&temp, sizeof(struct message), 0);
                    printf("Inquire sent to client %d\n", m_grant.id);
                                
                    pthread_mutex_lock(&inquireSentLock);
                        inquireSent = true;
                    pthread_mutex_unlock(&inquireSentLock); 
                                
                    pthread_mutex_lock(&inquireCounterLock);
                        ++inquireCounter;
                    pthread_mutex_unlock(&inquireCounterLock);                                                      
                }
            }
			//push local request to the queue
			printf("Local request queued\n");
			message l_msg;
			memset((void *)&l_msg, 0, sizeof(l_msg));
			l_msg.type = LOCAL_REQUEST;
			l_msg.seqNum = seqNumber;
			l_msg.id = context;
			l_msg.sockId = -1;			
			
			pthread_mutex_lock(&queueLock);
				reqQ.push(l_msg);
			pthread_mutex_unlock(&queueLock);
		}		
		
		//send request message to its request set.
		for(int j=0;j<reqSet[context].size();j++)
		{
			int index = reqSet[context][j] - 1;
			message reqData;
			memset((void *)&reqData, 0, sizeof(reqData));			
			
			reqData.seqNum = seqNumber;
			reqData.id = context;
			reqData.type = REQUEST;						
			
			send(sock->messageSock[index], (void *)&reqData, sizeof(struct message), 0);
			
			pthread_mutex_lock(&requestCounterLock);
				++requestCounter;
			pthread_mutex_unlock(&requestCounterLock);						
		}		
		
		//wait till we get grant message from nodes in a quorum to enter critical section
		while((grantCount < reqSet[context].size()) || !localGrant)
		{
			if((failedMsgReceived || (yieldSent > 0)) && (!reqQ.empty()))
			{
				if(localGrant)
				{
					message tmp;
					memset((void *)&tmp, 0, sizeof(tmp));
					pthread_mutex_lock(&queueLock);									
						tmp = reqQ.top();						
					pthread_mutex_unlock(&queueLock);
				
					if(tmp.seqNum < m_grant.seqNum ||
					((tmp.seqNum == m_grant.seqNum) && (tmp.id < m_grant.id)))
					{
						pthread_mutex_lock(&replySentLock);
							replySent = true;
						pthread_mutex_unlock(&replySentLock);						
							
						pthread_mutex_lock(&localGrantLock);
							localGrant = false;
						pthread_mutex_unlock(&localGrantLock);
						
						message m_pkt_temp;
						memset((void *)&m_pkt_temp, 0, sizeof(m_pkt_temp));
						m_pkt_temp.type = GRANT;
						m_pkt_temp.id = context;
						m_pkt_temp.seqNum = seqNumber;							
						send(tmp.sockId, (void *)&m_pkt_temp, sizeof(struct message), 0);
						
						pthread_mutex_lock(&queueLock);									
							reqQ.pop();						
						pthread_mutex_unlock(&queueLock);						
						
						pthread_mutex_lock(&grantMsgLock);
							memset((void *)&m_grant, 0, sizeof(m_grant));					
							m_grant = tmp;
						pthread_mutex_unlock(&grantMsgLock);						
							
						printf("Local grant sent after failed during waiting\n");
						
						//push back local request to the queue
						printf("Local request queued back\n");
						message l_msg;
						memset((void *)&l_msg, 0, sizeof(l_msg));
						l_msg.type = LOCAL_REQUEST;
						l_msg.seqNum = seqNumber;
						l_msg.id = context;
						l_msg.sockId = -1;			
			
						pthread_mutex_lock(&queueLock);
							reqQ.push(l_msg);
						pthread_mutex_unlock(&queueLock);	
						
						pthread_mutex_lock(&replyCounterLock);
							++replyCounter;
						pthread_mutex_unlock(&replyCounterLock);
					}				
				}
				
				while(!inquireRecSockQ.empty())
				{
					int inquireSoc = inquireRecSockQ.front();
					
					pthread_mutex_lock(&inquireRecSockQLock);
						inquireRecSockQ.pop();
					pthread_mutex_unlock(&inquireRecSockQLock);
						
					message m_pkt_temp;
					memset((void *)&m_pkt_temp, 0, sizeof(m_pkt_temp));
					m_pkt_temp.type = YIELD;
					m_pkt_temp.id = context;
					m_pkt_temp.seqNum = seqNumber;
					//m_pkt_temp.sockId = sock;
					send(inquireSoc, (void *)&m_pkt_temp, sizeof(struct message), 0);
					
					pthread_mutex_lock(&yieldSentLock);
						yieldSent++;
					pthread_mutex_unlock(&yieldSentLock);
					
					pthread_mutex_lock(&grantCountLock);
						grantCount--;
					pthread_mutex_unlock(&grantCountLock);					
					
					printf("...Yield sent after failed during waiting\n");
					
					pthread_mutex_lock(&yieldCounterLock);
						++yieldCounter;
					pthread_mutex_unlock(&yieldCounterLock);
				}				
			}				
		}	
		
		gettimeofday(&t2, NULL);
		elapsedTime = (t2.tv_sec - t1.tv_sec) * 1000.0;      // sec to ms
		elapsedTime += (t2.tv_usec - t1.tv_usec) / 1000.0;   // us to ms
		
		fprintf(fp,"----Time taken to enter critical section is: %f ms\n", elapsedTime);
		fflush(fp);
		
		fprintf(fp,"----Number of messages sent so far :\n Request : %d, Reply : %d, Release : %d, Failed : %d, Yield : %d, Inquire : %d\n", requestCounter, replyCounter, releaseCounter, failedCounter, yieldCounter, inquireCounter);
		fflush(fp);
		
		printf("Ready to write\n");
		pthread_mutex_lock(&failedMsgReceivedLock);
			failedMsgReceived = false;
		pthread_mutex_unlock(&failedMsgReceivedLock);
		
		//all requests are received
		pthread_mutex_lock(&performWriteLock);
			performWrite = true;
		pthread_mutex_unlock(&performWriteLock);
		
		pthread_mutex_lock(&replySentLock);
			replySent = false;
		pthread_mutex_unlock(&replySentLock);					
		
		while(performWrite)
		{
			//wait till writing is done.			
		}
		
		printf("Writing is done\n");		
		
		//relinquish local grant
		pthread_mutex_lock(&localGrantLock);
			localGrant = false;
		pthread_mutex_unlock(&localGrantLock);		
		
		pthread_mutex_lock(&grantCountLock);
			grantCount = 0;
		pthread_mutex_unlock(&grantCountLock);		
		
		while(!inquireRecSockQ.empty())
		{
			pthread_mutex_lock(&inquireRecSockQLock);
				inquireRecSockQ.pop();
			pthread_mutex_unlock(&inquireRecSockQLock);
		}	
		
		//release to other nodes
		for(int j=0;j<reqSet[context].size();j++)
		{
			int index = reqSet[context][j] - 1;
			message relData;
			memset((void *)&relData, 0, sizeof(relData));			
			
			relData.seqNum = seqNumber;
			relData.id = context;
			relData.type = RELEASE;			
			
			send(sock->messageSock[index], (void *)&relData, sizeof(struct message), 0);
			printf("Release sent to client %d\n", index);
			
			pthread_mutex_lock(&releaseCounterLock);
				++releaseCounter;
			pthread_mutex_unlock(&releaseCounterLock);
		}
		
		//send the grant message if the queue is not empty.
		if(!reqQ.empty())
		{
			pthread_mutex_lock(&replySentLock);
				replySent = true;
			pthread_mutex_unlock(&replySentLock);			
			
			//pop the front of the queue and send grant message
			message tmp;
			memset((void *)&tmp, 0, sizeof(tmp));	
			pthread_mutex_lock(&queueLock);									
				tmp = reqQ.top();
				reqQ.pop();
			pthread_mutex_unlock(&queueLock);
			
			pthread_mutex_lock(&grantMsgLock);
				memset((void *)&m_grant, 0, sizeof(m_grant));					
				m_grant = tmp;
			pthread_mutex_unlock(&grantMsgLock);			
			
			message temp;
			memset((void *)&temp, 0, sizeof(temp));	
			temp.id = context;				
			temp.type = GRANT;				
						
			send(tmp.sockId, (void *)&temp, sizeof(struct message), 0);			
			printf("...Grant sent to client %d after CS\n", tmp.id);
			
			pthread_mutex_lock(&replyCounterLock);
				++replyCounter;
			pthread_mutex_unlock(&replyCounterLock);			 					
		}
		else
		{
			printf("Request queue empty\n");
		}		
		//sleep for 30 milli seconds.
		usleep(SLEEP_TIME_MS * 1000);
		printf("Sleep done\n"); 		
	}
	
	int totalMessages = replyCounter + requestCounter + releaseCounter + failedCounter + yieldCounter + inquireCounter;
	fprintf(fp, "Total number of messages sent is %d\n", totalMessages);
	fflush(fp);
	
	printf(".................%d write operations is completed.........\n",NUM_OF_WRITES);
	pthread_mutex_lock(&doneLock);
		done = true;
	pthread_mutex_unlock(&doneLock);			
}

void *reqGranter(void *serverSock)
{
	//Get the socket descriptor
	int sock = *(int*)serverSock;	
	message pkt;
	memset((void *)&pkt, 0, sizeof(pkt));
	
	//Receive a message from the resource requester
	while( recv(sock , (void *)&pkt , sizeof(struct message) , 0) > 0 )
	{		
		switch(pkt.type)
		{			
			case REQUEST:							
				message temp;
				memset((void *)&temp, 0, sizeof(temp));	
				printf("Request received from client %d\n", pkt.id);
				printf("Local grant is %d :: Reply sent is %d :: Queue empty %d\n", localGrant, replySent, reqQ.empty());					
				
				//grant is available
				pthread_mutex_lock(&replySentLock);
                pthread_mutex_lock(&localGrantLock);
				if(!replySent && !localGrant && reqQ.empty())
				{
					//pthread_mutex_lock(&replySentLock);
						replySent = true;
					pthread_mutex_unlock(&replySentLock);
					
					//pthread_mutex_lock(&localGrantLock);
						localGrant = false;
					pthread_mutex_unlock(&localGrantLock);
					
					pthread_mutex_lock(&grantMsgLock);
						memset((void *)&m_grant, 0, sizeof(m_grant));					
						m_grant = pkt;
						m_grant.sockId = sock;
					pthread_mutex_unlock(&grantMsgLock);					
					//send grant message					
											
					temp.type = GRANT;
					temp.id = context;			
					
					send(sock, (void *)&temp, sizeof(struct message), 0);	
					printf("Grant sent to client %d as no reply and no local\n", pkt.id);
					
					pthread_mutex_lock(&replyCounterLock);
						++replyCounter;
					pthread_mutex_unlock(&replyCounterLock);				
				}
				
				//if grant is not available
				else
				{
					//if the grant is not local					
					if(!localGrant)
					{
						/*if new request has lower priority than the already granted
						then send the failed message */					
						if(m_grant.seqNum < pkt.seqNum ||
						   ((m_grant.seqNum == pkt.seqNum) && (m_grant.id < pkt.id)))
						{																		
							temp.type = FAILED;	
							temp.id = context;
							send(sock, (void *)&temp, sizeof(struct message), 0);
							printf("Failed sent for client %d\n", pkt.id);
							
							pthread_mutex_lock(&failedCounterLock);
								++failedCounter;
							pthread_mutex_unlock(&failedCounterLock);
						}
						
						else
						{
							/* send the inquire message */
							if(!inquireSent)
							{												
								temp.type = INQUIRE;
								temp.id = context;	
								send(m_grant.sockId, (void *)&temp, sizeof(struct message), 0);
								printf("Inquire sent to client %d\n", m_grant.id);
								
								pthread_mutex_lock(&inquireSentLock);
									inquireSent = true;
								pthread_mutex_unlock(&inquireSentLock);	
								
								pthread_mutex_lock(&inquireCounterLock);
									++inquireCounter;
								pthread_mutex_unlock(&inquireCounterLock);														
							}
						}
					}
					
					//if the grant is local
					else
					{
						if(m_grant.seqNum < pkt.seqNum ||
						   ((m_grant.seqNum == pkt.seqNum) && (m_grant.id < pkt.id)))
						{																		
							temp.type = FAILED;	
							temp.id = context;
							send(sock, (void *)&temp, sizeof(struct message), 0);
							printf("(Local grant)Failed sent for client %d\n", pkt.id);
							
							pthread_mutex_lock(&failedCounterLock);
								++failedCounter;
							pthread_mutex_unlock(&failedCounterLock);
						}						
						
						else if((failedMsgReceived || (yieldSent > 0)))
						{
							message m_pkt_temp;
							memset((void *)&m_pkt_temp, 0, sizeof(m_pkt_temp));
							m_pkt_temp.type = GRANT;
							m_pkt_temp.id = context;
							m_pkt_temp.seqNum = seqNumber;							
							
							if(!reqQ.empty())
							{
								message que = reqQ.top();
								
								if(que.seqNum < pkt.seqNum ||
								((que.seqNum == pkt.seqNum) && (que.id < pkt.id)))
								{
									
									//pthread_mutex_lock(&replySentLock);
										replySent = true;
									pthread_mutex_unlock(&replySentLock);
									
									//pthread_mutex_lock(&localGrantLock);
										localGrant = false;
									pthread_mutex_unlock(&localGrantLock);
									
									pthread_mutex_lock(&grantMsgLock);
										memset((void *)&m_grant, 0, sizeof(m_grant));					
										m_grant = que;
										m_grant.sockId = que.sockId;
									pthread_mutex_unlock(&grantMsgLock);
							
									send(que.sockId, (void *)&m_pkt_temp, sizeof(struct message), 0);
									printf("Local grant sent to client %d after failed\n", que.id);	
									pthread_mutex_lock(&queueLock);
										reqQ.pop();
									pthread_mutex_unlock(&queueLock);									
									
									/*Push the local request back to the queue */
									message l_msg;
									memset((void *)&l_msg, 0, sizeof(l_msg));
									l_msg.type = LOCAL_REQUEST;
									l_msg.seqNum = seqNumber;
									l_msg.id = context;
									l_msg.sockId = -1;			
			
									pthread_mutex_lock(&queueLock);
										reqQ.push(l_msg);
									pthread_mutex_unlock(&queueLock);
									
									pthread_mutex_lock(&replyCounterLock);
										++replyCounter;
									pthread_mutex_unlock(&replyCounterLock);
									
									break;																		
								}
							}
							else
							{
								//pthread_mutex_lock(&replySentLock);
									replySent = true;
								pthread_mutex_unlock(&replySentLock);
								
								//pthread_mutex_lock(&localGrantLock);
										localGrant = false;
								pthread_mutex_unlock(&localGrantLock);
								
								pthread_mutex_lock(&grantMsgLock);
									memset((void *)&m_grant, 0, sizeof(m_grant));					
									m_grant = pkt;
									m_grant.sockId = sock;
								pthread_mutex_unlock(&grantMsgLock);
									
								send(sock, (void *)&m_pkt_temp, sizeof(struct message), 0);
								printf("Local grant sent to client %d after failed\n", pkt.id);
								
								/*Push the local request back to the queue */
								message l_msg;
								memset((void *)&l_msg, 0, sizeof(l_msg));
								l_msg.type = LOCAL_REQUEST;
								l_msg.seqNum = seqNumber;
								l_msg.id = context;
								l_msg.sockId = -1;			
			
								pthread_mutex_lock(&queueLock);
									reqQ.push(l_msg);
								pthread_mutex_unlock(&queueLock);								
								
								pthread_mutex_lock(&replyCounterLock);
									++replyCounter;
								pthread_mutex_unlock(&replyCounterLock);
								
								break;	
							}							
						}
						else
						{
							printf("Grant is local...Just queued\n");
						}						
					}
					pthread_mutex_unlock(&replySentLock);
                    pthread_mutex_unlock(&localGrantLock);

					//push the request message to queue with sender's socket
					message qued;
					memset((void *)&qued, 0, sizeof(qued));
			
					qued = pkt;
					qued.sockId = sock;
					pthread_mutex_lock(&queueLock);
						reqQ.push(qued);
					pthread_mutex_unlock(&queueLock);
					printf("Message queued: Id:%d, SeqNum:%d\n", qued.id, qued.seqNum);
				}
				break;			
				
			case RELEASE:
			printf("Release from client %d\n", pkt.id);			
			
				if(!reqQ.empty())
				{
					//pop the front of the queue and send grant message	
					message tmp;
					memset((void *)&tmp, 0, sizeof(tmp));
					pthread_mutex_lock(&queueLock);									
						tmp = reqQ.top();
						reqQ.pop();
					pthread_mutex_unlock(&queueLock);
					
					pthread_mutex_lock(&grantMsgLock);
						memset((void *)&m_grant, 0, sizeof(m_grant));					
						m_grant = tmp;
					pthread_mutex_unlock(&grantMsgLock);
					
					if(tmp.type == LOCAL_REQUEST)
					{
						pthread_mutex_lock(&localGrantLock);
							localGrant = true;
						pthread_mutex_unlock(&localGrantLock);
						
						pthread_mutex_lock(&replySentLock);
							replySent = false;
						pthread_mutex_unlock(&replySentLock);
						printf("Local granted after release\n");
					}
					else
					{
						pthread_mutex_lock(&replySentLock);
							replySent = true;
						pthread_mutex_unlock(&replySentLock);	
						
						pthread_mutex_lock(&localGrantLock);
							localGrant = false;
						pthread_mutex_unlock(&localGrantLock);
						
						message temp;
						memset((void *)&temp, 0, sizeof(temp));					
						temp.type = GRANT;
						temp.id = context;
						
						if(tmp.type == YIELD)
						{
							pthread_mutex_lock(&yieldSentLock);
								yieldSent--;
							pthread_mutex_unlock(&yieldSentLock);
						}																
						
						send(tmp.sockId, (void *)&temp, sizeof(struct message), 0);	
						printf("Grant sent to client %d after release\n", tmp.id);
						
						pthread_mutex_lock(&replyCounterLock);
							++replyCounter;
						pthread_mutex_unlock(&replyCounterLock);					
					} 					
				}
				else
				{					
					pthread_mutex_lock(&replySentLock);
						replySent = false;
					pthread_mutex_unlock(&replySentLock);
					
					pthread_mutex_lock(&localGrantLock);
						localGrant = false;
					pthread_mutex_unlock(&localGrantLock);
				}
				break;
				
			case YIELD:
			printf("Yield from client %d\n", pkt.id);
			pthread_mutex_lock(&inquireSentLock);
				inquireSent = false;
			pthread_mutex_unlock(&inquireSentLock);			
			
				if(!reqQ.empty())
				{				
					/* pop the front of the queue and send grant message */
					message tmp;
					memset((void *)&tmp, 0, sizeof(tmp));					
					pthread_mutex_lock(&queueLock);
						tmp = reqQ.top();
						reqQ.pop();
					pthread_mutex_unlock(&queueLock);
					
					/* Store the request back to the request queue */
					message qued;
					memset((void *)&qued, 0, sizeof(qued));
					qued = pkt;
					qued.sockId = sock;
					pthread_mutex_lock(&queueLock);
						reqQ.push(qued);
					pthread_mutex_unlock(&queueLock);
					
					pthread_mutex_lock(&grantMsgLock);
						memset((void *)&m_grant, 0, sizeof(m_grant));					
						m_grant = tmp;
					pthread_mutex_unlock(&grantMsgLock);
					
					if(tmp.type == LOCAL_REQUEST)
					{
						pthread_mutex_lock(&localGrantLock);
							localGrant = true;
						pthread_mutex_unlock(&localGrantLock);
						
						pthread_mutex_lock(&replySentLock);
							replySent = false;
						pthread_mutex_unlock(&replySentLock);
						
						printf("Local granted after yield\n");
					}
					else
					{
						pthread_mutex_lock(&replySentLock);
							replySent = true;
						pthread_mutex_unlock(&replySentLock);
						
						pthread_mutex_lock(&localGrantLock);
							localGrant = false;
						pthread_mutex_unlock(&localGrantLock);
											
						message temp;
						memset((void *)&temp, 0, sizeof(temp));						
						temp.type = GRANT;
						temp.id = context;	
						send(tmp.sockId, (void *)&temp, sizeof(struct message), 0);
						printf("Grant sent to client %d after yield\n", tmp.id);						
						
						pthread_mutex_lock(&replyCounterLock);
							++replyCounter;
						pthread_mutex_unlock(&replyCounterLock);
					}													
				}
				break;
		}
		memset((void *)&pkt, 0, sizeof(pkt));
	}	
}

void *receiveHandler(void *servSock)
{
	int sock = *(int*)servSock;
	message m_pkt;
	memset((void *)&m_pkt, 0, sizeof(struct message));
	while( recv(sock , (void *)&m_pkt , sizeof(struct message) , 0) > 0 )
	{		
		switch(m_pkt.type)
		{
			case GRANT:
				printf("Grant from client %d\n", m_pkt.id);
				pthread_mutex_lock(&grantCountLock);
					grantCount++;
				pthread_mutex_unlock(&grantCountLock);										
				
				break;
				
			case FAILED:
			printf("Failed from client %d\n", m_pkt.id);			
				pthread_mutex_lock(&failedMsgReceivedLock);
					failedMsgReceived = true;
				pthread_mutex_unlock(&failedMsgReceivedLock);			
				
				break;
				
			case INQUIRE:
				printf("Inquire from client %d\n", m_pkt.id);
				
				if((failedMsgReceived || (yieldSent > 0)))
				{
					message m_pkt_temp;
					memset((void *)&m_pkt_temp, 0, sizeof(m_pkt_temp));
					m_pkt_temp.type = YIELD;
					m_pkt_temp.id = context;
					m_pkt_temp.seqNum = seqNumber;
					//m_pkt_temp.sockId = sock;
					send(sock, (void *)&m_pkt_temp, sizeof(struct message), 0);
					
					pthread_mutex_lock(&yieldSentLock);
						yieldSent++;
					pthread_mutex_unlock(&yieldSentLock);
					
					pthread_mutex_lock(&grantCountLock);
						grantCount--;
					pthread_mutex_unlock(&grantCountLock);					
					
					printf("Yield sent as failed received\n");
					
					pthread_mutex_lock(&yieldCounterLock);
						++yieldCounter;
					pthread_mutex_unlock(&yieldCounterLock);
				}
				else
				{
					printf("Inquire ignored and queued\n");
					pthread_mutex_lock(&inquireRecSockQLock);
						inquireRecSockQ.push(sock);
					pthread_mutex_unlock(&inquireRecSockQLock);
				}
				break;				
		}
		memset((void *)&m_pkt, 0, sizeof(struct message));
	}	
}
