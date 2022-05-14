
#include "common.h"

map<int, string> clientList;
map<int, int> clientPort;

int main(int argc, char *argv[])
{    
    clientList[0] = "dc30.utdallas.edu";
    clientList[1] = "dc31.utdallas.edu";
    clientList[2] = "dc32.utdallas.edu";
    clientList[3] = "dc33.utdallas.edu";
    clientList[4] = "dc34.utdallas.edu";
    clientList[5] = "dc35.utdallas.edu";
    clientList[6] = "dc36.utdallas.edu";
    
    clientPort[0] = 9000;
    clientPort[1] = 9001;
    clientPort[2] = 9002;
    clientPort[3] = 9003;
    clientPort[4] = 9004;
    clientPort[5] = 9005;
    clientPort[6] = 9006;    

	struct sockaddr_in my_addr[NUM_OF_CLIENTS];
    struct hostent *serv;
	int hsock[8] = {};
	int * p_int;
    int err;

    //client socket
    for(int i = 0;i < NUM_OF_CLIENTS;++i)
    {
		hsock[i] = socket(AF_INET, SOCK_STREAM, 0);        
    }
		
	for(int i = 0;i < NUM_OF_CLIENTS;++i)
	{
		if(hsock[i] <= 0)
		{
			printf("Error initializing socket %d\n",errno);
			return 1;
		}
	}
	
    //temporary int pointer for setsockopt call
	p_int = (int*)malloc(sizeof(int));
	*p_int = 1;
		
    //set client socket opt
    for(int i = 0;i < NUM_OF_CLIENTS;++i)
    {    
		if( (setsockopt(hsock[i], SOL_SOCKET, SO_REUSEADDR, (char*)p_int, sizeof(int)) == -1 )||
			(setsockopt(hsock[i], SOL_SOCKET, SO_KEEPALIVE, (char*)p_int, sizeof(int)) == -1 ) )
		{
			printf("Error setting options %d\n",errno);
			free(p_int);
			return 1;
		}
	}
	free(p_int);    
    //server socket setup details.
    for(int i = 0;i < NUM_OF_CLIENTS;++i)
    {
		my_addr[i].sin_family = AF_INET ;
		my_addr[i].sin_port = htons(clientPort.find(i)->second);
		memset(&(my_addr[i].sin_zero), 0, 8);
        //memset(serv, 0, sizeof(serv));
        serv = gethostbyname(clientList.find(i)->second.c_str());
		bcopy((char *)serv->h_addr, (char *)&my_addr[i].sin_addr.s_addr, serv->h_length);
	}
    
    //establish socket connection from client to server
    for(int i = 0; i < NUM_OF_CLIENTS;++i)
    {
		if((connect(hsock[i], (struct sockaddr*)&my_addr[i], sizeof(my_addr[i]))) == -1 )
		{            
			if((err = errno) != EINPROGRESS)
			{
				fprintf(stderr, "Error connecting socket %d\n", errno);
				return 1;
			}
		}
	}
	printf("********* Initiation completed successfully *************\n");  
}
