

#include "common.h"

void *SocketHandler(void *);

//map which will hold server Lists.
map<string, string> serverList;
map<string, int> serverPortList;

//global variable to hold other servers names(logical names)
char Server1[3];
char Server2[3];

int doneCounter = 0;
pthread_mutex_t doneCounterLock;

int main(int argc, char** argv){

    if(argc < 4)
    {
        printf("Bad arguments: Usage %s server_port second_server third_server e.g. ./server 8000 s2 s3", argv[0]);
        exit(1);
    }
	struct sockaddr_in my_addr;

    //server mapping from logical name to actual host name
    serverList["s1"] = "10.176.66.73";
	serverList["s2"] = "10.176.66.74";
	serverList["s3"] = "10.176.66.75";

	serverPortList["s1"] = 8000;
	serverPortList["s2"] = 8001;
	serverPortList["s3"] = 8002;
    
    //store other server's logical names passed as command line argument
    strcpy(Server1, argv[2]);
    strcpy(Server2, argv[3]);
	int hsock;
	int * p_int ;
	int err;

	socklen_t addr_size = 0;
	int* csock;
	sockaddr_in sadr;
	pthread_t thread_id=0;

    //create server socket
	hsock = socket(AF_INET, SOCK_STREAM, 0);
	if(hsock == -1)
	{
		printf("Error initializing socket %d\n", errno);
		goto FINISH;
	}
	
	p_int = (int*)malloc(sizeof(int));
	*p_int = 1;
		
    //set sock options for the server socket    
	if( (setsockopt(hsock, SOL_SOCKET, SO_REUSEADDR, (char*)p_int, sizeof(int)) == -1 )||
		(setsockopt(hsock, SOL_SOCKET, SO_KEEPALIVE, (char*)p_int, sizeof(int)) == -1 ) )
	{
		printf("Error setting options %d\n", errno);
		free(p_int);
		goto FINISH;
	}
	free(p_int);

    //server socket setup 
	my_addr.sin_family = AF_INET ;
	my_addr.sin_port = htons(atoi(argv[1]));
	memset(&(my_addr.sin_zero), 0, 8);
	my_addr.sin_addr.s_addr = INADDR_ANY ;
	
    //binding server socket with server details
	if( bind( hsock, (sockaddr*)&my_addr, sizeof(my_addr)) == -1 )
	{
		fprintf(stderr,"Error binding to socket, make sure nothing else is listening on this port %d\n",errno);
		goto FINISH;
	}

    //listen at server port
	if(listen( hsock, 10) == -1 )
	{
		fprintf(stderr, "Error listening %d\n",errno);
		goto FINISH;
	}
    //Now lets do the server stuff

	addr_size = sizeof(sockaddr_in);
   	
	while(true)
	{
		csock = (int*)malloc(sizeof(int));
		if((*csock = accept( hsock, (sockaddr*)&sadr, &addr_size))!= -1)
		{
			pthread_create(&thread_id,0,&SocketHandler, (void*)csock );
			pthread_detach(thread_id);
		}
		else
		{
			fprintf(stderr, "Error accepting %d\n", errno);
		}
	}
	
FINISH:
;
}

void* SocketHandler(void* lp)
{
    int *csock = (int*)lp;

    msgPkt m_pkt;
    memset(&m_pkt, 0, sizeof(m_pkt));
    bool flag = false;
    struct sockaddr_in my_addr[NUM_OF_SERVERS-1];
    struct hostent *serv[NUM_OF_SERVERS-1];
    int portNo[NUM_OF_SERVERS-1];

    //get other two server details
    //serv[0] = gethostbyname(serverList.find(Server1)->second.c_str());
    //serv[1] = gethostbyname(serverList.find(Server2)->second.c_str());
    portNo[0] = serverPortList.find(Server1)->second;
    portNo[1] = serverPortList.find(Server2)->second;
    int hsock[NUM_OF_SERVERS-1];
    int * p_int;
    int err;
    int fd;
    int offset = 0;

    //receive the first data after accepting the connection.
	while(recv(*csock, (void *)&m_pkt, sizeof(m_pkt), 0) != -1)
	{

    //If the message is coming from the client
        if(m_pkt.master_server == 1)
        {
            //create socket for other 2 servers
            if(!flag)
            {
                hsock[0] = socket(AF_INET, SOCK_STREAM, 0);
                hsock[1] = socket(AF_INET, SOCK_STREAM, 0);
                if(hsock[0] == -1 || hsock[1] == -1)
                {
                    printf("Error initializing socket %d\n",errno);
                    exit(1);
                }
                
                //temprorary int pointer for setsockopt call                                                
                p_int = (int*)malloc(sizeof(int));
                *p_int = 1;
        
                //set socket options for the 2 sockets.                                                                        
                if( (setsockopt(hsock[0], SOL_SOCKET, SO_REUSEADDR, (char*)p_int, sizeof(int)) == -1 )||
                    (setsockopt(hsock[0], SOL_SOCKET, SO_KEEPALIVE, (char*)p_int, sizeof(int)) == -1 )||
                    (setsockopt(hsock[1], SOL_SOCKET, SO_REUSEADDR, (char*)p_int, sizeof(int)) == -1 )||
                    (setsockopt(hsock[1], SOL_SOCKET, SO_KEEPALIVE, (char*)p_int, sizeof(int)) == -1 ))
                {
                    printf("Error setting options %d\n",errno);
                    free(p_int);
                    exit(1);
                }
                free(p_int);

                //set up socket details of other 2 servers.
                for(int i=0;i<NUM_OF_SERVERS-1;++i)
                {
                    my_addr[i].sin_family = AF_INET ;
                    my_addr[i].sin_port = htons(portNo[i]);
                    memset(&(my_addr[i].sin_zero), 0, 8);
                    //bcopy((char *)serv[i]->h_addr, (char *)&my_addr[i].sin_addr.s_addr, serv[i]->h_length);
                }

				my_addr[0].sin_addr.s_addr = inet_addr(serverList.find(Server1)->second.c_str());
                my_addr[1].sin_addr.s_addr = inet_addr(serverList.find(Server2)->second.c_str());

                //Establish socket connection with the other 2 severs.
                if( connect( hsock[0], (struct sockaddr*)&my_addr[0], sizeof(my_addr[0])) == -1  ||
                    connect( hsock[1], (struct sockaddr*)&my_addr[1], sizeof(my_addr[1])) == -1)
                {
                    
                    if((err = errno) != EINPROGRESS)
                    {
                        fprintf(stderr, "Error connecting socket %d\n", errno);
                        exit(1);
                    }
                }
                flag = true;
            }
            char temp1[6] = {0};
            char temp2[6] = {0};
            
            if(strcmp(m_pkt.cmd, "Done") == 0)
            {
				pthread_mutex_lock(&doneCounterLock);
					doneCounter++;
				pthread_mutex_unlock(&doneCounterLock); 
			}
			
			if(doneCounter == 7)
			{
				char temp[100] = {};
				strcpy(temp, "terminate");
				send(*csock, (void *)temp, sizeof(temp), 0);
				
				msgPkt p1, p2;
				p1.master_server = 0;
				p1.master_server = 0;
				memset(&p1, 0, sizeof(p1));
                memset(&p2, 0, sizeof(p2));
                strcpy(p1.cmd, "terminate");
                strcpy(p2.cmd, "terminate");
                send(hsock[0], (void *)&p1, sizeof(p1), 0);
                send(hsock[1], (void *)&p2, sizeof(p2), 0);
                sleep(0.1);
                printf("*********  TERMINATING ***********\n");
                exit(0);
			}

            //validation for write command
            if(strcmp(m_pkt.cmd, "write") == 0)
            {
                msgPkt pkt1, pkt2;
                char reslt[1024] = {0};
                memset(&pkt1, 0, sizeof(pkt1));
                memset(&pkt2, 0, sizeof(pkt2));
                pkt1 = m_pkt;
                pkt2 = m_pkt;
                pkt1.master_server = 0;
                strcpy(pkt1.context,Server1);
                strcpy(pkt1.cmd, "W_Valid");
           
                //forward the message to server 2 
                send(hsock[0], (void *)&pkt1, sizeof(pkt1), 0);
        
                pkt2.master_server = 0;
                strcpy(pkt2.context,Server2);
                strcpy(pkt2.cmd, "W_Valid");

                //forward the message to server 3
                send(hsock[1], (void *)&pkt2, sizeof(pkt2), 0);

                recv(hsock[0], (void *)&temp1, sizeof(temp1), 0);
                recv(hsock[1], (void *)&temp2, sizeof(temp2), 0);

                if(strcmp(temp1, "Fail") == 0 ||
                   strcmp(temp2, "Fail") == 0)
                  {
                    sprintf(reslt, "output :: %s doesn't exists in some or all 3 replicas", m_pkt.fname);
                    send(*csock, (void *)reslt, sizeof(reslt), 0);
                    continue;
                  }
            }

            //validation for create command
            else if(strcmp(m_pkt.cmd, "create") == 0)
            {
                msgPkt pkt1, pkt2;
                char reslt[1024] = {0};
                memset(&pkt1, 0, sizeof(pkt1));
                memset(&pkt2, 0, sizeof(pkt2));
                pkt1 = m_pkt;
                pkt2 = m_pkt;
                pkt1.master_server = 0;
                strcpy(pkt1.context,Server1);
                strcpy(pkt1.cmd, "C_Valid");
           
                //forward the message to server 2 
                send(hsock[0], (void *)&pkt1, sizeof(pkt1), 0);
        
                pkt2.master_server = 0;
                strcpy(pkt2.context,Server2);
                strcpy(pkt2.cmd, "C_Valid");

                //forward the message to server 3
                send(hsock[1], (void *)&pkt2, sizeof(pkt2), 0);

                recv(hsock[0], (void *)&temp1, sizeof(temp1), 0);
                recv(hsock[1], (void *)&temp2, sizeof(temp2), 0);

                if(strcmp(temp1, "Fail") == 0 ||
                   strcmp(temp2, "Fail") == 0)
                  {
                    sprintf(reslt, "output :: %s already exists in some replicas", m_pkt.fname);
                    send(*csock, (void *)reslt, sizeof(reslt), 0);
                    continue;
                  }
            }

            //check if the file exists
            if(strcmp(m_pkt.cmd, "read") == 0 ||
               strcmp(m_pkt.cmd, "delete") == 0 ||
               strcmp(m_pkt.cmd, "write") == 0 ||
               strcmp(m_pkt.cmd, "seek") == 0)
            {
                char filePath[100] = {0};
                strcpy(filePath, m_pkt.context);
                strcat(filePath, "/");
                strcat(filePath, m_pkt.fname);
                char reslt[1024] = {0};
                if(access(filePath, F_OK) == -1)
                {
                    printf("%s doesn't exists\n", m_pkt.fname);
                    sprintf(reslt, "output :: %s doesn't exists", m_pkt.fname);
                    send(*csock, (void *)reslt, sizeof(reslt), 0);
                    continue;
                }
            }

            //non read commands will be sent to other servers
            if(strcmp(m_pkt.cmd, "read") != 0)
            {
                msgPkt pkt1, pkt2;
                memset(&pkt1, 0, sizeof(pkt1));
                memset(&pkt2, 0, sizeof(pkt2));
                memset(&temp1, 0, sizeof(temp1));
                memset(&temp2, 0, sizeof(temp2));

                pkt1 = m_pkt;
                pkt2 = m_pkt;
                pkt1.master_server = 0;
                strcpy(pkt1.context,Server1);
           
                //forward the message to server 2 
                send(hsock[0], (void *)&pkt1, sizeof(pkt1), 0);
        
                pkt2.master_server = 0;
                strcpy(pkt2.context,Server2);

                //forward the message to server 3
                send(hsock[1], (void *)&pkt2, sizeof(pkt2), 0);
                if(strcmp(m_pkt.cmd, "terminate") == 0)
                {
                    free(csock);
                    close(fd);
                    break;
                }

                recv(hsock[0], (void *)&temp1, sizeof(temp1), 0);
                recv(hsock[1], (void *)&temp2, sizeof(temp2), 0);
            }

            //create command
            if(strcmp(m_pkt.cmd, "create") == 0)
            {
                char reslt[1024] = {0};

                //if the command is successful at the other servers
                if(strcmp(temp1, "Succ") == 0 &&
                   strcmp(temp2, "Succ") == 0)
                {
                    char filePath[100] = {0};
                    strcpy(filePath, m_pkt.context);
                    strcat(filePath, "/");
                    strcat(filePath, m_pkt.fname);
                    fd = open(filePath, O_RDWR|O_EXCL|O_CREAT, 0644);
                    if(fd != -1)
                    {
                        printf("%s was created\n", m_pkt.fname);
                        sprintf(reslt, "output :: %s was created", m_pkt.fname);
                        send(*csock, (void *)&reslt, sizeof(reslt), 0);
                    }
                    else
                    {
                        if(errno == EEXIST)
                        {
                            printf("%s already exists\n", m_pkt.fname);
                            sprintf(reslt, "output :: %s already exists", m_pkt.fname);
                            send(*csock, (void *)reslt, sizeof(reslt), 0);
                        }
                        else
                        {
                            printf("%s create failed\n", m_pkt.fname);
                            sprintf(reslt, "output :: %s create failed", m_pkt.fname);
                            send(*csock, (void *)reslt, sizeof(reslt), 0);
                        }
                    }

                }   
                else if(strcmp(temp1, "exis") == 0 ||
                        strcmp(temp2, "exis") == 0)
                {
                    printf("%s already exists\n", m_pkt.fname);
                    sprintf(reslt, "output :: %s already exists", m_pkt.fname);
                    send(*csock, (void *)reslt, sizeof(reslt), 0);
                }
                else
                {
                    printf("%s create failed\n", m_pkt.fname);
                    sprintf(reslt, "output :: %s create failed", m_pkt.fname);
                    send(*csock, (void *)reslt, sizeof(reslt), 0);
                }
            }

            //delete command
            else if(strcmp(m_pkt.cmd, "delete") == 0)
            {
                char reslt[1024] = {0};
                if(strncmp(temp1, "Succ", 4) == 0 &&
                   strncmp(temp2, "Succ", 4) == 0)
                {
                    char filePath[100] = {0};
                    strcpy(filePath, m_pkt.context);
                    strcat(filePath, "/");
                    strcat(filePath, m_pkt.fname);

                    int status = remove(filePath);
                    if(status == 0)
                    {
                        offset = 0;
                        printf("%s is deleted\n", m_pkt.fname);
                        sprintf(reslt, "output :: %s is deleted", m_pkt.fname);
                        send(*csock, (void *)&reslt, sizeof(reslt), 0);
                    }
                    else
                    {
                        printf("unable to delete %s\n", m_pkt.fname);
                        sprintf(reslt, "output :: %s delete failed", m_pkt.fname);
                        send(*csock, (void *)&reslt, sizeof(reslt), 0);
                    }
                }
                else
                {
                    printf("%s delete failed\n", m_pkt.fname);
                    sprintf(reslt, "output :: %s delete failed", m_pkt.fname);
                    send(*csock, (void *)reslt, sizeof(reslt), 0);
                }

            }

            //write command
            else if(strcmp(m_pkt.cmd, "write") == 0)
            {
                char reslt[1024] = {0};
                char filePath[100] = {0};
                strcpy(filePath, m_pkt.context);
                strcat(filePath, "/");
                strcat(filePath, m_pkt.fname);
                
                if(strcmp(temp1, "Succ") == 0 &&
                   strcmp(temp2, "Succ") == 0)
                { 
                    /*if(offset > 0)
                    {
                        fd = open(filePath, O_RDWR, 0644);
                        lseek(fd, offset, SEEK_SET);
                    }
                    else*/
                    {
                        fd = open(filePath, O_RDWR|O_APPEND, 0644);
                    } 
                    
                    int len = write(fd, m_pkt.data, strlen(m_pkt.data));
                    if(len > 0)
                    {
                        printf("A string is written in %s\n", m_pkt.fname);
                        sprintf(reslt, "output :: A string is written to %s", m_pkt.fname);
                        send(*csock, (void *)&reslt, sizeof(reslt), 0);
                    }
                    else
                    {
                        printf("write operation failed for %s\n", m_pkt.fname);
                        sprintf(reslt, "output :: write operation failed for %s",m_pkt.fname);
                        send(*csock, (void *)&reslt, sizeof(reslt), 0);
                    }            
                }
                else
                {
                    printf("write operation failed for %s\n", m_pkt.fname);
                    sprintf(reslt, "output :: write operation failed for %s",m_pkt.fname);
                    send(*csock, (void *)&reslt, sizeof(reslt), 0);
                }
            }

            //seek command
            else if(strcmp(m_pkt.cmd, "seek") == 0)
            {
                char reslt[1024] = {0};
                char filePath[100] = {0};
                strcpy(filePath, m_pkt.context);
                strcat(filePath, "/");
                strcat(filePath, m_pkt.fname);

                if(strcmp(temp1, "Succ") == 0 &&
                   strcmp(temp2, "Succ") == 0)
                {
                    fd = open(filePath, O_RDONLY);
                    offset = lseek(fd, atoi(m_pkt.data), SEEK_SET);
                    printf("Cursor is moved to index %d in %s\n", offset, m_pkt.fname);
                    sprintf(reslt, "output :: Cursor is moved to index %d in %s", offset, m_pkt.fname);
                    send(*csock, (void *)&reslt, sizeof(reslt), 0);
                }
                else
                {
                    printf("Seek command failed for %s : negative offset %d\n", m_pkt.fname, offset);
                    sprintf(reslt, "output :: seek failed for %s : offset was %d\n", m_pkt.fname, offset);
                    send(*csock, (void *)&reslt, sizeof(reslt), 0);
                }
            }

            //read command
            else if(strcmp(m_pkt.cmd, "read") == 0)
            {
                char filePath[100] = {0};
                char reslt[1024] = {0};
                strcpy(filePath, m_pkt.context);
                strcat(filePath, "/");
                strcat(filePath, m_pkt.fname);
                fd = open(filePath, O_RDONLY);
                //move the cursor to last seek
                lseek(fd, offset, SEEK_SET);
                int len;

                if(m_pkt.optParam)
                    len = atoi(m_pkt.data);
                else
                {
                    struct stat st;
                    fstat(fd, &st);
                    len = st.st_size - offset;
                }
                char buf[len+1];
                memset(&buf, 0, len+1);
                read(fd, &buf, len);
                strcpy(reslt, "output :: \"");
                strcat(reslt, buf);
                strcat(reslt,"\"");
                send(*csock, (void *)&reslt, sizeof(reslt), 0);
            }
        }

        //if the message is coming from the master server
        else
        {
            if(strcmp(m_pkt.cmd, "create") == 0)
            {
                char filePath[100] = {0};
                strcpy(filePath, m_pkt.context);
                strcat(filePath, "/");
                strcat(filePath, m_pkt.fname);
                fd = open(filePath, O_RDWR|O_EXCL|O_CREAT, 0644);
                char temp[6] = {0};
                          
                if(fd != -1)
                {
                    printf("%s was created\n", m_pkt.fname);
                    strcpy(temp, "Succ");
                    send(*csock, (void *)&temp, sizeof(temp), 0);
                }
                else
                {
                    if(errno == EEXIST)
                    {
                        printf("%s already exists\n", m_pkt.fname);    
                        strcpy(temp, "exis");
                        send(*csock, (void *)&temp, sizeof(temp), 0);
                    }
                    else
                    {
                        printf("%s create failed\n", m_pkt.fname);
                        strcpy(temp, "fail");
                        send(*csock, (void *)&temp, sizeof(temp), 0);
                    }
                }
            }

            //delete command for the slave servers
            else if(strcmp(m_pkt.cmd, "delete") == 0)
            {
                char filePath[100] = {0};
                strcpy(filePath, m_pkt.context);
                strcat(filePath, "/");
                strcat(filePath, m_pkt.fname);
                char temp[6] = {0};

                int status = remove(filePath);
                if(status == 0)
                {
                    offset = 0;
                    printf("%s is deleted\n", m_pkt.fname);
                    strcpy(temp, "Succ");
                    send(*csock, (void *)&temp, sizeof(temp), 0);
                }
                else
                {
                    printf("unable to delete %s\n", m_pkt.fname);
                    strcpy(temp, "fail");
                    send(*csock, (void *)&temp, sizeof(temp), 0);
                }
            }

            //terminate command for the slave servers
            else if(strcmp(m_pkt.cmd, "terminate") == 0)
            {
                free(csock);
                close(fd);
                printf("*********  TERMINATING ***********\n");
                exit(0);
            }

            //write command for slave servers
            else if(strcmp(m_pkt.cmd, "write") == 0)
            {
                char filePath[100] = {0};
                strcpy(filePath, m_pkt.context);
                strcat(filePath, "/");
                strcat(filePath, m_pkt.fname);
                char temp[6] ={0};

                /*if(offset > 0)
                {
                    fd = open(filePath, O_RDWR, 0644);  
                    lseek(fd, offset, SEEK_SET);
                }
                else*/
                {
                    fd = open(filePath, O_RDWR|O_APPEND, 0644);
                }
                int len = write(fd, m_pkt.data, strlen(m_pkt.data));
                if(len > 0)
                {
                    printf("A string is written in %s\n", m_pkt.fname);
                    strcpy(temp, "Succ");
                    send(*csock, (void *)&temp, sizeof(temp), 0);
                }
                else
                {
                    printf("write operation failed for %s\n", m_pkt.fname);
                    strcpy(temp, "Fail");
                    send(*csock, (void *)&temp, sizeof(temp), 0);
                }
            }

            //seek command for slave servers
            else if(strcmp(m_pkt.cmd, "seek") == 0)
            {
                char filePath[100] ={0};
                char temp[6] = {0};
                strcpy(filePath, m_pkt.context);
                strcat(filePath, "/");
                strcat(filePath, m_pkt.fname);
                fd = open(filePath, O_RDONLY);
                offset = lseek(fd, atoi(m_pkt.data), SEEK_SET);
                if(offset < 0)
                {
                    printf("Seek failed : negative offset\n");
                    strcpy(temp, "Fail");
                    send(*csock, (void *)&temp, sizeof(temp), 0);
                }
                else
                {
                    printf("Cursor is moved to index %d in %s\n", offset, m_pkt.fname);
                    strcpy(temp, "Succ");
                    send(*csock, (void *)&temp, sizeof(temp), 0);
                }
            }

            //read command will be handled by master server itself.
            else if(strcmp(m_pkt.cmd, "read") == 0)
            {

            }

            //validation for write command
            else if(strcmp(m_pkt.cmd, "W_Valid") == 0)
            {
                char filePath[100] = {0};
                strcpy(filePath, m_pkt.context);
                strcat(filePath, "/");
                strcat(filePath, m_pkt.fname);
                char temp[6] = {0};
                if(access(filePath, F_OK) == -1)
                {
                    printf("%s doesn't exists\n", m_pkt.fname);
                    sprintf(temp, "Fail");
                    send(*csock, (void *)temp, sizeof(temp), 0);                    
                }
                else
                {
                    sprintf(temp, "Good");
                    send(*csock, (void *)temp, sizeof(temp), 0);
                }
            }

            //validation for create command
            else if(strcmp(m_pkt.cmd, "C_Valid") == 0)
            {
                char filePath[100] = {0};
                strcpy(filePath, m_pkt.context);
                strcat(filePath, "/");
                strcat(filePath, m_pkt.fname);
                char temp[6] = {0};
                if(access(filePath, F_OK) != -1)
                {
                    printf("%s already exists\n", m_pkt.fname);
                    sprintf(temp, "Fail");
                    send(*csock, (void *)temp, sizeof(temp), 0);                    
                }
                else
                {
                    sprintf(temp, "Good");
                    send(*csock, (void *)temp, sizeof(temp), 0);
                }
            }

        } 
        memset(&m_pkt, 0, sizeof(m_pkt));     
    }
}
