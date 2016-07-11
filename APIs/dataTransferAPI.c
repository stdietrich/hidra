// API to ingest data into a data transfer unit

#include <dataTransferAPI.h>
#include <zmq.h>
#include <stdio.h>
#include <unistd.h>
#include <string.h>
#include <stdlib.h>
#include <assert.h>
#include <errno.h>
#include <glib.h>


// helper functions for sending and receiving messages
// source: Pieter Hintjens: ZeroMQ; O'Reilly
static char* s_recv (void *socket)
{
    zmq_msg_t message;
    zmq_msg_init (&message);
    int size = zmq_msg_recv (&message, socket, 0);
    if (size == -1)
        return NULL;
    char *string = malloc (size + 1);
    memcpy (string, zmq_msg_data (&message), size);
    printf("recieved string: %s\n", string);
    zmq_msg_close (&message);
    string [size] = 0;
    return string;
}


static int s_send (void * socket, char* string)
{
    printf("Sending: %s\n", string);
    int size = zmq_send (socket, string, strlen (string), 0);
    if (size == -1) {
        printf("Sending failed\n");
//        fprintf (stderr, "ERROR: Sending failed\n");
//        perror("");
    };
    return size;

}


int recv_multipartMessage (void *socket, char **multipartMessage, int *len)
{
    int i = 0;
    int size;
    int more;
    size_t more_size = sizeof(more);
    while (1)
    {

        //  Wait for next request from client
        zmq_msg_t message;
        zmq_msg_init (&message);
        size = zmq_msg_recv (&message, socket, 0);

        //Process message
        multipartMessage[i] = malloc (size + 1);
        memcpy (multipartMessage[i], zmq_msg_data (&message), size);
//        printf("recieved string: %s\n", multipartMessage[i]);
        multipartMessage[i] [size] = 0;

        i++;

        zmq_msg_close(&message);
        zmq_getsockopt (socket, ZMQ_RCVMORE, &more, &more_size);
        if (more == 0)
        {
//            printf("last received\n");
            *len = i;
            break;
        };

    }

    return 0;

}


struct dataTransfer {

    char *localhost;
    char *extIp;
    char *ipcPath;

    char *signalHost;
    char *fileOpPort;
    char *dataHost;
    char *dataPort;

    //  Prepare our context and socket
    void *context;
    void *fileOpSocket;
    void *dataSocket;

    char *nexusStarted;

    int socketResponseTimeout;

    int numberOfStreams;
    char **recvdCloseFrom;
    char *replyToSignal;
    int allCloseRecvd;

    int runLoop;

//    char **fileDescriptors;

    int fileOpened;
    int callbackParams;
    int openCallback;
    int readCallback;
    int closeCallback;

    char *connectionType;

};


int dataTransfer_init (dataTransfer **out)
{
    dataTransfer* dT = malloc(sizeof(dataTransfer));

    *out = NULL;

    dT->localhost   = "localhost";
    dT->extIp       = "0.0.0.0";
    dT->ipcPath     = "/tmp/HiDRA";

    dT->signalHost  = "zitpcx19282";
    dT->fileOpPort  = "50050";
    dT->dataHost    = "zitpcx19282";
    dT->dataPort    = "50100";

    dT->nexusStarted = NULL;

    dT->socketResponseTimeout = 1000;

    dT->numberOfStreams  = 0;
    dT->recvdCloseFrom   = NULL;
    dT->replyToSignal    = NULL;
    dT->allCloseRecvd    = 0;

    dT->runLoop          = 1;
//    dT->fileDescriptors = NULL;

    dT->fileOpened       = 0;
    dT->callbackParams   = 0;
    dT->openCallback     = 0;
    dT->readCallback     = 0;
    dT->closeCallback    = 0;

    if ( (dT->context = zmq_ctx_new ()) == NULL )
    {
		perror("ERROR: Cannot create 0MQ context.\n");
		exit(9);
	}

    dT->allCloseRecvd = 0;

    *out = dT;

    return 0;

}


int dataTransfer_initiate (dataTransfer *dT, char **targets)
{
    char *signal;
    char *signalPort;
    char signalConId[128];

    return 0;
}


int dataTransfer_start (dataTransfer *dT)
{

    char socketIdToBind[128];
    char connectionStr[128];
/*
    if ( !dT->nexusStarted )
    {
        strcpy(socketIdToBind, dT->nexusStarted);
    }

    if ( !socketIdToBind )
    {
        printf ("Reopening already started connection.");
        return 0;
    }
    else
    {
*/        snprintf(socketIdToBind, sizeof(socketIdToBind), "%s:%s", dT->extIp, dT->dataPort);
//    }

    // Create data socket
    if ( (dT->dataSocket = zmq_socket (dT->context, ZMQ_PULL)) == NULL )
    {
		perror("ERROR: Could not create 0MQ dataSocket.\n");
		exit(9);
	}

    snprintf(connectionStr, sizeof(connectionStr), "tcp://%s", socketIdToBind);
    if ( zmq_bind(dT->dataSocket, connectionStr) )
    {
        printf("ERROR: Failed to start Socket of type %s (bind): '%s'\n", dT->connectionType, connectionStr);
//        perror("ERROR: Failed to start Socket of type %s (bind): '%s'", dT->connectionType, connectionStr);
    }
    else
    {
        printf("Data socket of type %s started (bind) for '%s'\n", dT->connectionType, connectionStr);
    }

    // Create socket for file operation exchanging
    if ( (dT->fileOpSocket = zmq_socket (dT->context, ZMQ_REP)) == NULL )
    {
        perror("ERROR: Could not create 0MQ fileOpSocket.\n");
        exit(9);
    }

    snprintf(connectionStr, sizeof(connectionStr), "tcp://%s:%s", dT->extIp, dT->fileOpPort);
    if ( zmq_bind(dT->fileOpSocket, connectionStr) )
    {
        printf("ERROR: Failed to start Socket of type %s (bind): '%s'\n", dT->connectionType, connectionStr);
//        perror("ERROR: Failed to start Socket of type %s (bind): '%s'", dT->connectionType, connectionStr);
    }
    else
    {
        printf("File operation socket started (bind) for '%s'\n", connectionStr);
    }

    dT->nexusStarted = socketIdToBind;

    return 0;
}


int dataTransfer_read (dataTransfer *dT, char *data, int size)
{
    zmq_pollitem_t items [] = {
        { dT->fileOpSocket,   0, ZMQ_POLLIN, 0 },
        { dT->dataSocket, 0, ZMQ_POLLIN, 0 }
    };
    char *message;
    int rc;
    char *multipartMessage[2];
    int i;
    int len;

    dT->runLoop = 1;

    while (dT->runLoop)
    {
//        printf ("polling\n");
        zmq_poll (items, 2, -1);

        if (items [0].revents & ZMQ_POLLIN)
        {
            printf ("fileOpSocket is polling\n");

            message = s_recv (dT->fileOpSocket);
            printf ("fileOpSocket recv: '%s'\n", message);

            if (strcmp(message,"CLOSE_FILE") == 0)
            {
                if ( dT->allCloseRecvd )
                {
                    rc = zmq_send (dT->fileOpSocket, message, strlen(message), 0);
                    printf("fileOpSocket send: %s\n", message);
                    dT->runLoop = 0;
                    break;
                }
                else
                {
                    dT->replyToSignal = message;
                }
            }
            else if (strcmp(message,"OPEN_FILE") == 0)
            {
                rc = zmq_send (dT->fileOpSocket, message, strlen(message), 0);
                printf("fileOpSocket send: %s\n", message);

                dT->allCloseRecvd = 0;
                //TODO
//                dT->openCallback(dT->callbackParams, message);
//                dT->fileOpened = 1;
            }
            else
            {
                rc = zmq_send (dT->fileOpSocket, "ERROR", strlen("ERROR"), 0);
                printf("Not supported message received\n");
            }
        }

        if (items [1].revents & ZMQ_POLLIN)
        {
            printf ("dataSocket is polling\n");

            rc = recv_multipartMessage (dT->dataSocket, multipartMessage, &len);
            printf ("multipartMessage[0]=%s\nmultipartMessage[1]=%s\n",
                    multipartMessage[0], multipartMessage[1]);

            if (len < 2)
            {
                perror ("Received mutipart-message is too short. Either config or file content is missing.");
                //TODO correct errorcode
                return -1;
            }

            if (multipartMessage[1] == "ALIVE_TEST")
            {
                continue;
            }


            rc = reactOnMessage (dT, multipartMessage);

            for (i = 0; i < 2; i++)
            {
                free(multipartMessage[i]);
            };

        }
    }
}


int reactOnMessage (dataTransfer *dT, char **multipartMessage)
{
    char *id;
    int rc;
    int idNum;
    char **splitRes;
    int i;
    int totalRecvd = 0;

    if (strcmp(multipartMessage[0], "CLOSE_FILE") == 0)
    {
        id = multipartMessage[1];

        splitRes = g_strsplit (id, "/", 2);
        idNum = atoi(splitRes[0]);

        // get number of signals to wait for
        if (dT->numberOfStreams == 0)
        {
            dT->numberOfStreams = atoi(splitRes[1]);

            dT->recvdCloseFrom = malloc(sizeof(char*) * dT->numberOfStreams);
            for (i = 0; i < dT->numberOfStreams; i++)
            {
                dT->recvdCloseFrom[i] = NULL;
            }
        }

        dT->recvdCloseFrom[idNum] = strdup(id);
        printf("Received close-file-signal from DataDispatcher-%s\n", id);

        // have all been signals arrived?
        printf("numberOfStreams=%i\n", dT->numberOfStreams);
        for (i = 0; i < dT->numberOfStreams; i++)
        {
            printf("recvdCloseFrom[%i]=%s\n", i, dT->recvdCloseFrom[i]);
            printf("something\n");

            if (dT->recvdCloseFrom[i] != NULL)
            {
                totalRecvd++;
                printf ("not NULL, totalRecvd=%i\n", totalRecvd);
            }
        }

        printf ("before if\n");
        if ( totalRecvd >= dT->numberOfStreams)
        {
            printf("All close-file-signals arrived\n");
            dT->allCloseRecvd = 1;

            printf("replyToSignal: %s\n", dT->replyToSignal);
            if (dT->replyToSignal)
            {
                printf("replyToSignal not NULL\n");
                rc = zmq_send (dT->fileOpSocket, dT->replyToSignal, strlen(dT->replyToSignal), 0);
                printf("fileOpSocket send: %s\n", dT->replyToSignal);

                dT->replyToSignal = NULL;
                free(dT->recvdCloseFrom);
                dT->runLoop = 0;
            }

//            dT->closeCallback(dT->callbackParams, dT->multipartMessage)
        }
        else
        {
            printf("Still close events missing\n");
        }
    }
    else
    {
/*
        //TODO convert from python
        //extract multipart message
        try:
            //TODO exchange cPickle with json
            metadata = cPickle.loads(multipartMessage[0])
        except:
            self.log.error("Could not extract metadata from the multipart-message.", exc_info=True)
            metadata = None

        try:
            payload = multipartMessage[1:]
        except:
            self.log.warning("An empty file was received within the multipart-message", exc_info=True)
            payload = None

        self.readCallback(self.callbackParams, [metadata, payload])
*/
    }

    return 0;
}


int dataTransfer_stop (dataTransfer *dT)
{

    printf ("closing fileOpSocket...\n");
    zmq_close(dT->fileOpSocket);
    printf ("closing dataSocket...\n");
    zmq_close(dT->dataSocket);
//            self.log.error("closing ZMQ Sockets...failed.", exc_info=True)

    printf ("Closing ZMQ context...\n");
    zmq_ctx_destroy(dT->context);
//                self.log.error("Closing ZMQ context...failed.", exc_info=True)

//    free (dT);
    printf ("Cleanup finished.\n");

    return 0;
}

