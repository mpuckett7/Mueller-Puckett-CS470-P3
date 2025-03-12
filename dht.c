/**
 * dht.c
 *
 * CS 470 Project 4
 *
 * Implementation for distributed hash table (DHT).
 *
 * Name: Beau Mueller, Mason Puckett
 *
 */

#include <mpi.h>
#include <pthread.h>

#include "dht.h"

/*
 * Private module variable: current process ID (MPI rank)
 */
static int my_rank;
static int nprocs;

pthread_t server;           // server thread
bool executing = false;     // are we still using the hash table?
bool server_bool = false;   // condition variable failsafe
pthread_cond_t server_cond = PTHREAD_COND_INITIALIZER;
pthread_mutex_t server_lock = PTHREAD_MUTEX_INITIALIZER;

/*
 * Server threads wait here
 */
void *server_func(void *arg) {
    while(executing){
        while(!server_bool) {
            printf("im in herebbbbbbbbbb\n");
            pthread_mutex_lock(&server_lock);
            pthread_cond_wait(&server_cond, &server_lock);
            pthread_mutex_unlock(&server_lock);
        }
        // do a single task, go back to waiting
        printf("im in hereaaaaaaaa\n");
    }
    printf("i'm about to return\n");
    return NULL;
}
/*
 * Initialize a new hash table. Returns the current process ID (always zero in
 * the serial version)
 *
 * (In the parallel version, this should spawn the server thread.)
 */
int dht_init()
{
    int provided;

    MPI_Init_thread(NULL, NULL, MPI_THREAD_MULTIPLE, &provided);
    if (provided != MPI_THREAD_MULTIPLE) {
        printf("ERROR: Cannot initialize MPI in THREAD_MULTIPLE mode.\n");
        exit(EXIT_FAILURE);
    }
    MPI_Comm_size(MPI_COMM_WORLD, &nprocs);
    MPI_Comm_rank(MPI_COMM_WORLD, &my_rank);

    executing = true;
    pthread_create(&server, NULL, server_func, NULL);
    local_init();

    return my_rank;
}


/*
 * Save a key-value association. If the key already exists, the assocated value
 * is changed in the hash table. If the key does not already exist, a new pair
 * is created with the given value.
 *
 * (In the parallel version, this should perform point-to-point MPI
 * communication as necessary to complete the DHT operation.)
 */
void dht_put(const char *key, long value)
{
    local_put(key, value);
}

/*
 * Retrieve a value given a key. If the key is found, the resulting value is
 * returned. Otherwise, the function should return KEY_NOT_FOUND.
 *
 * (In the parallel version, this should perform point-to-point MPI
 * communication as necessary to complete the DHT operation.)
 */
long dht_get(const char *key)
{
    return local_get(key);
}


/*
 * Returns the total size of the DHT.
 *
 * (In the parallel version, this should perform MPI communication as necessary
 * to complete the DHT operation.)
 */
size_t dht_size()
{
    return local_size();
}


/*
 * Synchronize all client processes involved in the DHT. This function should
 * not return until other all client processes have also called this function.
 *
 * (In the parallel version, this should essentially be a global barrier.)
 */
void dht_sync()
{
    // nothing to do in the serial version
}


/*
 * Dump contents and clean up the hash table.
 *
 * (In the parallel version, this should terminate the server thread.)
 */
void dht_destroy(FILE *output)
{
    executing = false;
    pthread_cond_signal(&server_cond);
    pthread_join(server, NULL);
    char *filename = (char*)malloc(sizeof(char) * 32);
    snprintf(filename, 32, "dump-%d.txt", my_rank);
    FILE *file = fopen(filename, "w");
    fprintf(file, "this should be in a bunch of files.\n");
    fclose(file);
    local_destroy(output);
}

