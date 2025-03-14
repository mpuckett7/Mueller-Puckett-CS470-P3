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
static const int PUT = 0;
static const int GET = 1;
static const int SIZE = 2;
static const int DONE = 3;

pthread_t server;           // server thread
bool executing = false;     // are we still using the hash table?
// bool server_bool = false;   // condition variable failsafe
// pthread_cond_t server_cond = PTHREAD_COND_INITIALIZER;
// pthread_mutex_t server_lock = PTHREAD_MUTEX_INITIALIZER;

/*
 * Struct to hold key value pair 
 */
struct pair_t {
    int instruction;
    long value;
    char key[MAX_KEYLEN];
};

/*
 * given a key name, return the distributed hash table owner
 * (uses djb2 algorithm: http://www.cse.yorku.ca/~oz/hash.html)
 */
int hash(const char *name)
{
    unsigned hash = 5381;
    while (*name != '\0') {
        hash = ((hash << 5) + hash) + (unsigned)(*name++);
    }
    return hash % nprocs;
}

/*
 * Server threads wait here
 */
void *server_func(void *arg) {
    while(executing){
        // pthread_mutex_lock(&server_lock);
        // while(!server_bool) {
        //     pthread_cond_wait(&server_cond, &server_lock);
        // }
        // // do a single task, go back to waiting

        // pthread_mutex_unlock(&server_lock);

        struct pair_t pair;
        
        MPI_Recv(&pair, sizeof(pair), MPI_BYTE, MPI_ANY_SOURCE, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

        if(pair.instruction == PUT){
            local_put(pair.key, pair.value);
        }else if(pair.instruction == GET){

        }else if(pair.instruction == SIZE){

        }else if(pair.instruction == DONE){
            break;
        }
    }
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
    int owner = hash(key);

    struct pair_t pair;
    pair.instruction = PUT;
    snprintf(pair.key, strlen(key) + 1, key);
    pair.value = value;

    if(my_rank == owner){
        //lock
        local_put(key, value);
        //unlock
    }else{
        MPI_Ssend(&pair, sizeof(pair), MPI_BYTE, owner, 0, MPI_COMM_WORLD); 
    }
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
    size_t global_size = 0;

    printf("before local size call\n");
    size_t tmp = local_size();
    printf("after local_size call\n");
    printf("%ld\n", tmp);

    printf("Process %d: Before MPI_Reduce, tmp = %lu\n", my_rank, tmp);
    fflush(stdout);

    MPI_Reduce(&tmp, &global_size, 1, MPI_UNSIGNED_LONG, MPI_SUM, 0, MPI_COMM_WORLD);
    
    printf("Process %d: After MPI_Reduce\n", my_rank);
    fflush(stdout);

    return global_size;
}


/*
 * Synchronize all client processes involved in the DHT. This function should
 * not return until other all client processes have also called this function.
 *
 * (In the parallel version, this should essentially be a global barrier.)
 */
void dht_sync()
{
    // idrk what im doing nor do I know how to test this, probably a different input file
    // something like this? that forces y to be said after x
    // put x
    // put y
    // get x
    // sync
    // get y
    MPI_Barrier(MPI_COMM_WORLD);
}


/*
 * Dump contents and clean up the hash table.
 *
 * (In the parallel version, this should terminate the server thread.)
 */
void dht_destroy(FILE *output)
{
    MPI_Barrier(MPI_COMM_WORLD);

    struct pair_t pair;
    pair.instruction = DONE;

    executing = false;

    MPI_Ssend(&pair, sizeof(pair), MPI_BYTE, (my_rank + nprocs % nprocs), 0, MPI_COMM_WORLD);

    // server_bool = true;
    // pthread_cond_signal(&server_cond);
    pthread_join(server, NULL);
    local_destroy(output);
}

