/**
 * dht.c
 *
 * CS 470 Project 4
 *
 * Implementation for distributed hash table (DHT).
 *
 * Name: Beau Mueller, Mason Puckett
 *
 * 
 * 1. Yes, we consulted chatgpt for help with understanding general MPI method usage and generating test cases. 
 *    It was moderately helpful in the since that it would give us examples of MPI calls and uses but not a ton of direct application was gleamed. 
 * 
 * 2. Within our RPC protocol messages are distinguished by a field within a struct called "instruction" that allows us to tell what the proces is supposed to do
 *    with the information within the struct. The struct holds the instruction and a key and value representing what hash pair is related to the instruction. In both put
 *    and get point-to-point communication is utilized with a hash function on the key determining the owner of the said key-value pair which allows messages to be sent 
 *    confidently to the correct process. Our rpc protocol is all synchronous as the point-to-point communication is blocking on both the sending and receiving ends. Additionally,
 *    within the size command there is an MPI collective operation that we force every process into through point-to-point communication. The sync command is literally just a barrier
 *    which forces all processes to wait for the rest. 
 * 
 * 3. For input file 4 here are two resulting prints
 * 
 *      Get("a") = 1
        Get("b") = 2
        Get("i") = 13
        Get("c") = 3
        Size = 12
 * 
 *      Get("a") = 1
        Get("b") = 2
        Get("c") = 3
        Get("i") = 8
        Size = 12
 *  
 *    This input file has a put that updates a known key (i), the result from get varies based off which process puts for the key i first in ordering.
 * 
 * 4. The biggest difficulty in creating a sort that we anticipate would be correctly ditributing the sorted information. The proposed process would sort the values locally then 
 *    manually start to join together processes while sorting as we go. Since there is no MPI collective operation that allows for sorting to happen to the data there would be
 *    a need for every rank to stop and be added to the growing locally sorted list. Then scatter the data evenly back to the separate processes.
 * 
 *     Psuedocode:
 *     
 *      mergesort(): 
 *          local merge sort 
 * 
 *      mpi_merge():
 * 
 *          sorted_values = []
 * 
 *          if rank 0: 
 *              merge_sort(local)
 *              add rank 0's values
 * 
 *          for all procs
 *              if not rank 0: 
 *                  merge_sort(local)
 *                  merge_sort(procs_local, sorted_values)
 * 
 *          #rank 0 would have all of the sorted values at this point
 *          #could return or do whatever, this distributes it back to the processes
 * 
 *          MPI_Scatter 
 * 
 *      end
 * 
 * 5. 
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

pthread_t server;       // server thread
bool executing = false; // are we still using the hash table?
size_t global_size = 0; // global size variable

/*
 * Struct to hold key value pair
 */
struct pair_t
{
    int instruction;
    long value;
    char key[MAX_KEYLEN];
};

// stolen from main
size_t strnlen(const char *str, size_t max_len);
/*
 * given a key name, return the distributed hash table owner
 * (uses djb2 algorithm: http://www.cse.yorku.ca/~oz/hash.html)
 */
int hash(const char *name)
{
    unsigned hash = 5381;
    while (*name != '\0')
    {
        hash = ((hash << 5) + hash) + (unsigned)(*name++);
    }
    return hash % nprocs;
}

/*
 * Server threads wait here
 */
void *server_func(void *arg)
{
    while (executing)
    {

        struct pair_t pair;
        MPI_Status stat;

        MPI_Recv(&pair, sizeof(struct pair_t), MPI_BYTE, MPI_ANY_SOURCE, 0, MPI_COMM_WORLD, &stat);

        if (pair.instruction == PUT)
        {
            local_put(pair.key, pair.value);
        }
        else if (pair.instruction == GET)
        {
            struct pair_t info;
            snprintf(info.key, strlen(pair.key) + 1, pair.key);
            info.value = local_get(pair.key);

            MPI_Ssend(&info, sizeof(struct pair_t), MPI_BYTE, stat.MPI_SOURCE, 1, MPI_COMM_WORLD);
        }
        else if (pair.instruction == SIZE)
        {

            size_t s = local_size();
            
            MPI_Reduce(&s, &global_size, 1, MPI_UNSIGNED_LONG, MPI_SUM, stat.MPI_SOURCE, MPI_COMM_WORLD);
        }
        else if (pair.instruction == DONE)
        {
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
    if (provided != MPI_THREAD_MULTIPLE)
    {
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

    if (my_rank == owner)
    {
        // lock
        local_put(key, value);
        // unlock
    }
    else
    {
        MPI_Ssend(&pair, sizeof(struct pair_t), MPI_BYTE, owner, 0, MPI_COMM_WORLD);
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
    int owner = hash(key);

    if(my_rank == owner){
        return local_get(key);
    }else{

        struct pair_t pair;
        pair.instruction = GET;
        strncpy(pair.key, key, sizeof(char[MAX_KEYLEN]) - 1);

        MPI_Ssend(&pair, sizeof(struct pair_t), MPI_BYTE, owner, 0, MPI_COMM_WORLD);
   
        struct pair_t info;
        MPI_Status status;

        MPI_Recv(&info, sizeof(struct pair_t), MPI_BYTE, owner, 1, MPI_COMM_WORLD, &status);

        return info.value;
    }
}

/*
 * Returns the total size of the DHT.
 *
 * (In the parallel version, this should perform MPI communication as necessary
 * to complete the DHT operation.)
 */
size_t dht_size()
{

    struct pair_t pair;
    pair.instruction = SIZE;

    for(int i = 0; i < nprocs; i++){
        if(my_rank == i){
            continue;
        }
        MPI_Ssend(&pair, sizeof(struct pair_t), MPI_BYTE, i, 0, MPI_COMM_WORLD);
    }

    size_t s = local_size();
    
    MPI_Reduce(&s, &global_size, 1, MPI_UNSIGNED_LONG, MPI_SUM, my_rank, MPI_COMM_WORLD);

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

    MPI_Ssend(&pair, sizeof(struct pair_t), MPI_BYTE, (my_rank + nprocs % nprocs), 0, MPI_COMM_WORLD);

    pthread_join(server, NULL);
    local_destroy(output);
}

