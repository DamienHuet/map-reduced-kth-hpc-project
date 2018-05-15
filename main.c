#include <stdio.h>
#include <mpi.h>
#include <stdlib.h>
#include "user_case.h"

// Relevant macros for the working program
#define FILE_RMODE (MPI_MODE_RDONLY)

//  Relevant macros for debugging purposes
#define DEBUG_ON_MASTER
//Max string length we allow the master to read for debugging purposes
#define DEBUG_LEN_STR 10
// Debugging on the small test file
#define FILE_NAME "wikipedia_test_small.txt"
#define DEBUG_RAW_DATA_SCATTER 0
#define DEBUG_MAP 1
#define BLOCKSIZE 10

int main(int argc, char** argv){
    MPI_Init(&argc, &argv);

    //get rank or other head operations
    int rank, num_ranks;
    int i=0;
    MPI_File fh;
    MPI_Offset file_size, file_count;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &num_ranks);

    char* buffer;
    char* recver;
    //KEYVAL word;

    /***map and reduce phase***/

    //Initialize loop count
    if(rank == 0){
        MPI_File_open(MPI_COMM_SELF, FILE_NAME, FILE_RMODE,MPI_INFO_NULL,&fh);
        //--- It would be smart to include a test in case we fail to open the file ---//
        MPI_File_get_size(fh, &file_size); //Length of the file in bytes
        printf("Size is %lld.\n", file_size);
    }
    //use broadcast to pass size of the file
    MPI_Bcast(&file_size, 1, MPI_INT, 0, MPI_COMM_WORLD);

    buffer = new char[BLOCKSIZE*num_ranks];
    KEYVAL* word = new KEYVAL[1];
    file_count = 0;

    while(file_count < file_size - BLOCKSIZE){        //file is not drain after this draw
        //reading from input file
        if(rank == 0){
            MPI_File_read(fh,buffer+BLOCKSIZE,BLOCKSIZE*(num_ranks - 1),MPI_CHAR,MPI_STATUS_IGNORE);
        }
        MPI_Scatter(buffer, BLOCKSIZE, MPI_CHAR, recver, BLOCKSIZE, MPI_CHAR, 0, MPI_COMM_WORLD);
            #if DEBUG_RAW_DATA_SCATTER
                printf("rank %d receiving: ", rank);
                // for(i=0;i<BLOCKSIZE;i++){
                //     printf("%c", *(recver+i));
                // }
            #endif

        //call map function in all non-root ranks
        #if DEBUG_MAP
            if (rank==1)    //Debug on process 1 (as process 0 doesn't have valid data to process)
            {
                printf("Hello\n"); 
                fflush(stdout);          
                int block_count=0;
                word->val=5;
                // Map(recver,BLOCKSIZE,&block_count,word);
                // printf("%s\n",word->key);
                // delete[] word->key;
            }
        #endif

        if(rank==0){
            //advance offset
            file_count = file_count +  BLOCKSIZE*(num_ranks - 1);
            MPI_File_seek(fh, file_count, MPI_SEEK_SET);
        }
        // MPI_Barrier(MPI_COMM_WORLD);
        MPI_Bcast(&file_count, 1, MPI_OFFSET, 0, MPI_COMM_WORLD);
    }

    free(buffer);

    //synchronize all ranks

    /*
    till now, <key, value> should be stored in each process.
    They are orgainized according to target process in combine phase.
    */
    //data type to be specified

    /***combine phase***/
    //initialize arrays for all to all transmission
    //call all to all

    //call reduce

    //use gather to acquire result
    MPI_Finalize();

    return 0;
}
