#include <stdio.h>
#include <mpi.h>
#include <stdlib.h>
//#include "user_case.h"

#define FILE_RMODE (MPI_MODE_RDONLY)
#define FILE_NAME "wikipedia_test_small.txt"
#define BLOCKSIZE 10
//debug configuration
#define DEBUG_SCATTER 1
#define DEBUG_MAP 0

int main(int argc, char** argv){
    //get rank or other head operations
    int rank, num_ranks;
    int i=0;   
    char* buffer;
    char* recver;
    
    MPI_Init(&argc, &argv);
    MPI_File fh;
    MPI_Offset file_size, file_count;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &num_ranks);

    /***Initial and boardcast phase***/
    if(rank == 0){
        MPI_File_open(MPI_COMM_SELF, FILE_NAME, FILE_RMODE,MPI_INFO_NULL,&fh);
        //--- It would be smart to include a test in case we fail to open the file ---//
        MPI_File_get_size(fh, &file_size);      
    }
    MPI_Bcast(&file_size, 1, MPI_LONG_LONG, 0, MPI_COMM_WORLD);
    //printf("Size is %lld.\n", file_size);

    /***Local map and reduce phase***/
    //all variables created here
    buffer = new char[BLOCKSIZE*num_ranks];
    #if DEBUG_SCATTER
        char* outbuf = new char[BLOCKSIZE+1];
    #endif
    file_count = 0;

    while(file_count < file_size - BLOCKSIZE){    
        //reading from input file
        if(rank == 0){
            MPI_File_read(fh,buffer+BLOCKSIZE,BLOCKSIZE*(num_ranks - 1),MPI_CHAR,MPI_STATUS_IGNORE);
        }
        MPI_Scatter(buffer, BLOCKSIZE, MPI_CHAR, recver, BLOCKSIZE, MPI_CHAR, 0, MPI_COMM_WORLD);
            #if DEBUG_SCATTER
                if(rank != 0){
                    for(i=0;i<BLOCKSIZE;i++){
                        if(*(recver+i) == '\n' ){
                            outbuf[i] = 'E';                          
                        }else if(*(recver+i) == '\t'){
                            outbuf[i] = 'T';
                        }else{
                            outbuf[i] = *(recver+i);
                        }
                    }
                    outbuf[BLOCKSIZE] = '\0';
                    printf("rank %d receiving: %s \n", rank, outbuf);   
                }
            #endif

            #if DEBUG_MAP
                if (rank==1)    //Debug on process 1
                {
                    //map function here
                    //don't define variable inside while loop
                }
            #endif

        //advance offset
        file_count = file_count +  BLOCKSIZE*(num_ranks - 1);
        if(rank==0){
            MPI_File_seek(fh, file_count, MPI_SEEK_SET);
        }
    }
    
    #if DEBUG_SCATTER
        delete [] outbuf;
    #endif

    delete [] buffer;

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
