#include <stdio.h>
#include <mpi.h>
#include <stdlib.h>
//#include "user_case.h"

#define FILE_NAME "wikipedia_test_small.txt"
#define BLOCKSIZE 10

int main(int argc, char** argv){
    int rank, num_ranks,i;
    MPI_File fh;
    MPI_Offset file_size, file_count;
    char* buffer;
    char* recver;
        
    MPI_Init(&argc, &argv);
    //get rank or other head operations
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &num_ranks);
    
    /***map and reduce phase***/
    
    //Initialize loop count
    if(rank == 0){
        //MPI_File_open(MPI_COMM_SELF, FILE_NAME, MPI_MODE_RDWR,MPI_INFO_NULL,&fh);
        //MPI_File_get_size(fh, &file_size);
        printf("size is %d \n", file_size);
    }
    //use bordcast to pass size of the file
    /*MPI_Bcast(&file_size, 1, MPI_INT, 0, MPI_COMM_WORLD);
        
    buffer = malloc(sizeof(char)*BLOCKSIZE*num_ranks);
    file_count = 0;
            
    while(file_count < file_size - BLOCKSIZE){        //file is not drain after this draw
        //reading from input file
        if(rank == 0){
            MPI_File_read(fh,buffer+BLOCKSIZE,BLOCKSIZE*(num_ranks - 1),MPI_CHAR,MPI_STATUS_IGNORE);    
            //advance offset  
            file_count = file_count +  BLOCKSIZE*(num_ranks - 1);   
            MPI_File_seek(fh, file_count, MPI_SEEK_SET);
        }
        MPI_Scatter(buffer, BLOCKSIZE, MPI_CHAR, recver, BLOCKSIZE, MPI_CHAR, 0, MPI_COMM_WORLD);
            printf("rank %d receiving: ", rank);
            for(i=0;i<BLOCKSIZE;i++){
                printf("%c", *(recver+i));
            }
            printf("\n");
        
        //call map function in all non-root ranks
    }*/
    
    //free(buffer);
        
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
