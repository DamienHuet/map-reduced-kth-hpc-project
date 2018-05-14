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
#define FILE_NAME "../test_files/wikipedia_test_small.txt"

int main(int argc, char** argv){
    MPI_Init(&argc, &argv);
    //get rank or other head operations
    MPI_File fh;
    int rank, num_ranks;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &num_ranks);

    /***map and reduce phase***/
    if (rank==0)
    {
      MPI_Offset file_size_bytes;   // Apparently MPI_Offset is just a long long int (%lld)
      MPI_File_open(MPI_COMM_SELF, FILE_NAME, FILE_RMODE,MPI_INFO_NULL,&fh);
      //--- It would be smart to include a test in case we fail to open the file ---//
      MPI_File_get_size(fh, &file_size_bytes); //Length of the file in bytes

      int task_len=DEBUG_LEN_STR;
      if (task_len>file_size_bytes/sizeof(char)) task_len=file_size_bytes/sizeof(char);
      char* task = (char*) malloc(task_len*sizeof(char));
      MPI_File_read(fh,task,task_len,MPI_CHAR,MPI_STATUS_IGNORE);

      for (int i=0;i<task_len;i++){
        printf("%c",task[i]);
      }
      printf("\n");

      KEYVAL word;
      Map(task,task_len,&word);
      printf("%s\n",word.key);

      // Free the key, which was dynamically allocated
      delete[] word.key;

      MPI_File_close(&fh);
    }

      /*while(){        //file is not drain after this draw
        if(rank == 0){
            //reading from input file
        }

        //using scatter(add invalid data in the front to send to root), or scatterv
        //root won't receive valid data
        //data type to be specified

        if(rank != 0){
            //while task (file block is not drain)
                //call map
                //advance offset
                //call hash function to compute target rank
                //store <key,value> into buckets for each rank
                //remember checking redundancy
        }
        //data type to be specified
    }


    //Till now, <key, value> should be stored in each process.
    //They are orgainized according to target process in combine phase.

    //data type to be specified

    //---- Combine phase
    //initialize arrays for all to all transmission
    //call all to all

    //call reduce

    //use gather to acquire result
    */
    MPI_Finalize();
    return(0);
}
