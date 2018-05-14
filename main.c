#include <stdio.h>
#include <mpi.h>
#include <stdlib.h>
#include "user_case.h"

#define FILE_RMODE (MPI_MODE_RDONLY)
#define FILE_NAME "wikipedia_test_small.txt"

#define DEBUG_ON_MASTER
#define DEBUG_LEN_STR 10 //Max string length we allow the master to read for debugging purposes

int main(int argc, char** argv)
{
  MPI_Init(&argc, &argv);
  MPI_File fh;
  int rank, num_ranks;
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  MPI_Comm_size(MPI_COMM_WORLD, &num_ranks);

  // READING THE INPUT
  if (rank==0)
  {
    MPI_Offset file_size;   // Apparently MPI_Offset is just a long long int (%lld)
    MPI_File_open(MPI_COMM_SELF, FILE_NAME, FILE_RMODE,MPI_INFO_NULL,&fh);
    MPI_File_get_size(fh, &file_size);

    int task_len=DEBUG_LEN_STR;
    if (task_len>file_size) task_len=file_size;
    char task[task_len];
    MPI_File_read(fh,task,task_len,MPI_CHAR,MPI_STATUS_IGNORE);
    MPI_File_close(&fh);
    for (int i=0;i<task_len;i++){
      printf("%c",task[i]);
      printf("%d\n",task[i]);
    }
    printf("\n");
    KEYVAL word;
    Map(task,word)
    printf("%s\n",word.key);
  }

  MPI_Finalize();
  return(0);
}

