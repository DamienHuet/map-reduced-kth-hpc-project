#include <stdio.h>
#include <mpi.h>
#include <stdlib.h>
#include "user_case.h"

#define FILE_RMODE (MPI_MODE_RDONLY)
#define FILE_NAME "wikipedia_test_small.txt"
#define BLOCKSIZE 10
//debug configuration
#define DEBUG_SCATTER 0
#define DEBUG_MAP 1

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
    KEYVAL* words = new KEYVAL[file_size/(2*(num_ranks-1))];  //Worst case scenario is there are one new word every two characters
    int block_count;
    int count_words=0;
    int prev_block_count;
    #if DEBUG_SCATTER
        char* outbuf = new char[BLOCKSIZE+1];
    #endif
    file_count = 0;

    #if DEBUG_MAP
        if (rank==1) printf("Data received by process 1:\n");
    #endif

    while(file_count < file_size - BLOCKSIZE){
        //reading from input file
        if(rank == 0){
            MPI_File_read(fh,buffer+BLOCKSIZE,BLOCKSIZE*(num_ranks - 1),MPI_UNSIGNED_CHAR,MPI_STATUS_IGNORE);
        }
        MPI_Scatter(buffer, BLOCKSIZE, MPI_UNSIGNED_CHAR, recver, BLOCKSIZE, MPI_CHAR, 0, MPI_COMM_WORLD);
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

            // Mapping of the data
            #if DEBUG_MAP
                if (rank==1)    //Debug on process 1
                {
                    fflush(stdout);
                    // Have a simple visualization of what Map does
                    for(int j=0;j<BLOCKSIZE;j++) printf("%c",recver[j]);
                    printf(" - ");
                    // Comment the above two lines and uncomment the below lines to have a deeper vision of the work done by Map
                //     printf("Block processed: ");
                //     for(int j=0;j<BLOCKSIZE;j++) printf("%c",recver[j]);
                //     printf("\n");
                //     printf("Word(s) exctracted: ");
                //     block_count=0;
                //     while (block_count<BLOCKSIZE){
                //         prev_block_count = block_count;
                //         Map(recver,BLOCKSIZE,block_count,&words[0]);
                //         if(prev_block_count!=0 && words[0].key_len!=0) printf ("  ---  ");
                //         for(int k=0;k<words[0].key_len;k++) printf("%c",words[0].key[k]);
                //     }
                //     printf("\n\n");
                //     if(words[0].key_len!=0) delete[] words[0].key;
                }
            #endif
            if (rank!=0)
            {
                block_count=0;
                while(block_count<BLOCKSIZE){
                    words[count_words].key_len=0;
                    Map(recver,BLOCKSIZE,block_count,&words[count_words]);
                    if(words[count_words].key_len!=0) count_words++;     //if we have mapped a word, increment count_words
                }
            }
            
        //advance offset
        file_count = file_count +  BLOCKSIZE*(num_ranks - 1);
        if(rank==0){
            MPI_File_seek(fh, file_count, MPI_SEEK_SET);
        }
    }

    #if DEBUG_MAP
        // Show the mapped data by process 1
        if (rank==1)
        {
            printf("\n\n\nWords mapped by process 1:\n");
            for(int i=0;i<count_words;i++){
                for(int j=0;j<words[i].key_len;j++) printf("%c",words[i].key[j]);
                printf("  -  ");
            }
            printf("\n");
        }
    #endif

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

    // Free the mapped data
    if (rank!=0){
            for(int i=0;i<count_words;i++) delete[] words[i].key;
    }
    delete[] words;

    //use gather to acquire result
    MPI_Finalize();

    return 0;
}
