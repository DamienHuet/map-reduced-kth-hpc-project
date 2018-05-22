#include <stdio.h>
#include <mpi.h>
#include <stdlib.h>
#include "user_case.h"

#define FILE_RMODE (MPI_MODE_RDONLY)
#define FILE_NAME "wikipedia_test_small.txt"
#define BLOCKSIZE 100
//debug configuration
#define DEBUG_SCATTER 0
#define DEBUG_MAP 0
#define DEBUG_ALL2ALL 1

int main(int argc, char** argv){
    //get rank or other head operations
    int rank, num_ranks;
    char* buff;
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
    int count;
    int block_count;
    int count_words=0;
    int prev_block_count;

    //variable for input and scatter
    buff = new char[BLOCKSIZE*num_ranks];
    recver = new char[BLOCKSIZE];

    //variable for map and shift
    KEYVAL* words = new KEYVAL[file_size/(2*(num_ranks-1))];  //Worst case scenario is there are one new word every two characters

    //variable for all to all
    int* rankRecord;
    KEYVAL* allSend;
    KEYVAL* allrecv = new KEYVAL[BLOCKSIZE/2];
    int* allcount = new int[num_ranks];
    int* alllimit = new int[num_ranks];         //use reduce for optimized performance
    int* alldisp = new int[num_ranks];
    int* alldispr = new int[num_ranks];
    for(int i=0;i < num_ranks;i++){
        *(alldispr+i) = i*BLOCKSIZE/2;
        *(alllimit+i) = BLOCKSIZE/2;
    }
    //defining datatype for all to all
    MPI_Datatype MPI_KEYVAL;
    MPI_Datatype type[3] = {MPI_INT, MPI_INT, MPI_CHAR,};
    int blocklen[3] = {1, 1, 20};
    MPI_Aint disp[3];
    disp[0] = (MPI_Aint) &(allSend->key_len) - (MPI_Aint) allSend;
    disp[1] = (MPI_Aint) &(allSend->val) - (MPI_Aint) allSend;
    disp[2] = (MPI_Aint) &(allSend->key) - (MPI_Aint) allSend;
    MPI_Type_create_struct(3, blocklen, disp, type, &MPI_KEYVAL);
    MPI_Type_commit(&MPI_KEYVAL);

    #if DEBUG_SCATTER
        char* outbuf = new char[BLOCKSIZE+1];
    #endif
    file_count = 0;
    while(file_count < file_size - BLOCKSIZE){
        /***input and scatter phase***/
        if(rank == 0){
            MPI_File_read(fh,buff+BLOCKSIZE,BLOCKSIZE*(num_ranks - 1),MPI_UNSIGNED_CHAR,MPI_STATUS_IGNORE);
        }
        MPI_Scatter(buff, BLOCKSIZE, MPI_UNSIGNED_CHAR, recver, BLOCKSIZE, MPI_CHAR, 0, MPI_COMM_WORLD);

        #if DEBUG_SCATTER
            if(rank != 0){
                for(int i=0;i<BLOCKSIZE;i++){
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

        /***mapping and shift phase***/
        if (rank!=0)
        {
            block_count=0;
            while(block_count<BLOCKSIZE){
                words[count_words].key_len=0;
                Map(recver,BLOCKSIZE,block_count,words+count_words*sizeof(KEYVAL));  //block count is incremented in Map()
                if(words[count_words].key_len!=0) count_words++;     //if we have mapped a word, increment count_words
            }
        }
        //advance offset
        file_count = file_count +  BLOCKSIZE*(num_ranks - 1);
        if(rank==0){
            MPI_File_seek(fh, file_count, MPI_SEEK_SET);
        }
    }   //End of while loop


        /***all to all comm. phase***/
        allSend = new KEYVAL[count_words];
        rankRecord = new int[count_words];
        for(int i=0;i<count_words;i++){
            rankRecord[i] = calculateDestRank(words[i].key, words[i].key_len, num_ranks);
        }
        count = 0;
        for(int i=0;i<num_ranks;i++){
            block_count = 0;
            alldisp[i] = count;
            for(int j=0;j<count_words;j++){
                if(rankRecord[j] == i){
                    block_count++;
                    allSend[count] = words[j];
                    count++;
                }
            }
            allcount[i] = block_count;
        }

        // Send to and receive from the other processes the amount of data that is going to be sent/received
        MPI_Alltoall(allcount,1,MPI_INT,alllimit,1,MPI_INT,MPI_COMM_WORLD);
        // Calculate the new displacement and store it in alldispr
        count=0;
        for(int i=0;i<num_ranks;i++){
            count+=alllimit[i];
            alldispr[i]=count;
        }

        for(int i=0;i<num_ranks;i++){
            printf("%d, %d, %d, %d \n", allcount[i], alldisp[i], alllimit[i], alldispr[i]);
        }
        printf("%d \n", rank);

        printf("hello1, rank: %d\n",rank);
        MPI_Alltoallv(  allSend, allcount, alldisp,
                        MPI_KEYVAL,
                        allrecv,alllimit,alldispr,
                        MPI_KEYVAL,
                        MPI_COMM_WORLD);
        printf("hello2, rank: %d\n", rank);
        #if DEBUG_ALL2ALL
        for(int i=0;i<num_ranks*BLOCKSIZE/2;i++){
            printf("rank=%d, i=%d, (allrecv+i)->key_len=%d\n", rank, i,(allrecv+i)->key_len);
            // if((allrecv+i)->key_len>0){
            //     printf("<");
            //     for(int j=0;j<(allrecv+i)->key_len;j++) printf("%c", (allrecv+i)->key[j]);
            //     printf(",");
            //     printf("%d",(allrecv+i)->val);
            //     printf("> \n");
            // }
        }
        #endif

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

    //use gather to acquire result

    #if DEBUG_SCATTER
        delete [] outbuf;
    #endif

    delete [] buff;
    delete [] recver;

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
    delete[] words;

    MPI_Finalize();
    return 0;
}
