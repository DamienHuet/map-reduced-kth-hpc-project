#include <stdio.h>
#include <mpi.h>
#include <stdlib.h>
#include <vector>
#include "user_case.h"

#define FILE_NAME "wikipedia_test_small.txt"
#define BLOCKSIZE 100
//debug configuration
#define DEBUG_ALL2ALL 0
#define SHOW_RESULT 1

int main(int argc, char** argv){

    MPI_Init(&argc, &argv);

    //get rank or other head operations
    int rank, num_ranks;
    MPI_File fh;
    MPI_Offset file_size, file_count;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &num_ranks);

    /***Initial and boardcast phase***/
    if(rank == 0){
        MPI_File_open(MPI_COMM_SELF, FILE_NAME, MPI_MODE_RDONLY,MPI_INFO_NULL,&fh);
        //--- It would be smart to include a test in case we fail to open the file ---//
        MPI_File_get_size(fh, &file_size);
    }
    MPI_Bcast(&file_size, 1, MPI_LONG_LONG, 0, MPI_COMM_WORLD);
    //printf("Size is %lld.\n", file_size);

    //variable for input and scatter
    char* buff = new char[BLOCKSIZE*num_ranks];
    char* recver = new char[BLOCKSIZE];

    //variable for map and shift
    int block_count;
    int count_words=0;
    KEYVAL* words = new KEYVAL[file_size/(2*(num_ranks-1))];  //Worst case scenario is there are one new word every two characters

    // variable for gather
    int TotNbWords;
    int LocNbWords;
    KEYVAL* gatherRcv;
    int* gatherRecvCnt = new int[num_ranks];
    int* gatherRecvDsp = new int[num_ranks];

    //variable for all to all
    int cntCnt;
    int cntDsp;
    int cntRcv;
    int* rankRecord;
    KEYVAL* ataSend;
    KEYVAL* atarecv;

    int* ataSendCnt = new int[num_ranks];
    int* ataRecvCnt = new int[num_ranks];         //use reduce for optimized performance
    int* ataSendDsp = new int[num_ranks];
    int* ataRecvDsp = new int[num_ranks];
    for(int i=0;i < num_ranks;i++){
        *(ataRecvDsp+i) = i*BLOCKSIZE/2;
        *(ataRecvCnt+i) = BLOCKSIZE/2;
    }
    //defining datatype for all to all
    KEYVAL sample;
    MPI_Datatype MPI_KEYVAL;
    MPI_Datatype type[3] = { MPI_INT, MPI_INT, MPI_CHAR};
    int blocklen[3] = { 1,1,20};
    MPI_Aint disp[3];
    disp[0] = (MPI_Aint) &(sample.key_len) - (MPI_Aint) &sample;
    disp[1] = (MPI_Aint) &(sample.val) - (MPI_Aint) &sample;
    disp[2] = (MPI_Aint) (sample.key) - (MPI_Aint) &sample;
    MPI_Type_create_struct(3, blocklen, disp, type, &MPI_KEYVAL);
    MPI_Type_commit(&MPI_KEYVAL);

    file_count = 0;
    count_words= 0;
    while(file_count < file_size - BLOCKSIZE){  //while loop is containing all to all communication now, finish gathering data from all-to-all to proceed another iteration
        /***input and scatter phase***/
        if(rank == 0){
            MPI_File_read(fh,buff+BLOCKSIZE,BLOCKSIZE*(num_ranks - 1),MPI_UNSIGNED_CHAR,MPI_STATUS_IGNORE);
        }
        MPI_Scatter(buff, BLOCKSIZE, MPI_UNSIGNED_CHAR, recver, BLOCKSIZE, MPI_CHAR, 0, MPI_COMM_WORLD);

        /***mapping and shift phase***/
        block_count=0;
        if (rank!=0)
        {
            while(block_count<BLOCKSIZE){
                words[count_words].key_len=0;
                Map(recver,BLOCKSIZE,block_count,words+count_words);
                if(words[count_words].key_len!=0) count_words++;     //if we have mapped a word, increment count_words
            }
        }
        //advance offset
        file_count = file_count +  BLOCKSIZE*(num_ranks - 1);
        if(rank==0){
            MPI_File_seek(fh, file_count, MPI_SEEK_SET);
        }
    }

    /***all to all comm. phase***/
    ataSend = new KEYVAL[count_words];
    rankRecord = new int[count_words];
    for(int i=0;i<count_words;i++){
        rankRecord[i] = calculateDestRank(words[i].key, words[i].key_len, num_ranks);
    }

    cntCnt = 0;
    cntDsp = 0;
    for(int i=0;i<num_ranks;i++){
        cntCnt = 0;
        ataSendDsp[i] = cntDsp;
        for(int j=0;j<count_words;j++){
            if(rankRecord[j] == i){
                ataSend[cntDsp] = words[j];
                cntCnt++;
                cntDsp++;
            }
        }
        ataSendCnt[i] = cntCnt;
    }

    MPI_Alltoall(ataSendCnt,1,MPI_INT,ataRecvCnt,1,MPI_INT,MPI_COMM_WORLD);

    cntRcv=0;
    for(int i=0;i<num_ranks;i++){
        ataRecvDsp[i]=cntRcv;
        cntRcv+=ataRecvCnt[i];
    }
    atarecv = new KEYVAL[cntRcv];

    #if 0
    if(rank==2){
        for(int i=0;i<num_ranks;i++){
            printf("%d, %d, %d, %d \n", ataSendCnt[i], ataSendDsp[i], ataRecvCnt[i], ataRecvDsp[i]);
            for(int j=0;j<ataSendCnt[i];j++){
                printf("j=%d, key_len=%d\n",j, (ataSend+ataSendDsp[i]+j)->key_len);
            }
        }
    }
    #endif

    MPI_Alltoallv(ataSend, ataSendCnt, ataSendDsp,
                    MPI_KEYVAL,
                    atarecv, ataRecvCnt, ataRecvDsp,
                    MPI_KEYVAL,
                    MPI_COMM_WORLD);

    #if DEBUG_ALL2ALL
    // Print the length of the words
        if(rank==3){
            for(int i=0;i<num_ranks;i++){
                printf("%d, %d, %d, %d \n", ataSendCnt[i], ataSendDsp[i], ataRecvCnt[i], ataRecvDsp[i]);
                for(int j=0;j<ataRecvCnt[i];j++){
                    printf("j=%d, key_len=%d\n",j, (atarecv+ataRecvDsp[i]+j)->key_len);
                }
            }
        }
    #endif

    // Call reduce on each process
    std::vector<KEYVAL> reduceVec;
    reduceVec.clear();
    Reduce(reduceVec, atarecv, cntRcv);
    int reduceNb = reduceVec.size();
    KEYVAL* reduceAry = new KEYVAL[reduceNb];
    for(int i=0;i< reduceNb;i++){
        *(reduceAry+i) = reduceVec.back();
        reduceVec.pop_back();
    }
    
    // To be implemented

    // Gather the reduced result on master
    TotNbWords=0;
    LocNbWords=reduceNb;
    //for(int i=0;i<num_ranks;i++) LocNbWords+=ataRecvCnt[i];
    // MPI_Reduce(&LocNbWords,&TotNbWords,1,MPI_INT,MPI_SUM,0,MPI_COMM_WORLD);
    MPI_Allgather(&LocNbWords,1,MPI_INT,gatherRecvCnt,1,MPI_INT,MPI_COMM_WORLD);

    if (rank==0){
        for(int i=0;i<num_ranks;i++){
            gatherRecvDsp[i]=TotNbWords;
            TotNbWords+=gatherRecvCnt[i];
        }
        printf("There are %d different words:\n",TotNbWords);
        gatherRcv = new KEYVAL[TotNbWords];
    }
    MPI_Gatherv(reduceAry,LocNbWords,
                    MPI_KEYVAL,
                    gatherRcv,gatherRecvCnt,gatherRecvDsp,
                    MPI_KEYVAL,0,MPI_COMM_WORLD);

    #if SHOW_RESULT
        for(int i=0;i<TotNbWords;i++){
            printf("<");
            for(int j=0;j<gatherRcv[i].key_len;j++){
                printf("%c",gatherRcv[i].key[j]);
            }
            printf(",%d>\n",gatherRcv[i].val);
        }
    #endif

    delete [] buff;
    delete [] recver;
    delete [] words;
    delete [] ataSendCnt;
    delete [] ataRecvCnt;
    delete [] ataSendDsp;
    delete [] ataRecvDsp;
    delete [] ataSend;
    delete [] rankRecord;
    MPI_Finalize();

    return 0;
}
