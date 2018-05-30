#include <stdio.h>
#include <mpi.h>
#include <stdlib.h>
#include <time.h>
#include <vector>
#include "user_case.h"
#include "toolbox.h"

#define FILE_NAME "../test_files/wikipedia_test_small.txt"
#define BLOCKSIZE 1000
#define WRITE_PATH "words_output.csv"
// #define FILE_NAME "/cfs/klemming/scratch/s/sergiorg/DD2356/input/wikipedia_10GB.txt"
// #define BLOCKSIZE 67108864
//debug configuration
#define DEBUG_ALL2ALL 0

#define SORT_RESULT 1
#define SHOW_PROGRESS 1
#define SHOW_RESULT 0
#define TIME_REPORT 1

int main(int argc, char** argv){

    clock_t  ReadMapStart, ReadMapStop,
            ataStart    , ataStop    ,
            reduceGatherStart , reduceGatherStop;

    MPI_Init(&argc, &argv);

    //get rank or other head operations
    int rank, num_ranks;
    MPI_File fh;
    MPI_Offset file_size;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &num_ranks);
    long int blocksize=BLOCKSIZE;
    // MPI_Request ReadFileScatReq;

    // get command line arguments
    char* filename;
    char filenamemacro[]=FILE_NAME;
    filename = new char[200];
    int j=0; while(filenamemacro[j]!='\0') j++;
    for(int i=0;i<j+1;i++) filename[i]=filenamemacro[i];
    char* outputname;
    char outputnamemacro[]=WRITE_PATH;
    outputname = new char[200];
    while(outputnamemacro[j]!='\0') j++;
    for(int i=0;i<j+1;i++) outputname[i]=outputnamemacro[i];

    setCmdLineOptions(argc,argv,filename,outputname,&blocksize);
    if (rank==0){
        printf("Reading the file: ");
        j=0; while(filename[j]!='\0') j++;
        for (int i=0;i<j;i++) printf("%c",filename[i]);
        printf("\n");
        printf("Working with blocks of size %ld\n",blocksize);
        printf("\n");
    }

    //variable for map and shift
    int block_count;
    int count_words=0;
    KEYVAL* words;

    //variable for all to all
    int cntCnt;
    int cntDsp;
    int cntRcv;
    int* rankRecord;
    KEYVAL* ataSend;
    KEYVAL* atarecv;
    int* ataSendCnt;
    ataSendCnt = new int[num_ranks];
    int* ataRecvCnt;
    ataRecvCnt = new int[num_ranks];         //use reduce for optimized performance
    int* ataSendDsp;
    ataSendDsp = new int[num_ranks];
    int* ataRecvDsp;
    ataRecvDsp = new int[num_ranks];
    for(int i=0;i < num_ranks;i++){
        *(ataRecvDsp+i) = i*blocksize/2;
        *(ataRecvCnt+i) = blocksize/2;
    }

    //defining datatype for all to all
    KEYVAL sample;
    MPI_Datatype MPI_KEYVAL;
    MPI_Datatype type[3] = { MPI_INT, MPI_INT, MPI_CHAR};
    int blocklen[3] = {1,1,20};
    MPI_Aint disp[3];
    disp[0] = (MPI_Aint) &(sample.key_len) - (MPI_Aint) &sample;
    disp[1] = (MPI_Aint) &(sample.val) - (MPI_Aint) &sample;
    disp[2] = (MPI_Aint) (sample.key) - (MPI_Aint) &sample;
    MPI_Type_create_struct(3, blocklen, disp, type, &MPI_KEYVAL);
    MPI_Type_commit(&MPI_KEYVAL);

    // variables for gather (combine phase)
    int TotNbWords;
    int LocNbWords;
    KEYVAL* gatherRcv;
    int* gatherRecvCnt;
    gatherRecvCnt = new int[num_ranks];
    int* gatherRecvDsp;
    gatherRecvDsp = new int[num_ranks];

    /***Initial and boardcast phase***/
    // variable for collective reading
    char* recver;
    MPI_File_open(MPI_COMM_WORLD, FILE_NAME, MPI_MODE_RDONLY,MPI_INFO_NULL,&fh);
    MPI_File_get_size(fh, &file_size);
    MPI_Aint ColRdSize = blocksize * sizeof(char);
    MPI_Aint ColRdExtent = num_ranks*ColRdSize;
    MPI_Offset ColRdDisp = rank*ColRdSize;
    MPI_Datatype contig, filetype;
    MPI_Type_contiguous(ColRdSize,MPI_CHAR,&contig);
    MPI_Type_create_resized(contig,0,ColRdExtent,&filetype);
    // MPI_Type_commit(&contig);
    MPI_Type_commit(&filetype);
    recver=new char[ColRdSize];
    MPI_File_set_view(fh,ColRdDisp,MPI_CHAR,filetype,"native",MPI_INFO_NULL);
    words = new KEYVAL[file_size/(2*(num_ranks))];  //Worst case scenario is there are one new word every two characters
    // for(int i=0;i<file_size/(2*num_ranks);i++){
    //     words[i].key_len=0;     //initializing the lengths of the words to zero
    // }


    count_words=0;
    #if SHOW_PROGRESS
    if (rank==0){
         printf("Reading file and mapping steps\n");
    }
    #endif

    ReadMapStart = clock();

    int nbUsedProc;
    if (ColRdDisp-rank*ColRdSize>file_size-num_ranks*ColRdSize)
    {
        nbUsedProc=(file_size-ColRdDisp+rank*ColRdSize)/ColRdSize;
    }
    else nbUsedProc=num_ranks;

    while(nbUsedProc>0 && (ColRdDisp-rank*ColRdSize < file_size-file_size%(nbUsedProc*ColRdSize))){
        /***input and scatter phase***/
        if (nbUsedProc==num_ranks){
            MPI_File_read_all(fh,recver,ColRdSize,MPI_CHAR,MPI_STATUS_IGNORE);
        }
        else{
            if (rank<nbUsedProc){
                MPI_File_read(fh,recver,ColRdSize,MPI_CHAR,MPI_STATUS_IGNORE);
                // printf("rank %d successfully read some stuff!\n",rank);
            }
        }
        // Useful for the reading debugging
            // printf("WHILE LOOP --- rank=%d, file size=%lld, ColRdExtent=%ld; ColRdDisp=%lld; ColRdSize=%ld; nbUsedProc=%d\n",rank,file_size,ColRdExtent,ColRdDisp,ColRdSize,nbUsedProc);
            // for(int j=0;j<ColRdSize;j++) printf("%c",recver[j]);
            // printf("\n");
        /***mapping and shift phase***/
        if (rank<nbUsedProc){
            block_count=0;
            while(block_count<ColRdSize){
                words[count_words].key_len=0;
                Map(recver,ColRdSize,block_count,words[count_words].key_len,words[count_words].key,words[count_words].val);
                if(words[count_words].key_len!=0) count_words++;     //if we have mapped a word, increment count_words
            }
        }
        // No need to advance the offset with MPI_File_read_all
        ColRdDisp += nbUsedProc*ColRdSize;
        if (ColRdDisp-rank*ColRdSize>file_size-nbUsedProc*ColRdSize)
        {
            nbUsedProc=(file_size-ColRdDisp+rank*ColRdSize)/ColRdSize;
        }
    }   //--- End of while loop ---//

    if (rank==0){
         printf("File size is %lld bytes.\n", file_size);
         printf("Number of unread characters (at the end of the file): %lld\n",file_size-ColRdDisp);
    }

    MPI_File_close(&fh);
    delete [] recver;

    ReadMapStop = clock();

    /***all to all comm. phase***/

    ataStart = clock();

    #if SHOW_PROGRESS
        if (rank==0) printf("All to all communication step...\n");
    #endif
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

    delete [] words;

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
    delete [] ataSend;

    ataStop = clock();

    // Call reduce on each process

    reduceGatherStart = clock();

    #if SHOW_PROGRESS
        if (rank==0) printf("Reduce step...\n");
    #endif
    std::vector<KEYVAL> reduceVec;
    reduceVec.clear();
    Reduce(reduceVec, atarecv, cntRcv);
    int reduceNb = reduceVec.size();
    KEYVAL* reduceAry = new KEYVAL[reduceNb];
    for(int i=0;i< reduceNb;i++){
        *(reduceAry+i) = reduceVec.back();
        reduceVec.pop_back();
    }
    delete [] atarecv;

    if (rank==0) quickSort(reduceAry,0,reduceNb-1);

    #if SORT_RESULT
    // If we want to perform the sort in a parallel way (not implemented yet: to be implemented when the std::map will be available)
        // First, sort the lists on each process (sequentially here, but why not using OpenMP)
        // quickSort(reduceAry,0,reduceNb-1);
    #endif

    // Gather the reduced result on master
    #if SHOW_PROGRESS
        if (rank==0) printf("Gather to master thread step...\n");
    #endif
    TotNbWords=0;
    LocNbWords=reduceNb;
    MPI_Allgather(&LocNbWords,1,MPI_INT,gatherRecvCnt,1,MPI_INT,MPI_COMM_WORLD);

    if (rank==0){
        for(int i=0;i<num_ranks;i++){
            gatherRecvDsp[i]=TotNbWords;
            TotNbWords+=gatherRecvCnt[i];
        }
        printf("There are %d different words.\n",TotNbWords);
        gatherRcv = new KEYVAL[TotNbWords];
    }
    MPI_Gatherv(reduceAry,LocNbWords,
                    MPI_KEYVAL,
                    gatherRcv,gatherRecvCnt,gatherRecvDsp,
                    MPI_KEYVAL,0,MPI_COMM_WORLD);

    delete [] reduceAry;
    #if SORT_RESULT
        #if SHOW_PROGRESS
            if (rank==0) printf("Result sorting step...\n");
        #endif
        // If we do serial sort instead of parallel sort...
        if (rank==0){
            quickSort(gatherRcv,0,TotNbWords-1);
        }
    #endif

    reduceGatherStop = clock();

    #if SHOW_RESULT
        for(int i=0;i<TotNbWords;i++){
            printf("<");
            for(int j=0;j<gatherRcv[i].key_len;j++){
                printf("%c",gatherRcv[i].key[j]);
            }
            printf(",%d>\n",gatherRcv[i].val);
        }
    #endif

    #if SHOW_PROGRESS
    printf("Write to file step...\n");
    // If too long, this step can be parallelized with MPI I/O
        if (rank==0){
            // check if the file exists
            if( remove( outputname ) != 0 ) perror( "Suppression of the previous output file." );
            printf("Writing results to the file %s\n",outputname);
            MPI_File fh_write;
            MPI_File_open(MPI_COMM_SELF,outputname,(MPI_MODE_CREATE | MPI_MODE_WRONLY | MPI_MODE_APPEND),MPI_INFO_NULL,&fh_write);
            char info[50];
            int str_len;
            for(int i=0;i<TotNbWords;i++){
                str_len=sprintf(info,"%d; %s\n",gatherRcv[i].val,gatherRcv[i].key);
                MPI_File_write(fh_write,info,str_len,MPI_CHAR,MPI_STATUS_IGNORE);
            }
            MPI_File_close(&fh_write);
        }
    #endif

    delete [] ataSendCnt;
    delete [] ataRecvCnt;
    delete [] ataSendDsp;
    delete [] ataRecvDsp;

    delete [] rankRecord;
    delete [] gatherRecvCnt;
    delete [] gatherRecvDsp;
    if (rank==0) delete [] gatherRcv;



    #if SHOW_PROGRESS
        if (rank==0) printf("Job done.\n");
    #endif

    #if TIME_REPORT
        printf("rank %d: ReadMap %f \n", rank, (double)(ReadMapStop-ReadMapStart)/CLOCKS_PER_SEC);
        printf("rank %d: ata %f \n", rank, (double)(ataStop - ataStart)/CLOCKS_PER_SEC);
        printf("rank %d: reduceGather %f \n", rank, (double)(reduceGatherStop - reduceGatherStart)/CLOCKS_PER_SEC);
    #endif

    MPI_Finalize();

    return 0;
}
