#include <stdio.h>
#include <mpi.h>
#include <stdlib.h>
#include <time.h>
#include <vector>
#include <omp.h>
#include <math.h>
#include "user_case.h"
#include "toolbox.h"

#define FILE_NAME "wikipedia_test_small.txt"
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
#define WRITE_TO_FILE 1
#define TIME_REPORT_DETAIL 0

int main(int argc, char** argv){

    #if TIME_REPORT
            clock_t  ReadMapStart, reduceGatherStop;
    #endif
    #if TIME_REPORT_DETAIL
            clock_t ReadMapStop,
                ataStart    , ataStop ,
                reduceGatherStart;
    #endif

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
    int count_words=0;
    int lopEnd[2];
    KEYVAL* words;
    std::vector<KEYVAL> wordStore;
    wordStore.clear();

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

    #if (SORT_RESULT==0)
        // variables for gather (combine phase)
        int TotNbWords;
        int LocNbWords;
        KEYVAL* gatherRcv;
        int* gatherRecvCnt;
        gatherRecvCnt = new int[num_ranks];
        int* gatherRecvDsp;
        gatherRecvDsp = new int[num_ranks];
#endif

    /***Initial and boardcast phase***/
    // variable for collective reading
    char* recver;
    MPI_File_open(MPI_COMM_SELF, filename, MPI_MODE_RDONLY,MPI_INFO_NULL,&fh);
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
    //Worst case scenario is there are one new word every two characters
    // for(int i=0;i<file_size/(2*num_ranks);i++){
    //     words[i].key_len=0;     //initializing the lengths of the words to zero
    // }


    count_words=0;
    #if SHOW_PROGRESS
    if (rank==0){
         printf("Reading file and mapping steps\n");
    }
    #endif

    #if TIME_REPORT
            ReadMapStart = clock();
    #endif


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
            lopEnd[1] = ColRdSize;
            lopEnd[0] = ColRdSize/2;
            while(isDigit(*(recver+lopEnd[0])) || isLetter(*(recver+lopEnd[0]))) lopEnd[0]++;
            #pragma omp parallel num_threads(2)
            {
                int thd = omp_get_thread_num();
                int block_count=0;
                int readend = lopEnd[0];
                KEYVAL word;
                if(thd == 1){
                    block_count = lopEnd[0];
                    readend = lopEnd[1];
                }
                while(block_count<lopEnd[thd]){
                    //words[count_words].key_len=0;
                    Map(recver,readend,block_count,word);
                    #pragma omp critical
                    {
                        if(word.key_len != 0) wordStore.push_back(word);     //if we have mapped a word, increment count_words
                    }
                }
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
    delete [] filename;

    count_words = wordStore.size();
    words = wordStore.data();
    wordStore.clear();

    #if TIME_REPORT_DETAIL
        ReadMapStop = clock();
    #endif

    /***all to all comm. phase***/

    #if TIME_REPORT_DETAIL
        ataStart = clock();
    #endif

    #if SHOW_PROGRESS
        if (rank==0) printf("All to all communication steps\n");
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
    delete [] rankRecord;

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
    delete [] ataSendCnt;
    delete [] ataRecvCnt;
    delete [] ataSendDsp;
    delete [] ataRecvDsp;

    #if TIME_REPORT_DETAIL
        ataStop = clock();
    #endif

    // Call reduce on each process

    #if TIME_REPORT_DETAIL
        reduceGatherStart = clock();
    #endif

    #if SHOW_PROGRESS
        if (rank==0) printf("Reduce steps\n");
    #endif
    std::vector<KEYVAL> reduceVec;
    reduceVec.clear();
    Reduce(reduceVec, atarecv, cntRcv);
    delete [] atarecv;
    int reduceNb = reduceVec.size();
    KEYVAL* reduceAry = new KEYVAL[reduceNb];
    for(int i=0;i< reduceNb;i++){
        *(reduceAry+i) = reduceVec.back();
        reduceVec.pop_back();
    }
    reduceVec.clear();

    #if SORT_RESULT
        //  ------ This is a parallel merge sort. Each process first locally sort its own array using a
        //          quickSort, then it sends its data to a process or lower rank that will perform a
        //          merge of the received array and its own array, until there is eventually only process
        //          0 performing the last merge.
        //          If the number of processes is not of the form 2^N, all processes of ranks higher than
        //          the highest power of 2 < num_ranks will send their data to processes of rank lower
        //          than this power of 2, and exit the sort step. This way, exactly 2^N perform the
        //          actual merge step.  ------ //

        // First, sort the lists on each process
        quickSort(reduceAry,0,reduceNb-1);
        int rcvLength=0;
        int ownLength;
        bool own_array=1;
        int send_to=0;
        int recv_from=0;
        KEYVAL* rcvArray;
        KEYVAL* Array0;
        KEYVAL* Array1;
        ownLength=reduceNb;
        Array0 = new KEYVAL[1]; //just to be able to delete it in the coming while loop
        Array1 = new KEYVAL[ownLength];
        for(int i=0;i<ownLength;i++){
            Array1[i].key_len=reduceAry[i].key_len;
            Array1[i].val=reduceAry[i].val;
            for(int j=0;j<reduceAry[i].key_len;j++) Array1[i].key[j]=reduceAry[i].key[j];
        }
        delete [] reduceAry;
        int N,k;
        N=(int) log2(num_ranks);
        k=1;

        // If there are some processes of rank >= 2^N, they send their data to processes of rank < 2^N
        if (rank>=(int)pow(2,N)){
            // printf("Sending before the while: rank=%d, k=%d, N=%d, ownLength=%d\n",rank,k,N,ownLength);
            // send_to=rank%(num_ranks-(int)pow(2,N));
            send_to=rank%(int)pow(2,N);
            MPI_Send(&ownLength,1,MPI_INT,send_to,rank,MPI_COMM_WORLD);
            // printf("rank=%d, ownLength sent to %d is %d\n",rank,send_to,ownLength);
            if (own_array==1){
                MPI_Send(Array1,ownLength,MPI_KEYVAL,send_to,rank,MPI_COMM_WORLD);
                delete [] Array1;
            }
            else{
                MPI_Send(Array0,ownLength,MPI_KEYVAL,send_to,rank,MPI_COMM_WORLD);
                delete [] Array0;
            }
        }
        // Receive from ranks higher than 2^N
        if (rank<num_ranks%(int)pow(2,N)  &&  rank<(int)pow(2,N)){
            recv_from=(int)pow(2,N)+rank;
            // printf("Receiving before while: rank=%d, k=%d, N=%d, ownLength=%d\n",rank,k,N,ownLength);
            MPI_Recv(&rcvLength,1,MPI_INT,recv_from,recv_from,
                                MPI_COMM_WORLD,MPI_STATUS_IGNORE);
            // printf("rank=%d, rcvLength form %d is %d\n",rank,recv_from,rcvLength);
            rcvArray = new KEYVAL[rcvLength];
            if (own_array==1){
                delete [] Array0;
                Array0 = new KEYVAL[ownLength+rcvLength];
            }
            else{
                delete [] Array1;
                Array1 = new KEYVAL[ownLength+rcvLength];
            }
            MPI_Recv(rcvArray,rcvLength,MPI_KEYVAL,recv_from,recv_from,
                                MPI_COMM_WORLD,MPI_STATUS_IGNORE);
            if (own_array==1){
                merge(rcvArray,rcvLength,Array1,ownLength,Array0);
                // printf("4. rank=%d, k=%d, N=%d, ownLength=%d\n",rank,k,N,ownLength);
                ownLength=ownLength+rcvLength;
                delete [] rcvArray;
                own_array=0;
            }
            else{
                merge(rcvArray,rcvLength,Array0,ownLength,Array1);
                // printf("4. rank=%d, k=%d, N=%d, ownLength=%d\n",rank,k,N,ownLength);
                ownLength=ownLength+rcvLength;
                delete [] rcvArray;
                own_array=1;
            }
        }
        // printf("Before while: rank=%d, k=%d, N=%d, ownLength=%d\n",rank,k,N,ownLength);

        //  Here the actual merge step begins, with exactly 2^N processes involved
        while ((rank%((int)pow(2,k))==0) && (k<=N) && rank<(int)pow(2,N) ){
            // printf("rank=%d, k=%d, N=%d, ownLength=%d\n",rank,k,N,ownLength);
            MPI_Recv(&rcvLength,1,MPI_INT,rank+((int)pow(2,k-1)),rank+((int)pow(2,k-1)),
                                MPI_COMM_WORLD,MPI_STATUS_IGNORE);
            // printf("rank=%d, rcvLength form %d is %d\n",rank,rank+((int)pow(2,k-1)),rcvLength);
            rcvArray = new KEYVAL[rcvLength];
            if (own_array==1){
                delete [] Array0;
                Array0 = new KEYVAL[ownLength+rcvLength];
            }
            else{
                delete [] Array1;
                Array1 = new KEYVAL[ownLength+rcvLength];
            }
            MPI_Recv(rcvArray,rcvLength,MPI_KEYVAL,rank+((int)pow(2,k-1)),rank+((int)pow(2,k-1)),
                                MPI_COMM_WORLD,MPI_STATUS_IGNORE);
            if (own_array==1){
                merge(rcvArray,rcvLength,Array1,ownLength,Array0);
                // printf("4. rank=%d, k=%d, N=%d, ownLength=%d\n",rank,k,N,ownLength);
                ownLength=ownLength+rcvLength;
                delete [] rcvArray;
                own_array=0;
            }
            else{
                merge(rcvArray,rcvLength,Array0,ownLength,Array1);
                // printf("4. rank=%d, k=%d, N=%d, ownLength=%d\n",rank,k,N,ownLength);
                ownLength=ownLength+rcvLength;
                delete [] rcvArray;
                own_array=1;
            }
            k++;
        }
        if (rank!=0 && rank<(int)pow(2,N)){
            // printf("Out of the while: rank=%d, k=%d, N=%d, ownLength=%d\n",rank,k,N,ownLength);
            MPI_Send(&ownLength,1,MPI_INT,rank-((int)pow(2,k-1)),rank,MPI_COMM_WORLD);
            // printf("rank=%d, ownLength sent to %d is %d\n",rank,rank-((int)pow(2,k-1)),ownLength);
            if (own_array==1){
                MPI_Send(Array1,ownLength,MPI_KEYVAL,rank-((int)pow(2,k-1)),rank,MPI_COMM_WORLD);
                delete [] Array1;
            }
            else{
                MPI_Send(Array0,ownLength,MPI_KEYVAL,rank-((int)pow(2,k-1)),rank,MPI_COMM_WORLD);
                delete [] Array0;
            }
        }
        // End of merge step
        // End of merge sort

    #else
        // Gather the reduced result on master
        #if SHOW_PROGRESS
            if (rank==0) printf("Gather to master thread steps\n");
        #endif
        TotNbWords=0;
        LocNbWords=reduceNb;
        MPI_Allgather(&LocNbWords,1,MPI_INT,gatherRecvCnt,1,MPI_INT,MPI_COMM_WORLD);

        if (rank==0){
            for(int i=0;i<num_ranks;i++){
                gatherRecvDsp[i]=TotNbWords;
                TotNbWords+=gatherRecvCnt[i];
            }
            printf("%d words counted in total\n",TotNbWords);
            gatherRcv = new KEYVAL[TotNbWords];
        }
        MPI_Gatherv(reduceAry,LocNbWords,
                        MPI_KEYVAL,
                        gatherRcv,gatherRecvCnt,gatherRecvDsp,
                        MPI_KEYVAL,0,MPI_COMM_WORLD);

        delete [] reduceAry;
        delete [] gatherRecvDsp;
        delete [] gatherRecvCnt;
    #endif

#if TIME_REPORT
    reduceGatherStop = clock();
#endif

    #if WRITE_TO_FILE
    // If too long, this step can be parallelized with MPI I/O
        if (rank==0){
            printf("Write to file step...\n");
            // check if the file exists
            if( remove( outputname ) != 0 ) perror( "Suppression of the previous output file." );
            printf("Writing results to the file %s\n",outputname);
            MPI_File fh_write;
            MPI_File_open(MPI_COMM_SELF,outputname,(MPI_MODE_CREATE | MPI_MODE_WRONLY | MPI_MODE_APPEND),MPI_INFO_NULL,&fh_write);
            char info[50];
            int str_len;
            #if SORT_RESULT
                if (own_array==0){
                    for(int i=0;i<ownLength;i++){
                        str_len=sprintf(info,"%d; ",Array0[i].val);
                        for(int j=0;j<Array0[i].key_len;j++){
                            info[str_len]=Array0[i].key[j];
                            str_len++;
                        }
                        info[str_len]='\n';
                        str_len++;
                        MPI_File_write(fh_write,info,str_len,MPI_CHAR,MPI_STATUS_IGNORE);
                    }
                }
                else{
                    for(int i=0;i<ownLength;i++){
                        str_len=sprintf(info,"%d; ",Array1[i].val);
                        for(int j=0;j<Array1[i].key_len;j++){
                            info[str_len]=Array1[i].key[j];
                            str_len++;
                        }
                        info[str_len]='\n';
                        str_len++;
                        MPI_File_write(fh_write,info,str_len,MPI_CHAR,MPI_STATUS_IGNORE);
                    }
                }
            #else
                for(int i=0;i<TotNbWords;i++){
                    str_len=sprintf(info,"%d; ",gatherRcv[i].val);
                    for(int j=0;j<gatherRcv[i].key_len;j++){
                        info[str_len]=gatherRcv[i].key[j];
                        str_len++;
                    }
                    info[str_len]='\n';
                    str_len++;
                    MPI_File_write(fh_write,info,str_len,MPI_CHAR,MPI_STATUS_IGNORE);
                }
            #endif
            MPI_File_close(&fh_write);
        }
    #endif

    // #if SHOW_RESULT
    //     for(int i=0;i<TotNbWords;i++){
    //         printf("<");
    //         for(int j=0;j<gatherRcv[i].key_len;j++){
    //             printf("%c",gatherRcv[i].key[j]);
    //         }
    //         printf(",%d>\n",gatherRcv[i].val);
    //     }
    // #endif

    // deletion of remaning allocated arrays
    #if (SORT_RESULT==0)
        if (rank==0) delete [] gatherRcv;
    #else
        if (rank==0){
            if (own_array==0) delete[] Array0;
            else delete[] Array1;
        }
    #endif

    delete [] outputname;


    #if SHOW_PROGRESS
        if (rank==0)
            printf("Job done.\n");
    #endif

    #if TIME_REPORT
        if(rank==0)
        printf("rank %d: Total: %f \n", rank, (double)(reduceGatherStop-ReadMapStart)/CLOCKS_PER_SEC);
        #if TIME_REPORT_DETAIL
            printf("rank %d: ReadMap %f \n", rank, (double)(ReadMapStop-ReadMapStart)/CLOCKS_PER_SEC);
            printf("rank %d: ata %f \n", rank, (double)(ataStop - ataStart)/CLOCKS_PER_SEC);
            printf("rank %d: reduceGather %f \n", rank, (double)(reduceGatherStop - reduceGatherStart)/CLOCKS_PER_SEC);
        #endif
    #endif

    MPI_Finalize();

    return 0;
}
