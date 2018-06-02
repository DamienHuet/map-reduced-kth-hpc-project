#include <stdio.h>
#include <mpi.h>
#include <stdlib.h>
#include <time.h>
#include <vector>
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

#define SORT_RESULT 1      //WARNING: NUMBER OF PROCESSED NEED TO BE 2^N !!! (soon corrected for any nb of processes)
#define SHOW_PROGRESS 1
#define SHOW_RESULT 0
#define TIME_REPORT 0
#define WRITE_TO_FILE 1

int main(int argc, char** argv){

    #if TIME_REPORT
        clock_t  ReadMapStart, ReadMapStop,
            ataStart    , ataStop    ,
            reduceGatherStart , reduceGatherStop;
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
    int block_count;
    int count_words=0;
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
    MPI_File_open(MPI_COMM_WORLD, filename, MPI_MODE_RDONLY,MPI_INFO_NULL,&fh);
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
            block_count=0;
            while(block_count<ColRdSize){
                //words[count_words].key_len=0;
                Map(recver,ColRdSize,block_count,wordStore);
                //if(words[count_words].key_len!=0) count_words++;     //if we have mapped a word, increment count_words
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

    #if TIME_REPORT
        ReadMapStop = clock();
    #endif

    /***all to all comm. phase***/

    #if TIME_REPORT
        ataStart = clock();
    #endif

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

    #if TIME_REPORT
        ataStop = clock();
    #endif

    // Call reduce on each process

    #if TIME_REPORT
        reduceGatherStart = clock();
    #endif

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

    // if (rank==0) quickSort(reduceAry,0,reduceNb-1);

    #if SORT_RESULT     //WARNING: NUMBER OF PROCESSED NEED TO BE 2^N !!! (soon corrected for any nb of processes)
        // First, sort the lists on each process
        // printf("rank=%d, reduceNb=%d\n",rank,reduceNb);
        quickSort(reduceAry,0,reduceNb-1,rank);
        // printf("rank=%d, out of quickSort\n",rank);
        MPI_Barrier(MPI_COMM_WORLD);
        int k,l,N,A;
        int rcvLength=0;
        KEYVAL* rcvArray;
        KEYVAL* Array0;
        KEYVAL* Array1;
        int ownLength;
        ownLength=reduceNb;
        Array0 = new KEYVAL[1]; //just to be able to delete it in the coming while loop
        // printf("Hello1 from rank%d\n",rank);
        Array1 = new KEYVAL[ownLength];
        // printf("Hello2 from rank%d\n",rank);
        for(int i=0;i<ownLength;i++){
            Array1[i].key_len=reduceAry[i].key_len;
            Array1[i].val=reduceAry[i].val;
            for(int j=0;j<reduceAry[i].key_len;j++) Array1[i].key[j]=reduceAry[i].key[j];
        }
        bool own_array=1;
        delete [] reduceAry;
        // MPI_Request req;
        N=(int) log2(num_ranks);
        k=1;
        A=0;
        l=N;
        while (  (A+(int)pow(2,l))<=rank  &&  l>0  ){
            A+=(int)pow(2,l);
            l-=1;
        }

        printf("Before while: rank=%d, k=%d, N=%d, l=%d, A=%d, ownLength=%d\n",rank,k,N,l,A,ownLength);

        while (((rank-A)%((int)pow(2,k))==0) && ( (k<=l) || (k==l+1 && (num_ranks-A)>(int)pow(2,l)) )  && (num_ranks-1!=rank) ){
        // while (   ((rank-A)%(int)pow(2,k)==0) && k<= l && num_ranks-A>0  ){
            printf("rank=%d, k=%d, N=%d, l=%d, A=%d, ownLength=%d\n",rank,k,N,l,A,ownLength);
            MPI_Recv(&rcvLength,1,MPI_INT,rank+((int)pow(2,k-1)),rank+((int)pow(2,k-1)),
                                MPI_COMM_WORLD,MPI_STATUS_IGNORE);
            printf("rank=%d, rcvLength form %d is %d\n",rank,rank+((int)pow(2,k-1)),rcvLength);
            rcvArray = new KEYVAL[rcvLength];
            if (own_array==1){
                delete [] Array0;
                Array0 = new KEYVAL[ownLength+rcvLength];
            }
            else{
                delete [] Array1;
                Array1 = new KEYVAL[ownLength+rcvLength];
            }

            // printf("2. rank=%d, k=%d, N=%d, l=%d, A=%d, ownLength=%d\n",rank,k,N,l,A,ownLength);

            MPI_Recv(rcvArray,rcvLength,MPI_KEYVAL,rank+((int)pow(2,k-1)),rank+((int)pow(2,k-1)),
                                MPI_COMM_WORLD,MPI_STATUS_IGNORE);

            // printf("3. rank=%d, k=%d, N=%d, l=%d, A=%d, ownLength=%d\n",rank,k,N,l,A,ownLength);

            if (own_array==1){
                merge(rcvArray,rcvLength,Array1,ownLength,Array0,rank);
                printf("4. rank=%d, k=%d, N=%d, l=%d, A=%d, ownLength=%d\n",rank,k,N,l,A,ownLength);
                ownLength=ownLength+rcvLength;
                delete [] rcvArray;
                // printf("5. rank=%d, k=%d, N=%d, l=%d, A=%d, ownLength=%d\n",rank,k,N,l,A,ownLength);
                own_array=0;
            }
            else{
                merge(rcvArray,rcvLength,Array0,ownLength,Array1,rank);
                printf("4. rank=%d, k=%d, N=%d, l=%d, A=%d, ownLength=%d\n",rank,k,N,l,A,ownLength);
                ownLength=ownLength+rcvLength;
                delete [] rcvArray;
                own_array=1;
            }
            k++;
        }

        // if ( ((rank-A)%((int)pow(2,k))!=0)  ){
        if (rank!=0){
            printf("Out of the while: rank=%d, k=%d, N=%d, l=%d, A=%d, ownLength=%d\n",rank,k,N,l,A,ownLength);
            MPI_Send(&ownLength,1,MPI_INT,rank-((int)pow(2,k-1)),rank,MPI_COMM_WORLD);
            printf("rank=%d, ownLength sent to %d is %d\n",rank,rank-((int)pow(2,k-1)),ownLength);
            // if ((own_array==1 && k==1) || (own_array==0 && k>1)){
            if (own_array==1){
                MPI_Send(Array1,ownLength,MPI_KEYVAL,rank-((int)pow(2,k-1)),rank,MPI_COMM_WORLD);
                delete [] Array1;
            }
            // if ((own_array==0 && k==1) || (own_array==1 && k>1)){
            else{
                MPI_Send(Array0,ownLength,MPI_KEYVAL,rank-((int)pow(2,k-1)),rank,MPI_COMM_WORLD);
                delete [] Array0;
            }
        }
        if (rank!=0) printf("Rank %d done.\n",rank);
    #else
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
            #else
                for(int i=0;i<TotNbWords;i++){
                    str_len=sprintf(info,"%d; %s\n",gatherRcv[i].val,gatherRcv[i].key);
                    MPI_File_write(fh_write,info,str_len,MPI_CHAR,MPI_STATUS_IGNORE);
                }
            #endif
            MPI_File_close(&fh_write);
            }
        }
    #endif

    delete [] outputname;
    delete [] ataSendCnt;
    delete [] ataRecvCnt;
    delete [] ataSendDsp;
    delete [] ataRecvDsp;
    delete [] rankRecord;

    #if (SORT_RESULT==0)
        delete [] gatherRecvCnt;
        delete [] gatherRecvDsp;
        if (rank==0) delete [] gatherRcv;
    #endif

    #if SHOW_PROGRESS
        if (rank==0){
            printf("Rank 0 done.\nJob done.\n");
        }
    #endif

    #if TIME_REPORT
        printf("rank %d: ReadMap %f \n", rank, (double)(ReadMapStop-ReadMapStart)/CLOCKS_PER_SEC);
        printf("rank %d: ata %f \n", rank, (double)(ataStop - ataStart)/CLOCKS_PER_SEC);
        printf("rank %d: reduceGather %f \n", rank, (double)(reduceGatherStop - reduceGatherStart)/CLOCKS_PER_SEC);
    #endif

    MPI_Finalize();

    return 0;
}
