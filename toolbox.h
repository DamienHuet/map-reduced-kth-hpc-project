#include "user_case.h"

#ifndef TOOLBOX
#define TOOLBOX

// In-place serial quick sort
// A utility function to swap two elements
void swap(KEYVAL* a, KEYVAL* b)
    {
        KEYVAL t;

        // Copy a in t
        t.val=a->val;
        t.key_len=a->key_len;
        for(int i=0;i<a->key_len+1;i++) t.key[i]=a->key[i];    //a.key_len+1 because we want to copy the '\0' char at the end, and I'm not sure it is included in the number of letters

        // Copy b in a
        a->val=b->val;
        a->key_len=b->key_len;
        for(int i=0;i<b->key_len+1;i++) a->key[i]=b->key[i];

        // Copy t in b
        b->val=t.val;
        b->key_len=t.key_len;
        for(int i=0;i<t.key_len+1;i++) b->key[i]=t.key[i];
}

int partition (KEYVAL* arr, int low, int high)
{
    int pivot = arr[high].val;    // pivot
    int i = (low - 1);  // Index of smaller element

    for (int j = low; j <= high- 1; j++)
    {
        // If current element is smaller than or
        // equal to pivot
        if (arr[j].val >= pivot)
        {
            i++;    // increment index of smaller element
            swap(&(arr[i]), &(arr[j]));
        }
    }
    swap(&(arr[i + 1]), &(arr[high]));
    return (i + 1);
}

void quickSort(KEYVAL* arr, int low, int high)
{
    // printf("low=%d, high=%d\n",low,high);
    if (low < high)
    {
        /* pi is partitioning index, arr[p] is now
           at right place */
        int pi = partition(arr, low, high);

        // Separately sort elements before
        // partition and after partition
        quickSort(arr, low, pi - 1);
        quickSort(arr, pi + 1, high);
    }
}


// Merges two sorted arrays in one array (this is NOT an inplace operation, so there are 3 arrays processed)
void merge(KEYVAL* ary1, int len_ary1, KEYVAL* ary2, int len_ary2, KEYVAL* merged_ary){
    int count_1=0;
    int count_2=0;
    int count_merged=0;
    while((len_ary1-count_1!=0) && (len_ary2-count_2!=0)){
        if (ary1[count_1].val > ary2[count_2].val){
            merged_ary[count_merged].val= ary1[count_1].val;
            merged_ary[count_merged].key_len=ary1[count_1].key_len;
            for(int j=0;j<ary1[count_1].key_len;j++) merged_ary[count_merged].key[j]=ary1[count_1].key[j];
            // merged_ary[count_merged].key[merged_ary[count_merged].key_len]='\0';
            count_1++;
            count_merged++;
        }
        else{
            merged_ary[count_merged].val= ary2[count_2].val;
            merged_ary[count_merged].key_len=ary2[count_2].key_len;
            for(int j=0;j<ary2[count_2].key_len;j++) merged_ary[count_merged].key[j]=ary2[count_2].key[j];
            // merged_ary[count_merged].key[merged_ary[count_merged].key_len]='\0';
            count_2++;
            count_merged++;
        }
    }
    while (len_ary1-count_1!=0){
        merged_ary[count_merged].val= ary1[count_1].val;
        merged_ary[count_merged].key_len=ary1[count_1].key_len;
        for(int j=0;j<ary1[count_1].key_len;j++) merged_ary[count_merged].key[j]=ary1[count_1].key[j];
        if (merged_ary[count_merged].key_len<WORD_LEN) merged_ary[count_merged].key[merged_ary[count_merged].key_len]='\0';
        else merged_ary[count_merged].key[WORD_LEN-1]='\0';
        count_1++;
        count_merged++;
    }
    while (len_ary2-count_2!=0){
        merged_ary[count_merged].val= ary2[count_2].val;
        merged_ary[count_merged].key_len=ary2[count_2].key_len;
        for(int j=0;j<ary2[count_2].key_len;j++) merged_ary[count_merged].key[j]=ary2[count_2].key[j];
        if (merged_ary[count_merged].key_len<WORD_LEN) merged_ary[count_merged].key[merged_ary[count_merged].key_len]='\0';
        else merged_ary[count_merged].key[WORD_LEN-1]='\0';
        count_2++;
        count_merged++;
    }
}






int setCmdLineOptions(int nbArgs, char** Args, char* &fileName, char* &outputName, long int* blockSize){
    char flagFileName[] = "--filename";
    char flagOutputName[] = "--outputname";
    char flagBlockSize[] = "--blocksize";
    if (nbArgs%2!=1){
        printf("Not the right number of flags or arguments. All default values will be used.\n");
        return 1;
    }
    for(int i=1;i<nbArgs;i=i+2){
        int j=0;
        bool match=1;
        // bool endWord=0;
        do {
            while(match && Args[i][j]!='\0'){
                // printf("j=%d, char=%c\n",j, Args[i][j]);
                if (flagFileName[j]==Args[i][j]) j++;
                else match=0;
            }
        } while (match && Args[i][j]!='\0');
        if (match){
            int j=0;
            while(Args[i+1][j]!='\0') j++;
            if (j>200){
                printf("Path to file too long (200 char maximum)\n");
                return 1;
            }
            for(int k=0;k<j+1;k++){
                fileName[k] = Args[i+1][k];
            }
        }
        else match=1;
        j=0;

        do {
            while(match && Args[i][j]!='\0'){
                if (flagOutputName[j]==Args[i][j]) j++;
                else match=0;
            }
        } while (match && Args[i][j]!='\0');
        if (match){
            int j=0;
            while(Args[i+1][j]!='\0') j++;
            if (j>200){
                printf("Path to output file too long (200 char maximum)\n");
                return 1;
            }
            for(int k=0;k<j+1;k++){
                outputName[k] = Args[i+1][k];
            }
        }
        else match=1;
        j=0;

        do {
            while(match && Args[i][j]!='\0'){
                if (flagBlockSize[j]==Args[i][j]) j++;
                else match=0;
            }
        } while (match && Args[i][j]!='\0');
        if (match){
            *blockSize=0;
            int k=0;
            while (Args[i+1][k]!='\0'){
                *blockSize=10*(*blockSize)+ (Args[i+1][k]-48);
                k++;
            }
        }
    }
    return 0;
}



#endif
