#include <stdio.h>
#include <mpi.h>
#include <stdlib.h>
#include "user_case.h"

int main(int argc, char** argv){
    MPI_Init(&argc, &argv);
    //get rank or other head operations
    
    /***map and reduce phase***/
    
    while(){        //file is not drain after this draw
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
}