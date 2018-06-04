# Map Reduce project: parallel word count
This repository is related to a course project DD2356 in KTH. The objective is to implement a map-reduced
highly parallel algorithm using MPI, applied to word counting.
Our implementation includes additional features, such as:
- use collective I/O to read the input data. It is fully parallel, in the sense that the master process does not perform anything different from the slave processes during this step.
- the read data is independant of the number of slave processes (but it is a multiple of the block size)
- sort the result with a parallel merge sort (only available for a number of processors of the form 2^N)
- use OpenMP in map function
- set the input and output file paths and the blocksize with flags in command line

We also encourage you to have a look at the comments in our file backend.c to access additional informations
about our implementation. The report is also here to clarify the implementation, and further discuss about
performances. It is not present in this repository, but you can find it on canvas.

## Download and make it work (on local linux machine)
To download this repository and be able to run the test files on your local machine, we recommend that you run the commands:
```
mkdir map-reduced-kth-hpc-project
cd map-reduced-kth-hpc-project
git clone https://github.com/DamienHuet/map-reduced-kth-hpc-project.git
mkdir test-files
cd test-files
```
Here you should download the two test files wikipedia_test_small.txt and wikipedia_test_large.txt from the project description.
```
cd ../map-reduced-project
```
Once this is done, you can use `mpicxx backend.c -o be -fopenmp` to build the executable and you can run it with `mpirun -n 4 ./be` if you have four cores, for example.

## Run the files on Beskow (KTH cpu cluster)
Copy the files backend.c, toolbox.h and user_case.h in the same directory on Beskow, and compile them using
```
CC backend.c -o be -h omp
```
Then, allocate a number n of nodes (3 minimum to process the 10GB file) using
```
salloc --nodes=<your number of nodes here> -t 01:00:00 -A edu18.DD2356
```
and run the file with
```
aprun -n <your number of nodes *16> -N 16 -d 2 ./be
```
Here, the OpenMP part of the program will have 2 threads, and you will fully exploit your nodes as you will use the 32 processors of each node. You could choose not to use all of them, and to do so you would have to
replace the number 16 in the above command by any number between 1 and 16.

BE CAREFUL: our program is very memory consumming. It means that the 10GB file cannot be run with less
than 3 nodes. We tested our program up to 64 nodes: the 160GB file is processed fine if the result is not
sorted (merge sort is also memory consumming). The 320GB file require too much memory for the 64 nodes,
even without the merge sort set to on (fail in all to all communication).

## Set your own values with flags
With flags, you can change the input and output file paths and the blocksize. The flags are the following: --filename, --outputname and --blocksize. You need to add a space between the flag and its value. Here is
an example of running command with flags:
```
aprun -n 64 -N 16 -d 2 ./be --filename my/file/path --outputname my_output_name --blocksize 1000
```
The default values are
filename: /cfs/klemming/scratch/s/sergiorg/DD2356/input/wikipedia_10GB.txt
outputname: words_output.csv
blocksize (in bytes): 67108864
