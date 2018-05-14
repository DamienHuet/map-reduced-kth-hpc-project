# KTH High-performance computing project: highly parallel map-reduce
This repository is related to a course project DD2356 in KTH. The objectove is to implement a map-reduce
highly parallel algorithm using MPI, applied to word counting.

## Download and make it work (on linux machine)
To download this repository and be able to run the test files, we recommend that you run the commands:
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
Once this is done, you can use `mpicxx main.c -o main -I.` to build the executable and you can run it with `mpirun -n 4 ./main` if your processor has 4 cores.
