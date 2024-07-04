# Parallelism
The implementation of two classes: IterativeParallelism and ParallelMapperImpl.
* IterativeParallelism class, which implements NewListIP interface and processes lists in multiple threads.
* All functions are passed the threads parameter - how many threads should be used during the calculation.
* Class ParallelMapperImpl, implements the Parallel Mapper interface.
* The map method must evaluate the function f on each of the specified arguments (args) in parallel.
* The close method should stop all worker threads. 
* One ParallelMapperImpl can be accessed by multiple clients simultaneously.