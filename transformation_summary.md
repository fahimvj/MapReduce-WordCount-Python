# MapReduce Transformation Summary

## From Single-Node to Multi-Node MapReduce

This document summarizes the transformation of the original single-node MapReduce implementation to a true distributed Hadoop MapReduce solution.

## Files Created:

1. **hadoop_mapper.py**: 
   - A standalone mapper script that reads from stdin and outputs to stdout
   - Compatible with Hadoop Streaming
   - Processes words line by line as input splits are provided by Hadoop

2. **hadoop_reducer.py**:
   - A standalone reducer script that processes pre-sorted input from stdin
   - Aggregates word counts and outputs final results to stdout
   - Compatible with Hadoop Streaming's shuffle and sort phases

3. **run_hadoop.sh**:
   - Bash script to submit the MapReduce job to a Hadoop cluster
   - Configures job parameters and handles HDFS operations
   - Suitable for Linux/macOS environments
   - Uses input_file.txt by default when no arguments are provided

4. **Run-HadoopJob.ps1**:
   - PowerShell equivalent of the run_hadoop.sh script
   - Adapted for Windows environments
   - Takes parameters for input file, output directory, and number of reducers
   - Uses input_file.txt by default when no arguments are provided

5. **hadoop_setup.md**:
   - Comprehensive guide for setting up a Hadoop cluster
   - Includes configuration examples for core Hadoop components
   - Provides instructions for both multi-node and pseudo-distributed modes

6. **test_hadoop.py**:
   - Local testing suite for the Hadoop implementation
   - Validates mapper and reducer functionality independently
   - Simulates the complete MapReduce pipeline locally
   - Uses existing input_file.txt instead of creating temporary test files
   - Provides limited output for readability
   - Saves output to output_file_hadoop.txt (separate from the standard MapReduce output)

## Architecture Changes:

### Original Single-Node Implementation:
- Uses Python's multiprocessing to simulate parallelism
- Splits files manually using custom splitting logic
- Keeps all data in-memory or on local disk
- Limited by single machine's resources
- All mapper processes run on the same physical machine

### New Hadoop Distributed Implementation:
- Uses true distributed processing across multiple nodes
- Leverages HDFS for automatic file splitting and distribution
- Each mapper runs in a separate container on different nodes
- Hadoop handles data locality, job scheduling, and fault tolerance
- Scales horizontally by adding more machines to the cluster

## Key Advantages:

1. **True Scalability**: Process petabytes of data across hundreds or thousands of nodes
2. **Automatic Data Locality**: Hadoop tries to run computation near the data
3. **Built-in Fault Tolerance**: Failed tasks are automatically retried on different nodes
4. **Resource Management**: YARN handles cluster resources efficiently
5. **No Manual Splitting**: Input splitting is handled automatically by HDFS

## Usage:

The original implementation can still be used for smaller datasets:
```
python main.py
```

For large-scale processing, the Hadoop implementation is recommended:
```bash
# Linux/macOS
./run_hadoop.sh /user/hadoop/input/input_file.txt /user/hadoop/output/

# Windows PowerShell
./Run-HadoopJob.ps1 -InputFile "/user/hadoop/input/input_file.txt" -OutputDir "/user/hadoop/output/"
```

## Testing:

Run the local tests to verify the implementation:
```
python test_hadoop.py
```

All tests should pass before deploying to a production Hadoop cluster.

## Next Steps:

1. Set up a Hadoop cluster following the instructions in hadoop_setup.md
2. Upload test data to HDFS
3. Run the MapReduce job using the provided scripts
4. Examine the cluster utilization during job execution
5. Compare the performance between single-node and distributed implementations
