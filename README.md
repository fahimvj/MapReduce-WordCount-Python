# MapReduce Word Frequency Count

- **Name**: MD Fahim Shahriar Chowdhury
- **Contact**: fahimbcp@gmail.com
- **Date**: 11 May 2025
- **Last Updated**: 14 May 2025

---

##  Details
Develop a MapReduce-based solution in Python to perform a word frequency count. Use the provided input file for your implementation. Submit both your Python code and the resulting output.

### Input File:
- **File Name**: `input_file.txt`
- **Custom File**: You can specify a custom input file using environment variables (see Advanced Usage)

---

## Program Overview

This program offers two implementation modes:

### 1. Local Mode (Single-Node Implementation)
Python-based MapReduce simulation on a single machine with multiprocessing.

### 2. Hadoop Cluster Mode (Multi-Node Implementation)
True distributed processing using Hadoop MapReduce with Python streaming, distributing work across multiple nodes.

## Components:

### Local Mode Components:
1. **`split_new.py`**: Enhanced file splitting with robust error handling and encoding fallbacks.
2. **`mapping.py`**: Contains the mapper logic to generate key-value pairs.
3. **`shuffling.py`**: Groups intermediate key-value pairs by key.
4. **`reduce.py`**: Contains the reducer logic to aggregate word counts.
5. **`main.py`**: Orchestrates the entire MapReduce process with comprehensive error handling and performance metrics.

### Hadoop Cluster Mode Components:
1. **`hadoop_mapper.py`**: Standalone mapper script compatible with Hadoop Streaming
2. **`hadoop_reducer.py`**: Standalone reducer script compatible with Hadoop Streaming
3. **`run_hadoop.sh`**: Shell script to submit the job to Hadoop cluster (Linux/macOS)
4. **`Run-HadoopJob.ps1`**: PowerShell script to submit the job to Hadoop cluster (Windows)
5. **`hadoop_setup.md`**: Instructions for setting up Hadoop environment

---

## Execution Steps

1. **Splitting**:
   - Splits the input file into smaller chunks for parallel processing.
   - Uses file size or word count as the splitting technique based on input file size.
   - Handles encoding issues automatically with fallback mechanisms.
   - Reports detailed splitting metrics including technique, nodes, and timing.

2. **Mapping**:
   - Processes each chunk in parallel using Python's multiprocessing.
   - Generates intermediate key-value pairs `(word, 1)` for each word occurrence.
   - Dynamically allocates processor cores based on available resources and workload.
   - Provides detailed processing metrics and core utilization information.

3. **Shuffling**:
   - Groups all intermediate key-value pairs by key (word).
   - Efficiently consolidates distributed results from multiple mappers.
   - Reports shuffling performance metrics.

4. **Reducing**:
   - Aggregates the counts for each word to calculate total frequencies.
   - Employs efficient dictionary-based aggregation techniques.
   - Reports reducing performance metrics.

5. **Output**:
   - Writes the final word counts to `output_file.txt` (sorted by frequency).
   - Displays the top 10 word frequencies in the console.
   - Cleans up all temporary files even if errors occur.
   - Provides comprehensive processing summary.

---

## How to Run

## Local Mode (Single-Node)

### Basic Usage

1. Place the input file as `input_file.txt` in the same directory.
2. Run the `main.py` script using Python:
   ```bash
   python main.py
   ```

### Advanced Local Usage

You can specify custom input and output files using environment variables:

```bash
# Windows PowerShell
$env:INPUT_FILE="your_custom_input.txt"
$env:OUTPUT_FILE="your_custom_output.txt"
python main.py

# Windows Command Prompt
set INPUT_FILE=your_custom_input.txt
set OUTPUT_FILE=your_custom_output.txt
python main.py

# Linux/MacOS
INPUT_FILE=your_custom_input.txt OUTPUT_FILE=your_custom_output.txt python main.py
```

### Testing Local Mode

A test script is included to validate the local MapReduce implementation:

```bash
python test.py
```

This will:
1. Generate a test input file with known content
2. Run the MapReduce process on the test file
3. Measure performance metrics
4. Verify the output correctness

### Testing Hadoop Mode

A separate test script is available to validate the Hadoop implementation locally:

```bash
python test_hadoop.py
```

This will:
1. Test the mapper functionality
2. Test the reducer functionality
3. Run a complete Hadoop pipeline simulation locally using the existing input_file.txt
4. Save the output to output_file_hadoop.txt (separate from the standard MapReduce output)

## Hadoop Cluster Mode (Multi-Node)

### Setting Up Hadoop Cluster

See detailed instructions in `hadoop_setup.md` for configuring your Hadoop environment.

### Running on Hadoop Cluster

1. Upload your input file to HDFS:
   ```bash
   hdfs dfs -put input_file.txt /user/hadoop/input/
   ```

2. Run the Hadoop streaming job:

   **On Linux/macOS:**
   ```bash
   ./run_hadoop.sh /user/hadoop/input/input_file.txt /user/hadoop/output/
   ```

   **On Windows PowerShell:**
   ```powershell
   # Set the Hadoop streaming JAR path
   $env:HADOOP_STREAMING_JAR="$env:HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-3.3.4.jar"
   
   # Run the job
   ./Run-HadoopJob.ps1 -InputFile "/user/hadoop/input/input_file.txt" -OutputDir "/user/hadoop/output/" -NumReducers 4
   ```

3. The results will be:
   - Available in HDFS at the specified output directory
   - Merged and downloaded to `output_file_hadoop.txt` on your local machine

3. View the results:
   ```bash
   hdfs dfs -cat /user/hadoop/output/part-*
   ```

4. Download the results to your local filesystem:
   ```bash
   hdfs dfs -getmerge /user/hadoop/output/ hadoop_output.txt
   ```

## File Structure

### Local Mode Files
```
├── main.py             # Main orchestration script with error handling
├── split_new.py        # Enhanced file splitting with error handling
├── mapping.py          # Mapper implementation for local mode
├── shuffling.py        # Shuffling implementation for local mode
├── reduce.py           # Reducer implementation for local mode
├── test.py             # Automated testing script
├── input_file.txt      # Input data
└── output_file.txt     # Result output (generated)
```

### Hadoop Mode Files
```
├── hadoop_mapper.py    # Mapper script for Hadoop Streaming
├── hadoop_reducer.py   # Reducer script for Hadoop Streaming
├── run_hadoop.sh       # Shell script to submit Hadoop jobs (Linux/macOS)
├── Run-HadoopJob.ps1   # PowerShell script to submit Hadoop jobs (Windows)
├── hadoop_setup.md     # Hadoop setup instructions
├── test_hadoop.py      # Testing script for Hadoop implementation
└── output_file_hadoop.txt # Hadoop output results (generated)
```

## Requirements

- Python 3.6 or higher
- No external libraries required (only uses standard library)

## Error Handling & Improvements

The implementation includes robust error handling and optimizations:

- **Error Handling**
  - Unicode decode errors handled with encoding fallback mechanisms
  - Input file validation and detailed error reporting
  - Automatic cleanup of temporary files, even when errors occur
  - Comprehensive exception handling for all processing stages
  - Graceful handling of binary and malformed files

- **Optimizations**
  - Dynamic core allocation based on system resources
  - Efficient memory usage for large files
  - Performance metrics for each processing stage
  - Parallel processing with appropriate safeguards

## Performance Snapshot

![Word Count MapReduce Performance](https://github.com/fahimvj/MapReduce-WordCount-Python/blob/main/snapshot%20word%20count.png)

![Word Count MapReduce Performance Hadoop]([https://github.com/fahimvj/MapReduce-WordCount-Python/blob/main/snapshot%20word%20count.png](https://github.com/fahimvj/MapReduce-WordCount-Python/blob/main/main.py%20output.png))



## Implementation Details

### Split Stage

The split operation dynamically selects between two splitting techniques:

1. **File Size Based**: For large files (>128MB), splits by file size chunks
2. **Word Count Based**: For smaller files, splits by word count (default 1000 words per chunk)

Both techniques account for encoding issues and handle binary files by using encoding fallbacks.

### Map Stage

The mapper creates key-value pairs where:
- **Key**: Each word in the document
- **Value**: Always 1 (representing one occurrence)

Multiprocessing is employed with a dynamic pool size based on available cores and workload.

### Shuffle Stage

The shuffle operation:
- Takes the output from all mappers
- Groups values by keys (words)
- Creates a dictionary where each key has a list of values

### Reduce Stage

The reducer:
- Takes the shuffled data
- Sums the values for each key
- Produces a final word->count dictionary
- Sorts results by frequency (descending)

## Hadoop Implementation Details

### Architecture Comparison

| Feature | Local Implementation | Hadoop Implementation |
|---------|---------------------|----------------------|
| Scalability | Limited to single machine cores | Scales across multiple nodes |
| Fault Tolerance | Limited - fails if process crashes | High - auto-restarts failed tasks |
| Data Handling | In-memory and local disk | Distributed across HDFS |
| Network Transfer | None (all local) | Data shuffling between nodes |
| Setup Complexity | Simple | Requires cluster setup |
| Resource Utilization | Bound to single machine resources | Uses cluster-wide resources |
| Process Isolation | OS-level process isolation | Container-based isolation |

### Hadoop MapReduce Flow

1. **Input Splitting**: HDFS automatically splits input files into blocks (typically 128MB)
2. **Mapping**: Each mapper container processes one input split, ideally on the node where the data resides
3. **Shuffling**: Map outputs are partitioned, sorted, and transferred to reducer nodes
4. **Reducing**: Reducers aggregate results for their assigned keys
5. **Output**: Final results are written back to HDFS

### Key Advantages of Hadoop Implementation

1. **Data Locality**: Hadoop attempts to run map tasks on the nodes storing the input data
2. **Automatic Parallelization**: One mapper per input split runs in parallel across the cluster
3. **Fault Tolerance**: Failed tasks are automatically restarted on different nodes
4. **Scalability**: Add more nodes to handle larger datasets or improve performance
5. **Resource Management**: YARN manages resource allocation across the cluster

## Future Improvements

Potential enhancements for future versions:

1. Add Combiner functionality to improve network efficiency
2. Implement custom partitioning for better load balancing
3. Add word normalization (stemming, lemmatization) options
4. Support for compressed input/output files
5. Integrate with Apache Spark for in-memory processing
6. Add custom counters for better job monitoring
7. Implement custom InputFormat for specialized file parsing
