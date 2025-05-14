#!/bin/bash
# filepath: d:\EDU\MapReduce-WordCount-Python-main\run_hadoop.sh

# Check if arguments are provided and set defaults
if [ $# -eq 0 ]; then
    echo "No arguments provided, using default values:"
    INPUT_FILE="input_file.txt"
    OUTPUT_DIR="output_dir"
    NUM_REDUCERS=1
    echo "Input file: $INPUT_FILE"
    echo "Output directory: $OUTPUT_DIR"
    echo "Number of reducers: $NUM_REDUCERS"
elif [ $# -eq 1 ]; then
    INPUT_FILE=$1
    OUTPUT_DIR="output_dir"
    NUM_REDUCERS=1
    echo "Using output directory: $OUTPUT_DIR"
    echo "Using number of reducers: $NUM_REDUCERS"
else
    # Parse arguments
    INPUT_FILE=$1
    OUTPUT_DIR=$2
    NUM_REDUCERS=${3:-1}  # Default to 1 reducer if not specified
fi

# Define Hadoop job parameters
MAPPER="hadoop_mapper.py"
REDUCER="hadoop_reducer.py"
JOB_NAME="WordCount_$(date +%Y%m%d%H%M%S)"

# Ensure the scripts are executable
chmod +x $MAPPER $REDUCER

# Remove output directory if it exists
hdfs dfs -rm -r -f $OUTPUT_DIR

# Run the Hadoop streaming job
hadoop jar $HADOOP_STREAMING_JAR \
    -D mapred.job.name=$JOB_NAME \
    -D mapreduce.job.reduces=$NUM_REDUCERS \
    -files $MAPPER,$REDUCER \
    -mapper "$MAPPER" \
    -reducer "$REDUCER" \
    -input $INPUT_FILE \
    -output $OUTPUT_DIR

# Print job completion message
echo "Hadoop job $JOB_NAME completed"
echo "Output available in HDFS at $OUTPUT_DIR"
echo "To view results: hdfs dfs -cat ${OUTPUT_DIR}/part-*"

# Optional: Merge and download results to local file
hdfs dfs -getmerge $OUTPUT_DIR output_file_hadoop.txt
echo "Results merged to output_file_hadoop.txt"
