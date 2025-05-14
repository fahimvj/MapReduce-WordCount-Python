
param(
    [Parameter(Mandatory=$false)]
    [string]$InputFile = "input_file.txt",
    
    [Parameter(Mandatory=$false)]
    [string]$OutputDir = "output_dir",
    
    [Parameter(Mandatory=$false)]
    [int]$NumReducers = 1
)

# Display the parameters being used
Write-Host "Using the following parameters:"
Write-Host "Input file: $InputFile"
Write-Host "Output directory: $OutputDir"
Write-Host "Number of reducers: $NumReducers"

# Define Hadoop job parameters
$Mapper = "hadoop_mapper.py"
$Reducer = "hadoop_reducer.py"
$JobName = "WordCount_$(Get-Date -Format 'yyyyMMddHHmmss')"

# Check if HADOOP_STREAMING_JAR environment variable is set
if (-not $env:HADOOP_STREAMING_JAR) {
    Write-Error "HADOOP_STREAMING_JAR environment variable is not set."
    Write-Host "Please set it to the path of your hadoop-streaming.jar file."
    Write-Host "Example: `$env:HADOOP_STREAMING_JAR = 'C:\path\to\hadoop\share\hadoop\tools\lib\hadoop-streaming-3.3.4.jar'"
    exit 1
}

# Remove output directory if it exists
Write-Host "Removing output directory if it exists..."
& hdfs dfs -rm -r -f $OutputDir

# Run the Hadoop streaming job
Write-Host "Submitting Hadoop streaming job..."
& hadoop jar $env:HADOOP_STREAMING_JAR `
    -D mapred.job.name=$JobName `
    -D mapreduce.job.reduces=$NumReducers `
    -files $Mapper,$Reducer `
    -mapper $Mapper `
    -reducer $Reducer `
    -input $InputFile `
    -output $OutputDir

# Print job completion message
Write-Host "Hadoop job $JobName completed"
Write-Host "Output available in HDFS at $OutputDir"
Write-Host "To view results: hdfs dfs -cat ${OutputDir}/part-*"

# Optional: Merge and download results to local file
Write-Host "Merging results to local file..."
& hdfs dfs -getmerge $OutputDir output_file_hadoop.txt
Write-Host "Results merged to output_file_hadoop.txt"
