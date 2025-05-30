# filepath: d:\EDU\MapReduce-WordCount-Python-main\main.py
import time
import os
# Using the improved version with better error handling
from split_new import split_file  
from mapping import mapper
from shuffling import shuffle
from reduce import reducer
from multiprocessing import Pool

def trace_mapper_execution(chunk_name, line_offset, line):
    process_id = os.getpid()
    print(f"Process ID {process_id} is mapping data from {chunk_name}.")
    return mapper(line_offset, line)

def main():
    try:
        # Input and output file paths - allow override via environment variables for testing
        input_file = os.environ.get('INPUT_FILE', 'input_file.txt')
        output_file = os.environ.get('OUTPUT_FILE', 'output_file.txt')
        
        # Check if input file exists
        if not os.path.exists(input_file):
            raise FileNotFoundError(f"Input file '{input_file}' not found.")
            
        # Split stage
        start_time = time.time()
        chunks = split_file(input_file)
        file_size = os.path.getsize(input_file)
        technique = "File Size based" if file_size > 128 * 1024 * 1024 else "Word Count based"
        total_chunks = len(chunks)
        print(f"Splitting Details:\nFile size: {file_size / (1024 * 1024):.2f} MB\nSplitting Technique: {technique}\nTotal Chunks: {total_chunks}\nChunk Files: {chunks}\nSplitting completed in {time.time() - start_time:.2f} seconds.")
        
        # Map stage
        start_time = time.time()
        mapper_inputs = []
        for chunk in chunks:
            try:
                with open(chunk, 'r', encoding='utf-8') as f:
                    for line_offset, line in enumerate(f):
                        mapper_inputs.append((chunk, line_offset, line.strip()))
            except UnicodeDecodeError:
                print(f"Warning: Encoding issue in {chunk}, trying with 'latin-1' encoding")
                with open(chunk, 'r', encoding='latin-1') as f:
                    for line_offset, line in enumerate(f):
                        mapper_inputs.append((chunk, line_offset, line.strip()))
        
        # Configure the number of processes in the pool based on the number of chunks
        num_processes = min(os.cpu_count() or 4, 4 if len(chunks) <= 8 else 8)
        print(f"Starting mapping phase with {num_processes} parallel mapper processes.")

        with Pool(processes=num_processes) as pool:
            mapper_outputs = pool.starmap(trace_mapper_execution, mapper_inputs)
        sample_mapper_output = mapper_outputs[0][:5] if mapper_outputs and mapper_outputs[0] else []
        print(f"Mapping Details:\nSample Key-Value Pairs: {sample_mapper_output}\nMapping completed in {time.time() - start_time:.2f} seconds.")
        
        # Shuffle stage
        start_time = time.time()
        shuffled_data = shuffle(mapper_outputs)
        print(f"Shuffling completed in {time.time() - start_time:.2f} seconds.")

        # Reduce stage
        start_time = time.time()
        reduced_data = reducer(shuffled_data)
        sample_reduced_output = dict(list(reduced_data.items())[:5]) if reduced_data else {}
        print(f"Reducing Details:\nSample Word Frequencies: {sample_reduced_output}\nReducing completed in {time.time() - start_time:.2f} seconds.")

        # Write output
        try:
            with open(output_file, 'w', encoding='utf-8') as f:
                for word, count in sorted(reduced_data.items(), key=lambda x: x[1], reverse=True):
                    f.write(f"{word} {count}\n")
        except IOError as e:
            print(f"Error writing to output file: {e}")
            raise

        # Cleanup
        for chunk in chunks:
            try:
                if os.path.exists(chunk):
                    os.remove(chunk)
            except OSError as e:
                print(f"Warning: Could not remove chunk {chunk}: {e}")

        # Display top 10 word frequencies
        top_10_words = sorted(reduced_data.items(), key=lambda x: x[1], reverse=True)[:10]
        print("Top 10 Word Frequencies:")
        for word, count in top_10_words:
            print(f"{word}: {count}")

        print("Orchestration Summary:\nAll stages completed successfully. Output written to", output_file)
    
    except FileNotFoundError as e:
        print(f"Error: {e}")
    except Exception as e:
        print(f"Unexpected error: {e}")
        # Cleanup on error
        if 'chunks' in locals():
            for chunk in chunks:
                if os.path.exists(chunk):
                    try:
                        os.remove(chunk)
                    except:
                        pass

if __name__ == '__main__':
    main()
