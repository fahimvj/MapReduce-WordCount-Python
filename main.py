import time
import os
from split import split_file
from mapping import mapper
from shuffling import shuffle
from reduce import reducer
from multiprocessing import Pool

def main():
    # Input and output file paths
    input_file = 'input_file.txt' 
    output_file = 'output_file.txt'

    # Split stage
    start_time = time.time()
    chunks = split_file(input_file)
    file_size = os.path.getsize(input_file)
    technique = "File Size based" if file_size > 128 * 1024 * 1024 else "Word Count based"
    total_nodes = len(chunks)
    print(f"Splitting Details:\nFile size: {file_size / (1024 * 1024):.2f} MB\nTechnique: {technique}\nTotal Nodes: {total_nodes}\nChunk Files: {chunks}\nSplitting completed in {time.time() - start_time:.2f} seconds.")

    # Map stage
    start_time = time.time()
    mapper_inputs = []
    for chunk in chunks:
        with open(chunk, 'r', encoding='utf-8') as f:
            for line_offset, line in enumerate(f):
                mapper_inputs.append((line_offset, line.strip()))

    # Configure the number of processes in the pool based on the number of chunks
    num_processes = 4 if len(chunks) <= 8 else 8
    print(f"Using {num_processes} cores for mapping.")
    with Pool(processes=num_processes) as pool:
        mapper_outputs = pool.starmap(mapper, mapper_inputs)
    sample_mapper_output = mapper_outputs[0][:5] if mapper_outputs else []
    print(f"Mapping Details:\nSample Key-Value Pairs: {sample_mapper_output}\nMapping completed in {time.time() - start_time:.2f} seconds.")

    # Shuffle stage
    start_time = time.time()
    shuffled_data = shuffle(mapper_outputs)
    sample_shuffled_output = dict(list(shuffled_data.items())[:5])
    print(f"Shuffling Details:\nSample Grouped Key-Value Pairs: {sample_shuffled_output}\nShuffling completed in {time.time() - start_time:.2f} seconds.")

    # Reduce stage
    start_time = time.time()
    reduced_data = reducer(shuffled_data)
    sample_reduced_output = dict(list(reduced_data.items())[:5])
    print(f"Reducing Details:\nSample Word Frequencies: {sample_reduced_output}\nReducing completed in {time.time() - start_time:.2f} seconds.")

    # Write output
    with open(output_file, 'w', encoding='utf-8') as f:
        for word, count in sorted(reduced_data.items(), key=lambda x: x[1], reverse=True):
            f.write(f"{word} {count}\n")

    # Cleanup
    for chunk in chunks:
        os.remove(chunk)

    # Display top 10 word frequencies
    top_10_words = sorted(reduced_data.items(), key=lambda x: x[1], reverse=True)[:10]
    print("Top 10 Word Frequencies:")
    for word, count in top_10_words:
        print(f"{word}: {count}")

    print("Orchestration Summary:\nAll stages completed successfully. Output written to", output_file)

if __name__ == '__main__':
    main()
