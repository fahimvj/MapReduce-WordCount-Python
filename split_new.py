# filepath: d:\EDU\MapReduce-WordCount-Python-main\split.py
import os
import math
from multiprocessing import Pool

def split_file(input_file, chunk_size=128 * 1024 * 1024, word_chunk_size=1000):
    try:
        file_size = os.path.getsize(input_file)
        chunks = []

        if file_size > chunk_size:
            try:
                with open(input_file, 'r', encoding='utf-8') as f:
                    chunk_number = 0
                    while True:
                        chunk_data = f.read(chunk_size)
                        if not chunk_data:
                            break
                        chunk_file = f'chunk_{chunk_number}.txt'
                        with open(chunk_file, 'w', encoding='utf-8') as chunk:
                            chunk.write(chunk_data)
                        chunks.append(chunk_file)
                        chunk_number += 1
            except UnicodeDecodeError as e:
                print(f"Unicode decode error in file {input_file}: {e}, trying with 'latin-1' encoding")
                with open(input_file, 'r', encoding='latin-1') as f:
                    chunk_number = 0
                    while True:
                        chunk_data = f.read(chunk_size)
                        if not chunk_data:
                            break
                        chunk_file = f'chunk_{chunk_number}.txt'
                        with open(chunk_file, 'w', encoding='latin-1') as chunk:
                            chunk.write(chunk_data)
                        chunks.append(chunk_file)
                        chunk_number += 1
        else:
            try:
                with open(input_file, 'r', encoding='utf-8') as f:
                    words = f.read().split()
                    total_words = len(words)
                    num_chunks = math.ceil(total_words / word_chunk_size)
                    for i in range(num_chunks):
                        chunk_file = f'chunk_{i}.txt'
                        with open(chunk_file, 'w', encoding='utf-8') as chunk:
                            chunk.write(' '.join(words[i * word_chunk_size:(i + 1) * word_chunk_size]))
                        chunks.append(chunk_file)
            except UnicodeDecodeError as e:
                print(f"Unicode decode error in file {input_file}: {e}, trying with 'latin-1' encoding")
                with open(input_file, 'r', encoding='latin-1') as f:
                    words = f.read().split()
                    total_words = len(words)
                    num_chunks = math.ceil(total_words / word_chunk_size)
                    for i in range(num_chunks):
                        chunk_file = f'chunk_{i}.txt'
                        with open(chunk_file, 'w', encoding='latin-1') as chunk:
                            chunk.write(' '.join(words[i * word_chunk_size:(i + 1) * word_chunk_size]))
                        chunks.append(chunk_file)
        
        return chunks
    except Exception as e:
        print(f"Error in split_file: {e}")
        return []

if __name__ == '__main__':
    input_file = 'input_file.txt'
    split_file(input_file)
