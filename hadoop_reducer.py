#!/usr/bin/env python3
# filepath: d:\EDU\MapReduce-WordCount-Python-main\hadoop_reducer.py

import sys

def main():
    """
    Hadoop Streaming reducer function for word count.
    Reads (word, count) pairs from stdin, aggregates by word, and outputs totals.
    """
    current_word = None
    current_count = 0

    # Process each line from standard input
    # Input is sorted by key (word) from Hadoop shuffle phase
    for line in sys.stdin:
        # Parse the input from mapper
        line = line.strip()
        word, count = line.split('\t', 1)
        
        try:
            count = int(count)
        except ValueError:
            # Skip lines with invalid counts
            continue
            
        # If we're still on the same word, accumulate the count
        if current_word == word:
            current_count += count
        else:
            # We've encountered a new word
            # Output the previous word's result (except for the first word)
            if current_word:
                print(f"{current_word}\t{current_count}")
                
            # Reset for the new word
            current_word = word
            current_count = count
    
    # Output the final word's count
    if current_word:
        print(f"{current_word}\t{current_count}")

if __name__ == "__main__":
    main()
