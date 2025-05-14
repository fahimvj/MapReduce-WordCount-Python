#!/usr/bin/env python3
# filepath: d:\EDU\MapReduce-WordCount-Python-main\hadoop_mapper.py

import sys
import re

def main():
    """
    Hadoop Streaming mapper function for word count.
    Reads lines from stdin and outputs (word, 1) pairs.
    """
    # Regular expression to split words and remove punctuation
    word_pattern = re.compile(r'[A-Za-z]+')
    
    # Process each line from standard input
    for line in sys.stdin:
        # Remove leading/trailing whitespace and convert to lowercase
        line = line.strip().lower()
        
        # Find all words in the line
        words = word_pattern.findall(line)
        
        # Emit intermediate key-value pairs
        for word in words:
            # Format output as: word\t1
            print(f"{word}\t1")

if __name__ == "__main__":
    main()
