# MapReduce Word Frequency Count

- **Name**: MD Fahim Shahriar Chowdhury
- **Contact**: fahimbcp@gmail.com
- **Date**: 11 May 2025

---

##  Details
Develop a MapReduce-based solution in Python to perform a word frequency count. Use the provided input file for your implementation. Submit both your Python code and the resulting output.

### Input File:
- **File Name**: `input_file.txt`

---

## Program Overview

This Python program implements a MapReduce-based solution to count word frequencies in a text file. The program is divided into five main components:

1. **`split.py`**: Handles file splitting dynamically based on file size or word count.
2. **`mapping.py`**: Contains the mapper logic to generate key-value pairs.
3. **`shuffling.py`**: Groups intermediate key-value pairs by key.
4. **`reduce.py`**: Contains the reducer logic to aggregate word counts.
5. **`main.py`**: Orchestrates the entire MapReduce process and measures processing time for each stage.

---

## Execution Steps

1. **Splitting**:
   - Splits the input file into smaller chunks for parallel processing.
   - Uses file size or word count as the splitting technique.

2. **Mapping**:
   - Processes each chunk to generate intermediate key-value pairs `(word, 1)`.

3. **Shuffling**:
   - Groups all intermediate key-value pairs by key (word).

4. **Reducing**:
   - Aggregates the counts for each word to calculate total frequencies.

5. **Output**:
   - Writes the final word counts to `output_file.txt`.
   - Displays the top 10 word frequencies in the console.

---

## How to Run

1. Place the input file as `input_file.txt` in the same directory.
2. Run the `main.py` script using Python:
   ```bash
   python main.py
