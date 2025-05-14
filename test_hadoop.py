#!/usr/bin/env python3
# filepath: d:\EDU\MapReduce-WordCount-Python-main\test_hadoop.py

import os
import subprocess

def print_section(title):
    """Print a section header"""
    print("\n" + "="*80)
    print(f" {title} ".center(80, "="))
    print("="*80 + "\n")

def test_mapper():
    """Test the Hadoop mapper locally"""
    print_section("TESTING MAPPER")
    
    # Create a small test input
    test_input = """Hello world
    This is a test
    Hello again, world"""
    
    # Run the mapper script
    print("Input to mapper:")
    print(test_input)
    print("\nOutput from mapper:")
    
    # Use subprocess to pipe the test input to the mapper script
    mapper_path = os.path.join(os.getcwd(), "hadoop_mapper.py")
    if not os.path.exists(mapper_path):
        print(f"Error: Mapper script not found at {mapper_path}")
        return False
    
    try:
        process = subprocess.run(
            ["python", mapper_path],
            input=test_input,
            text=True,
            capture_output=True
        )
        print(process.stdout)
        if process.returncode != 0:
            print(f"Mapper test failed with exit code {process.returncode}")
            print(f"Error: {process.stderr}")
            return False
    except Exception as e:
        print(f"Error running mapper: {e}")
        return False
    
    return True

def test_reducer():
    """Test the Hadoop reducer locally"""
    print_section("TESTING REDUCER")
    
    # Create a sample mapper output (already sorted as it would be by Hadoop)
    test_input = """hello\t1
    hello\t1
    test\t1
    world\t1
    world\t1"""
    
    # Run the reducer script
    print("Input to reducer (sorted mapper output):")
    print(test_input)
    print("\nOutput from reducer:")
    
    # Use subprocess to pipe the test input to the reducer script
    reducer_path = os.path.join(os.getcwd(), "hadoop_reducer.py")
    if not os.path.exists(reducer_path):
        print(f"Error: Reducer script not found at {reducer_path}")
        return False
    
    try:
        process = subprocess.run(
            ["python", reducer_path],
            input=test_input,
            text=True,
            capture_output=True
        )
        print(process.stdout)
        if process.returncode != 0:
            print(f"Reducer test failed with exit code {process.returncode}")
            print(f"Error: {process.stderr}")
            return False
    except Exception as e:
        print(f"Error running reducer: {e}")
        return False
    
    return True

def test_full_pipeline():
    """Run the full Hadoop pipeline on the input file"""
    print_section("FULL HADOOP PIPELINE TEST")
    
    # Use the existing input file
    input_path = os.path.join(os.getcwd(), "input_file.txt")
    
    if not os.path.exists(input_path):
        print(f"Error: Input file not found at {input_path}")
        return False
    
    print(f"Using input file: {input_path}")
    
    # Read the input file
    with open(input_path, 'r', encoding='utf-8') as f:
        test_input = f.read()
    
    # Paths to mapper and reducer
    mapper_path = os.path.join(os.getcwd(), "hadoop_mapper.py")
    reducer_path = os.path.join(os.getcwd(), "hadoop_reducer.py")
    
    # Run mapper
    print("Running mapper...")
    mapper_process = subprocess.run(
        ["python", mapper_path],
        input=test_input,
        text=True,
        capture_output=True
    )
    
    if mapper_process.returncode != 0:
        print(f"Mapper failed: {mapper_process.stderr}")
        return False
    
    # Sort the mapper output
    print("Sorting mapper output...")
    sorted_lines = sorted(mapper_process.stdout.strip().split('\n'))
    sorted_output = '\n'.join(sorted_lines)
    
    # Run reducer
    print("Running reducer...")
    reducer_process = subprocess.run(
        ["python", reducer_path],
        input=sorted_output,
        text=True,
        capture_output=True
    )
    
    if reducer_process.returncode != 0:
        print(f"Reducer failed: {reducer_process.stderr}")
        return False
    
    # Save the output to output_file_hadoop.txt
    output_file = "output_file_hadoop.txt"
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write(reducer_process.stdout)
    
    print(f"Output saved to {output_file}")
      # Show a preview
    word_counts = reducer_process.stdout.strip().split('\n')
    for line in word_counts[:10]:
        print(line)
    if len(word_counts) > 10:
        print(f"... and {len(word_counts) - 10} more words")
    
    return True

def main():
    """Run all Hadoop tests"""
    print_section("HADOOP MAPREDUCE TESTING SUITE")
    
    passed = 0
    total = 3
    
    # Test 1: Mapper test
    if test_mapper():
        print("\n[PASS] Mapper test PASSED")
        passed += 1
    else:
        print("\n[FAIL] Mapper test FAILED")
    
    # Test 2: Reducer test
    if test_reducer():
        print("\n[PASS] Reducer test PASSED")
        passed += 1
    else:
        print("\n[FAIL] Reducer test FAILED")
    
    # Test 3: Full pipeline test
    if test_full_pipeline():
        print("\n[PASS] Full Hadoop pipeline test PASSED")
        passed += 1
    else:
        print("\n[FAIL] Full Hadoop pipeline test FAILED")
    
    print_section(f"TEST RESULTS: {passed}/{total} TESTS PASSED")
    
    print("\nTo run on a real Hadoop cluster:")
    print("1. Upload input file to HDFS: hdfs dfs -put input_file.txt /user/hadoop/input/")
    print("2. Run the Hadoop job:")
    print("   - Linux/macOS: ./run_hadoop.sh /user/hadoop/input/input_file.txt /user/hadoop/output/")
    print("   - Windows PowerShell: ./Run-HadoopJob.ps1 -InputFile \"/user/hadoop/input/input_file.txt\" -OutputDir \"/user/hadoop/output/\"")

if __name__ == "__main__":
    main()
