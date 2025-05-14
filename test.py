# filepath: d:\EDU\MapReduce-WordCount-Python-main\test.py
import os
import time

def generate_test_file(filename="test_input.txt", size_in_kb=100):
    """Generate a test input file of approximately the specified size."""
    print(f"Generating test file of size ~{size_in_kb} KB...")
    
    # Sample text to repeat
    sample = "This is a test file for the MapReduce word count program. " \
             "It contains various words that will be counted. " \
             "Some words appear multiple times to test the counting functionality. " \
             "The MapReduce framework should handle this file efficiently. " \
             "Words like test, file, and count should have higher frequencies. " \
             "This sample will be repeated to reach the desired file size. "
    
    # Calculate approximately how many repeats we need
    bytes_per_repeat = len(sample.encode('utf-8'))
    repeats_needed = int((size_in_kb * 1024) / bytes_per_repeat) + 1
    
    try:
        with open(filename, 'w', encoding='utf-8') as f:
            for _ in range(repeats_needed):
                f.write(sample)
        
        actual_size = os.path.getsize(filename) / 1024
        print(f"Created test file: {filename}")
        print(f"Actual file size: {actual_size:.2f} KB")
        return True
    except Exception as e:
        print(f"Error generating test file: {e}")
        return False

def run_test():
    """Run a complete test of the MapReduce word count program."""
    # Generate test input
    if not generate_test_file():
        return
    
    print("\nRunning MapReduce word count on test file...")
    
    # Use direct import to avoid circular imports
    import main
    
    # Record start time
    start_time = time.time()
    
    # Run the main function with our test file
    os.environ['INPUT_FILE'] = 'test_input.txt'
    os.environ['OUTPUT_FILE'] = 'test_output.txt'
    main.main()
    
    # Calculate total execution time
    total_time = time.time() - start_time
    
    print(f"\nTotal execution time: {total_time:.2f} seconds")
    
    # Verify output file
    if os.path.exists('test_output.txt'):
        print("\nTest completed successfully!")
        print("Output written to: test_output.txt")
        
        # Display file sizes
        input_size = os.path.getsize('test_input.txt') / 1024
        output_size = os.path.getsize('test_output.txt') / 1024
        print(f"Input file size: {input_size:.2f} KB")
        print(f"Output file size: {output_size:.2f} KB")
    else:
        print("\nTest failed! No output file was generated.")

if __name__ == "__main__":
    run_test()
