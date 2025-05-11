def mapper(key, value):
    word_counts = []
    words = value.split()
    for word in words:
        word_counts.append((word, 1))
    return word_counts

if __name__ == '__main__':
    key = 0  
    value = "hello world hello mapreduce"  # Example value (line content)
    print(mapper(key, value))
