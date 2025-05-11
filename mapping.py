def mapper(chunk_file):
    word_counts = []
    with open(chunk_file, 'r', encoding='utf-8') as f:
        for line in f:
            words = line.split()
            for word in words:
                word_counts.append((word, 1))
    return word_counts

if __name__ == '__main__':
    chunk_file = 'chunk_0.txt'
    print(mapper(chunk_file))
