def reducer(shuffled_data):
    reduced = {}
    for word, counts in shuffled_data.items():
        reduced[word] = sum(counts)
    return reduced

if __name__ == '__main__':
    shuffled_data = {'word1': [1, 1], 'word2': [1], 'word3': [1]}
    print(reducer(shuffled_data))
