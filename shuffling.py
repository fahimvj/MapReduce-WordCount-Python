from collections import defaultdict

def shuffle(mapper_outputs):
    shuffled = defaultdict(list)
    for output in mapper_outputs:
        for word, count in output:
            shuffled[word].append(count)
    return shuffled

if __name__ == '__main__':
    mapper_outputs = [[('word1', 1), ('word2', 1)], [('word1', 1), ('word3', 1)]]
    print(shuffle(mapper_outputs))
