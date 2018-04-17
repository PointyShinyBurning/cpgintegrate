import cpgintegrate
import random
import string


def test_ordering_sequence_unique():
    items = {''.join(random.choices(string.ascii_letters, k=random.randrange(1, 10)))
             for _ in range(20, random.randrange(200))}
    sequences = [[] for _ in range(2, 20)]
    for item in items:
        random.choice(sequences).append(item)

    result = cpgintegrate.ordering_sequence(sequences)
    assert all([item for item in result if item in sequence] == sequence for sequence in sequences)
