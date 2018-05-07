import numpy as np
import hashlib

def jaccard_similarity(mhs1, mhs2):
    if(len(mhs1)!=len(mhs2)):
        return -1.0
    return (mhs1 == mhs2).sum() / float(len(mhs1))

def js(a,b):
    a_union_b = len(a) + len(b)

# Class for generating a set of random MinHash hash functions, computing MinHash signatures
class MinHash(object):
    def __init__(self,k,random_seed=50):
        self._k = k
        self._random_seed = random_seed
        # Choose k random integers to XOR to create hashes
        self._masks = (np.random.RandomState(seed=self._random_seed).randint(np.iinfo(np.int64).min, np.iinfo(np.int64).max, self._k))
    def update_min_hash_signature(self, word, min_hash_signature):
        # Create root hash 
        # root_hash = hashlib.md5(word.encode("utf-8")).hexdigest()
        root_hash = hash(word)
        # XOR hash with randomly generated integer to simulate k hash functions
        # Can add bitroll if there's time
        word_hashes = np.bitwise_xor(self._masks, root_hash)
        min_hash_signature = np.minimum(min_hash_signature, word_hashes)
        return min_hash_signature
    def calc_min_hash_signature(self, tokens):
        min_hash_signature = np.empty(self._k, dtype=np.int64)
        min_hash_signature.fill(np.iinfo(np.int64).max)
        for token in tokens:
            min_hash_signature = self.update_min_hash_signature(token, min_hash_signature)
        return min_hash_signature



   
