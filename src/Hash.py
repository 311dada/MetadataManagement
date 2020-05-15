def BKDRHash(path, seed, mod):
    hash_value = 0
    for ch in path:
        hash_value = hash_value * seed + ord(ch)
        hash_value %= mod
    hash_value = hash_value & 0x7FFFFFFF
    hash_value %= mod
    return hash_value
