from tools import *

arr = [
    [0, 0, 0, 0],
    [0, 1, 1, 0],
    [0, 1, 1, 0],
    [0, 0, 0, 0]
]


test1,shape = compress_array(arr)
print(test1,shape)
print(decompress_array(test1,shape))