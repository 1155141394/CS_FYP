from hash import HashTable

tag1 = ["node1", "node2"]
tag2 = ["cpu1", "cpu2", "cpu3"]
indx = HashTable(5000)


def char_sum(str):
    res = 0
    count = 1
    for c in str:
        res += ord(c) * count
        count *= 256
    return res


for i in tag1:
    for j in tag2:
        x = char_sum(i)
        y = char_sum(j)
        print(indx.put(x, 1), indx.put(y, 1))
