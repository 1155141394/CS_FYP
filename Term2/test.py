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

def index(index_map, tag1="", tag2=""):
   tag2 = char_sum(tag2)
   tag1 = char_sum(tag1)
   tag1_val =  index_map.get(tag1)
   tag2_val = index_map.get(tag2)
   res = [index_map.put(tag1, 1), index_map.put(tag2, 1)]
   if tag1_val and tag2_val:
      res.append(True)
   else:
      res.append(False)
      print("not existed")
   return res


for i in tag1:
    for j in tag2:

        print(index(indx, i, j))
