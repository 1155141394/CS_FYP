import pickle


class HashTable:
    def __init__(self, slots=None, data=None, length=10):  # 初始化散列表
        self.size = length  # 长度为11
        if not slots and not data:
            self.slots = [None] * self.size  # 生成slot列表，将所有槽（列表值）初始为None
            self.data = [None] * self.size  # 生成data列表，将所有槽中数据设置为None
        else:
            self.slots = slots
            self.data = data

    def save_hash(self, filename):
        with open(filename, 'wb') as f:
            pickle.dump(self, f)

    import pickle

    def read_hash(filename):
        with open(filename, 'rb') as f:
            hash_table = pickle.load(f)
            return hash_table

    def hashfunction(self, key):  # 查表函数
        return key % self.size  # 该函数的输入为值返回为值对应的槽

    def rehash(self, oldhashvalue):  # 解决碰撞问题,寻找返回一个新的槽
        return (oldhashvalue + 1) % self.size  # 将原来的哈希值+1取余，寻找一个新的槽位

    def put(self, key, data):  # 储存函数
        hashvalue = self.hashfunction(key)  # 先获得槽，槽就是两个列表共用的索引

        if self.slots[hashvalue] == None:  # 如果该槽为空则直接存入即可
            self.slots[hashvalue] = key  # 将slots列表中对应的位置放入用户设置的键
            self.data[hashvalue] = data  # 将data列表中对应位置放入用户设置的值
            return hashvalue
        else:  # 如果该槽不为空,即哈希值相同且，该哈希值对应的键值对已经被占用了，那么分两种情况
            if self.slots[hashvalue] == key:  # 如果是同一个键
                self.data[hashvalue] = data  # 直接将原有的数据覆盖为新数据
                return hashvalue
            else:  # 如果不是同一个键则开始寻找某一个哈希值（键值对为None）
                nextslot = self.rehash(hashvalue)  # 获取下一个搜寻的槽位
                while self.slots[nextslot] is not None and self.slots[nextslot] != key:
                    # 当下一个待搜寻的槽位不是空且，该槽位对应的键与新插入的键不是一个键时，再接着寻找
                    nextslot = self.rehash(nextslot)
                if self.slots[nextslot] == None:  # 如果找到一个空槽则填入
                    self.slots[nextslot] = key
                    self.data[nextslot] = data
                else:  # 如果发现一个槽位的键与用户输入的键一致则只需要覆盖数据即可
                    self.data[nextslot] = data
                return nextslot

    def get(self, key):  # 输入键寻找值
        startslot = self.hashfunction(key)
        data = None
        stop = False
        found = False
        position = startslot
        while self.slots[position] is not None and not found and not stop:
            if self.slots[position] == key:
                found = True
                data = self.data[position]
            else:
                position = self.rehash(position)  # 接着寻找
                if position == startslot:  # 直到，找了一遍回到最初寻找的位置，都没有找到为止
                    stop = True
        return data

    # def __getitem__(self,key):
    # return self.get(key)附加功能

    # def __setitem__(self,key,data):
    # self.put(key,data)
