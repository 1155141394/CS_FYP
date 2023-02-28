def list_to_int(array):
    value = 0
    length = len(array)
    for i in range(length):
        value += array[length-i-1]*pow(2,i)
    return value

def int_to_list(value):
    array = []
    temp = format(value,"b")
    temp = str(temp)
    for char in temp:
        array.append(int(char))
    return array

array = [1]*500
value = list_to_int(array)
print(int_to_list(value))
