# -*- coding: utf-8 -*-
"""
Created on Thu Dec 01 14:44:28 2016

@author: jguo
"""

######find most frequent integer 
def most_common(lst):
    return max(set(lst), key=lst.count) 

###########find pairs in integer array who's sum is = 0
x = [1, 2, 4, 5, 3, 2, 5, 3, 6, 10, 0, 11, 15]
def sum_10(lst):
    y = []
    for x in lst:
        rm = 10 - x
        if rm in lst and lst.count(x) != 1:
            y.append((x, rm))
            lst.remove(x)
        elif x == 10 and (0 in lst):
            y.append((x, 0))
            lst.remove(x)
    return y
print sum_10(x)

###########################fib sequence
def fib(x, cache = {0: 0, 1: 1}):
    if x in cache:
        return cache[x]
    else:
        sum = 0
        sum = fib(x - 1, cache) + fib (x - 2, cache)
        cache[x] = sum
        return sum
    
print fib(20)

#########################Find most common element in 2 arrays
def common(x, y):
    b = x + y
    return max(set(b), key = b.count)
    
print common([29, 29, 30], [29, 30, 31])

############################implement binary search on 2 arrays 
def find(x, y, high = len(y) - 1, low = 0):
        mid = high + low / 2
        if x == y[mid]:
            return true
        else:
            h = high + 1
            b = y[low : mid]
            a = y[mid + 1: h]
            return find(x, b, mid - 1, 0) or find(x, a, h, mid)
    
print find(5, [99, 4, 3, 33, 22, 11, 44, 6, 5, 99, 100])

            
    