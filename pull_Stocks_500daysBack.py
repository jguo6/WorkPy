# -*- coding: utf-8 -*-
"""
Created on Tue Jan 03 16:29:43 2017

@author: jguo
"""
import csv 

def read_csv(name):
    with open(name, 'rb') as csv_file:
        reader = csv.reader(csv_file)
        mydict = dict(reader) 
    return mydict
    
if __name__ == '__main__':
    #reading in csv file back to dictionary
    highs = read_csv('highs.csv')
    lows = read_csv('lows.csv')
    
    #lows' contents 
    #print lows
