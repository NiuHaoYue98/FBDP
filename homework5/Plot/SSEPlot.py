# -*- coding: utf-8 -*-
"""
Created on Sat Nov 10 21:09:44 2018

@author: apple
"""

import matplotlib.pyplot as plt

x = [2,3,4,5,6,7,8]
SSE = []
for i in range(2,9):
    path = r"F:\南京大学(备份)\大三上课程\金融大数据处理技术\作业5\result"
    filename = "(" + str(i) + ",10)\cluster-10\part-r-00000"
    path = path + filename
    f = open(path)
    temp_sse = 0
    for line in f.readlines():
        temp_sse += float(line.split(',')[2])
    SSE.append(temp_sse)
    print(SSE[i-2])
    
plt.plot(x, SSE, marker='o', mec='r', mfc='w')
plt.xlabel("Cluster Number")
plt.ylabel("SSE")
plt.show()