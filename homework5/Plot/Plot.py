# -*- coding: utf-8 -*-
"""
Created on Wed Nov  7 21:39:39 2018

@author: apple
"""

import matplotlib.pyplot as plt

f = open("Instance.txt")
x = []
y = []
for line in f.readlines():
    point = line.split(",")
    x.append(point[0])
    y.append(point[1])

plt.scatter(x,y,s=5)
#设置标题，坐标轴标签
plt.title("Data Sample",fontsize = 24)
plt.xlabel("X-Value",fontsize = 14)
plt.ylabel("Y-Value",fontsize = 14)
#设置刻度标记大小
plt.tick_params(axis='both',which='major',labelsize=14)
#设置坐标轴取值范围
plt.axis([0,100,0,100])
plt.show()

