# -*- coding: utf-8 -*-
"""
Created on Sat Nov 10 11:41:27 2018

@author: apple
"""

import matplotlib.pyplot as plt

def color_change(num):
    if num == 1:
        return 'b'
    elif num == 2:
        return 'g'
    elif num == 3:
        return 'r'
    elif num == 4:
        return 'y'
    elif num == 5:
        return 'c'
    elif num == 6:
        return 'k'
    elif num == 7:
        return 'm'
    elif num == 8:
        return 'orange'

def scatter_plot(x,y,color,i):
    plt.scatter(x,y,c=color,s=5)
    #设置标题，坐标轴标签
    title = "Data Result - " + str(i) + "Clusters"
    plt.title(title,fontsize = 24)
    plt.xlabel("X-Value",fontsize = 14)
    plt.ylabel("Y-Value",fontsize = 14)
    #设置刻度标记大小
    plt.tick_params(axis='both',which='major',labelsize=14)
    #设置坐标轴取值范围
    plt.axis([0,100,0,100])
    plt.show()


for i in range(2,9):
    path = r"F:\南京大学(备份)\大三上课程\金融大数据处理技术\作业5\result"
    filename = "(" + str(i) + ",10)\clusteredInstances-10\part-m-00000"
    path = path + filename
    f = open(path)
    x = []
    y = []
    color = []
    for line in f.readlines():
        point = line.split('\t')
        color.append(color_change(int(point[1])))
        x.append(point[0].split(",")[0])
        y.append(point[0].split(",")[1])
    scatter_plot(x,y,color,i)
    

