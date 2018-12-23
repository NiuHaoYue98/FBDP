# -*- coding: utf-8 -*-
"""
Created on Sat Dec 15 00:10:31 2018

@author: apple
"""

import jieba.analyse
import os
import re
filePath = 'F:/南京大学(备份)/大三上课程/金融大数据处理技术/实验/实验4 MapReduce高级编程/training_data'
# 进入分类目录，获取当前路径下的文件的名称列表，遍历文件，统计词频数        
classlist = {"/negative","/neutral","/positive"}
for name in classlist:
    #print(name)
    text = ''
    path = filePath + name
    files = os.listdir(path)
    for i in range(0,len(files)):
        f = open(path + "/" + str(i) + ".txt")
        lines = f.readlines()
        for line in lines:
            text = text + line
            text = re.sub("[0-9\ \!\%\《\》\=\[\]\　\─\？\,\.\?\，\。\！\(\)\<\>\：\；\:\;\-\/\"\"\“\”\（\）\+\【\】\、]","",text)
    keywords = jieba.analyse.extract_tags(text, topK=500, withWeight=True, allowPOS=())
    filename = './Prefiles/py' + name + ".txt"
    new_file  = open(filename,"w")
    #print(len(keywords))
    for item in keywords:
        print (item[0], item[1])
        new_file.write(item[0] + "\t" + str(item[1]) + "\n")
    print('\n')
    new_file.close()
            
