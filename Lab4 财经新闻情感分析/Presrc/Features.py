# -*- coding: utf-8 -*-
"""
Created on Sat Dec 15 10:23:34 2018

@author: apple
"""

# -*- coding: utf-8 -*-
"""
Created on Wed Dec 12 20:55:40 2018

@author: apple
"""
'''
评估的第二部分，用于将训练集向量化，写入文件，将测试集向量化，写入文件
'''


import re
import os

def readfeatures(featurelist):
    filePath = 'F:/南京大学(备份)/大三上课程/金融大数据处理技术/实验/实验4 MapReduce高级编程/Assessment/py'
    classlist = {"negative","neutral","positive"}
    count = 0
    for name in classlist:
        path = filePath + "/" + name + ".txt"
        f = open(path)
        lines = f.readlines()
        for line in lines:
            one = []
            word = line.split('\t')[0]
            tfidf = float(re.sub("\n","",line.split("\t")[1]))
            one.append(tfidf)
            one.append(name)
            if word in featurelist.keys():
                if featurelist[word][0] < one[0]:
                    featurelist[word] = one
                    count += 1
            else:
                featurelist[word] = one
    print(count)
    
#生成全部的特征向量tf-idf表示
def trainingfeature(featurelist):
    filePath = 'F:/南京大学(备份)/大三上课程/金融大数据处理技术/实验/实验4 MapReduce高级编程/training_data'
    classlist = {"negative","neutral","positive"}
    index = 0
    newfile = open("./Prefiles/py/train.txt","w")
    for name in classlist:
        path = filePath + '/' + name
        # 进入分类目录，获取当前路径下的文件的名称列表，遍历文件，统计词频数
        files = os.listdir(path)
        print(path)
        for i in range(0,len(files)):
            f = open(path + "/" + str(i) + '.txt')
            iter_f = iter(f)
            text = ''
            for line in iter_f:
                text = text + line
            #去除数字、字母、符号
            text = re.sub("[A-Za-z0-9\ \!\%\《\》\=\[\]\　\─\？\,\.\?\，\。\！\(\)\<\>\：\；\:\;\-\/\"\"\“\”\（\）\+\【\】\、]","",text)
            #向量化
            one = []
            one.append(index)
            for word in featurelist.keys():
                if word in text:
                    one.append(featurelist[word][0])
                else:
                    one.append(0)
            one.append(name)
            j = 0
            for item in one:
                if j == 0:
                    newfile.write(str(item) + '\t')
                else:
                    newfile.write(str(item) + ' ')
                j = j + 1
            newfile.write('\n')
            index += 1
    newfile.close()
    
if __name__ == "__main__":
    #读取特征属性及其对应的tf-idf值，写入内存中
    featurelist = {}
    readfeatures(featurelist)
    print(len(featurelist))
    
    #将样本集向量化，写入文件
    trainingfeature(featurelist)
                        
