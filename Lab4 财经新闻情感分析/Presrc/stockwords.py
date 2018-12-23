# -*- coding: utf-8 -*-
"""
Created on Fri Dec 14 00:04:25 2018

@author: apple
"""
import re

'''
得到测试集的分词结果文件
'''
'''
获取特征词的列表，如果某个词在样本中，就将刚才的tf-idf值填入
'''
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

#处理测试集数据
def testfiles():
    f = open("new.txt",'r')
    lines = f.readlines()
    f.close()
    lastname = ''
    text = ''
    for line in lines:
        items = line.split('\t')
        lastname = items[1]
        lastindex = items[0]
        break
    newfile = open("./Prefiles/py/test.txt","w")
    for line in lines:
        items = line.split('\t')
        temp_index= items[0]
        temp_name = items[1]
        if temp_name ==lastname:
            tempwords = re.sub("[A-Za-z0-9\ \!\%\《\》\=\[\]\　\─\？\,\.\?\，\。\！\(\)\<\>\：\；\:\;\-\/\"\"\“\”\（\）\+\【\】\、]","",items[4])
            text = text + tempwords
        else:
            lastname = re.sub("[*]",'',lastname)
            print(lastname)
        
            one = []
            one.append(lastindex + lastname)
            for word in featurelist.keys():
                if word in text:
                    one.append(featurelist[word][0])
                else:
                    one.append(0)
            one.append(-1)
            print(len(one))
            j = 0
            for item in one:
                if j == 0:
                    newfile.write(str(item) + '\t')
                else:
                    newfile.write(str(item) + ' ')
                j = j + 1
            newfile.write('\n')
            text = ''
            lastname = temp_name
            lastindex = temp_index
    newfile.close()
    
if __name__ == "__main__":
    #读取特征属性及其对应的tf-idf值，写入内存中
    featurelist = {}
    readfeatures(featurelist)
    print('total length is ' + str(len(featurelist)))
    
    #处理测试集数据
    testfiles()

    













