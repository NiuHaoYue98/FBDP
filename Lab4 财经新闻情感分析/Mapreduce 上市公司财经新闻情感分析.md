# MapReduce 上市公司财经新闻情感分析

>互联网技术不断发展，给人类带来了更快速的信息传播媒介。在这个互联网时代，不仅是时事新闻，股市新闻传播得也更加快速。股市新闻中往往包含了大量信息，除了上市公司的财务数据外，还包括经营公告、行业动向、国家政策等大量文本信息。这些文本信息中常常包含了一定的情感倾向，会影响股民对公司股票未来走势的预期，进一步造成公司的股价波动。

## 需求
使用多种机器学习算法对文本进行情感判别，包括KNN、决策树、朴素贝叶斯、支持向量机等，学习如何进行模型训练，如何进行分类预测。

## 实验数据集
* 测试集：实验3数据集fulldata.txt
* 样本数据集:training_data.zip
## 主要设计思路
整体的工作可以分为三大部分：数据预处理、模型训练、分类算法评估。其中模型训练预测时，由于测试集单条新闻包含的词语数量过少，选择将某一只股票的新闻集中，而后预测这只股票的整体情感，而非单条新闻的情感。

| 阶段 | 功能 | 实现方法 |
| :-: | :-----:   | :---: |
|数据预处理|文本分词，选择特征向量，样本文本向量化，测试集文本向量化|Python|
|模型训练|根据训练集数据及其分类信息，应用KNN和朴素贝叶斯算法生成训练模型，并对测试集做出分类预测|MapReduce|
|分类效果评估|利用训练数据集，使用交叉验证的方法对分类算法进行评估|MapReduce|
### 数据预处理 
1. 首先需要提取训练数据集的内容，选择特征值。这里选用tf-idf值较高的词语作为特征属性，其对应的tf-idf的值作为特征值。按照原有分类分别统计，每个分类输出tf-idf值最高的500个词语。
2. 读取特征词文件，去除其中的相同词语并做适当的筛选，最终得到的特征属性的数量为904。根据选定的特征值，将样本数据集的数据向量化，格式为tri(tid,A,y)，其中A为各特征属性的值。由于训练集可以训练的文本内容较少，A实际是一个稀疏向量。
3. 读取测试集数据，同样首先进行文本分词及停用词去除，然后将测试数据集向量化，产生最终的待训练文本。以下是使用到的.py文件及其说明：

|功能|文件/函数 | 结果文件 | 输出格式 |
| :---: |:---:| :---: | --- |
| 提取特征词| `tf-idf.py`	|`./py/negative.txt`、`./py/positive.txt`、`./py/neutral.txt` |`下滑	0.01766767815160078`|
核心代码：

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

|功能|文件/函数 | 结果文件 | 输出格式 |
| :---: |:---:| :---: | --- |
|训练样本特征化|`features.txt`|`train.txt`|`12	0.07457087611974442 0 0 0 0 0 0.05187946315696957 ······ 0.0022368721591347557 0 0.0022262100354080144 0 0 0 0 negative `|
|测试样本特征化| `stockwords.py` |`test.txt`|`sh600000浦发银行	0.07457087611974442 0 0 0 0 0 0 0.026442377281342792 0.027889043415843688 0 0 0 0 0 0 0  ······ 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0.0022128256896506064 -1 `|

核心代码：

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



### 模型训练
	KNN
	1. 从文件系统中读入训练集和测试集的数据，读取到缓存中
	2. Map阶段对每个读出的测试样本数据ts(trid,A',y')计算与每个训练样本中数据的距离，选取最小的K个
	3. 根据模型加权计算出测试数据的分类，发射(tsid,y')
	4. Reduce阶段直接将序号和类别输出到结果文件
	
核心代码：

            for (int i = 0; i < trainSet.size(); i++) {
               	testInstance.getattr().length);
                try {
                    if(trainSet.get(i).attr.length != testInstance.attr.length)
                        System.out.println(testInstance.tid  + " " + "train " + trainSet.get(i).attr.length + "temp " + testInstance.attr.length);
            
                    double dis = Distance.EuclideanDistance(trainSet.get(i).getattr(), testInstance.getattr());
              
                    int index = indexOfMax(distance);
                    if (dis < distance.get(index)) {
                        distance.remove(index);
                        traintag.remove(index);
                        distance.add(dis);
                        traintag.add(trainSet.get(i).tag);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

#
	朴素贝叶斯
	1. 从文件系统中读入训练集和测试集的数据
	2. 扫描训练集数据，计算每个分类Y出现的频度以及每个属性值出现在Yi中的频度，输出保存到结果文件中
	3. 读入测试样本数据、上一步产生的频度文件
	4. Map求出P(X|Yi)*P(Yi)最大的分类类别，此时Yi就是X所属的分类


核心代码：

            while(scan.hasNextLine())
            {
                str = scan.nextLine();
                vals = str.split(" ");
                String classname = vals[vals.length-1];
                word.set(classname);
                context.write(word, one);
                for(i = 1; i<vals.length-1; i++)
                {
                    word = new Text();
                    temp = classname + "#" + nBConf.proNames.get(i-1);
                    temp += "#" + vals[i];
                    word.set(temp);
                    context.write(word, one);
                }
            }


### 分类算法评估
将数据集划分为K（K=10）个大小相同的互斥子集，每次用K-1(9)个作为训练集，剩余的一个作为测试集，使用上述的分类算法得到结果，与原本的训练集结果做出比较。使用了Cross（用于划分文件）和Analysis(用于统计结果)

* Cross ：划分训练集

            if (id < min || id >= max) {
                IntWritable textIndex = new IntWritable();
                textIndex.set(id);
                Text cont = new Text();
                String mes = "";
                double [] attr = testInstance.getattr();
                for(int i = 0;i < attr.length;i++){
                    mes = mes + (double)attr[i] + ' ';
                }
                //System.out.println(attr.length);
                mes = mes + testInstance.tag;
                cont.set(mes);
                context.write(textIndex, cont);
            }

* Analysis：统计每一类中的数目以及正确的数目

            if(tag.equals(trainSet.get(index).tag))
                classkey.set("correct_" + tag);
            else
                classkey.set(tag);
            context.write(classkey,one);
            classkey.set("total");
            context.write(classkey,one);


## 类说明
| 类名 | 功能 | Map | Reduce |
| :--: |:---:|:---:|:--:|
|EmotionAnalysisDriver|统筹各项任务进程，main函数|——| ——|
|Tri| 训练集、测试集数据标准化，定义了tri(tid,A,y)的变量及相关方法 |——|——|
| KNN |使用KNN方法分类|`(<textid> <class>)`|——|
| Cross| 为交叉验证划分训练集文件，最终从原训练集中提取出10个新的用于验证的训练集 |`(<textid> <Attr_list class>)`|——|
|Analysis|统计交叉验证的结果,统计每一类的正确数，总数|`(<classkey> <one>)`|`(<key> <result>)`|
|NaiveBayesTrain|使用朴素贝叶斯的方法训练|`(<classname> <one>)`、`(<attrindex> <one>)`|`(<classname> <num>)`、`(<attrindex> <num>)`|
|NaiveBayesTest|使用朴素贝叶斯的方法预测|`(<textid> <class>)`|——|

## 执行及结果分析
* 程序执行时输入的参数为：`finalinput/train.txt finalinput/test.txt 3 newcrossoutput 10 crossinput`
* 完成实验是分阶段进行的，因此KNN，CrossTest，NaiveBayes没有同时在Driver中执行。如果同时执行NaiveBayes输出结果会覆盖KNN的结果。执行代码时可以考虑注释掉某一部分以提高效率
* KNNKNN分类结果文件为`KNNoutput.txt`，朴素贝叶斯分类结果文件为`NavieBayes/result.txt`
* KNN交叉验证得到的分类准确率为43.88%，准确率较低。
* 朴素贝叶斯算法中由于连乘的计算方法，可能导致数据过过大而溢出。因此在程序中采用了超过某一阈值就缩小倍数的方法。调试时统计了每个股票比较时的缩放次数，发现positive和neutral的次数相近，而negative的缩放次数明显较小，因此最终结果里几乎没有negative类型的新闻，与KNN分类的结果类似。


## 不足及拓展

### 拓展
| 优化 | 详情 |
| :--- | :---   |
|朴素贝叶斯乘积|由于某些词语频次较高，连乘中会出现结果过大从而溢出。因此在fxyi更新后会进行判断，大于某一阈值后会缩小一定的倍数|
|分类结果评估|实现了交叉验证功能，从而进行了多次调试。在该过程中调整了特征属性的数量及K的值，以便提高预测的正确率。在此过程中发现：1.KNN的方法很容易将测试样本判定为positive，在三类中Positive的正确率最高 2.特征属性较少时（比如尝试过150和500个特征属性）分类准确率接近甚至某类会低于随机分类水平|

### 不足
| 不足 | 详情 | 改进|
| :--: | :---:| :--:	|
|文件读写次数过多| 交叉验证中文件划分和训练的过程进行了多次文件读写，使得程序整体效率不高| 考虑改写文件划分的方法，比如在KNN中加入是否进行验证的判断，从而使得读入训练集时只需要从原始输入文件中读取适当序号内的文件|
|Tri变量类型设计|交叉验证与KNN分类公用KNN的分类方法，交叉验证中判断是否分类正确时，设计要求在输出分类结果时按序号顺序输出，因此Key的类型需要时IntWritable。而为训练集分类时，KNN输出的Key是股票代码和股票名,Key的类型是Text|可以考虑修改交叉验证的统计函数，使判断结果不依赖于序号顺序|
|分类准确率不高|使用自己的交叉验证得到的分类准确率仅仅比随机分类略高|认为原因是训练集和测试集的文本量都不够大，选择的特征词对文本情感的描述性不强。可以考虑找专用的情感分析的特征词库进行实验|
|可拓展性不强|朴素贝叶斯方法实现中直接将需要的配置写入到了程序里。样本特征属性值发生变化时需要修改程序。且配置的部分参数由手工算得到。|考虑增加针对朴素贝叶斯的预处理程序，从而由代码产生针对性强的配置文件|


