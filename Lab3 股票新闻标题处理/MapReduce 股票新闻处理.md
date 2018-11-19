# MapReduce 股票新闻标题处理

## 需求
需求1：针对股票新闻数据集中的新闻标题，编写WordCount程序，统计所有除Stop-word（如“的”，“得”，“在”等）出现次数k次以上的单词计数，最后的结果按照词频从高到低排序输出。

需求2：针对股票新闻数据集，以新闻标题中的词组为key，编写带URL属性和词频的文档倒排索引程序，并按照词频从大到小排序，将结果输出到指定文件。输出格式可以如下： 

 * 高速公路， 10， 股票代码，[url0, url1,...,url9]   
 * 高速公路， 8， 股票代码，[url0, url1,...,url7]

## 主要设计思路
需求可以归纳为三个部分：语句分词(InvertedIndex.java)，词频统计(WordCount.java)，排序(Sort.java)
![](https://i.imgur.com/hJQ2dtj.png)
### 语句分词 ( InvertedIndex.java )
1. 随机选取小样本，按行读取文件。
2. 根据原始文件的结构提取标题文本内容，去除数字及符号，使用HanLP分词器标准分词。 
3. Mapper按照设计格式发送信息给Reducer，Reducer计算文件内的词频，输出到文件中。
4. 查看以上分词结果，在分词器词典目录/data/dictionary/custom下新增文件"股票词典.txt"，其中包含了一些常见的股票专有名词（A股、H股等）、新闻名词及新兴词汇（一带一路等）；在分词器词典目录/data/dictionary/下创建"mystopwords.txt"，用于记录样本的中文停用词。
4. 使用新增的词典及停词表，进行以上步骤，处理全局文件，结果写入InvertedIndex-temp。

### 词频统计 ( WordCount.java )
1. 逐行读入InvertedIndex-temp，以word为Key，重新汇总信息，发给Reduce端。
2. Reduce汇总得到总词频，同时保留与词语相关的信息，包括股票代码(id)，部分和(sum)，url(urls)，写入WordCount-temp文件。

### 排序 ( Sort.java )
1. 逐行读入WordCount-temp。
2. 创建Inpair符合主键，包含<total, sum>作为Mapper发送的Key。
3. 设计二次排序规则，整体依据词语总词频由高到低排序，内部依据每个文件中的词频由高到低排序。
4. 完成排序，按格式写入Result。

## 算法及数据结构

分词及词频统计较为简单，类似英文词频统计WordCount。这里重点说明二次排序的实现，其中定义的`IntPair`结构实现如下。IntPair作为SortJob中Map阶段的key值，内容为<total,sum>  
```  
public static class IntPair implements WritableComparable<IntPair> {  
        private int first = 0;	//total num
        private int second = 0;	//file sum

        public void set(int left, int right) {
            first = left;
            second = right;
        }
        public int getFirst() {
            return first;
        }
        public int getSecond() {
            return second;
        }
     
        public void readFields(DataInput in) throws IOException {
            first = in.readInt() + Integer.MIN_VALUE;
            second = in.readInt() + Integer.MIN_VALUE;
        }
        public void write(DataOutput out) throws IOException {
            out.writeInt(first - Integer.MIN_VALUE);
            out.writeInt(second - Integer.MIN_VALUE);
        }
        public int hashCode() {
            return first * 157 + second;
        }
        public boolean equals(Object right) {
            if (right instanceof IntPair) {
                IntPair r = (IntPair) right;
                return r.first == first && r.second == second;
            } else {
                return false;
            }
        }
        public static class Comparator extends WritableComparator {
            public Comparator() {
                super(IntPair.class);
            }

            public int compare(byte[] b1, int s1, int l1,
                               byte[] b2, int s2, int l2) {
                return -compareBytes(b1, s1, l1, b2, s2, l2);
            }
        }

        static {                                        // register this comparator
            WritableComparator.define(IntPair.class, new Comparator());
        }

        public int compareTo(IntPair o) {
            if (first != o.first) {
                return first < o.first ? -1 : 1;
            }
            else if (second != o.second) {
                return second < o.second ? -1 : 1;
            } else {
                return 0;
            }
        }
    }
```

该结构中实现了WritableComparable接口，并且实现compareTo方法的比较策略。这个用于mapreduce的第一次默认排序，也就是发生在map阶段的sort小阶段，但是其对最终的二次排序结果是没有影响的。二次排序的最终结果是由自定义比较器决定的。

自定义比较器决定了二次排序的结果。自定义比较器需要继承WritableComparator类，并且重写compare方法实现自己的比较策略。自定义比较器及分区实现如下：

```  
public static class FirstPartitioner extends Partitioner<IntPair,IntWritable>{
        @Override
        public int getPartition(IntPair key, IntWritable value,int numPartitions) {
            return Math.abs(key.getFirst() * 127) % numPartitions;
        }
    }

public static class FirstGroupingComparator implements RawComparator<IntPair> {
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            return WritableComparator.compareBytes(b1, s1, Integer.SIZE/8,
                    b2, s2, Integer.SIZE/8);
        }

        public int compare(IntPair o1, IntPair o2) {
            int l = o1.getFirst();
            int r = o2.getFirst();
            return l == r ? 0 : (l < r ? -1 : 1);
        }
    }

```


## 类说明
| 类名 | 功能 | Map | Reduce |
| :---         |     :---       |          :--- |
|InvertedIndex|分词，统计单个文件中词频，整合单个文件中url信息|`(<word,id> <url>)`| `(<word sum> <id url1,url2……>)`|
输出样例

``` 
A股 1	sh600007 http://finance.sina.com.cn/stock/s/2017-01-20/doc-ifxzuswr9614661.shtml
```

| 类名 | 功能 | Map | Reduce |
| :---         |     :---       |          :--- |
|WordCount|统计全局词频，同时保留文件词频及url信息 |`(<word> <id sum url1,url2……>)`|`（<word totalsum> <sum,id,url1,urs2…… sum,id,url1,urs2…… >）`|

输出样例

```  
产能 5	sz300408,2,http://finance.sina.com.cn/stock/t/2017-02-06/doc-ifyafenm2841915.shtml,http://finance.sina.com.cn/stock/s/2017-02-06/doc-ifyaexzn9035076.shtml sh600389,1,http://finance.sina.com.cn/stock/t/2017-08-01/doc-ifyinwmp1298485.shtml sz300611,1,http://finance.sina.com.cn/stock/t/2017-06-15/doc-ifyhfhrt4349880.shtml sz300658,1,http://finance.sina.com.cn/roll/2017-06-13/doc-ifyfzaaq6234544.shtml 
```

| 类名 | 功能 |Map | Reduce |
| :---         |     :---       |          :--- |
|Sort|排序|`(<IntPair> <word,id,urls>)`|`(<totalnum word> <"\n"id sum urls>)`|
输出样例

```
样例  
6 解除	
sz000005 2 http://finance.sina.com.cn/stock/t/2016-12-29/doc-ifxzcvfp5098389.shtml http://finance.sina.com.cn/stock/t/2017-01-04/doc-ifxzczsu6735346.shtml
sh600528 1 http://finance.sina.com.cn/stock/t/2017-03-23/doc-ifycsukm3247854.shtml
sz000551 1 http://finance.sina.com.cn/stock/t/2017-01-25/doc-ifxzunxf2026284.shtml
sz000718 1 http://finance.sina.com.cn/stock/t/2017-01-09/doc-ifxzkssy1312242.shtml
sz002471 1 http://finance.sina.com.cn/stock/t/2016-12-10/doc-ifxypcqa9228556.shtml
```

## 结果分析
最终输出了出现次数在100次以上的词语，路径为output/Result。它作为唯一的结果文件，可以满足需求1和需求2。[完整输出文件：https://pan.baidu.com/s/1j4LeDZZ6uPq0SvxPwIQGfA](https://pan.baidu.com/s/1j4LeDZZ6uPq0SvxPwIQGfA)

## 不足及优化
### 不足
| 不足 | 详情 | 改进|
| :--- | :---   | :--	|
| 数据清洗|没有使用MapReduce进行文件名错误检测，文件内容乱码检测及修正|在运行过程中发现，`600765中航重机，sh600483福能股份，sh601777力帆股份，sh603101汇嘉时代，sh000661长春高新`文件内新闻标题为乱码。实验完成后用Python程序扫描文件名，发现约有40个文件名的股票名称错误或不完整。|
|分词|直接使用分词器的标准分词方法，不识别某些股票新闻中的专有名词|虽然加入了小样本的专有词及特殊停用词，但是对全局来说不够全面，人工审核容易有疏漏。可以考虑尝试使用多个分词器，比较不同的分词效果，择优选用。|

### 优化及创新
| 优化 | 详情 |
| :--- | :---   |
|分词|利用部分样本归纳，补充新的词典及停词表，使分词结果更加精确|
|排序  |同时实现两个需求。利用Map阶段的天然排序，实现二次排序。这样就整合了两次排序的过程，减少遍历原文件的次数，最大化地利用一次并行中的信息 |
|输出|最终的输出的结果文件中，相同的词语在一起输出；且相同词语的顺序是依据该词在各个文件中的词频由高到低排序，更能看出某个某个词语与某只股票的关联程度|
## 参考资料
[二次排序：http://www.cnblogs.com/huaxiaoyao/p/4302210.html](http://www.cnblogs.com/huaxiaoyao/p/4302210.html)