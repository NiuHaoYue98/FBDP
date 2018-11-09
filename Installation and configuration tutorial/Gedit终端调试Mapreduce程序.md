以WordCount编译调试为例
* 建立有代码的文件夹，内部包含两个子文件夹，结构如下：
```
-WordCount
  -src
  -classes
```
* 在 /WordCount/src 中使用vim 新建一个java类，写入程序代码
```
vim WordCount.java  
```
* 进入WordCount文件夹 
```
cd WordCount  #这里注意文件夹的层次，根据自己电脑上的文件层次决定位置
```
* 使用如下命令编译WordCount.java程序代码
```
javac -classpath /usr/local/hadoop-2.9.1/share/hadoop/common/hadoop-common-2.9.1.jar:/usr/local/hadoop-2.9.1/share/hadoop/common/lib/commons-cli-1.2.jar:/usr/local/hadoop-2.9.1/share/hadoop/mapreduce/hadoop-mapreduce-client-core-2.9.1.jar -d ./classes/ ./src/*.java
```
  作用是用hadoop安装环境下hadoop-common-2.9.1.jar、commons-cli-1.2.jar、hadoop-mapreduce-client-core-2.9.1.jar三个库文件，编译src中所有的.java文件。-classpath后接着3个绝对路径，是电脑上3个库文件的所在位置，绝对路径用‘:’分割；-d后面跟着的是编译后存放路径和源路径。
执行后就会发现classes下生成了三级文件目录，里面有编译好的.class文件，而且你会发现，对于.java中的每一个类，都有对应的文件生成。
* 将编译好的class文件打包成Jar包，打包后的文件会出现在WordCount目录中。
```
jar -cvf Mywordcount.jar -C ./classes/ .  
```
* 将以上生成的Mywordcount.jar文件移动到Hadoop目录下，这里我们可以自己新建一个存放自己编写的程序的目录文件夹，比如这里是homework4文件夹。
```
cp Mywordcount.jar /usr/local/hadoop-2.9.1/homework4  
```
* 启动hdfs。与伪分布式运行基本相同，在hdfs系统中建立wordcount_input文件夹，将测试文件移入。同时删除已有的wordcount_output文件夹。做好运行前的准备工作。
```
./sbin/start-dfs.sh  
jps   #查看是否开启成功    
#新建文件夹,将测试文件移入  
./bin/hdfs dfs -mkdir wordcount_input  
./bin/hdfs dfs -put /home/hadoop/Downloads/I_Have_a_Dream.txt wordcount_input    
#网页端查看发现有I_Have_a_Dream.txt文件说明成功  
```
* 运行程序
```
./bin/hadoop jar ./homework4/Mywordcount.jar WordCount wordcount_input wordcount_output  
```
* 结果取回本地并查看
```
./bin/hdfs dfs -get wordcount_output ./wordcount_output    
 cat ./wordcount_output/*  
 ```
