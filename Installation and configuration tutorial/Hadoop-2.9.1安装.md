# Hadoo 2.9.1安装

## 基础安装及配置
### 创建Hadoop用户
* 添加Hadoop用户
```
niuhy@ubuntu:~$ sudo useradd -m hadoop -s /bin/bash  
```
* 给Hadoop用户设置密码
```
niuhy@ubuntu:~$ sudo passwd hadoop  
```
* 给Hadoop用户增加管理员权限
```
niuhy@ubuntu:~$ sudo adduser hadoop sudo  
```
* 使用Hadoop账户登录（首先要更新apt），之后在登录的时候选择hadoop账户登录
```
niuhy@ubuntu:~$ sudo apt-get update 
```
###安装配置ssh无密码登录
* 安装ssh server
```
hadoop@ubuntu:~$ sudo apt-get install openssh-server  
```
* 登录本机测试，然后退出
```
hadoop@ubuntu:~$ ssh localhost  
hadoop@ubuntu:~$ exit  
```
* 配置本机无密码登录（步骤同MPI配置）
```
hadoop@ubuntu:~$ cd ~/.ssh/ 
hadoop@ubuntu:~/.ssh$ ssh-keygen -t rsa 
hadoop@ubuntu:~/.ssh$ cat ./id_rsa.pub >> ./authorized_keys 
```
* 验证免密登录是否配置成功
```
hadoop@ubuntu:~/.ssh$ ssh localhost  
```
### 安装jdk
* 官网下载jdk8
* 解压，将解压的文件夹移动到/usr/local目录下
```
hadoop@ubuntu:~$ cd ~/Downloads/  
hadoop@ubuntu:~/Downloads$ tar zxvf jdk-8u91-linux-x64.tar.gz  
hadoop@ubuntu:~/Downloads$ sudo mv jdk1.8.0_91 /usr/local  
```
* 配置配置jdk环境变量,使用 gedit打开 /etc/profile 文件
```
hadoop@ubuntu:~/Downloads$ sudo gedit /etc/profile  
```
* 在文件最后一行复制以下几行，并保存
```
export JAVA_HOME=/usr/local/jdk1.8.0_91    
export JRE_HOME=/usr/local/jdk1.8.0_91/jre    
export CLASSPATH=.:$JAVA_HOME/lib:$JRE_HOME/lib:$CLASSPATH    
export PATH=$JAVA_HOME/bin:$JRE_HOME/bin:$PATH  
```
* 执行： source /etc/profile使修改立即生效
```
hadoop@ubuntu:~/Downloads$ source /etc/profile  
```
* 使用：java -version查看安装是否成功
```
hadoop@ubuntu:~/Downloads$ java -version 
```
### 安装hadoop2
* 官网下载hadoop-2.9.1.tar.gz
* 解压，移动到/usr/local目录下
```
hadoop@ubuntu:~/Downloads$ cd ~/Downloads/ #进入到下载目录  
hadoop@ubuntu:~/Downloads$ tar zxvf hadoop-2.9.1.tar.gz   
hadoop@ubuntu:~/Downloads$ sudo mv hadoop-2.9.1 /usr/local  
```
* 将hadoop文件夹及子目录的所有者更改为hadoop用户
```
hadoop@ubuntu:~/Downloads$ cd /usr/local  
hadoop@ubuntu:/usr/local$ sudo chown -R hadoop hadoop-2.9.21  
```
* 查看hadoop版本，验证是否安装成功
```
hadoop@ubuntu:/usr/local$ hadoop-2.9.1/bin/hadoop version 
``

## 单机模式Hadoop配置及样例运行
### 进入/usr/local/hadoop-2.9.1目录
```
hadoop@ubuntu:~$ cd /usr/local/hadoop-2.9.1/  
```
### 新建输入文件夹input
```
hadoop@ubuntu:/usr/local/hadoop-2.9.1$ mkdir ./input  
```
### 将配置文件作为输入文件，移动到input文件夹中
```
hadoop@ubuntu:/usr/local/hadoop-2.9.1$ cp ./etc/hadoop/*.xml ./input/ 
```
### 执行样例程序
* 选择运行grep例子，我们将input文件夹中的所有文件作为输入，筛选当中符合正则表达式dfs[a-z.]+的单词病统计出现的次数，最后输出结果到output文件夹中
```
hadoop@ubuntu:/usr/local/hadoop-2.9.1$ ./bin/hadoop jar ./share/hadoop/mapreduce/hadoop-mapreduce-examples-2.9.1.jar grep ./input ./output 'dfs[a-z.]+'  
```
* 查看运行结果
```
hadoop@ubuntu:/usr/local/hadoop-2.9.1$ cat ./output/*  
```
* 为运行另一个例子，需要先将output文件夹删除，否则会提示出错
```
hadoop@ubuntu:/usr/local/hadoop-2.9.1$ rm -rf output 
```
* 运行wordcount例子，我们将input文件夹中的所有文件作为输入，统计各文件中的单词出现的频率，最后输出结果到output文件夹中hadoop@ubuntu:/usr/local/hadoop-2.9.1$ ./bin/hadoop jar ./share/hadoop/mapreduce/hadoop-mapreduce-examples-2.9.1.jar wordcount ./input ./output  


## 伪分布模式Hadoop配置及样例运行
### 修改配置文件
* 修改core-site.xml
```
hadoop@ubuntu:/usr/local/hadoop-2.9.1$ sudo gedit ./etc/hadoop/core-site.xml  
```
* 修改后的文件内容
```
<configuration>  
    <property>  
      <name>hadoop.tmp.dir</name>  
      <value>file:/usr/local/hadoop-2.9.1/tmp</value>  
     <description>Abase for other temporary directories.</description>  
    </property>  
    <property>  
         <name>fs.defaultFS</name>  
         <value>hdfs://localhost:9000</value>  
    </property>  
</configuration>  
```
* 修改hdfs-site.xml
```
hadoop@ubuntu:/usr/local/hadoop-2.9.1$ sudo gedit ./etc/hadoop/hdfs-site.xml  
```
* 修改后的文件内容
```
<configuration>  
    <property>  
       <name>dfs.replication</name>  
       <value>1</value>  
    </property>  
    <property>  
     <name>dfs.namenode.name.dir</name>  
     <value>file:/usr/local/hadoop-2.9.1/tmp/dfs/name</value>  
    </property>  
    <property>  
       <name>dfs.datanode.data.dir</name>  
       <value>file:/usr/local/hadoop-2.9.1/tmp/dfs/data</value>
    </property>  
</configuration>  
```
### NameNode格式化
```
hadoop@ubuntu:/usr/local/hadoop-2.9.1$ ./bin/hdfs namenode -format  
```
### 开启NameNode和DataNode守护进程
```
hadoop@ubuntu:/usr/local/hadoop-2.9.1$ ./sbin/start-dfs.sh  
```
### 使用jps判断是否成功启动
```
hadoop@ubuntu:/usr/local/hadoop-2.9.1$ jps  
```
### 访问成功启动后，访问web界面http://localhost:50070查看NameNode和DataNode信息，并在线查看HDFS中的文件
### 执行样例程序（grep）
* 在HDFS中创建用户目录
```
hadoop@ubuntu:/usr/local/hadoop-2.9.1$ ./bin/hdfs dfs -mkdir -p /user/hadoop  
```
* 在HDFS中创建input文件夹，并将配置文件加入到input文件夹中
```
hadoop@ubuntu:/usr/local/hadoop-2.9.1$ ./bin/hdfs dfs -mkdir input  
hadoop@ubuntu:/usr/local/hadoop-2.9.1$ ./bin/hdfs dfs -put ./etc/hadoop/*.xml input  
```
* 查看HDFS-input文件夹中的文件
```
hadoop@ubuntu:/usr/local/hadoop-2.9.1$ ./bin/hdfs dfs -ls input
```
* 运行grep程序
```
hadoop@ubuntu:/usr/local/hadoop-2.9.1$ ./bin/hadoop jar ./share/hadoop/mapreduce/hadoop-mapreduce-examples-2.9.1.jar grep input output 'dfs[a-z.]+'  
```
* 查看运行结果
```
hadoop@ubuntu:/usr/local/hadoop-2.9.1$ ./bin/hdfs dfs -cat output/*  
```
* 将运行结果取回本地并查看（首先要删除本地已经有的output文件夹）
```
hadoop@ubuntu:/usr/local/hadoop-2.9.1$ rm -rf ./output  
hadoop@ubuntu:/usr/local/hadoop-2.9.1$ ./bin/hdfs dfs -get output ./output  
hadoop@ubuntu:/usr/local/hadoop-2.9.1$ cat ./output/*  
```
### 启动yarn
* 修改配置文件mapred-site.xml
```
hadoop@ubuntu:/usr/local/hadoop-2.9.1$ mv ./etc/hadoop/mapred-site.xml.template  ./etc/hadoop/mapred-site.xml  
hadoop@ubuntu:/usr/local/hadoop-2.9.1$ sudo gedit ./etc/hadoop/mapred-site.xml  
```
* 修改后的文件内容
```
<configuration>  
     <property>  
         <name>mapreduce.framework.name</name>  
         <value>yarn</value>  
    </property>  
</configuration>  
```
* 修改配置文件yarn-site.xml
```
hadoop@ubuntu:/usr/local/hadoop-2.9.1$ sudo gedit ./etc/hadoop/yarn-site.xml   
```
* 修改后的文件内容
```
<configuration>  
    <property>  
         <name>yarn.nodemanager.aux-services</name>  
         <value>mapreduce_shuffle</value>  
    </property>  
</configuration> 
```
* 启动yarn
```
hadoop@ubuntu:/usr/local/hadoop-2.9.1$ ./sbin/start-yarn.sh  
```
* 开启历史服务器，保证可以在web中查看任务运行情况
```
hadoop@ubuntu:/usr/local/hadoop-2.9.1$ ./sbin/mr-jobhistory-daemon.sh start historyserver  
```
* 开启后通过jps查看
```
hadoop@ubuntu:/usr/local/hadoop-2.9.1$ jps 
```
* 访问web界面http://localhost:8088/cluster查看任务的运行情况
###执行样例程序（wordcount）
```
hadoop@ubuntu:/usr/local/hadoop-2.9.1$ ./bin/hadoop jar ./share/hadoop/mapreduce/hadoop-mapreduce-examples-2.9.1.jar wordcount input output  
```
