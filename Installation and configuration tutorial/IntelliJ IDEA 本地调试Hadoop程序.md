# IntelliJ IDEA 本地调试Hadoop程序
## 相关配置
项目|版本|路径
:-|:-|:-:
|系统支持|Ubuntu(64位)16.04，2G以上内存，20G硬盘|-
Hadoop|2.9.1|/usr/local/hadoop-2.9.1
JDK|1.7.1|/usr/bin/java

## 安装IDEA
* JetBrains官网下载最新版IntelliJ IDEA Linux系统的安装包，我下载的是ideaIU-2018.2.5.tar.gz
* 将压缩包移动到/opt目录下，解压`cp ideaIU-2018.2.5.tar.gz /opt`、`tar zxvf ideaIU-2018.2.5.tar.gz`解压出来的名字是`idea-IU-182.4892.20`
* 进入`/opt/idea-IU-182.4892.20/bin`目录，运行`./idea.sh`即可开始安装，根据安装步骤操作即可

## Hadoop开发环境搭建
* 打开Idea,file->new->Project,左侧面板选择maven工程,JDK选择1.7，点击Next
* 设置GroupId和ArtifactId（自己填），下一步
* 设置工程存储路径，我设置在了`/home/FBDP/`目录下，projectname根据项目情况填，Finish
* 打开Intellij的Preference偏好设置，定位到Build, Execution, Deployment->Compiler->Java Compiler，将WordCount的Target bytecode version修改为1.7
### 编辑pom.xml
* 在project尾部添加
```	
	<repositories>
	    <repository>
	        <id>apache</id>
	        <url>http://maven.apache.org</url>
	    </repository>
	</repositories>
```
* 在project尾部添加依赖
```
    <dependencies>
        <dependency>
            <groupId>org.apache.hadoop</groupId>
            <artifactId>hadoop-core</artifactId>
            <version>1.2.1</version>
        </dependency>
        <dependency>
            <groupId>org.apache.hadoop</groupId>
            <artifactId>hadoop-common</artifactId>
            <version>2.9.1</version>
        </dependency>
        <dependency>
            <groupId>org.apache.hadoop</groupId>
            <artifactId>hadoop-hdfs</artifactId>
            <version>2.9.1</version>
        </dependency>
        <dependency>
            <groupId>org.apache.hadoop</groupId>
            <artifactId>hadoop-client</artifactId>
            <version>2.9.1</version>
        </dependency>
        <dependency>
            <groupId>org.apache.hadoop</groupId>
            <artifactId>hadoop-mapreduce-client-core</artifactId>
            <version>2.9.1</version>
        </dependency>
        <dependency>
            <groupId>org.apache.hadoop</groupId>
            <artifactId>hadoop-mapreduce-client-jobclient</artifactId>
            <version>2.9.1</version>
        </dependency>
        <dependency>
            <groupId>log4j</groupId>
            <artifactId>log4j</artifactId>
            <version>1.2.15</version>
        </dependency>
    </dependencies>
 ```    
* 修改pom.xml完成后，Intellij右上角会提示Maven projects need to be Imported，点击Import Changes以更新依赖
* 导入jar包

```
/usr/local/hadoop/share/hadoop/common 目录下的hadoop-common-2.7.1.jar和haoop-nfs-2.7.1.jar； 
/usr/local/hadoop/share/hadoop/common/lib 目录下的所有JAR包； 
/usr/local/hadoop/share/hadoop/hdfs 目录下的haoop-hdfs-2.7.1.jar和haoop-hdfs-nfs-2.7.1.jar； 
/usr/local/hadoop/share/hadoop/hdfs/lib 目录下的所有JAR包;
/usr/local/hadoop/share/hadoop/mapreduce 目录下的所有jar包；
/usr/local/hadoop/share/hadoop/yarn 目录下所有jar包
```
* 把Hadoop安装目录（usr/local/hadoop-2.9.1/etc/hadoop）中的core-site.xml和hdfs-site.xml放到project中的src目录中

## 运行
在Intellij菜单栏中选择Run->Edit Configurations，在弹出来的对话框中点击+，新建一个Application配置。配置Main class（可以点击右边的...选择），Program arguments为执行程序时需要输入的参数。

## 参考资料
* https://www.polarxiong.com/archives/Hadoop-Intellij%E7%BB%93%E5%90%88Maven%E6%9C%AC%E5%9C%B0%E8%BF%90%E8%A1%8C%E5%92%8C%E8%B0%83%E8%AF%95MapReduce%E7%A8%8B%E5%BA%8F-%E6%97%A0%E9%9C%80%E6%90%AD%E8%BD%BDHadoop%E5%92%8CHDFS%E7%8E%AF%E5%A2%83.html
* https://blog.csdn.net/Ding_xiaofei/article/details/80376049
