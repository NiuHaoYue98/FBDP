## KMeans
  聚类代码，参考书上的代码实现。
  修改了其中的`Cluster.java`、`KMeans.java`、`RandomClusterGenerator.java`。对其中的Cluster类结构做出了调整，包含的基本变量如下：
```
    private int clusterID;        #ID
    private long numOfPoints;     #数量
    private Instance center;      #中心
    private double SumDistance;   #聚簇内各点到中心的距离和
```
  方便计算SSE
* 其中的`output`文件夹为最后一次实验的输出结果，参数为"2 10 input output"

## result
  不同k值对应的结果文件夹

## Plot
  可视化`.py`程序，绘制原始及聚簇之后的散点图，绘制SSE随k的变化折线图
