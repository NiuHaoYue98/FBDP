import java.io.BufferedReader;
import java.io.IOException;
import java.util.ArrayList;
import king.Utils.Distance;
import java.io.InputStreamReader;
import org.apache.hadoop.io.*;
import java.io.FileInputStream;
import org.apache.hadoop.mapreduce.Mapper;

public class KNN {

    public static class KNNMap extends Mapper<Object,Text, Text,Text> {
        //private String trainset;            //样本集
        private String testset;             //待训练集
        private int k;                      //邻居个数
        private String outputPath;          //输出路径
        private ArrayList<Tri> trainSet;

        //read the trainset and write them into the cash
        protected void setup(Context context) throws IOException, InterruptedException {
            trainSet = new ArrayList<Tri>();
            k = context.getConfiguration().getInt("k", 1);
            //System.out.println(k);      //can get k
            String trainset = context.getConfiguration().get("trainset");
            //System.out.println(trainset);
            try {
                FileInputStream inputStream = new FileInputStream(trainset);
                BufferedReader br = new BufferedReader(new InputStreamReader(inputStream));
                String line = "";
                while ((line = br.readLine()) != null) {
                    //System.out.println(line);
                    Tri temptri = new Tri(line);
                    //System.out.println(temptri.tid + " "  + temptri.getattr().length);
                    trainSet.add(temptri);
                }
                System.out.println(trainSet.size());
                br.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        //读取测试集的数据，向量化，计算距离，写入结果

        public void map(Object key, Text textLine, Context context)
                throws IOException, InterruptedException {
            ArrayList<Double> distance = new ArrayList<Double>(k);
            ArrayList<String> traintag = new ArrayList<String>(k);
            //前k个直接初始化为距离，之后的比较计算
            for (int i = 0; i < k; i++) {
                distance.add(Double.MAX_VALUE);
                traintag.add(String.valueOf(-1));
            }
            Tri testInstance = new Tri(textLine.toString());
            for (int i = 0; i < trainSet.size(); i++) {
                //System.out.println(trainSet.get(i).getattr().length + " " + testInstance.getattr().length);
                try {
                    if(trainSet.get(i).attr.length != testInstance.attr.length)
                        System.out.println(testInstance.tid  + " " + "train " + trainSet.get(i).attr.length + "temp " + testInstance.attr.length);
                    //System.out.println(trainSet.get(i).tid);
                    double dis = Distance.EuclideanDistance(trainSet.get(i).getattr(), testInstance.getattr());
                    //System.out.println(dis);  //right
                    int index = indexOfMax(distance);
                    if (dis < distance.get(index)) {
                        distance.remove(index);
                        traintag.remove(index);
                        distance.add(dis);
                        traintag.add(trainSet.get(i).tag);
                    }
                } catch (Exception e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }
            String pos = "positive";
            String neg = "negative";
            String neu = "neutral";
            Double pos_num = 0.0;
            Double neg_num = 0.0;
            Double neu_num = 0.0;
            for(int i = 0;i<k;i++){
                if(traintag.get(i).equals(pos))
                    pos_num = pos_num + distance.get(i);
                else if (traintag.get(i).equals(neg))
                    neg_num = neg_num + distance.get(i);
                else if (traintag.get(i).equals(neu))
                    neu_num = neu_num + distance.get(i);
            }
            Text tag = new Text();

            if (pos_num > neg_num){
                if (neg_num > neu_num)
                    tag.set(neu);   //pos > neg , neg > neu ,the min is neu
                else
                    tag.set(neg);   //pos > neg , neu > neg ,the min is neg
            }
            else if(pos_num > neu_num)
                tag.set(neu);       //neg > pos , pos > neu ,the min is neu
            else
                tag.set(pos);       //neg > pos , neu > pos ,the min is pos
            Text textIndex = new Text();
            textIndex.set(testInstance.tid);

            System.out.println(textIndex);
            System.out.println("the positive distance is " + pos_num.toString());
            System.out.println("the negative distance is " + neg_num.toString());
            System.out.println("the neutral distance is " + neu_num.toString());
            System.out.println('\n');

            context.write(textIndex, tag);
        }

        public int indexOfMax(ArrayList<Double> array){
            int index = -1;
            Double min = Double.MIN_VALUE;
            for (int i = 0;i < array.size();i++){
                if(array.get(i) > min){
                    min = array.get(i);
                    index = i;
                }
            }
            return index;
        }

    }

}
