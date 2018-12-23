import java.io.BufferedReader;
import java.io.IOException;
import java.util.ArrayList;
import java.io.InputStreamReader;
import org.apache.hadoop.io.*;
import java.io.FileInputStream;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class Analysis {

    public static class AnalysisMap extends Mapper<Object,Text, Text,IntWritable> {
        //private String trainset;            //样本集
        private String testset;             //待训练集
        private int k;                      //邻居个数
        private String outputPath;          //输出路径
        private ArrayList<Tri> trainSet;

        //read the trainset and write them into the cash
        protected void setup(Context context) throws IOException, InterruptedException {
            trainSet = new ArrayList<Tri>();
            k = context.getConfiguration().getInt("k", 1);
            String trainset = context.getConfiguration().get("trainset");
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
            Text classkey = new Text();
            IntWritable one = new IntWritable( 1);
            //true number + real num
            int index = Integer.parseInt(textLine.toString().split("\t")[0]);
            String tag = textLine.toString().split("\t")[1];
            /*if correct ,return the classname correct_tag, or return the temp tag*/
            System.out.println(index + ' ' + tag);
            if(tag.equals(trainSet.get(index).tag))
                classkey.set("correct_" + tag);
            else
                classkey.set(tag);
            context.write(classkey,one);
            classkey.set("total");
            context.write(classkey,one);
        }


    }

    public static class AnalysisReduce extends Reducer<Text,IntWritable,Text,IntWritable> {

        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            IntWritable result = new IntWritable();
            int sum = 0;
            for (IntWritable val : values){
                sum += val.get();
            }
            result.set(sum);
            context.write(key,result);
        }
    }
}
