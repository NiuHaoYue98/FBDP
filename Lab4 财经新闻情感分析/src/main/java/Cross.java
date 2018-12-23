import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

public class Cross {

    public static class Map extends Mapper<Object,Text, IntWritable,Text> {
        private int k;                      //邻居个数
        private  int crossnum;
        private int min;
        private int max;

        protected void setup(Context context) throws IOException, InterruptedException {
            k = context.getConfiguration().getInt("k", 1);
            crossnum = context.getConfiguration().getInt("crossnum",1);
            min = Integer.valueOf(150 * crossnum);
            max = Integer.valueOf(150 * (crossnum + 1));
            //System.out.println(min + " " + max);
        }

        //读取测试集的数据，向量化，计算距离，写入结果
        public void map(Object key, Text textLine, Context context)
                throws IOException, InterruptedException {
            Tri testInstance = new Tri(textLine.toString());
            //if the index doesn't in the range, the temp textilne is in the trainset
            int id = Integer.valueOf(testInstance.tid);
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
        }

    }

}
