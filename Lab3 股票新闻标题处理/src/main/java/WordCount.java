import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class WordCount {
    private int k ;
    public WordCount(int k){
        this.k = k;
    }
    //(<word sum> <id url1,url2……>)
    public static class TokenizerMapper extends Mapper<Object, Text, Text, Text>{
        private Text result = new Text();
        private Text word = new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String [] mes = value.toString().split("\t");

            word.set(mes[0].split(" ")[0]);
            String sum = mes[0].split(" ")[1];
            String id = mes[1].split(" ")[0];
            String urls = mes[1].split(" ")[1];

            result.set(sum + " " + id + " " + urls);
            //(<word> <id sum url1,url2……>)
            context.write(word,result);
        }

    }

    //(<word> <id sum url1,url2……>)
    public static class IntSumReducer
            extends Reducer<Text, Text, Text,Text> {
        private Text result = new Text();
        private Text word = new Text();

        private int k = 1;
        public void setup(Context context) throws IOException{
            Configuration conf = context.getConfiguration();
            k = Integer.parseInt(conf.get("k"));
        }
        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            String txt = "";
            for (Text val : values) {
                String [] mes = val.toString().split(" ");

                sum = sum + Integer.parseInt(mes[0]);
                txt = txt + mes[1] + "," + mes[0] + "," + mes[2] + " ";
            }
            if(sum >= k){
                word.set(key + " " + sum);
                result.set(txt);
                //（<word totalsum> <sum,id,url1,urs2…… sum,id,url1,urs2…… >）
                context.write(word, result);
            }
        }
    }
}
