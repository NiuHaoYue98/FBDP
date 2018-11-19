import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;
import java.util.List;
import java.io.File;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import com.hankcs.hanlp.seg.common.Term;
import com.hankcs.hanlp.tokenizer.StandardTokenizer;

public class InvertedIndex {

    public static class InvertedIndexMapper extends Mapper<Object, Text, Text, Text>{
        private Text word = new Text();
        private Text result = new Text();
        String id;
        String url;

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {

            //get the news message.
            StringTokenizer itr = new StringTokenizer(value.toString()," ");
            id = itr.nextToken();
            String [] mes = getMes(itr).split(" ");
            //already get the full message
            url = mes[1];

            //cut the text into words
            File file = new File("/home/hadoop/FBDP/data/dictionary/mystopwords.txt");
            List<String>  stopword = FileUtils.readLines(file,"utf8");
            List<Term> termList = StandardTokenizer.segment(mes[0]);
            ArrayList<String> TermList = new ArrayList();
            for (int i = 0;i < termList.size();i++){
                TermList.add(termList.get(i).toString());
            }
            TermList.removeAll(stopword);

            for (int i = 0;i < TermList.size();i++){
                word.set(TermList.get(i) + "," + id );
                result.set(url);
                //(<word,id> <url>)
                context.write(word,result);
            }
        }
        public String getMes(StringTokenizer itr){
            int j = itr.countTokens();
            String mes = null;
            for(int i = 1;i<=3;i++)
                mes = itr.nextToken ();
            for(int i = 3;i<j-1;i++)
                mes = mes + itr.nextToken();
            mes = mes.replaceAll("[`[》 《.２０１５３４1234︱5｜67890%\"\":~!@#$^&*()+=]|{}':;',\\\\[\\\\]<>/?~！@#￥……& amp;*（）——+|{}【】‘；：”“’。，、？|-]","");
            mes = mes + " " + itr.nextToken();
            return mes;
        }

    }

    //(<word,id> <url>)
    public static class InvertedIndexReducer extends Reducer<Text, Text, Text, Text > {
        private int k = 1;

        public void setup(Context context) throws IOException {
            Configuration conf = context.getConfiguration();
            k = Integer.parseInt(conf.get("k"));
        }

        private Text new_key = new Text();
        private Text result = new Text();

        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            String urls = "";
            for (Text val : values) {
                sum += 1;
                if (urls.length() == 0)
                    urls = urls +  val.toString();
                else
                    urls = urls + "," + val.toString();
            }
            new_key.set(key.toString().split(",")[0] + " " + sum);
            result.set(key.toString().split(",")[1] +" " +  urls);
            //(<word sum> <id url1,url2……>)
            context.write(new_key,result);
        }
    }
}
