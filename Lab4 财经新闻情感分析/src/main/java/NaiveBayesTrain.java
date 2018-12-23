import java.util.Scanner;
import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

public class NaiveBayesTrain{

    public static class NaiveBayesConf
    {
        public int dimen;
        public int class_num;
        public ArrayList<String> classNames;
        public ArrayList<String> proNames;
        public ArrayList<Integer>	proRanges;

        public NaiveBayesConf() {
            dimen = class_num = 0;
            classNames = new ArrayList<String>();
            proNames = new ArrayList<String>();
            proRanges = new ArrayList<Integer>();
        }

        public void ReadNaiveBayesConf(String file, Configuration conf) throws Exception {
            String[] vals = {"negative","neutral","positive"};
            class_num = 3;
            for(int i = 0; i<vals.length; i++)
                classNames.add(vals[i]);

            dimen = 904;

            for(int i = 1; i<904; i++)
            {
                proNames.add(String.valueOf(i));
                proRanges.add(new Integer(1));
            }
            System.out.println(proNames.size());
        }
    }

    public static class TrainMapper
            extends Mapper<Object, Text, Text, IntWritable>
    {
        public NaiveBayesConf nBConf;
        private final static IntWritable one = new IntWritable(1);
        private Text word;

        public void setup(Context context)
        {
            try{
                nBConf = new NaiveBayesConf();
                Configuration conf = context.getConfiguration();
                nBConf.ReadNaiveBayesConf("temp", conf);
            }
            catch(Exception ex)
            {
                ex.printStackTrace();
                System.exit(1);
            }
            System.out.println("setup");
        }
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException
        {
            Scanner scan = new Scanner(value.toString());
            String str, vals[], temp;
            int i;
            word = new Text();
            while(scan.hasNextLine())
            {
                str = scan.nextLine();
                vals = str.split(" ");
                String classname = vals[vals.length-1];
                word.set(classname);
                context.write(word, one);   //class total num
                //System.out.println(word + "\t" + String.valueOf(vals.length));
                for(i = 1; i<vals.length-1; i++)
                {
                    word = new Text();
                    temp = classname + "#" + nBConf.proNames.get(i-1);
                    temp += "#" + vals[i];
                    word.set(temp);
                    context.write(word, one);
                }
            }
        }
    }

    public static class TrainReducer
            extends Reducer<Text,IntWritable,Text,IntWritable>
    {
        private IntWritable result = new IntWritable();
        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context) throws IOException, InterruptedException
        {
            System.out.println(key);
            int sum = 0;
            for (IntWritable val : values)
            {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }
}

