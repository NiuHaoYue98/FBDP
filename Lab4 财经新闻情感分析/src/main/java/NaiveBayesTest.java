import java.util.Scanner;
import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.Text;
import java.util.ArrayList;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;

public class NaiveBayesTest
{
    public static class NaiveBayesConf
    {
        public int dimen;
        public int class_num;
        public ArrayList<String> classNames;
        public ArrayList<String> proNames;
        public ArrayList<Integer>	proRanges;

        public NaiveBayesConf() {
            dimen = class_num = 3;
            classNames = new ArrayList<String>();
            proNames = new ArrayList<String>();
            proRanges = new ArrayList<Integer>();
        }

        public void ReadNaiveBayesConf(String file, Configuration conf) throws Exception {
            String[] vals = {"positive","negative","neutral"};
            class_num = 3;
            for(int i = 0; i<vals.length; i++)
                classNames.add(vals[i]);

            dimen = 904;

            for(int i = 1; i<904; i++)
            {
                proNames.add(String.valueOf(i));
                proRanges.add(new Integer(1));
            }
        }
    }

    public static class NaiveBayesTrainData
    {
        public HashMap<String, Integer> freq;
        public NaiveBayesTrainData()
        {
            freq = new HashMap<String, Integer>();
        }
        public void getData(String file, Configuration conf) throws IOException
        {
            int i;
            Path data_path = new Path(file);
            Path file_path;
            String temp[], line;
            FileSystem hdfs = data_path.getFileSystem(conf);

            FileStatus[] status = hdfs.listStatus(data_path);

            for(i = 0; i<status.length; i++)
            {
                file_path = status[i].getPath();
                if(hdfs.getFileStatus(file_path).isDir() == true)
                    continue;
                line = file_path.toString();
                temp = line.split("/");
                if(temp[temp.length-1].substring(0,5).equals("part-") == false)
                    continue;
                System.err.println(line);
                FSDataInputStream fin = hdfs.open(file_path);
                InputStreamReader inr = new InputStreamReader(fin);
                BufferedReader bfr = new BufferedReader(inr);
                while((line = bfr.readLine()) != null)
                {
                    String res[] = line.split("\t");
                    freq.put(res[0], new Integer(res[1]));
                    //System.out.println(freq.get(res[0]));
                }
                bfr.close();
                inr.close();
                fin.close();
            }
        }

    }

    public static class TestMapper
            extends Mapper<Object, Text, Text, Text>
    {
        public NaiveBayesConf nBConf;
        public NaiveBayesTrainData nBTData;
        public void setup(Context context)
        {
            try{
                Configuration conf = context.getConfiguration();

                nBConf = new NaiveBayesConf();
                nBConf.ReadNaiveBayesConf("temp", conf);
                nBTData = new NaiveBayesTrainData();
                nBTData.getData(conf.get("train_result"), conf);
            }
            catch(Exception ex)
            {
                ex.printStackTrace();
                System.exit(1);
            }
        }

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException
        {
            Scanner scan = new Scanner(value.toString());
            String str, vals[], temp;
            int i,j,idx;
            double fxyi,fyi,maxf;
            Text id;
            Text cls;

            while(scan.hasNextLine())
            {
                str = scan.nextLine();
                //System.out.println(str);
                vals = str.split(" ");
                maxf = -1;
                idx = -1;
                int reducenum = 0;
                for(i = 0; i<nBConf.class_num; i++)
                {
                    fxyi = 1;
                    String cl = nBConf.classNames.get(i);
                    Integer integer = nBTData.freq.get(cl);
                    if(integer == null)
                        fyi = 0;
                    else
                        fyi = integer.intValue();
                    int tempreduce = 0;
                    for(j = 1; j<vals.length-1; j++)
                    {
                        temp = cl + "#" + nBConf.proNames.get(j-1) + "#" + vals[j];

                        integer = nBTData.freq.get(temp);
                        //System.out.println(temp + "\t" + integer);

                        if(integer != null) {
                            if(Integer.valueOf(integer)!=0) {
                                fxyi = fxyi * (double)integer/100;
                                if (fxyi > 1000) {
                                    fxyi = fxyi / 100;
                                    tempreduce += 1;
                                }
                                //System.out.println(fxyi);
                            }
                            else
                                System.out.println("integer is 0");
                        }
                        else {
                            System.out.println("here");
                        }
                    }

                    System.out.println(tempreduce + "\t" + fyi + " \t" + fxyi);
                    if(tempreduce >= reducenum){
                        reducenum = tempreduce ;
                        idx = i;
                    }
                    /*
                    if(fyi*fxyi >= maxf)
                    {
                        maxf = fyi*fxyi;
                        idx = i;
                        //System.out.println(idx);
                    }
                    */
                }
                System.out.println("\n");

                id = new Text(vals[0].split("\t")[0]);
                cls = new Text(nBConf.classNames.get(idx));
                context.write(id, cls);
            }
        }
    }
}
