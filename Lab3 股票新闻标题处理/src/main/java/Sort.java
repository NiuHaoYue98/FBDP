import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;

public class Sort{
    private int k ;
    public Sort(int k){
        this.k = k;
    }

    public static class IntPair implements WritableComparable<IntPair> {
        private int first = 0;
        private int second = 0;

        public void set(int left, int right) {
            first = left;
            second = right;
            //key = key;
        }
        public int getFirst() {
            return first;
        }
        public int getSecond() {
            return second;
        }

        public void readFields(DataInput in) throws IOException {
            first = in.readInt() + Integer.MIN_VALUE;
            second = in.readInt() + Integer.MIN_VALUE;
        }
        public void write(DataOutput out) throws IOException {
            out.writeInt(first - Integer.MIN_VALUE);
            out.writeInt(second - Integer.MIN_VALUE);
        }
        public int hashCode() {
            return first * 157 + second;// why multiply 157?
        }
        public boolean equals(Object right) {
            if (right instanceof IntPair) {
                IntPair r = (IntPair) right;
                return r.first == first && r.second == second;
            } else {
                return false;
            }
        }
        public static class Comparator extends WritableComparator {
            public Comparator() {
                super(IntPair.class);
            }

            public int compare(byte[] b1, int s1, int l1,
                               byte[] b2, int s2, int l2) {
                return -compareBytes(b1, s1, l1, b2, s2, l2);
            }
        }

        static {                                        // register this comparator
            WritableComparator.define(IntPair.class, new Comparator());
        }

        public int compareTo(IntPair o) {
            if (first != o.first) {
                return first < o.first ? -1 : 1;
            }
            else if (second != o.second) {
                return second < o.second ? -1 : 1;
            } else {
                return 0;
            }
        }
    }

    public static class FirstPartitioner extends Partitioner<IntPair,IntWritable>{
        @Override
        public int getPartition(IntPair key, IntWritable value,
                                int numPartitions) {
            return Math.abs(key.getFirst() * 127) % numPartitions;
        }
    }

    public static class FirstGroupingComparator implements RawComparator<IntPair> {
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            return WritableComparator.compareBytes(b1, s1, Integer.SIZE/8,
                    b2, s2, Integer.SIZE/8);
        }

        public int compare(IntPair o1, IntPair o2) {
            int l = o1.getFirst();
            int r = o2.getFirst();
            return l == r ? 0 : (l < r ? -1 : 1);
        }
    }

    //（<word totalsum> <sum,id,url1,urs2…… sum,id,url1,urs2…… >）
    public static class SortMapper extends Mapper<Object, Text, IntPair, Text>{
        private IntWritable new_key = new IntWritable();
        private Text new_value = new Text();
        //group key and value
        private final IntPair IntPair_key = new IntPair();
        private final Text IntPair_value = new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {

            String [] mes = value.toString().split("\t");
            String word = mes[0].split(" ")[0];
            int total = Integer.valueOf(mes[0].split(" ")[1]);

            //mes[1]:(id,sum,urls id,sum,urls)
            String [] file_mes = mes[1].split(" ");
            for (int i = 0;i < file_mes.length;i++){
                String id = file_mes[i].split(",")[0];
                int sum = Integer.parseInt(file_mes[i].split(",")[1]);
                String urls = "";
                for(int j = 2;j<file_mes[i].split(",").length;j++) {
                    if (j == 2)
                        urls = urls + file_mes[i].split(",")[j];
                    else
                        urls = urls + " " + file_mes[i].split(",")[j];
                }
                new_key.set(total);
                new_value.set(word + "," + id + "," + urls);

                //(<total,sum> <word,id,urls>)
                IntPair_key.set(total,sum);
                IntPair_value.set(new_value);
                //(<IntPair> <word,id,urls>)
                context.write(IntPair_key,IntPair_value);
            }
        }
    }

    //(<IntPair> <word,id,urls>)
    public static class IntSumReducer
            extends Reducer<IntPair, Text, Text,Text> {
        private Text result = new Text();
        private Text word = new Text();

        private int k = 1;
        public void setup(Context context) throws IOException{
            Configuration conf = context.getConfiguration();
            k = Integer.parseInt(conf.get("k"));
        }
        public void reduce(IntPair key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            int total = key.getFirst();
            if (total > k) {
                HashMap<String, String> wordList = new HashMap<String, String>();
                for (Text val : values) {
                    String[] mes = val.toString().split(",");
                    String thisword = mes[0];
                    if (wordList.containsKey(thisword)) {
                        wordList.put(thisword, wordList.get(thisword) + mes[1] + " " + key.getSecond() + " " + mes[2] + "\n");
                    } else {
                        wordList.put(thisword, mes[1] + " " + key.getSecond() + " " + mes[2] + "\n");
                    }
                }
                Set<String> keySet = wordList.keySet();
                Iterator<String> iter = keySet.iterator();
                while (iter.hasNext()) {
                    String temp_word = iter.next();
                    word.set(total + " " + temp_word);
                    String temp_val = wordList.get(temp_word);
                    result.set("\n" + temp_val);
                    //(<totalnum word> <"\n"id sum urls>)
                    context.write(word, result);
                }
            }
        }
    }

}