import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class StockNewsDriver {
    private int k;
    private String inputPath;
    private String outputPath;

    public StockNewsDriver(int k, String inputPath, String outputPath){
        this.k = k;
        this.inputPath = inputPath;
        this.outputPath = outputPath;
    }

    public void wordcountJob() throws IOException,InterruptedException,ClassNotFoundException{
        System.out.println("WordCount Start!");
        Configuration conf = new Configuration();
        conf.setInt("k",k);
        Job wordcountJob = Job.getInstance(conf);

        Path path = new Path(outputPath + "/WordCount-temp");
        FileSystem filesystem = path.getFileSystem(conf);
        if(filesystem.exists(path)){
            filesystem.delete(path,true);
        }

        wordcountJob.setJobName("WordCount");
        wordcountJob.setJarByClass(WordCount.class);

        wordcountJob.setMapperClass((WordCount.TokenizerMapper.class));
        wordcountJob.setMapOutputKeyClass(Text.class);
        wordcountJob.setMapOutputValueClass(Text.class);

        wordcountJob.setReducerClass(WordCount.IntSumReducer.class);
        wordcountJob.setOutputKeyClass(Text.class);
        wordcountJob.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(wordcountJob, new Path(outputPath + "/InvertedIndex-temp"));
        Path tempDir = new Path(outputPath + "/WordCount-temp");
        FileOutputFormat.setOutputPath(wordcountJob, tempDir);
        wordcountJob.waitForCompletion(true);

        System.out.println("Wordcount output finished!");
    }

    public void invertedindexJob() throws IOException,InterruptedException,ClassNotFoundException{
        System.out.println("InvertedIndex Start!");

        Configuration conf = new Configuration();
        conf.setInt("k",k);

        for(int i = 1;i < 6;i++) {
            Path path = new Path(outputPath + "/InvertedIndex-temp/Part-" + i);
            FileSystem filesystem = path.getFileSystem(conf);
            if(filesystem.exists(path)){
                filesystem.delete(path,true);
            }
            Job job = new Job(conf, "invertedindex");
            job.setJarByClass(InvertedIndex.class);

            job.setMapperClass(InvertedIndex.InvertedIndexMapper.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);

            job.setReducerClass(InvertedIndex.InvertedIndexReducer.class);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            FileInputFormat.addInputPath(job, new Path(inputPath + "/Part-" + i));
            Path tempDir = new Path(outputPath + "/InvertedIndex-temp/Part-" + i);
            FileOutputFormat.setOutputPath(job, tempDir);
            job.waitForCompletion(true);
        }

    }

    public void sortJob() throws IOException,InterruptedException,ClassNotFoundException{
        System.out.println("Sort Start!");
        Configuration conf = new Configuration();
        conf.setInt("k",k);
        Job SortJob = Job.getInstance(conf);

        //delete the existed output file
        Path path = new Path(outputPath + "/Result");
        //Path path = new Path("wordcount-temp");
        FileSystem filesystem = path.getFileSystem(conf);
        if(filesystem.exists(path)){
            filesystem.delete(path,true);
        }

        SortJob.setJobName("WordCount");
        SortJob.setJarByClass(Sort.class);

        SortJob.setMapperClass(Sort.SortMapper.class);
        SortJob.setMapOutputKeyClass(Sort.IntPair.class);
        SortJob.setMapOutputValueClass(Text.class);

        //wordcountJob.setCombinerClass(WordCount.IntSumReducer.class);

        SortJob.setPartitionerClass(Sort.FirstPartitioner.class);
        SortJob.setGroupingComparatorClass(Sort.FirstGroupingComparator.class);

        SortJob.setReducerClass(Sort.IntSumReducer.class);
        SortJob.setOutputKeyClass(Text.class);
        SortJob.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(SortJob, new Path(outputPath + "/WordCount-temp"));
        FileOutputFormat.setOutputPath(SortJob, new Path(outputPath + "/Result"));
        SortJob.waitForCompletion(true);
        System.exit(0);
        System.out.println("Sort finished!");

    }

    public static void main(String[] args) throws Exception {
        String inputPath = args[0];
        String outputPath = args[1];
        int k = Integer.parseInt(args[2]);

        StockNewsDriver driver = new StockNewsDriver(k,inputPath,outputPath);
        driver.invertedindexJob();
        driver.wordcountJob();
        driver.sortJob();

    }
}
