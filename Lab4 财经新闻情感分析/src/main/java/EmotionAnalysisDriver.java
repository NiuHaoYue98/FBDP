import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class EmotionAnalysisDriver {
    private String trainset;            //样本集
    private String testset;             //待训练集
    private int k;                      //邻居个数
    private String outputPath;          //输出路径
    private int crossnum;
    private String crossinput;

    public EmotionAnalysisDriver(String trainset, String testset,int k, String outputPath,int crossnum, String crossinput){
        this.trainset = trainset;
        this.testset = testset;
        this.k = k;
        this.outputPath = outputPath;
        this.crossnum = crossnum;
        this.crossinput = crossinput;
    }

    public void KNNJob() throws IOException,InterruptedException,ClassNotFoundException {
        System.out.println("KNN Start!");
        Configuration conf = new Configuration();
        conf.set("trainset", trainset);
        conf.set("testset",testset);
        conf.setInt("k",k);
        conf.set("outputPath",outputPath);

        Job job = new Job(conf,"KNN");

        Path path = new Path(outputPath);
        FileSystem filesystem = path.getFileSystem(conf);
        if (filesystem.exists(path)) {
            filesystem.delete(path, true);
        }
        job.setJarByClass(KNN.class);
        job.setMapperClass((KNN.KNNMap.class));
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(testset));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
        System.out.println("KNN job finished!");
    }

    //KNN generate the train files
    public void CrossTestJob() throws IOException,InterruptedException,ClassNotFoundException {
        System.out.println("CrossTest Start!");

        Configuration conf = new Configuration();
        conf.setInt("k",k);

        for (int i = 0;i < crossnum;i++) {
            conf.setInt("crossnum",i);
            Job job = new Job(conf, "CrossTest");
            Path path = new Path(outputPath+ "/CrossTest-" + i);
            FileSystem filesystem = path.getFileSystem(conf);
            if (filesystem.exists(path)) {
                filesystem.delete(path, true);
            }
            job.setJarByClass(Cross.class);

            job.setMapperClass(Cross.Map.class);
            job.setMapOutputKeyClass(IntWritable.class);
            job.setMapOutputValueClass(Text.class);

            FileInputFormat.addInputPath(job, new Path(crossinput));
            FileOutputFormat.setOutputPath(job, new Path(outputPath + "/CrossTest-" + i));
            job.waitForCompletion(true);
            System.out.println("Generate the CrossTest job file " + i + " finished!");
            //break;
        }
    }
    //KNN cross test
    public void KNNCrossJob() throws IOException,InterruptedException,ClassNotFoundException {
        System.out.println("KNN Cross test Start!");
        Configuration conf = new Configuration();

        conf.setInt("k",k);
        conf.set("outputPath",outputPath);

        for (int i = 2;i < crossnum;i++) {
            conf.setInt("crossnum",i);
            conf.set("trainset", outputPath + "/CrossTest-" + i + "/part-r-00000");
            Job job = new Job(conf, "KNNCross");

            Path path = new Path(outputPath+ "/CrossTest-result" + i);
            FileSystem filesystem = path.getFileSystem(conf);
            if (filesystem.exists(path)) {
                filesystem.delete(path, true);
            }
            job.setJarByClass(KNN.class);

            job.setMapperClass((KNN.KNNMap.class));
            job.setMapOutputKeyClass(IntWritable.class);
            job.setMapOutputValueClass(Text.class);

            FileInputFormat.addInputPath(job, new Path(crossinput));
            FileOutputFormat.setOutputPath(job, path);
            job.waitForCompletion(true);
            System.out.println("CrossTest job " + i + " finished!");
            //break;
        }
    }
    //KNN analysis
    public void AnalysisJob() throws IOException,InterruptedException,ClassNotFoundException {
        System.out.println("Ration computing test Start!");
        Configuration conf = new Configuration();

        conf.setInt("k",k);
        conf.set("outputPath",outputPath);

        conf.set("trainset", trainset);
        for (int i = 8;i < crossnum;i ++) {
            Job job = new Job(conf, "Analysis");
            Path path = new Path(outputPath + "/Ratio" + i);
            FileSystem filesystem = path.getFileSystem(conf);
            if (filesystem.exists(path)) {
                filesystem.delete(path, true);
            }
            job.setJarByClass(Analysis.class);
            job.setMapperClass((Analysis.AnalysisMap.class));
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(IntWritable.class);
            job.setReducerClass(Analysis.AnalysisReduce.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(IntWritable.class);

            FileInputFormat.addInputPath(job, new Path(outputPath+ "/CrossTest-result" + i));
            FileOutputFormat.setOutputPath(job, path);
            job.waitForCompletion(true);
            //break;
        }
        System.out.println("Ration computing finished!");
    }

    public void NavieBayesJob() throws IOException,InterruptedException,ClassNotFoundException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        Path path_train, path_temp, path_test, path_out;

        conf.set("train", trainset);
        conf.set("test", testset);
        conf.set("output", outputPath);

        path_train = new Path(trainset);
        path_temp = new Path(outputPath + "/train");
        path_test = new Path(testset);
        path_out = new Path(outputPath + "/result");

        Job job_train = new Job(conf, "naive bayse training");
        job_train.setJarByClass(NaiveBayesTrain.class);
        job_train.setMapperClass(NaiveBayesTrain.TrainMapper.class);
        job_train.setCombinerClass(NaiveBayesTrain.TrainReducer.class);
        job_train.setReducerClass(NaiveBayesTrain.TrainReducer.class);
        job_train.setOutputKeyClass(Text.class);
        job_train.setOutputValueClass(IntWritable.class);

        FileInputFormat.setInputPaths(job_train, path_train);
        if(fs.exists(path_temp))
            fs.delete(path_temp, true);
        FileOutputFormat.setOutputPath(job_train, path_temp);
        if(job_train.waitForCompletion(true) == false)
            System.exit(1);


        conf.set("train_result",outputPath + "/train/part-r-00000");
        Job job_test = new Job(conf, "naive bayse testing");
        job_test.setJarByClass(NaiveBayesTest.class);
        job_test.setMapperClass(NaiveBayesTest.TestMapper.class);
        job_test.setOutputKeyClass(Text.class);
        job_test.setOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job_test, path_test);
        if(fs.exists(path_out))
            fs.delete(path_out, true);
        FileOutputFormat.setOutputPath(job_test, path_out);
        if(job_test.waitForCompletion(true) == false)
            System.exit(1);

        System.exit(0);

    }
    public static void main(String[] args) throws Exception {
        String trainset = args[0];                  //样本集
        String testset = args[1];                   //待训练集
        int k = Integer.parseInt(args[2]);          //邻居个数
        String outputPath = args[3];                //输出路径
        int crossnum = Integer.parseInt(args[4]);   //交叉集数目
        String crossinput = args[5];                //全部训练集路径，用于交叉验证

        EmotionAnalysisDriver driver = new EmotionAnalysisDriver(trainset,testset,k,outputPath,crossnum,crossinput);
        //KNN
        driver.KNNJob();

        //cross test:
        //generate the cross file
        driver.CrossTestJob();
        //cross test for KNN
        driver.KNNCrossJob();
        //compute the ratio
        driver.AnalysisJob();

        //NavieBayes
        driver.NavieBayesJob();
    }
}
