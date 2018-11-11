import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.net.URI;
import java.util.ArrayList;
import java.util.Random;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.LineReader;

public class KMeansDriver {
	private int k;
	private int iterationNum;
	private String sourcePath;
	private String outputPath;
	
	private Configuration conf;
	
	public KMeansDriver(int k, int iterationNum, String sourcePath, String outputPath, Configuration conf){
		this.k = k;
		this.iterationNum = iterationNum;
		this.sourcePath = sourcePath;
		this.outputPath = outputPath;
		this.conf = conf;
	}
	
	public void clusterCenterJob(int i) throws IOException, InterruptedException, ClassNotFoundException{

			//start a round of iteration,hope to change here
			Job clusterCenterJob = new Job();
			clusterCenterJob .setJobName("clusterCenterJob" + i);
			clusterCenterJob .setJarByClass(KMeans.class);
			
			clusterCenterJob.getConfiguration().set("clusterPath", outputPath + "/cluster-" + i +"/");

			clusterCenterJob.setMapperClass(KMeans.KMeansMapper.class);
			clusterCenterJob.setMapOutputKeyClass(IntWritable.class);
			clusterCenterJob.setMapOutputValueClass(Cluster.class);

			clusterCenterJob.setCombinerClass(KMeans.KMeansCombiner.class);
			clusterCenterJob.setReducerClass(KMeans.KMeansReducer .class);
			clusterCenterJob.setOutputKeyClass(NullWritable.class);
			clusterCenterJob.setOutputValueClass(Cluster.class);

			FileInputFormat.addInputPath(clusterCenterJob, new Path(sourcePath));
			FileOutputFormat.setOutputPath(clusterCenterJob, new Path(outputPath + "/cluster-" + (i + 1) +"/"));
			
			clusterCenterJob.waitForCompletion(true);
			System.out.println("finished!");

	}
	
	public void KMeansClusterJod(int i ) throws IOException, InterruptedException, ClassNotFoundException{
		Job kMeansClusterJob = new Job();
		kMeansClusterJob.setJobName("KMeansClusterJob");
		kMeansClusterJob.setJarByClass(KMeansCluster.class);
		
		kMeansClusterJob.getConfiguration().set("clusterPath", outputPath + "/cluster-" + (i+1) +"/");

		kMeansClusterJob.setMapperClass(KMeansCluster.KMeansClusterMapper.class);
		kMeansClusterJob.setMapOutputKeyClass(Text.class);
		kMeansClusterJob.setMapOutputValueClass(IntWritable.class);

		kMeansClusterJob.setNumReduceTasks(0);

		FileInputFormat.addInputPath(kMeansClusterJob, new Path(sourcePath));
		FileOutputFormat.setOutputPath(kMeansClusterJob, new Path(outputPath + "/clusteredInstances-" + (i+1) + "/"));
		
		kMeansClusterJob.waitForCompletion(true);
		System.out.println("finished!");
	}
	
	public void generateInitialCluster(){
		RandomClusterGenerator generator = new RandomClusterGenerator(conf, sourcePath, k);
		generator.generateInitialCluster(outputPath + "/");
	}
	
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException{
		System.out.println("start");
		Configuration conf = new Configuration();
		int k = Integer.parseInt(args[0]);
		int iterationNum = Integer.parseInt(args[1]);
		String sourcePath = args[2];
		String outputPath = args[3];
		KMeansDriver driver = new KMeansDriver(k, iterationNum, sourcePath, outputPath, conf);
		driver.generateInitialCluster();
		System.out.println("initial cluster finished");
		for(int i = 0;i < iterationNum; i++) {
			driver.clusterCenterJob(i);
			driver.KMeansClusterJod(i);
		}
	}
}
