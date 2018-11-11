import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.LineReader;

/**
 * This class generates the initial Cluster centers as the input
 * of successive process.
 * it randomly chooses k instances as the initial k centers and 
 * store it as a sequenceFile.Specificly,we scan all the instances
 * and each time when we scan a new instance.we first check if 
 * the number of clusters no less than k. we simply add current 
 * instance to our cluster if the condition is satisfied or we will
 * replace the first cluster with it with probability 1/(currentNumber
 * + 1). 
 * @author KING
 *
 */
public final class RandomClusterGenerator {
	private int k;
	
	private FileStatus[] fileList;
	private FileSystem fs;
	private ArrayList<Cluster> kClusters;
	private Configuration conf;
	
	public RandomClusterGenerator(Configuration conf,String filePath,int k){
		this.k = k;
		try {
			fs = FileSystem.get(URI.create(filePath),conf);
			fileList = fs.listStatus((new Path(filePath)));
			kClusters = new ArrayList<Cluster>(k);
			this.conf = conf;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	/**
	 * 
	 * @param destinationPath the destination Path we will store
	 * our cluster file in.the initial file will be named clusters-0
	 */
	public void generateInitialCluster(String destinationPath){
		Text line = new Text();
		FSDataInputStream fsi = null;
		try {
			for(int i = 0;i < fileList.length;i++){
				fsi = fs.open(fileList[i].getPath());
				LineReader lineReader = new LineReader(fsi,conf);
				while(lineReader.readLine(line) > 0){
					System.out.println("read a line:" + line);
					Instance instance = new Instance(line.toString());
					Cluster cluster = new Cluster(kClusters.size() + 1, instance);
					kClusters.add(cluster);
					if(kClusters.size() == k)
						break;
				}
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			try {
				//in.close();
				fsi.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		}
		
		writeBackToFile(destinationPath);
		
	}
	
	public void writeBackToFile(String destinationPath){
		Path path = new Path(destinationPath + "cluster-0/clusters");
		FSDataOutputStream fsi = null;
		try {
			fsi = fs.create(path);
			for(Cluster cluster : kClusters){
				fsi.write((cluster.toString() + "\n").getBytes());
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			try {
				fsi.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
	}	
}
