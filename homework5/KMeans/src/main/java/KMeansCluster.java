import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;

import king.Utils.Distance;
import king.Utils.EuclideanDistance;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 在收敛条件满足且所有簇中心的文件最后产生后，再对输入文件
 * 中的所有实例进行划分簇的工作，最后把所有实例按照(实例,簇id)
 * 的方式写进结果文件
 * @author KING
 *
 */
public class KMeansCluster {
	public static class KMeansClusterMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
		private ArrayList<Cluster> kClusters = new ArrayList<Cluster>();
		/**
		 * 读入目前的簇信息
		 */
		@Override
		protected void setup(Context context) throws IOException,InterruptedException{
			super.setup(context);
			FileSystem fs = FileSystem.get(context.getConfiguration());
	        FileStatus[] fileList = fs.listStatus(new Path(context.getConfiguration().get("clusterPath")));
	        BufferedReader in = null;
			FSDataInputStream fsi = null;
			String line = null;
	        for(int i = 0; i < fileList.length; i++){
	        	if(!fileList[i].isDir()){
	        		fsi = fs.open(fileList[i].getPath());
					in = new BufferedReader(new InputStreamReader(fsi,"UTF-8"));
					while((line = in.readLine()) != null){
						System.out.println("read a line:" + line);
						Cluster cluster = new Cluster(line);
						cluster.setNumOfPoints(0);
						kClusters.add(cluster);
					}
	        	}
	        }
	        in.close();
	        fsi.close();
		}
		
		/**
		 * 读取一行然后寻找离该点最近的簇id发射(instance,clusterID)
		 */
		@Override
		public void map(LongWritable key, Text value, Context context)throws 
		IOException, InterruptedException{
			Instance instance = new Instance(value.toString());
			int id;
			try {
				id = getNearest(instance);
				if(id == -1)
					throw new InterruptedException("id == -1");
				else{
					context.write(value, new IntWritable(id));
				}
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		public int getNearest(Instance instance) throws Exception{
			int id = -1;
			double distance = Double.MAX_VALUE;
			Distance<Double> distanceMeasure = new EuclideanDistance<Double>();
			double newDis = 0.0;
			for(Cluster cluster : kClusters){	
				newDis = distanceMeasure.getDistance(cluster.getCenter().getValue()
						, instance.getValue());
				if(newDis < distance){
					id = cluster.getClusterID();
					distance = newDis;
				}
			}
			return id;
		}
	}
}
