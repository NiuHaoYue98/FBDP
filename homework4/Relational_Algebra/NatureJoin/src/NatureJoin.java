import java.io.IOException;
import java.util.ArrayList;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class NatureJoin{
	public static class RelationA{
	    private int id;
	    private String name;
	    private int age;
	    private double weight;

	    public RelationA(){}

	    public RelationA(int id, String name, int age, double weight){
		this.setId(id);
		this.setName(name);
		this.setAge(age);
		this.setWeight(weight);
	    }

	    public RelationA(String line){
		String[] value = line.split(",");
		this.setId(Integer.parseInt(value[0]));
		this.setName(value[1]);
		this.setAge(Integer.parseInt(value[2]));
		this.setWeight(Double.parseDouble(value[3]));
	    }

	    public void setId(int id) {
		this.id = id;
	    }

	    public void setName(String name) {
		this.name = name;
	    }

	    public void setAge(int age) {
		this.age = age;
	    }

	    public void setWeight(double weight) {
		this.weight = weight;
	    }

	    public String getCol(int col){
		switch(col){
		    case 0: return String.valueOf(id);
		    case 1: return name;
		    case 2: return String.valueOf(age);
		    case 3: return String.valueOf(weight);
		    default: return null;
		}
	    }

	    public String getValueExcept(int col){
		switch(col){
		    case 0: return name + "," + String.valueOf(age) + "," + String.valueOf(weight);
		    case 1: return String.valueOf(id) + "," + String.valueOf(age) + "," + String.valueOf(weight);
		    case 2: return String.valueOf(id) + "," + name + "," + String.valueOf(weight);
		    case 3: return String.valueOf(id) + "," + name + "," + String.valueOf(age);
		    default: return null;
		}
	    }
	}
	public static class RelationB{
		private int id;
		private String gender;
		private int height;
		
		public RelationB(){}

	    public RelationB(String line){
		String[] value = line.split(",");
		this.setId(Integer.parseInt(value[0]));
		this.setGender(value[1]);
		this.setHeight(Integer.parseInt(value[2]));
	    }
	    public void setId(int id) {
		this.id = id;
	    }

	    public void setGender(String gender) {
		this.gender = gender;
	    }

	    public void setHeight(int height) {
		this.height = height;
	    }
		public String getCol(int col) {
			switch (col) {
				case 0:return String.valueOf(id);
				case 1:return gender;
				case 2:return String.valueOf(height);
				default:
				return null;
			}
		}
		public String getValExcept(int col) {
			switch (col) {
				case 0:return gender + "," + String.valueOf(height);
				case 1:return String.valueOf(id) + "," + String.valueOf(height);
				case 2:return String.valueOf(id) + "," + gender;
				default:
				return null;
			}
		}
		
	}

	public static class NatureJoinMap extends Mapper<LongWritable, Text, IntWritable, Text > {
		//private Text map_key = new Text();
		//private Text map_value = new Text();
		private int col = 0;

		@Override
		protected void map(LongWritable offset, Text line, Context context) throws IOException, InterruptedException {
			//String[] records = line.toString().split("\\n");
			FileSplit split = (FileSplit)context.getInputSplit();
		    	String filename = split.getPath().getName();
		    	System.out.println(filename);
			if(filename.contains("a")){
				RelationA record = new RelationA(line.toString());
				context.write(new IntWritable(Integer.parseInt(record.getCol(col))),new Text(filename + "," + record.name + "," + record.age + "," + record.weight));
				//context.write(new IntWritable(Integer.parseInt(record.getCol(col))),new Text(filename + "," + record.getValExcept(col)));
			}
			else if (filename.contains("b")){
				RelationB record = new RelationB(line.toString());
				context.write(new IntWritable(Integer.parseInt(record.getCol(col))),new Text(filename +  "," + record.gender + "," + record.height));
				//context.write(new IntWritable(Integer.parseInt(record.getCol(col))),new Text(filename + "," + record.getValExcept(col)));
			}
				//context.write(new Text(record.getCol(col)),new Text(relationName.toString() + " " + record.getValExcept(col)));	
	    	}
	}

	public static class NatureJoinReduce extends Reducer<IntWritable, Text, NullWritable, Text>{
		@Override
		public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
			int count = 0;
			String setAstr = new String();
			String setBstr = new String();
			ArrayList<Text>attributes = new ArrayList<Text>();

			for(Text val : values){
				count += 1;
				System.out.println(val);
				Text each = new Text();
				each.set(val.toString());
				attributes.add(each);
			}

			if (count == 1)
				return;
			else if (count == 2){
				System.out.println(attributes.get(0));
				System.out.println(key);
				System.out.println(attributes.get(1));
				for(int i = 0; i < attributes.size(); i++){
					if (attributes.get(i).toString().split(",")[0].equals("Ra.txt")){
						setAstr = attributes.get(i).toString();
					}
					else if(attributes.get(i).toString().split(",")[0].equals("Rb.txt")){
						setBstr = attributes.get(i).toString();
						//System.out.println("setB");
					}
				}
				//String[] array_setBstr = setBstr.split(",");
				Text result = new Text();
				result.set(key.toString()+","+setAstr.split(",")[1]+","+setAstr.split(",")[2]+","+setAstr.split(",")[3]+","+setBstr.split(",")[1]+","+setBstr.split(",")[2]);
				//result.set(key.toString()+","+setAstr+","+setBstr);
				context.write(NullWritable.get(), result);
			}
		}
	}


	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();

		Job natureJoinJob = new Job(conf, "natureJoinJob");
		natureJoinJob.setJarByClass(NatureJoin.class);
		natureJoinJob.setMapperClass(NatureJoinMap.class);
		natureJoinJob.setReducerClass(NatureJoinReduce.class);
		natureJoinJob.setMapOutputKeyClass(IntWritable.class);
		natureJoinJob.setMapOutputValueClass(Text.class);
		natureJoinJob.setOutputKeyClass(NullWritable.class);
		natureJoinJob.setOutputValueClass(Text.class);

		FileInputFormat.setInputPaths(natureJoinJob, new Path(args[0]), new Path(args[1]));
        FileOutputFormat.setOutputPath(natureJoinJob, new Path(args[2]));

        natureJoinJob.waitForCompletion(true);

        System.out.println("Finished!");
	}
}
