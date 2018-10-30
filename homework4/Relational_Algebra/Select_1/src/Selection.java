import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.WritableComparable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Selection {
	public static class RelationA implements WritableComparable<RelationA>{
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

	    public boolean isCondition(int col, String value){
		if(col ==2 && Integer.parseInt(value) == this.age)
		    return true;
		else
		    return false;
	    }

	    public int getId() {
		return id;
	    }

	    public void setId(int id) {
		this.id = id;
	    }

	    public String getName() {
		return name;
	    }

	    public void setName(String name) {
		this.name = name;
	    }

	    public int getAge() {
		return age;
	    }

	    public void setAge(int age) {
		this.age = age;
	    }

	    public double getWeight() {
		return weight;
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

	    @Override
	    public String toString(){
		return id + "," + name + "," + age + "," + weight;
	    }


	    public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeInt(id);
		out.writeUTF(name);
		out.writeInt(age);
		out.writeDouble(weight);
	    }


	    public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		id = in.readInt();
		name = in.readUTF();
		age = in.readInt();
		weight = in.readDouble();
	    }


	    public int compareTo(RelationA o) {
		if(id == o.getId() && name.equals(o.getName())
		        && age == o.getAge() && weight == o.getWeight())
		    return 0;
		else if(id < o.getId())
		    return -1;
		else
		    return 1;
	    }
	}

    public static  class SelectionMap extends Mapper<LongWritable, Text, RelationA, NullWritable>{
        private int id;
        private String value;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            id = context.getConfiguration().getInt("col", 0);
            value = context.getConfiguration().get("value");
        }

        @Override
        protected void map(LongWritable key, Text line, Context context) throws IOException, InterruptedException {
            RelationA record = new RelationA(line.toString());
            if(record.isCondition(id, value))
                context.write(record, NullWritable.get());
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
            //selectionJob.setJobName("selectionJob");

        conf.setInt("col", Integer.parseInt(args[2]));
        conf.set("value", args[3]);
        Job selectionJob = Job.getInstance(conf, "selectionJob");

        selectionJob.setJarByClass(Selection.class);
        selectionJob.setMapperClass(SelectionMap.class);
        selectionJob.setMapOutputKeyClass(RelationA.class);
        selectionJob.setMapOutputKeyClass(NullWritable.class);

        selectionJob.setNumReduceTasks(0);


        selectionJob.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.addInputPath(selectionJob, new Path(args[0]));
        FileOutputFormat.setOutputPath(selectionJob, new Path(args[1]));
        selectionJob.waitForCompletion(true);
        System.out.println("Finished!");

    }
}
