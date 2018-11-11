import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;

/*
 * k-means�����㷨����Ϣ
 */
public class Cluster implements Writable{
	private int clusterID;
	private long numOfPoints;
	private Instance center;
	private double SumDistance;
	
	public Cluster(){
		this.setClusterID(-1);
		this.setNumOfPoints(0);
		this.setCenter(new Instance());
		this.setSumDistance(0);
	}
	
	public Cluster(int clusterID,Instance center){
		this.setClusterID(clusterID);
		this.setNumOfPoints(0);
		this.setCenter(center);
		this.setSumDistance(0);
	}
	
	public Cluster(String line){
		String[] value = line.split(",",4);
		clusterID = Integer.parseInt(value[0]);
		numOfPoints = Long.parseLong(value[1]);
        SumDistance = Double.parseDouble(value[2]);
        center = new Instance(value[3]);
	}
	
	public String toString(){
		String  result = String.valueOf(clusterID) + "," 
				+ String.valueOf(numOfPoints) + "," + String.valueOf(SumDistance) + "," + center.toString()  ;
		return result;
	}

	public int getClusterID() {
		return clusterID;
	}

	public void setClusterID(int clusterID) {
		this.clusterID = clusterID;
	}

	public long getNumOfPoints() {
		return numOfPoints;
	}

	public void setNumOfPoints(long numOfPoints) {
		this.numOfPoints = numOfPoints;
	}

	public Instance getCenter() {
		return center;
	}

	public void setCenter(Instance center) {
		this.center = center;
	}


	public double getSumDistance() {
		return SumDistance;
	}

	public void setSumDistance(double distance ) {
		this.SumDistance = distance;
	}
	
	public void observeInstance(Instance instance){
		try {
			Instance sum = center.multiply(numOfPoints).add(instance);
			numOfPoints++;
			center = sum.divide(numOfPoints);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeInt(clusterID);
		out.writeLong(numOfPoints);
        out.writeDouble(SumDistance);
		center.write(out);

	}

	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		clusterID = in.readInt();
		numOfPoints = in.readLong();
        SumDistance = in.readDouble();
		center.readFields(in);
	}
}
