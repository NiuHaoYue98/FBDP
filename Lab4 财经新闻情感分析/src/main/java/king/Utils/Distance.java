package king.Utils;


public class Distance {
	public static double EuclideanDistance(double[] a,double[] b) throws Exception{
		if(a.length != b.length)
			throw new Exception("size not compatible!");
		double sum = 0.0;
        for(int i = 0;i < a.length;i++){
			sum += Math.pow(a[i] - b[i], 2);
        }
		return Math.sqrt(sum);
	}
}
