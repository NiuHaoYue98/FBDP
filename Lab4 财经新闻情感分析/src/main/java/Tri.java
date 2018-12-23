public class Tri{
    public String tid;
    public double [] attr;
    public String tag;


    public Tri(String line){
        tid = line.split("\t")[0];
        String [] value = line.split("\t")[1].split(" ");
        int length = value.length;
        attr = new double[length-1];
        for (int i = 0;i < length-1;i++){
            attr[i] = Double.parseDouble(value[i]);
        }
        tag = value[length-1];
    }

    public double[] getattr(){
        return attr;
    }

    public String gettag(){
        return tag;
    }
}
