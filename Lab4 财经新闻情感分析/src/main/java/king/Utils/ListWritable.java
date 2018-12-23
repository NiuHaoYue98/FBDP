package king.Utils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Writable;

public class ListWritable<T extends Writable> implements Writable {
	private List<T> list;
	private Class<T> clazz;

	public ListWritable(){
		list = null;
		clazz = null;
	}
	public ListWritable(Class<T> clazz) {
	       this.clazz = clazz;
	       list = new ArrayList<T>();
	    }
	
	public void setList(List<T> list){
		this.list = list;
	}
	
	public boolean isEmpty(){
		return list.isEmpty();
	}
	
	public int size(){
		return list.size();
	}
	
	public void add(T element){
		list.add(element);
	}
	
	public void add(int index,T element){
		list.add(index, element);
	}
	
	public T get(int index){
		return list.get(index);
	}
	
	public T remove(int index){
		return list.remove(index);
	}
	
	public void set(int index,T element){
		list.set(index, element);
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
	    out.writeUTF(clazz.getName());
	    out.writeInt(list.size());
	    for (T element : list) {
	        element.write(out);
	    }
	 }

	 @SuppressWarnings("unchecked")
	@Override
	 public void readFields(DataInput in) throws IOException{
	 try {
		clazz = (Class<T>) Class.forName(in.readUTF());
	} catch (ClassNotFoundException e1) {
		// TODO Auto-generated catch block
		e1.printStackTrace();
	}
	 int count = in.readInt();
	 this.list = new ArrayList<T>();
	 for (int i = 0; i < count; i++) {
	    try {
	        T obj = clazz.newInstance();
	        obj.readFields(in);
	        list.add(obj);
	    } catch (InstantiationException e) {
	        e.printStackTrace();
	    } catch (IllegalAccessException e) {
	        e.printStackTrace();
	    }
	  }
	}

}
