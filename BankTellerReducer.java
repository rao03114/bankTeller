package stubs;

import java.io.*;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class BankTellerReducer
  extends Reducer<Text,IntWritable, Text, Text> 
{
	Text digit = new Text ();
	int total=0;
  
  @Override
 public void reduce(Text key, Iterable<IntWritable> values,
      Context context)
      throws IOException, InterruptedException 
{  	
     	
	//String output="";
int count=0;
int sum=0;
float avg;
     for(IntWritable value : values)
    {
      sum += value.get();
	count+=1;
    
   }
     
 total+=sum;
 avg=((float)sum/count);

String output=String.valueOf(sum)+"\t"+String.valueOf(avg);
	//int results=Integer.parseInt(output.trim());
	digit.set(output);	
    context.write(key,digit);
}
  
@Override
protected void cleanup(Context context) throws IOException, InterruptedException
{
	String key="Total amount of money deposited in the bank is:";
	String val =String.valueOf(total)+"dollars";
	context.write(new Text(key),new Text(val));
}
}

