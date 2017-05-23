package stubs;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class BankTellerMapper 
  extends Mapper<LongWritable, Text, Text, IntWritable>
 {
	Text text = new Text();
	IntWritable digit = new IntWritable();
  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException 
	{
    
        String line = value.toString();
        String[] lineArray = line.split(",");
        
        if(lineArray.length >= 2)
        {
		String string=lineArray[0];
		String name = string.toLowerCase().trim();
		text.set(name);
		
		String values = lineArray[1];
        String valueArray[] = values.split(" ");
        
		int variable=0;
                for(String number:valueArray)
		{
              	variable = Integer.parseInt(number); 
		digit.set(variable);
		 
   		  context.write(text,digit);
               
		}

      }
 	}
}
