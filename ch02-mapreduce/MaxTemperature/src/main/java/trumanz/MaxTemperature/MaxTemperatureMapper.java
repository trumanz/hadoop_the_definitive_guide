package trumanz.MaxTemperature;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;



public class MaxTemperatureMapper extends 
	Mapper<LongWritable, Text, Text, IntWritable>{
	private static final int MISSING = 9999;
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
		//0029029070999991901010106004+64333+023450FM-12+000599999V0202701N015919999999N0000001N9-00781+99999102001ADDGF108991999999999999999999
		String line = value.toString();
		String year = line.substring(15, 19);
		int airTemperature;
		airTemperature  = Integer.parseInt(line.substring(88, 92));
		if(line.charAt(87) == '-'){
			airTemperature = 0 - airTemperature;
		}
		String quality = line.substring(92,93);
		if(airTemperature != MISSING && quality.matches("[01459]")){
			context.write(new Text(year), new IntWritable(airTemperature));
		}
		
	}

}
