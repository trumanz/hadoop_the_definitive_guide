package trumanz.MaxTemperature;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.sun.jersey.core.impl.provider.entity.XMLJAXBElementProvider.Text;

/**
 * Hello world!
 *
 */
public class MaxTemperature {
	public static void main(String[] args) throws IOException {

		Path inputPath = new Path("");
		Path outputPath = new Path("");

		Configuration conf = new Configuration();
		
		Job job = Job.getInstance(conf);

		job.setJarByClass(MaxTemperature.class);
		job.setJobName("Max Temperature");

		FileInputFormat.addInputPath(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);
		
		job.setMapperClass(MaxTemperatureMapper.class);
		job.setReducerClass(MaxTemperatureRecuder.class);
		
		//The ouput types for the reduce functioon,
		//The map output types defauls to the same types; if differents, must set by setMapouputKeyClass and ..!!
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
