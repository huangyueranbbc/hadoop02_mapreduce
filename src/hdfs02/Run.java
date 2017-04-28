package hdfs02;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * @category MapperReduce 分布式计算
 * @author huangyueran
 *
 */
public class Run {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
//		conf.set("fs.default.name", "hdfs://master:8020");
//		conf.set("yarn.resourcemanager.hostname","master");
		conf.set("mapred.jar", "H:\\jar\\wc.jar");
		
		Job job = Job.getInstance(conf);

		job.setMapperClass(WcMapper.class);
		job.setReducerClass(WcReduce.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);

		// job.setNumReduceTasks(20); // 设置Reduce任务个数 

		FileInputFormat.setInputPaths(job, "/user/input/wc");
		FileOutputFormat.setOutputPath(job, new Path("/user/ouput/wc2"));

		FileSystem fs = FileSystem.get(conf);
		if(fs.exists(new Path("/user/ouput/wc2"))){
			fs.delete(new Path("/user/ouput/wc2"),true);
		}
		
		job.waitForCompletion(true); // 是否等待完成

	}
}
