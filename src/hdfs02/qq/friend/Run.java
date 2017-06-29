package hdfs02.qq.friend;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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
		conf.set("fs.default.name", "hdfs://master:8020");
		conf.set("mapred.jar", "H:\\jar\\qq.jar");
		Job job = Job.getInstance(conf);

		job.setJobName("qq");
		job.setMapperClass(FriendMapper.class);
		job.setReducerClass(FriendReduce.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		// job.setNumReduceTasks(20); // 设置Reduce任务个数

		FileInputFormat.setInputPaths(job, "/user/input/qq");
		FileOutputFormat.setOutputPath(job, new Path("/user/ouput/qq"));
		
		FileSystem fs = FileSystem.get(conf);
		if(fs.exists(new Path("/user/ouput/qq"))){
			fs.delete(new Path("/user/ouput/qq"),true);
		}

		job.waitForCompletion(true); // 是否等待完成

	}
}
