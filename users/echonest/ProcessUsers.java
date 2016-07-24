package users.echonest;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.Reducer;

public class ProcessUsers {

	public static void main(String[] args) {
		JobClient client = new JobClient();
		JobConf conf = new JobConf(ProcessUsers.class);
		conf.setJobName("Users Song Preferance");
		
		// TODO: specify output types
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

		// TODO: specify input and output DIRECTORIES (not files)
		//conf.setInputPath(new Path("src"));
		FileInputFormat.addInputPath(conf, new Path("/Users/sumitdeshmukh/Eclipse/workspace/ProcessMillionUsers/input"));
		//conf.setOutputPath(new Path("out"));
		FileOutputFormat.setOutputPath(conf, new Path("/Users/sumitdeshmukh/Eclipse/workspace/ProcessMillionUsers/output"));
		// TODO: specify a mapper
		conf.setMapperClass(UserCountMapper.class);
		
		// TODO: specify a reducer
		conf.setReducerClass(UserCountReducer.class);

		client.setConf(conf);
		try {
			JobClient.runJob(conf);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
