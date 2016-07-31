package neu;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;



public class SongsSungByArtist {

	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {



		public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
			String line = value.toString();
			String[] words=line.split("\t");
			
			if(words[4].indexOf("AR")!=-1){
				Text artistId=new Text(words[4].trim());
				String name=words[8].trim();
				String nameAndCount="1\t"+name;
				output.collect(artistId,new Text(nameAndCount));
			}





		}



	}

	public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {			       
			int sumOfSongCount=0;
			StringBuilder sb=new StringBuilder();
			while (values.hasNext()) {
				String[] nameAndCount=values.next().toString().split("\t");
				
				sumOfSongCount+=Integer.parseInt(nameAndCount[0]);
				sb.append(nameAndCount[1].trim()).append(" ");
				
			}

			String finalValue=sumOfSongCount+"\t"+sb.toString().trim();

			output.collect(key, new Text(finalValue));
		}
	}

	public static void main(String[] args) throws Exception {
		JobConf conf = new JobConf(SongUserAndCount.class);
		conf.setJobName("Song Sung By Artist");

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

		conf.setMapperClass(Map.class);

		conf.setReducerClass(Reduce.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));

		JobClient.runJob(conf);
	}
}
