package users.echonest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class UserCountMapper extends MapReduceBase implements Mapper<Writable, Text, Text, Text> {

	public void map(Writable key, Text value, OutputCollector<Text,Text> output, Reporter reporter)
			throws IOException {
		String userline = value.toString();
		String[] splitline = userline.split("\t");
		output.collect(new Text(splitline[1]), new Text(splitline[0] +"\t" +splitline[2]));
	}

}
