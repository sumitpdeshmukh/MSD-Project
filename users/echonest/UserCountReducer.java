package users.echonest;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class UserCountReducer extends MapReduceBase implements Reducer<Text,Text,Text,Text> {

	public void reduce(Text key, Iterator<Text> values, OutputCollector<Text,Text> output, Reporter reporter)
			throws IOException {
		// replace KeyType with the real type of your key
		int count = 0;
		int sum = 0;
		while (values.hasNext()) {
			// replace ValueType with the real type of your value
			//Text value = (Text) values.next();
			String value = values.next().toString();
			String[] valueline = value.split("\t");
			try {
				int i	= Integer.valueOf(valueline[1]);
				sum = sum + i;
			}
			catch(Exception e) {
				
			}
			count++;
			// process value
		}
		output.collect(key, new Text(String.valueOf(count) + "\t" + String.valueOf(sum)));
	}

}
