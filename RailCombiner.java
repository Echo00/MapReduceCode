package de.tuberlin.dbpra.mapreduce.rail;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class RailCombiner extends Reducer<Text, IntWritable, Text, IntWritable> {


	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		System.out.println("Entering RailCombiner.reduce()");

		Iterator<IntWritable> i = values.iterator();
		int sum = 0;
		while (i.hasNext()) {
			i.next();
			sum += 1;
		}
		context.write(key, new IntWritable(sum));

	}
}

