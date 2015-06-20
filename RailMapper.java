package de.tuberlin.dbpra.mapreduce.rail;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

// Bin mir nicht sicher ob wir die Statiche Methode Tokenizer brauchen.
//import org.apache.tools.ant.filters.TokenFilter.StringTokenizer;


import java.io.IOException;
import java.util.*;

public class RailMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	
	// These values wild build my <key, value> tuples
	private final static IntWritable one = new IntWritable(1);
	private Text word = new Text();


	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
		StringTokenizer tokenizer = new StringTokenizer(line); 
		while (tokenizer.hasMoreTokens()) {
			word.set(tokenizer.nextToken());
			context.write(word, one);
		}
	}

}

