package org.apache.hadoop.examples;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.opencsv.CSVReader;
import com.google.gson.JsonParser;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

public class WordCount {

	static final int COMPANY = 9;
	static final int REVENUE = 12;
	static final int TITLE = 17;
	static final int SCORE = 18;
	
	static ArrayList<String> parseCompanies(String s){
		JsonParser parse = new JsonParser();
		ArrayList<String> companies = new ArrayList();

		JsonArray jArray = parse.parse(s).getAsJsonArray();
		for (int i = 0; i < jArray.size(); ++i) {
			JsonObject subObj = jArray.get(i).getAsJsonObject();
			companies.add(subObj.get("name").getAsString());
		}
		return companies;
	}

	public static class MyMapper extends Mapper<Object, Text, Text, DoubleWritable> {
		
		private Text company = new Text();
		
		

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			CSVReader csvReader = new CSVReader(new StringReader(value.toString()));
			String[] record = null;
			while ((record = csvReader.readNext()) != null) {

				System.out.println(record[REVENUE]);
				System.out.println(record[TITLE]);
				try {
					if (Double.parseDouble(record[SCORE]) >= 6.5) {
						
						ArrayList<String> companies = parseCompanies(record[COMPANY]);
						for (String com : companies) {
							company.set(com);
							context.write(company, new DoubleWritable(Double.parseDouble(record[REVENUE])));
						}
					}
				} catch (Exception e) {
					e.printStackTrace();
					continue;
				}

			}
			csvReader.close();

		}
	}

	public static class MyReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
		private DoubleWritable result = new DoubleWritable();

		public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			for (DoubleWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: wordcount <in> <out>");
			System.exit(2);
		}
		Job job = new Job(conf, "word count");
		job.setJarByClass(WordCount.class);
		job.setMapperClass(MyMapper.class);
		job.setCombinerClass(MyReducer.class);
		job.setReducerClass(MyReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
