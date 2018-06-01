package product;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Iterator;  

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;  
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;  
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;  
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;  


public class Product { 
	  
    public static class MatrixMapper extends Mapper<LongWritable, Text, Text, Text> {  
  
        private String flag;// m1 or m2  
  
        private int rowNum = 10;// 矩阵A的行数  
        private int colNum = 10;// 矩阵B的列数  
  
        @Override  
        protected void setup(Context context) throws IOException, InterruptedException {  
            FileSplit split = (FileSplit) context.getInputSplit();  
            flag = split.getPath().getParent().getName();// 判断读的数据集  
            System.out.println(flag);
        }  
  
        @Override  
        public void map(LongWritable key, Text values, Context context) throws IOException, InterruptedException {  
            String[] tokens = values.toString().split(",");  
            
            if (flag.equals("m1")) {  
                String row = tokens[0];  
                String col = tokens[1];  
                String val = tokens[2];  
                System.out.println("m1:"+row+col+val);
                for (int i = 1; i <= colNum; i++) {  
                    Text k = new Text(row + "," + i);  
                    Text v = new Text("A:" + col + "," + val);  
                    context.write(k, v);  
                    System.out.println(k.toString() + "  " + v.toString());  
                }  
  
            } else if (flag.equals("m2")) {  
                String row = tokens[0];  
                String col = tokens[1];  
                String val = tokens[2];  
                System.out.println("m2:"+row+col+val);
                for (int i = 1; i <= rowNum; i++) {  
                    Text k = new Text(i + "," + col);  
                    Text v = new Text("B:" + row + "," + val);  
                    context.write(k, v);  
                    System.out.println(k.toString() + "  " + v.toString());  
  
                }  
            }  
        }  
    }  
  
    public static class MatrixReducer extends Reducer<Text, Text, Text, IntWritable> {  
  
        @Override  
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {  
  
            Map<String, String> mapA = new HashMap<String, String>();  
            Map<String, String> mapB = new HashMap<String, String>();  
  
            System.out.print(key.toString() + ":");  
  
            for (Text line : values) {  
                String val = line.toString();  
                System.out.print("(" + val + ")");  
  
                if (val.startsWith("A:")) {  
                    String[] kv = val.substring(2).split(",");  
                    mapA.put(kv[0], kv[1]);  
  
                    System.out.println("A:" + kv[0] + "," + kv[1]);  
  
                } else if (val.startsWith("B:")) {  
                    String[] kv = val.substring(2).split(",");  
                    mapB.put(kv[0], kv[1]);  
  
                    System.out.println("B:" + kv[0] + "," + kv[1]);  
                }  
            }  
  
            int result = 0;  
            Iterator<String> iter = mapA.keySet().iterator();  
            while (iter.hasNext()) {  
                String mapk = iter.next();  
                String bVal = mapB.containsKey(mapk) ? mapB.get(mapk) : "0";  
                result += Integer.parseInt(mapA.get(mapk)) * Integer.parseInt(bVal);  
            }  
            context.write(key, new IntWritable(result));  
            System.out.println();  
  
            System.out.println("C:" + key.toString() + "," + result);  
        }  
    }  
  
    public static void run(Map<String, String> path) throws IOException, InterruptedException, ClassNotFoundException {  
    	
        String input1 = path.get("input1");  
        String input2 = path.get("input2");  
        String output = path.get("output");  
    
        Configuration conf = new Configuration();
        Job job = new Job(conf);  
        job.setJarByClass(Product.class);  
  
        job.setOutputKeyClass(Text.class);  
        job.setOutputValueClass(Text.class);  
  
        job.setMapperClass(MatrixMapper.class);  
        job.setReducerClass(MatrixReducer.class);  
  
        job.setInputFormatClass(TextInputFormat.class);  
        job.setOutputFormatClass(TextOutputFormat.class);  
  
        FileInputFormat.setInputPaths(job, new Path(input1), new Path(input2));// 加载2个输入数据集  
        FileOutputFormat.setOutputPath(job, new Path(output));  
  
        job.waitForCompletion(true);  
    }  
    public static void main(String[] args) {  
        sparseMartrixMultiply();  
    }      
      
    public static void sparseMartrixMultiply() {  
        Map<String, String> path = new HashMap<String, String>();  
        path.put("input1", "graph_input/m1");  // left matrix
        path.put("input2", "graph_input/m2");  // right matrix
        path.put("output", "graph_output");  
  
        try {  
            Product.run(path);// 启动程序  
        } catch (Exception e) {  
            e.printStackTrace();  
        }
        System.exit(0);
    }  
}  

