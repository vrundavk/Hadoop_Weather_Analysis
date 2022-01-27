import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;
 
public class MyMaxMin {
 
     
    // Mapper
     
   
    public static class MaxTemperatureMapper extends
            Mapper<LongWritable, Text, Text, IntWritable> {
         
    
    public static final int MISSING = 9999;
         
    @Override
        public void map(LongWritable Key, Text Value, Context context)
                throws IOException, InterruptedException {
 
       String line = Value.toString();
             
            
            if (!(line.length() == 0)) {
                 
                String year = line.substring(18,22);
                int temperature = Integer.parseInt(line.substring(36,38));
                //float temp = Float.parseFloat(line.substring(38,40).trim());
                //context.write(new Text(year),new Text(String.valueOf(temp)));
                context.write(new Text(year),new IntWritable(temperature));  
            }
        }
 
    }
 
// Reducer
      
    public static class MaxTemperatureReducer extends
            Reducer<Text, IntWritable,Text, IntWritable> {
   
        public void reduce(Text Key, Iterable<IntWritable> Values, Context context)
                throws IOException, InterruptedException {
 
        	int max_temp = 0; 
            int count = 0;
            for (IntWritable value : Values)
                        {
                                    max_temp += value.get();     
                                    count+=1;
                        }
            context.write(Key, new IntWritable(max_temp/count));
            
        }
 
    }
 

    public static void main(String[] args) throws Exception {
 
        // reads the default configuration of the
        // cluster from the configuration XML files
        Configuration conf = new Configuration();
         
        // Initializing the job with the
        // default configuration of the cluster    
        Job job = new Job(conf, "weather example");
         
        // Assigning the driver class name
        job.setJarByClass(MyMaxMin.class);
 
        // Key type coming out of mapper
        job.setMapOutputKeyClass(Text.class);
         
        // value type coming out of mapper
        job.setMapOutputValueClass(IntWritable.class);
 
        // Defining the mapper class name
        job.setMapperClass(MaxTemperatureMapper.class);
         
        // Defining the reducer class name
        job.setReducerClass(MaxTemperatureReducer.class);
 
        // Defining input Format class which is
        // responsible to parse the dataset
        // into a key value pair
        job.setInputFormatClass(TextInputFormat.class);
         
        // Defining output Format class which is
        // responsible to parse the dataset
        // into a key value pair
        job.setOutputFormatClass(TextOutputFormat.class);
 
        // setting the second argument
        // as a path in a path variable
        Path OutputPath = new Path(args[1]);
 
        // Configuring the input path
        // from the filesystem into the job
        FileInputFormat.addInputPath(job, new Path(args[0]));
 
        // Configuring the output path from
        // the filesystem into the job
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
 
        // deleting the context path automatically
        // from hdfs so that we don't have
        // to delete it explicitly
        OutputPath.getFileSystem(conf).delete(OutputPath);
 
        // exiting the job only if the
        // flag value becomes false
        System.exit(job.waitForCompletion(true) ? 0 : 1);
 
    }
}
