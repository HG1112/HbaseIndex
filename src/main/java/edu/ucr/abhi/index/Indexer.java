package edu.ucr.abhi.index;

import edu.ucr.abhi.pojos.Posting;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class Indexer extends Configured implements Tool{
  @Override
  public int run(String[] args) throws Exception {
    Configuration conf = HBaseConfiguration.create();
    Job job = Job.getInstance(conf, "Let's Index Ppl");
    job.setJarByClass(Indexer.class);
    job.setInputFormatClass(TextInputFormat.class);
    job.setMapperClass(IndexerM.class);
    job.setReducerClass(IndexerR.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Posting.class);
    
    String outputTable = "search";
    TableMapReduceUtil.initTableReducerJob(
    outputTable,
    IndexerR.class,
    job
    );
    
    FileInputFormat.addInputPath(job, new Path(args[0]));
    job.waitForCompletion(true);
    return job.isSuccessful() ? 0 : -1;
  }
  public static void main(String[] args) throws Exception {
    int result;
    try{
      result= ToolRunner.run(new Configuration(), new Indexer(), args);
      if(0 == result)
      {
        System.out.println("Job completed Successfully...");
      }
      else
      {
        System.out.println("Job Failed...");
      }
    }
    catch(Exception exception)
    {
      exception.printStackTrace();
    }
    
  }
  
  
}
