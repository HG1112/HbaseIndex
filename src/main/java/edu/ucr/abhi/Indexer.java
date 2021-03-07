package edu.ucr.abhi;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;


public class Indexer {
    public static void main(String[] args) throws Exception {
        Configuration conf = HBaseConfiguration.create();
        Job job = Job.getInstance(conf, "Let's Index Ppl");
        job.setJarByClass(Indexer.class);
        job.setMapperClass(IndexerM.class);

        String outputTable = "search";
        TableMapReduceUtil.initTableReducerJob(
          outputTable,
          IndexerR.class,
          job
        );

        FileInputFormat.addInputPath(job, new Path(args[0]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
      }
}
