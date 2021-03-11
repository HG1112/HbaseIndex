package edu.ucr.abhi.search;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import edu.ucr.abhi.constants.Search;
import edu.ucr.abhi.pojos.Posting;


public class Rank extends Configured implements Tool{
    @Override
    public int run(String[] args) throws Exception {
        
        String query = args[0].toLowerCase();

        Configuration conf = getConf();
        conf.set(Search.QUERY, query);
        setConf(HBaseConfiguration.create(conf));

        Job job = Job.getInstance(getConf() ,"Searching for " + query);
        job.setJarByClass(Rank.class);
        String table = "search";
        TableMapReduceUtil.initTableMapperJob(
            table,      // input table
            Query.getScan(),             // Scan instance to control CF and attribute selection
            RankM.class,   // mapper class
            Text.class,             // mapper output key
            Posting.class,             // mapper output value
            job
        );
        
        // job.setReducerClass(RankR.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.waitForCompletion(true);
        return job.isSuccessful() ? 0 : -1;
    }
    public static void main(String[] args) throws Exception {
        int result;
        try{
            result= ToolRunner.run(new Configuration(), new Rank(), args);
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
