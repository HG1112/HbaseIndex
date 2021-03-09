package edu.ucr.abhi;
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;


public class IndexerM extends Mapper<Object, Text, Text, Posting>{

    private static final String EDU = ".edu";
    private static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    private Text file = new Text();

    public String fileName(Context context) {
      String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
      fileName = fileName.split(EDU)[0] + EDU;
      return fileName;
    }

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      file.set(fileName(context));
      StringTokenizer itr = new StringTokenizer(value.toString());
      while (itr.hasMoreTokens()) {
        word.set(itr.nextToken());
        context.write(word, new Posting(file, one));
      }
    }
  }