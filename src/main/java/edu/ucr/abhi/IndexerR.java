package edu.ucr.abhi;
import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;

import org.apache.hadoop.io.Text;

public class IndexerR  extends TableReducer<Text, Posting, ImmutableBytesWritable> {

    private static final String COUNT = "count";

    @Override
    protected void reduce(
      Text word, 
      Iterable<Posting> postings,
      Context context
      ) throws IOException, InterruptedException {
        HashMap<String, Integer> map = new HashMap<String, Integer>();
        for (Posting posting: postings) {
          String website = posting.getWebsite().toString();
          Integer count = posting.getCount().get();
          if (map.containsKey(website)){
            map.put(website, map.get(website) + count) ;
          }
          else {
            map.put(website, count);
          }
        }
        // TODO : this is only insert , need to handle update case where the key already is present
        ImmutableBytesWritable key = new ImmutableBytesWritable(Bytes.toBytes(word.toString()));
        Put put = new Put(Bytes.toBytes(word.toString()));
        
        for (String website: map.keySet()) {
          put.addColumn(Bytes.toBytes(website), Bytes.toBytes(COUNT), Bytes.toBytes(map.get(website).toString()));
        }
        context.write(key, put);
    }
}