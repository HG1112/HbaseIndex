package edu.ucr.abhi;
import java.io.IOException;
import java.util.HashMap;

import com.google.gson.Gson;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;

public class IndexerR  extends TableReducer<Text, Posting, ImmutableBytesWritable> {

    private static final Gson gson = new Gson();

    @Override
    protected void reduce(
      Text word, 
      Iterable<Posting> postings,
      Context context
      ) throws IOException, InterruptedException {
        Integer count;
        HashMap<Aggregator, Integer> map = new HashMap<Aggregator, Integer>();
        for (Posting posting: postings) {
          Aggregator agg = Aggregator.create(posting);
          count = posting.getCount().get();
          if (map.containsKey(agg)){
            map.put(agg, map.get(agg) + count) ;
          }
          else {
            map.put(agg, count);
          }
        }

        // TODO : this is only insert , need to handle update case where the key already is put
        ImmutableBytesWritable key = new ImmutableBytesWritable(Bytes.toBytes(word.toString()));
        Put put = new Put(Bytes.toBytes(word.toString()));
        
        for (Aggregator agg : map.keySet()) {
          put.addColumn(Bytes.toBytes(agg.getRoot().toString()), Bytes.toBytes("count"), Bytes.toBytes(Integer.toString(map.get(agg))));
          put.addColumn(Bytes.toBytes(agg.getRoot().toString()), Bytes.toBytes("href"), Bytes.toBytes(agg.getHyperlink().toString()));
          put.addColumn(Bytes.toBytes(agg.getRoot().toString()), Bytes.toBytes("title"), Bytes.toBytes(agg.getTitle().toString()));
        }
        context.write(key, put);
    }
}