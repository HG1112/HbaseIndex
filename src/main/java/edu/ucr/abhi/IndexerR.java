package edu.ucr.abhi;
import java.io.IOException;
import java.util.HashMap;

import com.google.gson.Gson;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.Text;

public class IndexerR  extends TableReducer<Text, Posting, Text> {

    private static final Gson gson = new Gson();
    private static final Text json = new Text();

    @Override
    protected void reduce(
      Text word, 
      Iterable<Posting> postings,
      Context context
      ) throws IOException, InterruptedException {
        HashMap<String, Integer> map = new HashMap<String, Integer>();
        for (Posting posting: postings) {
          String website = posting.getWebsite().getBytes().toString();
          Integer count = posting.getCount().get();
          if (map.containsKey(website)){
            map.put(website, map.get(website) + count) ;
          }
          else {
            map.put(website, count);
          }
        }
        json.set(gson.toJson(map));
        Put put = new Put(word.getBytes());
        
        put.addColumn("Index".getBytes(), "Json".getBytes(), json.getBytes());
        context.write(word, put);
    }
}