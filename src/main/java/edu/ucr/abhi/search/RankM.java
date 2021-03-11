package edu.ucr.abhi.search;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;

import edu.ucr.abhi.constants.Search;
import edu.ucr.abhi.pojos.Posting;

public class RankM extends TableMapper<Text, Posting>{
  
    private static Text qKey = new Text() ; 

    @Override
    protected void map(
        ImmutableBytesWritable key, 
        Result value,
        Context context
    ) throws IOException, InterruptedException {
        String query = context.getConfiguration().get(Search.QUERY);
        qKey.set(query);
        String rowKey = Bytes.toString(key.get());
        Set<String> q = new HashSet<String>();
        for (String s: query.split(" ")) q.add(s);
        if (Query.match(rowKey, q)) {
            System.out.println("RowKey -- " + rowKey);
            for (Posting posting : Posting.fromResult(value)) {
                System.out.println("Map : posting -- "+ posting);
                context.write(qKey, posting);
            }
        }
    }
}
