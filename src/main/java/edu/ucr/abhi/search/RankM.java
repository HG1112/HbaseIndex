package edu.ucr.abhi.search;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

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
        if (Query.match(rowKey, query)) {
            for (Posting posting : Posting.fromResult(value)) {
                context.write(qKey, posting);
            }
        }
    }
}
