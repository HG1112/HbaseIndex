package edu.ucr.abhi.search;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.google.gson.Gson;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import edu.ucr.abhi.pojos.Posting;

public class RankR extends Reducer<Text, Posting, Text, Text>{
    
    public static final Log log = LogFactory.getLog(RankR.class);
    private static final Gson gson = new Gson();
    private static final Text result = new Text();

    @Override
    protected void reduce(Text key, Iterable<Posting> value, Context context)
    throws IOException, InterruptedException {
        List<Posting> list = new ArrayList<Posting>();
        for (Posting posting: value) {
            list.add(posting);
        }
        list.sort(new PostingComparator());
        result.set(gson.toJson(list));
        context.write(key, result);
    }
}
