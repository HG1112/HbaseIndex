package edu.ucr.abhi;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class Posting implements Writable  {

    private Text website;
    private IntWritable count;

    public Posting() {
        website = new Text();
        count = new IntWritable(0);
    }

    public Posting(Text txt, IntWritable writable) {
        website = txt;
        count = writable;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        website.write(out);
        count.write(out);        
    }

    public IntWritable getCount() {
        return count;
    }

    public void setCount(IntWritable count) {
        this.count = count;
    }

    public Text getWebsite() {
        return website;
    }

    public void setWebsite(Text website) {
        this.website = website;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        website.readFields(in);
        count.readFields(in);
    }
    
    @Override
    public String toString() {
        return website + "|" + count;
    }
}
