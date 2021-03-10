package edu.ucr.abhi.pojos;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class Posting implements Writable {
    
    private Text root;
    private Text hyperlink;
    private Text title;
    private IntWritable count;
    
    public Posting() {
        root = new Text("");
        hyperlink = new Text("");
        title = new Text("");
        count = new IntWritable(0);
    }
    public Posting(Text r, Text h, Text t, IntWritable c) {
        root = r;
        hyperlink = h;
        title = t;
        count = c;
    }
    
    public Posting(byte[] r, byte[] h, byte[] t, byte[] c) {
        root = new Text(Bytes.toString(r));
        hyperlink = new Text(Bytes.toString(h));
        title = new Text(Bytes.toString(t));
        count = new IntWritable(Integer.parseInt(Bytes.toString(c)));
    }
    
    @Override
    public void readFields(DataInput in) throws IOException {
        root.readFields(in);
        hyperlink.readFields(in);
        title.readFields(in);
        count.readFields(in);
    }
    
    @Override
    public void write(DataOutput out) throws IOException {
        root.write(out);
        hyperlink.write(out);
        title.write(out);
        count.write(out);        
    }
    
    public Text getTitle() {
        return title;
    }
    
    public void setTitle(Text title) {
        this.title = title;
    }
    
    public Text getHyperlink() {
        return hyperlink;
    }
    
    public void setHyperlink(Text hyperlink) {
        this.hyperlink = hyperlink;
    }
    
    public IntWritable getCount() {
        return count;
    }
    
    public void setCount(IntWritable count) {
        this.count = count;
    }
    
    public Text getRoot() {
        return root;
    }
    
    public void setRoot(Text root) {
        this.root = root;
    }
    
    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
        .append(root)
        .append(hyperlink)
        .append(title)
        .append(count)
        .toHashCode();
    }
    
    @Override
    public boolean equals(Object o) {
        if (o == this) return true;
        if (!(o instanceof Posting)) {
            return false;
        }
        
        Posting posting = (Posting) o;
        
        return new EqualsBuilder()
        .append(root, posting.root)
        .append(hyperlink, posting.hyperlink)
        .append(title, posting.title)
        .append(count , posting.count)
        .isEquals();
    }
    
    @Override
    public String toString() {
        return this.root.toString() + "|" + this.hyperlink.toString() + "|" + this.title.toString() + "|" + this.count.toString();
    }
    
    public static List<Posting> fromResult(Result result) {
        List<Posting> postings = new ArrayList<Posting>();
        for (byte[] cf : result.getMap().keySet()) {
            postings.add(
                new Posting(
                    cf,
                    result.getValue(cf, Bytes.toBytes("href")),
                    result.getValue(cf, Bytes.toBytes("title")),
                    result.getValue(cf, Bytes.toBytes("count"))
                )
            );
        }
        return postings;
    }
}
