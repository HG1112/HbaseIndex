package edu.ucr.abhi;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class Posting implements Writable  {

    private Text root;
    private Text hyperlink;
    private Text title;
    private IntWritable count;

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
    public String toString() {
        return this.root.toString() + this.hyperlink.toString() + "|" + this.count.toString();
    }
}
