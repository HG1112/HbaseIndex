package edu.ucr.abhi.search;

import java.util.Comparator;

import org.apache.hadoop.hbase.util.Bytes;

import edu.ucr.abhi.pojos.Posting;

public class PostingComparator implements Comparator<Posting> {

    @Override
    public int compare(Posting p1, Posting p2) {
        int x = p2.getCount().get() - p1.getCount().get();
        if (x == 0) 
            {
                int y = Bytes.toString(p2.getRoot().getBytes()).length() - Bytes.toString(p1.getRoot().getBytes()).length();
                if (y == 0)
                    {
                        int z = Bytes.toString(p2.getHyperlink().getBytes()).length() - Bytes.toString(p1.getHyperlink().getBytes()).length();
                        return z;
                    }
                else 
                    return y;
            }
        else 
            return x;
    }

}