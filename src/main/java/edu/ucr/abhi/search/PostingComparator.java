package edu.ucr.abhi.search;

import java.util.Comparator;

import edu.ucr.abhi.pojos.Posting;

public class PostingComparator implements Comparator<Posting> {

    @Override
    public int compare(Posting p1, Posting p2) {
        return p2.getCount().get() - p1.getCount().get();
    }

}