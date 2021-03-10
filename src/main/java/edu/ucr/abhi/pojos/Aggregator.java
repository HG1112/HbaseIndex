package edu.ucr.abhi.pojos;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.hadoop.io.Text;

public class Aggregator {
    
    private Text root;
    private Text hyperlink;
    private Text title;

    public Aggregator() {
        this.root = new Text("");
        this.hyperlink = new Text("");
        this.title = new Text("");
    }

    public Aggregator(Text root, Text hyperlink, Text title) {
        this.root = root;
        this.hyperlink = hyperlink;
        this.title = title;
    }

    public Text getTitle() {
        return title;
    }
    public void setTitle(Text text) {
        this.title = text;
    }
    public Text getHyperlink() {
        return hyperlink;
    }
    public void setHyperlink(Text hyperlink) {
        this.hyperlink = hyperlink;
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
                .toHashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) return true;
        if (!(o instanceof Aggregator)) {
            return false;
        }

        Aggregator aggregator = (Aggregator) o;

        return new EqualsBuilder()
                .append(root, aggregator.root)
                .append(hyperlink, aggregator.hyperlink)
                .append(title, aggregator.title)
                .isEquals();
    }

    @Override
    public String toString() {
        return this.root.toString() + "|" + this.hyperlink.toString() + "|" + this.title.toString();
    }
    
    public static Aggregator create(Posting posting) {
        return new Aggregator(
            posting.getRoot(),
            posting.getHyperlink(),
            posting.getTitle()
        );
    }
}
