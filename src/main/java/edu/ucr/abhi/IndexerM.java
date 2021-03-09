package edu.ucr.abhi;
import java.io.IOException;
import java.net.URL;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;


public class IndexerM extends Mapper<Object, Text, Text, Posting>{

    private static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    private Text root = new Text();
    private Text hyperlink = new Text();
    private Text title = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      Document html = Jsoup.parse(value.toString());

      Elements url = html.select("url");
      if (!url.isEmpty()) root.set(new URL(url.text()).getHost());

      title.set(html.title());

      for (Element elem : html.getAllElements()) {

        if (elem.hasAttr("href")) 
          hyperlink.set(elem.attr("href"));

        StringTokenizer itr = new StringTokenizer(elem.ownText());

        while (itr.hasMoreTokens()) {
          word.set(itr.nextToken());
          context.write(word, new Posting(root, hyperlink, title, one));
        }
      }
      
    }
  }