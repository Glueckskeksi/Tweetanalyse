	import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
	import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.Locale;

import org.json.simple.JSONArray;
	import org.json.simple.JSONObject;
	import org.json.simple.parser.JSONParser;
	import org.json.simple.parser.ParseException;
	import org.apache.hadoop.conf.*;
	import org.apache.hadoop.conf.Configuration;
	import org.apache.hadoop.io.*;
	import org.apache.hadoop.io.IOUtils;
	import org.apache.hadoop.io.IntWritable;
	import org.apache.hadoop.io.SequenceFile;
	import org.apache.hadoop.io.SequenceFile.CompressionType;
	import org.apache.hadoop.io.SequenceFile.Metadata;
	import org.apache.hadoop.io.SequenceFile.Writer;
	import org.apache.hadoop.io.compress.CompressionCodec;
	import org.apache.hadoop.io.compress.DefaultCodec;
	import org.apache.hadoop.io.Text;
	import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.fs.Options.CreateOpts;
	import org.apache.hadoop.fs.*;
	import org.apache.hadoop.hdfs.*;
	import org.apache.commons.lang3.*;
	
public class JSONReader {

public ArrayList<Tweets> JsonzuTweet (String file) {
	    Configuration conf =new Configuration();
	    conf.addResource(new Path("C:/cygwin64/usr/local/hadoop-common/hadoop-dist/target/hadoop-2.5.0-SNAPSHOT/etc/hadoop/core-site.xml"));
	    conf.addResource(new Path("C:/cygwin64/usr/local/hadoop-common/hadoop-dist/target/hadoop-2.5.0-SNAPSHOT/etc/hadoop/hdfs-site.xml"));
	    conf.addResource(new Path("C:/cygwin64/usr/local/hadoop-common/hadoop-dist/target/hadoop-2.5.0-SNAPSHOT/etc/hadoop/mapred-site.xml"));
	    conf.set("fs.default.name", "localhost:9000");
		JSONParser parser = new JSONParser();
		ArrayList<Tweets> tweetliste =new ArrayList<Tweets>();
		
		Path path = new Path(file);
		JSONObject obj;

		try {
			FileSystem fileSystem = FileSystem.get(conf);
			 BufferedReader br=new BufferedReader(new InputStreamReader(fileSystem.open(path)));
	            String line;
	            while ((line = br.readLine()) != null) {
	            //	System.out.println(line);
	            obj = (JSONObject) parser.parse(line);
	            String user =(String) obj.get("user");
	            String content =(String) obj.get("content");
	            String key =(String) obj.get("key");
	            String zeit =(String) obj.get("time");
	            SimpleDateFormat sdfToDate = new SimpleDateFormat(
		                    "EEE MMM dd HH:mm:ss zzz yyyy", Locale.US);
		            Date time = sdfToDate.parse(zeit);
		          //  System.out.println(time);
	            int favorit=((Long) obj.get("favorit")).intValue();
	            int retweet =((Long) obj.get("retweet")).intValue();
	          //  System.out.println("tweet: " + user + content + key + time + favorit + retweet);
	            Tweets tw= new Tweets(user,content ,key ,time ,favorit , retweet);
	            tweetliste.add(tw);
	            
	            }

		}catch(Exception e){System.out.println("hier" + e);}
		return tweetliste;
		}

public Date NeuesterTweet(ArrayList<Tweets> tweets){
	int i=0;
	int basis=0;
	Date date1=null;
	Date date2=null;
	try{
while(i<tweets.size()){
		
       date1 = tweets.get(basis).getZeit();
        date2 = tweets.get(i).getZeit();
 
        //System.out.println(date1 + "Tweetinhalt: " + tweets.get(i).getContent());
        //System.out.println(date2 + "Tweetinhalt: " + tweets.get(basis).getContent());

        if(date1.after(date2)){
           // System.out.println("Date1 is after Date2");
        }

        if(date1.before(date2)){
           // System.out.println("Date1 is before Date2 -> neues neuestes Datum: " + date2 + "altes Datum:" + date1);
            basis=i;
        }

        if(date1.equals(date2)){
           // System.out.println("Date1 is equal Date2");
        }
        i++;
    }
    }catch(Exception ex){
        ex.printStackTrace();
    }
	return date1;
}
	     
public static void main(String[] args){
	JSONReader read =new JSONReader();
	ArrayList<Tweets> tw = read.JsonzuTweet("twitterprak/Twitterprak.txt");	
 read.NeuesterTweet(tw);
	}
	
}
