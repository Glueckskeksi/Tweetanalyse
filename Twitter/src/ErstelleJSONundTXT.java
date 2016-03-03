 import java.util.ArrayList;
import java.util.Date;
import java.util.EnumSet;
import java.util.List;
import java.io.*;
import java.text.SimpleDateFormat;

import org.json.simple.JSONObject;

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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Options.CreateOpts;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hdfs.*;
import org.apache.commons.lang3.*;

//import twitter4j.*;
import twitter4j.Paging;
import twitter4j.Status;
import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.TwitterFactory;
import twitter4j.User;
import twitter4j.conf.ConfigurationBuilder;

public class ErstelleJSONundTXT {

public static void main(String[] args) throws IOException {
	FileWriter fw = new FileWriter("C:/Users/Sun_2/workspace/Twitterprak.json");
	FileWriter fw2 = new FileWriter("C:/Users/Sun_2/workspace/Twitterprak.txt");
//Writing data to a file

	BufferedWriter bw = new BufferedWriter(fw);
	BufferedWriter bw2 = new BufferedWriter(fw2);
String content; 
Date zeit;
String key; 
int favorit;
int retweet;
String User;

	 Tweets[] tweetliste = Tweetlisteerstellen();
	 //Vorbereitung fürs schreiben in HDFS   
	   
	    for(int a = 0; a<tweetliste.length;a++){
	   //System.out.println("Index" + i + "Länge" + tweetliste.length);

	      content=tweetliste[a].getContent();
	       //System.out.println(content);
	      zeit=tweetliste[a].getZeit();
	      key=tweetliste[a].getID(); 
	      favorit=tweetliste[a].getFavorit();
	      retweet=tweetliste[a].getRetweet();
	      User=tweetliste[a].getUser(); 
	      String neuzeit= "" + zeit + "";
	      
	      JSONObject value = new JSONObject();

	      value.put("user", User);
	      value.put("content", content);
	      value.put("time", neuzeit);
	      value.put("favorit", favorit);
	      value.put("retweet", retweet);
	      value.put("key", key);
	      
bw.write(value.toString());

bw.newLine();

bw2.write(value.toJSONString());
bw2.newLine();


//System.out.println(key + " " + value);
bw.close();
bw2.close();
}
	    
}	    
public static Tweets[] Tweetlisteerstellen(){
    int[] zeitungsid = {2834511, 222929165, 5494392, 114508061};
    //IDs von Zeitungen: Spiegel 2834511; jungeWelt 222929165; focus 5494392, SZ 114508061
    String content; //um all newlines rauszuschmeißen
        Date zeit;
        String idtweet; 
        int favorit;
        int retweet;
        int i =0;
        String name;
        User User;
        Tweets[] Tweetliste = new Tweets[80];
      
      
      //Verbingungsaufbau
      ConfigurationBuilder cb = new ConfigurationBuilder();
      cb.setDebugEnabled(true)
        .setOAuthConsumerKey("pGKzeyDypmLEwo2gK7IpBcTrv")
        .setOAuthConsumerSecret("bboz8AZ1KcwWNL3xHXHjkecrj7F102dCa8snimIpFMR3I4I6Hz")
        .setOAuthAccessToken("179650080-PZlynJ2LOncHdtrzXF1sm1mUQClWPTVhb5E0QgXF")
        .setOAuthAccessTokenSecret("Ue0hoM9hi5jIOabL1b1BHJYADFB4MJZV5OFbasYpnhXaU");
      TwitterFactory tf = new TwitterFactory(cb.build());
      Twitter twitter = tf.getInstance();
      //Verbingungsaufbau ende
      
      try {
        List<Status> statuses = null; //List für die Timeline
        ArrayList<List<Status>> statusesOS = new ArrayList<List<Status>>(); //Arraylist für die Zeitungen
        
        for(int id : zeitungsid){
          Paging paging = new Paging(1, 200); //max 200
          statuses = twitter.getUserTimeline(id); // ,paging returned die timeline
          statusesOS.add(statuses); //added die aktuelle timeline zur array liste der Zeitungen
        }
       int a=0;
            for (List<Status> timeline : statusesOS){
          for (Status status : timeline) {
            Tweets tweet = new Tweets();
            content = status.getText();
            content = content.replace("\n", " ").replace("\r", " ");
            tweet.setContent(content);
            idtweet = String.valueOf(status.getId());
            tweet.setID(idtweet);
            zeit = status.getCreatedAt();
            tweet.setZeit(zeit);
            favorit= status.getFavoriteCount();
            tweet.setFavorit(favorit);
            retweet=status.getRetweetCount();
            tweet.setRetweet(retweet);
            User=status.getUser();
            name=User.getName();
            tweet.setUser(name);
            
            Tweetliste[i] = tweet; i++;
      
         //  System.out.println("Index" + i + "Kontent" + tweet.getContent() + "\n Favoriten " + tweet.getFavorit() + " Retweet " +tweet.getRetweet() + "\n ID " +tweet.getID() + "\n ZEIT " +tweet.getZeit() + "\n User " +tweet.getUser());
                }    
        
            }
        
      } catch (TwitterException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();       }
    return Tweetliste;
    
    }
}
