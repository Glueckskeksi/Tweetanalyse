import java.util.ArrayList;
import java.util.Date;
import java.util.EnumSet;
import java.util.List;
import java.io.*;
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

public class TweetUpdatetoHDFS{
    public static void main(String[] args) throws Exception {
    	  Configuration obj =new Configuration();
    	  obj.addResource(new Path("C:/cygwin64/usr/local/hadoop-common/hadoop-dist/target/hadoop-2.5.0-SNAPSHOT/etc/hadoop/core-site.xml"));
    	  obj.addResource(new Path("C:/cygwin64/usr/local/hadoop-common/hadoop-dist/target/hadoop-2.5.0-SNAPSHOT/etc/hadoop/hdfs-site.xml"));
    	  obj.addResource(new Path("C:/cygwin64/usr/local/hadoop-common/hadoop-dist/target/hadoop-2.5.0-SNAPSHOT/etc/hadoop/mapred-site.xml"));
    	  obj.set("fs.default.name", "localhost:9000");
    	  FileSystem hdfs = FileSystem.get(obj);
    	  Path path = null; 
    	  String content; 
    	      Date zeit;
    	      String key; 
    	      int favorit;
    	      int retweet;
    	      String User;
    	      path = new Path("twitterprak/Twitterprak.txt");
     	     Path path2 = new Path("twitterprak2/Twitterprak.json");  
     	 // Path path3 = new Path("wordcount/wordcount.txt"); 
    	     for(int e=1;e<20;e++){
     	     ArrayList<Tweets> tweetliste = Tweetlisteerstellen(e);
    	      
     
 
    
    		    BufferedWriter br=new BufferedWriter(new OutputStreamWriter(hdfs.append(path)));
    		    BufferedWriter br2=new BufferedWriter(new OutputStreamWriter(hdfs.append(path2)));
       //   BufferedWriter br3=new BufferedWriter(new OutputStreamWriter(hdfs.append(path3)));
                                          // TO append data to a file, use fs.append(Path f)
              for(int i = 0; i<tweetliste.size();i++){
                  //System.out.println("Index" + i + "Länge" + tweetliste.size());

                     content=tweetliste.get(i).getContent();
                     zeit=tweetliste.get(i).getZeit();
                     key=tweetliste.get(i).getID(); 
                     favorit=tweetliste.get(i).getFavorit();
                     retweet=tweetliste.get(i).getRetweet();
                     User=tweetliste.get(i).getUser(); 
                     String neuzeit= "" + zeit + "";
                     
                     JSONObject value = new JSONObject();
                     value.put("key", key);
                     value.put("user", User);
                     value.put("content", content);
                     value.put("time", neuzeit);
                     value.put("favorit", favorit);
                     value.put("retweet", retweet);
                     //System.out.print("JSON Objekt" + value + "\n");

                   br.write(value.toJSONString());
             	 	 br.newLine();
                 	 	 br2.write(value.toJSONString());
                     br2.newLine();
             //	 br3.write(content);
             //		 br3.newLine();
                   }
            
                 br.close();
            br2.close();
            //  br3.close();
    	     }
              // System.out.println("Fehler :( " + e);
       

        //reading
      /*  
        try{
            BufferedReader br=new BufferedReader(new InputStreamReader(hdfs.open(path)));
            String line;
            line=br.readLine();
            while (line != null){
                    //System.out.println(line);
                    line=br.readLine();
            }
    }catch(Exception e){
    }
       */
        hdfs.close();
    }
    
    public static ArrayList<Tweets> Tweetlisteerstellen(int o) throws TwitterException{
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
	            ArrayList<Tweets> Tweetliste = new ArrayList<Tweets>();
	          JSONReader read= new JSONReader();
	          ArrayList<Tweets> tw = null;
	      	  tw= read.JsonzuTweet("twitterprak/Twitterprak.txt");	
	      	Date vergleich  = read.NeuesterTweet(tw);
	          
	          
	          //Verbingungsaufbau
	          ConfigurationBuilder cb = new ConfigurationBuilder();
	          cb.setDebugEnabled(true)
	            .setOAuthConsumerKey("pGKzeyDypmLEwo2gK7IpBcTrv")
	            .setOAuthConsumerSecret("bboz8AZ1KcwWNL3xHXHjkecrj7F102dCa8snimIpFMR3I4I6Hz")
	            .setOAuthAccessToken("179650080-PZlynJ2LOncHdtrzXF1sm1mUQClWPTVhb5E0QgXF")
	            .setOAuthAccessTokenSecret("Ue0hoM9hi5jIOabL1b1BHJYADFB4MJZV5OFbasYpnhXaU");
	          TwitterFactory tf = new TwitterFactory(cb.build());
	          Twitter twitter = tf.getInstance();
	        
	      
	            List<Status> statuses = null; //List für die Timeline
	            ArrayList<List<Status>> statusesOS = new ArrayList<List<Status>>(); //Arraylist für die Zeitungen
	            
	            for(int id : zeitungsid){
	            	
	              Paging paging = new Paging(o, 40); //max 200
	              statuses = twitter.getUserTimeline(id, paging); // ,paging returned die timeline
	              statusesOS.add(statuses); //added die aktuelle timeline zur array liste der Zeitungen
	            }
	          
	           
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
	               
	              if(vergleich.before(zeit)){
	                Tweetliste.add(tweet);
	               }
	                i++;
	          
	             //System.out.println("Index" + i + "Kontent" + tweet.getContent() + "\n Favoriten " + tweet.getFavorit() + " Retweet " +tweet.getRetweet() + "\n ID " +tweet.getID() + "\n ZEIT " +tweet.getZeit() + "\n User " +tweet.getUser());
	                
	              }    
	            
	                }
	        
	           
	        //  System.out.println("Test" + Tweetliste.toString());
	          return Tweetliste;
	          }
    
    
}