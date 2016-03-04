import java.util.ArrayList;
import java.util.Date;
import java.util.EnumSet;
import java.util.List;
import java.io.*;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
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
    	  TweetUpdatetoHDFS client = new TweetUpdatetoHDFS();
    	  client.addFile("C:/Users/Sun_2/workspace/Twitterprak.json", "twitterprak4", obj);
    	  client.addFile("C:/Users/Sun_2/workspace/Twitterprak.json", "twitterprak3", obj);
    	  client.addFile("C:/Users/Sun_2/workspace/Twitterprak.json", "twitterprak2", obj);
    	  client.addFile("C:/Users/Sun_2/workspace/Twitterprak.txt", "twitterprak", obj);
    	  client.addFile("C:/Users/Sun_2/workspace/Twitter/src/wordcount.txt", "wordcount", obj);
    	  //  client.readFile("twitterprak/Twitterprak.txt", obj);
    	  //client.schreibeContent("twitterprak/wordcount.txt");
    	  
    	  FileSystem hdfs = FileSystem.get(obj);
    	  String content; 
    	      Date zeit;
    	      String key; 
    	      int favorit;
    	      int retweet;
    	      String User;
    	 Path path = new Path("twitterprak/Twitterprak.txt");
     	 Path path2 = new Path("twitterprak2/Twitterprak.json");   	     
     	 Path path3 = new Path("wordcount/wordcount.txt"); 
     	 Path path4 = new Path("twitterprak4/Twitterprak.json"); 
    	     for(int e=1;e<45;e++){
     	     ArrayList<Tweets> tweetliste = client.Tweetlisteerstellen(e);

     	     
    	BufferedWriter br=new BufferedWriter(new OutputStreamWriter(hdfs.append(path)));
    	BufferedWriter br2=new BufferedWriter(new OutputStreamWriter(hdfs.append(path2)));
        BufferedWriter br3=new BufferedWriter(new OutputStreamWriter(hdfs.append(path3)));
        BufferedWriter br4=new BufferedWriter(new OutputStreamWriter(hdfs.append(path4)));
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
              br3.write(content);
             		 br3.newLine();
             		br4.write(value.toJSONString());
             		br4.newLine();
                   }
            
              br.close();
              br2.close();
              br3.close();
              br4.close();
    	     }

        hdfs.close();
    }

	  /**
	   * create a existing file from local filesystem to hdfs
	   * @param source
	   * @param dest
	   * @param conf
	   * @throws IOException
	   */
	  public void addFile(String source, String dest, Configuration conf) throws IOException {

	    FileSystem fileSystem = FileSystem.get(conf);

	    // Get the filename out of the file path
	    String filename = source.substring(source.lastIndexOf('/') + 1,source.length());

	    // Create the destination path including the filename.
	    if (dest.charAt(dest.length() - 1) != '/') {
	      dest = dest + "/" + filename;
	    } else {
	      dest = dest + filename;
	    }

	    // System.out.println("Adding file to " + destination);

	    // Check if the file already exists
	    Path path = new Path(dest);
	    if (fileSystem.exists(path)) {
	      System.out.println("File " + dest + " already exists");
	      return;
	    }

	    // Create a new file and write data to it.
	    
	    FSDataOutputStream out = fileSystem.create(path);
	    InputStream in = new BufferedInputStream(new FileInputStream(new File(
	        source)));

	    byte[] b = new byte[1024];
	    int numBytes = 0;
	    while ((numBytes = in.read(b)) > 0) {
	      out.write(b, 0, numBytes);
	    }

	    // Close all the file descriptors
	    in.close();
	    out.close();
	    fileSystem.close();
	  }

	  
	  public ArrayList<Tweets> SchreibeContent (String file) throws FileNotFoundException {
		    Configuration conf =new Configuration();
		    conf.addResource(new Path("C:/cygwin64/usr/local/hadoop-common/hadoop-dist/target/hadoop-2.5.0-SNAPSHOT/etc/hadoop/core-site.xml"));
		    conf.addResource(new Path("C:/cygwin64/usr/local/hadoop-common/hadoop-dist/target/hadoop-2.5.0-SNAPSHOT/etc/hadoop/hdfs-site.xml"));
		    conf.addResource(new Path("C:/cygwin64/usr/local/hadoop-common/hadoop-dist/target/hadoop-2.5.0-SNAPSHOT/etc/hadoop/mapred-site.xml"));
		    conf.set("fs.default.name", "localhost:9000");
			JSONParser parser = new JSONParser();
			ArrayList<Tweets> tweetliste =new ArrayList<Tweets>();
			
			Path path = new Path(file);
			JSONObject obj;
			FileOutputStream schreibeStrom = 
	                new FileOutputStream("twitterprak.txt");
			try {
				FileSystem fileSystem = FileSystem.get(conf);
				 BufferedReader br=new BufferedReader(new InputStreamReader(fileSystem.open(path)));
		            String line;
		            while ((line = br.readLine()) != null) {
		            	
		            obj = (JSONObject) parser.parse(line);
		           
		            String content =(String) obj.get("content");
		            
		            for (int i=0; i < content.length(); i++){
		            	schreibeStrom.write((byte)content.charAt(i));
		            }
		            
		            //System.out.println(content);
	        
		            }

			}catch(Exception e){System.out.println("hier" + e);}
			
			return tweetliste;
			}
	  /**
	   * read a file from hdfs
	   * @param file
	   * @param conf
	   * @throws IOException
	   */
	  public void readFile(String file, Configuration conf) throws IOException {
	    FileSystem fileSystem = FileSystem.get(conf);

	    Path path = new Path(file);
	    if (!fileSystem.exists(path)) {
	      System.out.println("File " + file + " does not exists" + path);
	      return;
	    }

	    FSDataInputStream in = fileSystem.open(path);

	    String filename = file.substring(file.lastIndexOf('/') + 1,
	        file.length());

	    OutputStream out = new BufferedOutputStream(new FileOutputStream(
	        new File(filename)));

	    byte[] b = new byte[1024];
	    int numBytes = 0;
	    while ((numBytes = in.read(b)) > 0) {
	      out.write(b, 0, numBytes);
	    }

	    in.close();
	    out.close();
	    fileSystem.close();
	  }
	
    public ArrayList<Tweets> Tweetlisteerstellen(int o) throws TwitterException{
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