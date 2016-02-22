

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Locale;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;


/**
 * Simple Driver to read/write to hdfs
 * @author ashrith
 *
 */
public class AddAndReadFile{
  public AddAndReadFile() {

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

  public static void main(String[] args) throws IOException {


	  AddAndReadFile client = new AddAndReadFile();
    Configuration conf =new Configuration();
    conf.addResource(new Path("C:/cygwin64/usr/local/hadoop-common/hadoop-dist/target/hadoop-2.5.0-SNAPSHOT/etc/hadoop/core-site.xml"));
    conf.addResource(new Path("C:/cygwin64/usr/local/hadoop-common/hadoop-dist/target/hadoop-2.5.0-SNAPSHOT/etc/hadoop/hdfs-site.xml"));
    conf.addResource(new Path("C:/cygwin64/usr/local/hadoop-common/hadoop-dist/target/hadoop-2.5.0-SNAPSHOT/etc/hadoop/mapred-site.xml"));
    conf.set("fs.default.name", "localhost:9000");
    //Addet das File und unten liest das File
  //  client.addFile("C:/Users/Sun_2/workspace/Twitterprak.json", "twitterprak2", conf);
   // client.addFile("C:/Users/Sun_2/workspace/Twitterprak.txt", "twitterprak", conf);
    client.addFile("C:/Users/Sun_2/workspace/Twitter/src/wordcount.txt", "wordcount", conf);
    //  client.readFile("twitterprak/Twitterprak.txt", conf);
    //client.schreibeContent("twitterprak/wordcount.txt");

  }
}
