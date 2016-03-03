
import twitter4j.*;

import java.util.Date;

public class Tweets {

	  public Tweets(String user, String content, String iD, Date zeit, int favorit, int retweet) {
		User = user;
		Content = content;
		ID = iD;
		Zeit = zeit;
		Favorit = favorit;
		Retweet = retweet;
	}

	public Tweets(){
	}

	// Anfang Attribute
	  public String User;
	  public String Content;
	  public String ID;
	  public Date Zeit;
	  public int Favorit;
	  public int Retweet;
	  // Ende Attribute
	  
	  // Anfang Methoden
	  public String getUser() {
	    return User;
	  }

	  public void setUser(String User) {
	    this.User = User;
	  }

	  public String getContent() {
	    return Content;
	  }

	  public void setContent(String Content) {
	    this.Content = Content;
	  }

	  public String getID() {
	    return ID;
	  }

	  public void setID(String ID) {
	    this.ID = ID;
	  }

	  public Date getZeit() {
	    return Zeit;
	  }

	  public void setZeit(Date Zeit) {
	    this.Zeit = Zeit;
	  }

	  public int getFavorit() {
	    return Favorit;
	  }

	  public void setFavorit(int Favorit) {
	    this.Favorit = Favorit;
	  }

	  public int getRetweet() {
	    return Retweet;
	  }

	  public void setRetweet(int Retweet) {
	    this.Retweet = Retweet;
	  }

	  // Ende Methoden
	
	
}
