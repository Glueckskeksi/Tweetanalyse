import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;


        
public class Wordcount {

    public ArrayList<String> st =new ArrayList<String>();
    public ArrayList<String> hashtagliste =new ArrayList<String>(); 
    
	public static String[] stopwords = {"aber", "alle", "allen", "alles", "als", "also", "andere", "anderem", "anderer", "anderes", "anders", "auch", "auf", "aus", "ausser", "ausserdem", "bei", "beide", "beiden", "beides", "beim", "bereits", "bestehen", "besteht", "bevor", "bin", "bis", "bloss", "brauchen", "braucht", "dabei", "dadurch", "dagegen", "daher", "damit", "danach", "dann", "darf", "darueber", "darum", "darunter", "das", "dass", "davon", "dazu", "dem", "den", "denn", "der", "des", "deshalb", "dessen", "die", "dies", "diese", "diesem", "diesen", "dieser", "dieses", "doch", "dort", "duerfen", "durch", "durfte", "durften", "ebenfalls", "ebenso", "ein", "eine", "einem", "einen", "einer", "eines", "einige", "einiges", "einzig", "entweder", "erst", "erste", "ersten", "etwa", "etwas", "falls", "fast", "ferner", "folgender", "folglich", "fuer", "ganz", "geben", "gegen", "gehabt", "gekonnt", "gemaess", "getan", "gewesen", "gewollt", "geworden", "gibt", "habe", "haben", "haette", "haetten", "hallo", "hat", "hatte", "hatten", "heraus", "herein", "hier", "hin", "hinein", "hinter", "ich", "ihm", "ihn", "ihnen", "ihr", "ihre", "ihrem", "ihren", "ihres", "immer", "indem", "infolge", "innen", "innerhalb", "ins", "inzwischen", "irgend", "irgendwas", "irgendwen", "irgendwer", "irgendwie", "irgendwo", "ist", "jede", "jedem", "jeden", "jeder", "jedes", "jedoch", "jene", "jenem", "jenen", "jener", "jenes", "kann", "kein", "keine", "keinem", "keinen", "keiner", "keines", "koennen", "koennte", "koennten", "konnte", "konnten", "kuenftig", "leer", "machen", "macht", "machte", "machten", "man", "mehr", "mein", "meine", "meinen", "meinem", "meiner", "meist", "meiste", "meisten", "mich", "mit", "moechte", "moechten", "muessen", "muessten", "muss", "musste", "mussten", "nach", "nachdem", "nacher", "naemlich", "neben", "nein", "nicht", "nichts", "noch", "nuetzt", "nur", "nutzt", "obgleich", "obwohl", "oder", "ohne", "per", "pro", "rund", "schon", "sehr", "seid", "sein", "seine", "seinem", "seinen", "seiner", "seit", "seitdem", "seither", "selber", "sich", "sie", "siehe", "sind", "sobald", "solange", "solch", "solche", "solchem", "solchen", "solcher", "solches", "soll", "sollen", "sollte", "sollten", "somit", "sondern", "soweit", "sowie", "spaeter", "stets", "such", "ueber", "ums", "und", "uns", "unser", "unsere", "unserem", "unseren", "viel", "viele", "vollstaendig", "vom", "von", "vor", "vorbei", "vorher", "vorueber", "waehrend", "waere", "waeren", "wann", "war", "waren", "warum", "was", "wegen", "weil", "weiter", "weitere", "weiterem", "weiteren", "weiterer", "weiteres", "weiterhin", "welche", "welchem", "welchen", "welcher", "welches", "wem", "wen", "wenigstens", "wenn", "wenngleich", "wer", "werde", "werden", "weshalb", "wessen", "wie", "wieder", "will", "wir", "wird", "wodurch", "wohin", "wollen", "wollte", "wollten", "worin", "wuerde", "wuerden", "wurde", "wurden", "zufolge", "zum", "zusammen", "zur", "zwar", "zwischen"};
	public static Set<String> stopWordSet = new HashSet<String>(Arrays.asList(stopwords));
	
	public static boolean isStopword(String word) {
		if(word.length() < 3) return true;
		if(word.charAt(0) >= '0' && word.charAt(0) <= '9') return true; //remove numbers, "25th", etc
		if(word.charAt(0)=='h' && word.charAt(1)=='t' && word.charAt(2)=='t' && word.charAt(3)=='p' && word.charAt(4)=='s') return true;
		if(word.charAt(0)=='@') return true;
		if(word.charAt(0)=='+')return true;
		if(word.equals("derspiegel"))return true;
		if(stopWordSet.contains(word)) return true;
		else return false;
	}
	
	public void map(String key, String value, Date start, Date ende, Date keydate) throws IOException, InterruptedException {
		
		if(keydate.after(start) && keydate.before(ende)){
		   String line = value;
           //System.out.println(value);
           String[] tokens = line.split(" ");

          //st.add(key);
   			int tokenCount = tokens.length;
   		  //System.out.println(tokens.length);
   			for (int j = 0; j <tokenCount; j++) {
   	
   			// System.out.println(tokens[j]);
   			 String wort2 = tokens[j];
   			 wort2 = wort2.toLowerCase();
   			 String wort1 =wort2.replaceAll("[.,_:;-_+*~=$§!%&//<>]", "");
   			 try{
   				String wort3 =wort1.replaceAll("ä", "ae");
      			 String wort4 =wort3.replaceAll("ö", "oe");
      			 String wort5 =wort4.replaceAll("ü", "ue");
      			 if(wort5.isEmpty()){} else{
      			 if(wort5.charAt(0)=='#') {this.hashtagliste.add(wort5);}
   				String wort =wort5.replaceAll("#", "");
   				if(wort.isEmpty()){} else{
				boolean ja =isStopword(wort);
        	    if(ja==false) {this.st.add(wort);
        	   // System.out.println(ja + wort);
        	    } } }
        	 //   else {System.out.println(ja + wort);  }
   			 }catch(Exception e){System.out.println("Fehler: " +e);}
        	    
            
            			
           }
           }
		 
       }

           
    public void reduce() throws IOException, InterruptedException	{
   
	   
    	//String breite 2 länge 1
    	ArrayList<Worthaeufigkeit> words=new ArrayList<Worthaeufigkeit>();
    	ArrayList<String> woerter=new ArrayList<String>();
    	ArrayList<Integer> anzahl=new ArrayList<Integer>();
    	ArrayList<Worthaeufigkeit> hashwords=new ArrayList<Worthaeufigkeit>();
    	ArrayList<String> hashwoerter=new ArrayList<String>();
    	ArrayList<Integer> hashanzahl=new ArrayList<Integer>();
    	
    	String wort=new String();
    	int count=0;
    	//System.out.println(st.size());
    	for(int i = 0; i <st.size(); i++) {
    		Worthaeufigkeit hf =new Worthaeufigkeit();
    		wort =this.st.get(i);
    		if(woerter.contains(wort)==false){
    		//	System.out.println(i + " Vergleichswort: " + wort);
        		woerter.add(wort);
        		hf.setwort(wort);
        		for(int a = 0; a <st.size(); a++){ 
        			if(wort.equals(this.st.get(a))==true) {
        			count++;
        		//	System.out.println(a + " Das Selbe Wort: " + this.st.get(a));							
        			}
        			//System.out.println(a + " Ein andres Wort: " + this.st.get(a));	
        		}
        		
        		hf.setcount(count);
        		//anzahl.add(count);
        		words.add(hf);
        		//System.out.println(count);
        		count=0;	
    			
    		}}
    		
    
        	
        	for(int e = 0; e <hashtagliste.size(); e++) {
        		Worthaeufigkeit hff =new Worthaeufigkeit();
        		wort =this.hashtagliste.get(e);
        		if(hashwoerter.contains(wort)==false){
        		//	System.out.println(i + " Vergleichswort: " + wort);
            		hashwoerter.add(wort);
            		hff.setwort(wort);
            		for(int t = 0; t <hashtagliste.size(); t++){ 
            			if(wort.equals(this.hashtagliste.get(t))==true) {
            			count++;
            		//	System.out.println(t + " Das Selbe Wort: " + this.st.get(t));							
            			}
            			//System.out.println(t + " Ein andres Wort: " + this.st.get(t));	
            		}
            		
            		hff.setcount(count);
            		//anzahl.add(count);
            		hashwords.add(hff);
            		//System.out.println(count);
            		count=0;	
        			
        		}	
    		
    	}	
    
        	System.out.println("Häufigste Wörter:");
	words.sort(getComp());
	for(int u = 0; u < words.size(); u++) {
		if(words.get(u).getcount()>3){
	System.out.println(words.get(u).getwort() + " " + words.get(u).getcount());	
		}
	}
	System.out.println("Häufigste Hashtags:");
    	hashwords.sort(getComp());
    	for(int u = 0; u < hashwords.size(); u++) {
    		if(hashwords.get(u).getcount()>3){
    	System.out.println(hashwords.get(u).getwort() + " " + hashwords.get(u).getcount());	
    		}
    	}
    	
    	
    	
    	
    }

    public static Comparator<Worthaeufigkeit> getComp()
    {   
     Comparator comp = new Comparator<Worthaeufigkeit>(){
         @Override
         public int compare(Worthaeufigkeit s1, Worthaeufigkeit s2)
         {
             return s2.getcount()-s1.getcount();
             
         }        
     };
     return comp;
    }         

    public static void starteWordcount(String zeitung, Date start, Date ende) throws IOException, InterruptedException{
    	Wordcount count =new Wordcount();
    	String key;
         String value;
         Date keydate;
         String user;
         JSONReader read =new JSONReader();
     	   ArrayList<Tweets> tw = read.JsonzuTweet("twitterprak4/Twitterprak.json");   
     	   if(zeitung.equals("alle")){
     	   System.out.println("wordcount für alle newsletter");
    	   for(int i=0;i<tw.size();i++){
    	   	   key = tw.get(i).getID();
    	   	   value= tw.get(i).getContent();
    	   	   keydate=tw.get(i).getZeit();
    	   	   user = tw.get(i).getUser();
    	   	   
    	   	   count.map(key, value, start, ende, keydate);
    	   	   } 
    	     count.reduce();
     	   }else if(zeitung.equals("SPIEGEL ONLINE")){
    	     System.out.println("wordcount für SPIEGEL ONLINE");  
    	     for(int i=0;i<tw.size();i++){
    	     	   key = tw.get(i).getID();
    	     	   value= tw.get(i).getContent();
    	     	   keydate=tw.get(i).getZeit();
    	     	   user = tw.get(i).getUser();
    	     	   
    	     	  if(user.equals("SPIEGEL ONLINE")){
    	     	 		count.map(key, value, start, ende, keydate);   
    	     	 	   } 
    	     	   } 
    	       count.reduce();
     	  }else if(zeitung.equals("junge Welt")){
    	       System.out.println("wordcount für junge Welt");  
    	 	  for(int i=0;i<tw.size();i++){
    	 	   	   key = tw.get(i).getID();
    	 	   	   value= tw.get(i).getContent();
    	 	   	   keydate=tw.get(i).getZeit();
    	 	   	   user = tw.get(i).getUser();
    	 	   	   
    	 	   	 if(user.equals("junge Welt")){
    	 	 		count.map(key, value, start, ende, keydate);   
    	 	 	   }
    	 	   	   } 
    	 	     count.reduce();
     	 }else if(zeitung.equals("FOCUS Online")){
    	 	    System.out.println("wordcount für FOCUS Online");
    	 	  for(int i=0;i<tw.size();i++){
    	 	   	   key = tw.get(i).getID();
    	 	   	   value= tw.get(i).getContent();
    	 	   	   keydate=tw.get(i).getZeit();
    	 	   	   user = tw.get(i).getUser();
    	 	   	   
    	 	   	  if(user.equals("FOCUS Online")){
    	 	  		count.map(key, value, start, ende, keydate);   
    	 	  	   }
    	 	   	   } 
    	 	     count.reduce();
     	}else if(zeitung.equals("Süddeutsche Zeitung")){
    	 	    System.out.println("wordcount für Süddeutsche Zeitung");
    	 	  for(int i=0;i<tw.size();i++){
    	 	   	   key = tw.get(i).getID();
    	 	   	   value= tw.get(i).getContent();
    	 	   	   keydate=tw.get(i).getZeit();
    	 	   	   user = tw.get(i).getUser();
    	 	   	   
    	 	   	  if(user.equals("Süddeutsche Zeitung")){
    	 	  		count.map(key, value, start, ende, keydate);   
    	 	  	   }
    	 	   	   } 
    	 	     count.reduce();
     	}
    	   	
    	
    }
    
	public static void main(String[] args) throws Exception {
   	//Was ist das Wort/Thema des Zeitraumes:  

       String ende1= "Thu Mar 03 20:01:01 CET 2016";
       String start1 = "Wed Feb 10 21:21:40 CET 2016";
       SimpleDateFormat sdfToDate = new SimpleDateFormat(
               "EEE MMM dd HH:mm:ss zzz yyyy", Locale.US);
       Date start = sdfToDate.parse(start1);
       Date ende = sdfToDate.parse(ende1);
      
       
       //Starte für [alle, Süddeutsche Zeitung, FOCUS Online, junge Welt, SPIEGEL ONLINE]
       starteWordcount("SPIEGEL ONLINE", start, ende); 
  
   	  
   	   
   	   //  Starte Map Reduce
   	   
   	   
   	
     
     
    }
           
   }