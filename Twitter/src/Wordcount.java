import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;


        
public class Wordcount {

    public ArrayList<String> st =new ArrayList<String>();
    
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
   			 String wort = tokens[j];
   			 try{

        	    this.st.add(wort);
   			 }catch(Exception e){System.out.println("Fehler: " +e);}
        	    
            
            			
           }
           }
		 
       }

           
    public void reduce() throws IOException, InterruptedException	{
   
	   
    	//String breite 2 länge 1
    	ArrayList<Worthaeufigkeit> words=new ArrayList<Worthaeufigkeit>();
    	ArrayList<String> woerter=new ArrayList<String>();
    	ArrayList<Integer> anzahl=new ArrayList<Integer>();
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
    			
    		}
    		
    		
    	}	

    	words.sort(getComp());
    	for(int i = 0; i < words.size(); i++) {
    		if(words.get(i).getcount()>1){
    	System.out.println(words.get(i).getwort() + " " + words.get(i).getcount());	
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

	public static void main(String[] args) throws Exception {
   	//Was ist das Wort/Thema des Zeitraumes:  
Wordcount count =new Wordcount();
       String ende1= "Sat Feb 20 21:21:40 CET 2016";
       String start1 = "Wed Feb 10 21:21:40 CET 2016";
       SimpleDateFormat sdfToDate = new SimpleDateFormat(
               "EEE MMM dd HH:mm:ss zzz yyyy", Locale.US);
       Date start = sdfToDate.parse(start1);
       Date ende = sdfToDate.parse(ende1);
       String key;
       String value;
       Date keydate;
       JSONReader read =new JSONReader();
   	   ArrayList<Tweets> tw = read.JsonzuTweet("twitterprak/Twitterprak.txt");         
   	 //  Starte Map Reduce
   for(int i=0;i<tw.size();i++){
   	   key = tw.get(i).getID();
   	   value= tw.get(i).getContent();
   	   keydate=tw.get(i).getZeit();
   	   count.map(key, value, start, ende, keydate);
   	   } 
     count.reduce();
    }
           
   }