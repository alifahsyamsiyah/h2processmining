package nl.tue.is.alifah.test.slidingwindow;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;

import nl.tue.is.alifah.test.slidingwindow.Event;

public class Data {
	private static ArrayList<Event> allEvents = new ArrayList<Event>();
	private static BufferedReader in;
	
	public static ArrayList<Event> read(String path) throws IOException, ParseException {
		in = new BufferedReader(new FileReader(path));
		String line = in.readLine(); // read the header
		int eventIDs = 1;
		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss.SSS");		
		
		while((line = in.readLine()) != null) {
			Event e = new Event(eventIDs);
			String[] arr = line.split(",");
			e.traceID  = arr[0].substring(1, arr[0].length()-1);
			e.task = arr[1].substring(1, arr[1].length()-1); 
			
			// convert time to epoch 
			String time = arr[2].substring(1, arr[2].length()-1);
			Date parsedTime = dateFormat.parse(time);
			Timestamp timestamp = new java.sql.Timestamp(parsedTime.getTime());
			e.time = timestamp;
			long epoch  = timestamp.getTime();							
			long scaledEpoch =  (long) (((double)(epoch - 1451602800000L)/86400000L)*1000L); // set scaling, 1 jan 2016, 1 day = 1 secs
			e.scaledEpoch = scaledEpoch;
			
			allEvents.add(e);
			eventIDs++;
		}
		return allEvents;

	}
	
	public static ArrayList<Event> sort(ArrayList<Event> events) {
		Collections.sort(events, new Comparator<Event>() {
		    public int compare(Event e1, Event e2) {
		        int comp =  Long.compare(e1.scaledEpoch, e2.scaledEpoch);
		        
		        if(comp != 0) {
		        	return comp;
		        } else {
		        	return Integer.compare(e1.eventID, e2.eventID);
		        }
		    }
		});
				
		return allEvents;
	}
}
