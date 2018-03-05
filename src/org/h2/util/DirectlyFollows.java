package org.h2.util;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.h2.tools.SimpleResultSet;

public class DirectlyFollows {	
	
	public static ResultSet directlyFollows(Connection conn, ResultSet eventLog) throws SQLException {
		
		System.out.println("enter directly follows " + System.nanoTime());
		
		class LogEvent implements Comparable<LogEvent> {
			long time;
			String label;
			
			LogEvent(long time, String label){
				this.time = time;
				this.label = label;
			}
			
			@Override
			public int compareTo(LogEvent e2) {
				if (time - e2.time < 0){
					return -1;
				}else{
					if (time - e2.time > 0){
						return 1;
					}else{
						System.out.println("2 events sama");
						return 0;
					}
				}
			}
		}
		
		//If it is the first or second call of the query, or there is no data in the eventLog, return an empty result
		String url = conn.getMetaData().getURL();
		if (url.equals("jdbc:columnlist:connection") || (eventLog == null)  || (eventLog.getMetaData().getColumnCount() != 3)){
			SimpleResultSet result = new SimpleResultSet();
			result.addColumn("EVENT_LABEL_P", Types.VARCHAR, 255, 0);
			result.addColumn("EVENT_LABEL_S", Types.VARCHAR, 255, 0);
			result.addColumn("FREQUENCY", Types.INTEGER, 0, 0);
			return result;
		}

		//Map of case identifiers to sequences for that case. Each sequence is a list of log events.
		Map<Object,List<LogEvent>> caseId2Sequence = new HashMap<Object,List<LogEvent>>();
		//Map for directly follows relations
		Map<String, Integer> directlyFollows = new HashMap<String, Integer>();
		//Map for the last event in a trace
		//Map<Object, String> lastEvents = new HashMap<Object, String>();
		
		//For each event in the event log:
		while (eventLog.next()){
			String caseId = null, eventClass = null;
			Timestamp timestamp = null;
			//STILL BUG
			/*if(eventLog.getMetaData().getColumnCount() == 1) {
				String tuple = eventLog.getString(1);
				System.out.println("tuple " + tuple);
				String nontuple = tuple.substring(1, tuple.length() - 1);
				String[] arr = nontuple.split(", ");
				caseId = arr[0];
				eventClass = arr[1];
				timestamp = Timestamp.valueOf(arr[2]);			
				System.out.println(caseId + " " + eventClass);
			} else {*/
				caseId = eventLog.getString(1);		
				eventClass = eventLog.getString(2);
				timestamp = eventLog.getTimestamp(3);
			//}
			
			List<LogEvent> sequence = caseId2Sequence.get(caseId);
			if (sequence == null){
				sequence = new ArrayList<LogEvent>();
				caseId2Sequence.put(caseId, sequence);
			}
			//Add the event to the sequence that belongs to the case identifier of that event. 
			sequence.add(new LogEvent(timestamp.getTime(), eventClass));
			//System.out.println("timestamp: " + timestamp + " getTime: " + timestamp.getTime());
		}
		
		/*//For each sequence that is constructed in this manner:
		for (List<LogEvent> sequence: caseId2Sequence.values()){
			//Sort the sequence.
			Collections.sort(sequence);		*/
			
		for (Object caseId : caseId2Sequence.keySet()) {
			
			List<LogEvent> sequence = caseId2Sequence.get(caseId);
			Collections.sort(sequence);		
			//String lastEvent = null;
			
			for(int i = 0; i < sequence.size(); i++) {
				// start event class
				if(i == 0) {
					String ec1 = "START";
					String ec2 = (String) sequence.get(i).label;						
					
					String key = ec1 + "---" + ec2;
					
					Integer val = directlyFollows.get(key);
					Integer temp = (val == null) ? directlyFollows.put(key, 1) : directlyFollows.put(key, val + 1);
					
					//lastEvent = ec2;
					
				}
				// end event class
				if(i == sequence.size() - 1) {
					String ec1 = (String) sequence.get(i).label;
					String ec2 = "END";
					
					String key = ec1 + "---" + ec2;
					
					Integer val = directlyFollows.get(key);
					Integer temp = (val == null) ? directlyFollows.put(key, 1) : directlyFollows.put(key, val + 1);		
					
				} else { 
					
				// pair
				//if(i != sequence.size() - 1) {
					String ec1 = (String) sequence.get(i).label;
					String ec2 = sequence.get(i+1).label;
					
					String key = ec1 + "---" + ec2;
					
					Integer val = directlyFollows.get(key);
					Integer temp = (val == null) ? directlyFollows.put(key, 1) : directlyFollows.put(key, val + 1);		
					
					//lastEvent = ec2;
				}				
			}
			
			//lastEvents.put(caseId, lastEvent);
			
		}
		
		
		//Create the result.
		SimpleResultSet result = new SimpleResultSet();
		result.addColumn("EVENT_LABEL_P", Types.VARCHAR, 255, 0);
		result.addColumn("EVENT_LABEL_S", Types.VARCHAR, 255, 0);
		result.addColumn("FREQUENCY", Types.INTEGER, 0, 0);
			
		for (String key: directlyFollows.keySet()){
			String[] arr = key.split("---");
			String ec1 = arr[0];
			String ec2 = arr[1];
			Integer freq = directlyFollows.get(key);
			
			result.addRow(ec1,ec2,freq);
		}
		
	    eventLog.getStatement().close();
	    
	    /*// create table last_event
	    Statement stat = conn.createStatement();
	    stat.execute("drop table if exists last_event");
	    stat.execute("create table last_event(caseid varchar(255), event_label varchar(255));");
		
	    String insertquery = "";
	    for (Object caseId : lastEvents.keySet()) {
	    	insertquery = insertquery +  "('" + caseId + "','" + lastEvents.get(caseId) + "'),";
	    }
	    insertquery = insertquery.substring(0, insertquery.length()-1);
	    	    
	    stat.execute("insert into last_event values " + insertquery);*/
	    
	    System.out.println("end directly follows");
		
		return result;
	}
}
