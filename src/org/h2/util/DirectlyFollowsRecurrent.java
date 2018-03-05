package org.h2.util;

import java.sql.Connection;
import java.sql.PreparedStatement;
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

public class DirectlyFollowsRecurrent {	
	
	public void directlyFollowsRecurrent(Connection conn, ResultSet eventLog, long interval) throws SQLException {
		
		System.out.println("enter directly follows recurrent " + System.nanoTime());
		
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
		
		// create table dfr, start_event, last_event, to_delete_event, dfr
		Statement stat = conn.createStatement();
		stat.execute("drop table if exists dfr;");
		stat.execute("create table dfr(event_label_p varchar(255), event_label_s varchar(255), frequency int, primary key(event_label_p, event_label_s));");
		stat.execute("drop table if exists start_event");
		stat.execute("create table start_event(caseid varchar(255), event_label varchar(255), delete_time bigint, primary key(caseid));");
		stat.execute("drop table if exists last_event");
		stat.execute("create table last_event(caseid varchar(255), event_label varchar(255), delete_time bigint, primary key(caseid));");
		stat.execute("drop table if exists to_delete_event");
		stat.execute("create table to_delete_event(caseid varchar(255), event_label_p varchar(255), event_label_s varchar(255), delete_time_p bigint, delete_time_s bigint);");
		
		//If it is the first or second call of the query, or there is no data in the eventLog, return an empty result
		String url = conn.getMetaData().getURL();
		if (url.equals("jdbc:columnlist:connection") || (eventLog.next() == false)  || (eventLog.getMetaData().getColumnCount() != 3)){
			/*SimpleResultSet result = new SimpleResultSet();
			result.addColumn("EVENT_LABEL_P", Types.VARCHAR, 255, 0);
			result.addColumn("EVENT_LABEL_S", Types.VARCHAR, 255, 0);
			result.addColumn("FREQUENCY", Types.INTEGER, 0, 0);
			return result;*/
			return;
		}
		
		//Map of case identifiers to sequences for that case. Each sequence is a list of log events.
		Map<Object,List<LogEvent>> caseId2Sequence = new HashMap<Object,List<LogEvent>>();
		//Map for directly follows relations
		Map<String, Integer> directlyFollows = new HashMap<String, Integer>();
		//Map for the last event in a trace
		Map<Object, LogEvent> lastEvents = new HashMap<Object, LogEvent>();
		//Map for the start event in a trace
		Map<Object, LogEvent> startEvents = new HashMap<Object, LogEvent>();
		
		// put the first event
		String caseID = eventLog.getString(1);		
		String eventClass = eventLog.getString(2);
		Timestamp timestamp = eventLog.getTimestamp(3);
		
		List<LogEvent> seq = new ArrayList<LogEvent>();
		seq.add(new LogEvent(timestamp.getTime(), eventClass));
		caseId2Sequence.put(caseID, seq);		
		
		//For each event in the event log:
		while (eventLog.next()){
			caseID = eventLog.getString(1);		
			eventClass = eventLog.getString(2);
			timestamp = eventLog.getTimestamp(3);
			
			seq = caseId2Sequence.get(caseID);
			if (seq == null){
				seq = new ArrayList<LogEvent>();
				caseId2Sequence.put(caseID, seq);
			}
			//Add the event to the sequence that belongs to the case identifier of that event. 
			seq.add(new LogEvent(timestamp.getTime(), eventClass));
		}
		
		// looping per events
		for (Object caseId : caseId2Sequence.keySet()) {
			
			List<LogEvent> sequence = caseId2Sequence.get(caseId);
			Collections.sort(sequence);		
			
			int i;
			for(i = 0; i < sequence.size(); i++) {
				// start event class
				if(i == 0) {
					String ec = (String) sequence.get(i).label;												
					long time = sequence.get(i).time;
					startEvents.put(caseId, new LogEvent(time, ec));	
				}
					
				// pair
				if(i != sequence.size() - 1) {
					String ec1 = (String) sequence.get(i).label;
					String ec2 = sequence.get(i+1).label;
					String key = ec1 + "---" + ec2;
					
					Integer val = directlyFollows.get(key);
					Integer temp = (val == null) ? directlyFollows.put(key, 1) : directlyFollows.put(key, val + 1);		
					
					long deletetime_p = sequence.get(i).time + interval;
					long deletetime_s = sequence.get(i+1).time + interval;
					stat.execute("insert into to_delete_event values('" + caseId + "','" + ec1 + "','" + ec2 + "'," + deletetime_p + "," + deletetime_s + ");");
				}				
			}
			
			lastEvents.put(caseId, new LogEvent(sequence.get(i-1).time, sequence.get(i-1).label));	
		}		
		
		// insert the result and table dfr
		/*SimpleResultSet result = new SimpleResultSet();
		result.addColumn("EVENT_LABEL_P", Types.VARCHAR, 255, 0);
		result.addColumn("EVENT_LABEL_S", Types.VARCHAR, 255, 0);
		result.addColumn("FREQUENCY", Types.INTEGER, 0, 0);*/
			
		// inserting DFR pairs to table DFR		
		for (String key: directlyFollows.keySet()){
			String[] arr = key.split("---");
			String ec1 = arr[0];
			String ec2 = arr[1];
			Integer freq = directlyFollows.get(key);
			
			//result.addRow(ec1,ec2,freq);
			stat.execute("insert into dfr values('" + ec1 + "','" + ec2 + "'," + freq + ");");
		}
		
		// inserting start events to table start_event	    
	    String insertquery = "";
	    for (Object caseId : startEvents.keySet()) {
	    	long deletetime = startEvents.get(caseId).time + interval;
	    	insertquery = insertquery +  "('" + caseId + "','" + startEvents.get(caseId).label + "'," + deletetime +"),";
	    }
	   
	    if(!insertquery.isEmpty()) {
	    	insertquery = insertquery.substring(0, insertquery.length()-1);	    	    
	    	stat.execute("insert into start_event values " + insertquery);
	    }
	    
	    // inserting last events to table last_event	    
	    insertquery = "";
	    for (Object caseId : lastEvents.keySet()) {
	    	long deletetime = lastEvents.get(caseId).time + interval;
	    	insertquery = insertquery +  "('" + caseId + "','" + lastEvents.get(caseId).label + "'," + deletetime +"),";
	    }
	   
	    if(!insertquery.isEmpty()) {
	    	insertquery = insertquery.substring(0, insertquery.length()-1);	    	    
	    	stat.execute("insert into last_event values " + insertquery);
	    }
	    
	   
	    System.out.println("end directly follows recurrent");		
		//return result;
	}
}
