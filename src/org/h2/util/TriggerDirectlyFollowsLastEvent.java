package org.h2.util;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.h2.api.Trigger;

public class TriggerDirectlyFollowsLastEvent implements Trigger {
	
    private static final Map<String, TriggerDirectlyFollowsLastEvent> TRIGGERS =
            Collections.synchronizedMap(new HashMap<String, TriggerDirectlyFollowsLastEvent>());
	
	private long interval;
	
	private long startTime; 
		
	public void startTimeMeasurement(){
		startTime = System.nanoTime();
	}
	
	public void printTimeTaken(){
		long endTime = System.nanoTime();
		double timeTaken =  (endTime - startTime)/1000000.0; 
		System.out.println(timeTaken);
	}

	@Override
	public void init(Connection conn, String schemaName, String triggerName, String tableName, boolean before, int type)
			throws SQLException {
		TRIGGERS.put(getPrefix(conn) + triggerName, this);
	}
	
	private static String getPrefix(Connection conn) throws SQLException {
        Statement stat = conn.createStatement();
        ResultSet rs = stat.executeQuery(
                "call ifnull(database_path() || '_', '') || database() || '_'");
        rs.next();
        return rs.getString(1);
    }
	
	public static void setInterval(Connection conn, String trigger,
            long interval) throws SQLException {
        TRIGGERS.get(getPrefix(conn) + trigger).interval = interval;
    }

	@Override
	public void fire(Connection conn, Object[] oldRow, Object[] newRow) throws SQLException {
		startTimeMeasurement();
		
		Statement stat = conn.createStatement();
		
		// a new inserted row
		String newCase = (String) newRow[0];
		String newEC = (String) newRow[1];
		Timestamp newTimestamp = (Timestamp) newRow[2];
		long newTime = newTimestamp.getTime();
		
		// get previous event from table last_event
		String prevEC = null; Integer prevFreq; long prevDeleteTime = 0;
		ResultSet rs = stat.executeQuery("select event_label, delete_time from last_event where caseid = '"+ newCase + "'");
		while(rs.next()){
			prevEC = rs.getString(1);
			prevDeleteTime = rs.getLong(2);
		}
		
		if(prevEC == null) { // if it is the first event class of the trace or the middle event whose the first event is already cut
			long newDeleteTime = newTime + interval;
			
			// insert into start_event			
			stat.execute("insert into start_event values('" + newCase + "','" + newEC + "'," + newDeleteTime + ")");		
			
			// insert the last event to newEC 
			stat.execute("insert into last_event values ('" + newCase+ "','" + newEC + "'," + newDeleteTime + ")");					
		} 
		else { // if it is not the first event class	
			
			// get prevFreq of (prevEC, newEC)
			ResultSet rs2 = stat.executeQuery("select frequency from dfr where event_label_p = '" + prevEC + "' and event_label_s = '" + newEC + "'");
			prevFreq = rs2.next() == true ? rs2.getInt(1) : 0;
			
			// update or insert (prevEC, newEC)
			if(prevFreq == 0) {
				stat.execute("insert into dfr values ('" + prevEC + "','" + newEC + "',1)");
			} else {
				Integer newFreq = prevFreq + 1;
				stat.execute("update dfr set frequency = " + newFreq + " where event_label_p = '" + prevEC +"' and event_label_s = '" + newEC + "'");
			}
			
			// add a new instance to to_delete_event			
			long newDeleteTime = newTime + interval;
			stat.execute("insert into to_delete_event values('" + newCase + "','" +  prevEC + "','" + newEC + "'," + prevDeleteTime + "," + newDeleteTime + ");");			
			
			// update the last event to newEC 
			stat.execute("update last_event set event_label = '" + newEC + "', delete_time = " + newDeleteTime + " where caseid = '" + newCase + "'");		
		}
				
		printTimeTaken();
	}	

	@Override
	public void close() throws SQLException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void remove() throws SQLException {
		// TODO Auto-generated method stub
		
	}

}
