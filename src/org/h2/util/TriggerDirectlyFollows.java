package org.h2.util;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.h2.api.Trigger;

public class TriggerDirectlyFollows implements Trigger {
	
	String tableName;
	String caseIdColumn;
	String eventClassColumn;
	String timeColumn;	

	@Override
	public void init(Connection conn, String schemaName, String triggerName, String tableName, boolean before, int type)
			throws SQLException {
		this.tableName = tableName;
		
		Statement stat = conn.createStatement();
		
		ResultSet rs = stat.executeQuery("show columns from " + tableName);
		for(int i = 0; rs.next(); i++) {
			switch(i) {
				case 0: this.caseIdColumn = rs.getString(1); break;
				case 1: this.eventClassColumn = rs.getString(1); break;
				case 2: this.timeColumn = rs.getString(1); break;
				default: break;
			}
		}
		
	}

	@Override
	public void fire(Connection conn, Object[] oldRow, Object[] newRow) throws SQLException {
		System.out.println("fire " + tableName);
		
		Statement stat = conn.createStatement();
		
		Integer newCase = (Integer) newRow[0];
		String newEC = (String) newRow[1];
		
		String prevTime = null, prevEC = null;
		Integer prevFreq = 0;
		
		// get prevTime
		ResultSet rs = stat.executeQuery("select max(" + timeColumn + ") " + " from " + tableName + " where " + caseIdColumn +" = " + newCase);
		while(rs.next()) {
			prevTime = rs.getString(1);
		}		
		
		if(prevTime == null) { // if it is the first event class of the trace
			// get prevFreq of (START, newEC)
			rs = stat.executeQuery("select frequency from auto_directlyfollows where event_label_p = '" + "START" + "' and event_label_s = '" + newEC + "'");
			prevFreq = rs.next() == true ? rs.getInt(1) : 0;
			
			// update or insert (START, newEC)
			if(prevFreq == 0) {
				stat.execute("insert into auto_directlyfollows values ('" + "START" + "','" + newEC + "',1)");
			} else {
				Integer newFreq = prevFreq + 1;
				stat.execute("update auto_directlyfollows set frequency = " + newFreq + "where event_label_p = '" + "START" +"' and event_label_s = '" + newEC + "'");
			}			
			
		} 
		else { // if it is not the first event class
			// get prevEC
			rs = stat.executeQuery("select " + eventClassColumn + " from " + tableName + " where " + caseIdColumn + " = " + newCase + " and " + timeColumn + " = '" + prevTime + "'");
			prevEC = rs.next() == true ? rs.getString(1) : "";			
			
			// get prevFreq of (prevEC, newEC)
			rs = stat.executeQuery("select frequency from auto_directlyfollows where event_label_p = '" + prevEC + "' and event_label_s = '" + newEC + "'");
			prevFreq = rs.next() == true ? rs.getInt(1) : 0;
			
			// update or insert (prevEC, newEC)
			if(prevFreq == 0) {
				stat.execute("insert into auto_directlyfollows values ('" + prevEC + "','" + newEC + "',1)");
			} else {
				Integer newFreq = prevFreq + 1;
				stat.execute("update auto_directlyfollows set frequency = " + newFreq + "where event_label_p = '" + prevEC +"' and event_label_s = '" + newEC + "'");
			}
			
			// get prevFreq of (prevEC, END)
			rs = stat.executeQuery("select frequency from auto_directlyfollows where event_label_p = '" + prevEC + "' and event_label_s = '" + "END" + "'");
			prevFreq = rs.next() == true ? rs.getInt(1) : 0;
			
			// update or insert (prevEC, END)
			if(prevFreq == 1) {
				stat.execute("delete from auto_directlyfollows where event_label_p ='" + prevEC + "' and event_label_s = '" + "END" + "'");
			} else {
				Integer newFreq = prevFreq - 1;
				stat.execute("update auto_directlyfollows set frequency = " + newFreq + "where event_label_p = '" + prevEC +"' and event_label_s = '" + "END" + "'");
			}
		}
		
		// get prevFreq of (newEC, END)
		rs = stat.executeQuery("select frequency from auto_directlyfollows where event_label_p = '" + newEC + "' and event_label_s = '" + "END" + "'");
		prevFreq = rs.next() == true ? rs.getInt(1) : 0;
		
		// update or insert (newEC, END)
		if(prevFreq == 0) {
			stat.execute("insert into auto_directlyfollows values ('" + newEC + "','" + "END" + "',1)");			
		} else {
			Integer newFreq = prevFreq + 1;
			stat.execute("update auto_directlyfollows set frequency = " + newFreq + "where event_label_p = '" + newEC +"' and event_label_s = '" + "END" + "'");
		}		
		
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
