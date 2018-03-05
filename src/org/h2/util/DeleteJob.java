package org.h2.util;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;

import org.quartz.InterruptableJob;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.UnableToInterruptJobException;

public class DeleteJob implements InterruptableJob {
	long startTime;
	 
	public void startTimeMeasurement(){
		startTime = System.nanoTime();
	}
	
	public void printTimeTaken(){
		long endTime = System.nanoTime();
		double timeTaken =  (endTime - startTime)/1000000.0; 
		System.out.println("\t\t" + timeTaken);
	}
	
	@Override
	public void execute(JobExecutionContext context) throws JobExecutionException {
		System.out.println("\n---delete");
		
		startTimeMeasurement();
		
		Timestamp timestamp = new Timestamp(System.currentTimeMillis());
		long currenttime = timestamp.getTime();
		
		JobDataMap dataMap = context.getJobDetail().getJobDataMap();
		
		Connection conn = (Connection) dataMap.get("connection");
		Statement stat = null;
		try {			
			stat = conn.createStatement();
			stat.setFetchSize(100000);
			
			// take all (aggregated) pairs which are going to be deleted
			ResultSet totalDeletedPairs = stat.executeQuery("select event_label_p, event_label_s, count(*) from to_delete_event where delete_time_p <= " + currenttime + " group by event_label_p, event_label_s");
			
			// select frequency of the pair in DFR
			String selectSQL = "select frequency from dfr where event_label_p = ? and event_label_s = ?";
			PreparedStatement psSelect = conn.prepareStatement(selectSQL);
			
			// update DFR with the updated frequency (after deducted)
			String updateSQL = "update dfr set frequency = ? where event_label_p = ? and event_label_s = ?";
			PreparedStatement psUpdate = conn.prepareStatement(updateSQL);
			
			// delete from DFR if the frequency is 0
			String delSQL = "delete from dfr where event_label_p = ? and event_label_s = ?";
			PreparedStatement psDel = conn.prepareStatement(delSQL);
			
			while(totalDeletedPairs.next()) {
				// a pair which is going to be deleted
				String event_p = totalDeletedPairs.getString(1);
				String event_s = totalDeletedPairs.getString(2);
				Integer tobedeleted = totalDeletedPairs.getInt(3);
				
				// take frequency of the pair in DFR, then deduct it
				psSelect.setString(1, event_p);
				psSelect.setString(2, event_s);
				ResultSet rsSelect = psSelect.executeQuery();
				Integer updatedfreq = null;
				while(rsSelect.next()) {
					Integer freq = rsSelect.getInt(1);
					updatedfreq = freq - tobedeleted;
				}				
				
				// update the frequency in DFR
				if(updatedfreq == 0) {
					psDel.setString(1, event_p);
					psDel.setString(2, event_s);
					psDel.execute();
				} else {
					psUpdate.setInt(1, updatedfreq);
					psUpdate.setString(2, event_p);
					psUpdate.setString(3, event_s);
					psUpdate.execute();
				}
			}
			
			// update start_event since some pairs are deleted
			ResultSet maxDelTime = stat.executeQuery("select caseid, max(delete_time_s) from to_delete_event group by caseid");
			
			String updSQL = "update start_event set (event_label, delete_time) = (?, ?) where caseid = ?";
			PreparedStatement psUpd = conn.prepareStatement(updSQL);
			
			String selSQL = "select event_label_s from to_delete_event where caseid = ? and delete_time_s = ?";
			PreparedStatement psSel = conn.prepareStatement(selSQL);
			
			while(maxDelTime.next()) {
				String caseid = maxDelTime.getString(1);
				Long del_time_s = maxDelTime.getLong(2);
				psSel.setString(1, caseid);
				psSel.setLong(2, del_time_s);
				ResultSet newStartEvent = psSel.executeQuery();
				
				newStartEvent.next();
				String event_s = newStartEvent.getString(1);
				
				psUpd.setString(1, event_s);
				psUpd.setLong(2, del_time_s);
				psUpd.setString(3, caseid);
				psUpd.execute();				
			}
			
			// delete the pairs in to_delete_event which are already expired  (already processed above)
			ResultSet deletedPairs = stat.executeQuery("select * from to_delete_event where delete_time_p <= "+ currenttime);
			
			String deleteSQL = "delete from to_delete_event where event_label_p = ? and event_label_s = ? and delete_time_p = ?";
			PreparedStatement psDelete = conn.prepareStatement(deleteSQL);
			
			while(deletedPairs.next()) {
				String event_p = deletedPairs.getString(2);
				String event_s = deletedPairs.getString(3);
				Long time = deletedPairs.getLong(4);
				psDelete.setString(1,event_p);
				psDelete.setString(2, event_s);
				psDelete.setLong(3, time);
				psDelete.execute();
			}
			
			// delete last_event which are already expired
			ResultSet deletedLasts = stat.executeQuery("select caseid, event_label from last_event where delete_time <= " + currenttime);
			
			String delLastSQL = "delete from last_event where caseid = ? and event_label = ?";
			PreparedStatement psDelLast = conn.prepareStatement(delLastSQL);
			
			while(deletedLasts.next()) {
				String caseid = deletedLasts.getString(1);
				String event = deletedLasts.getString(2);
				psDelLast.setString(1, caseid);
				psDelLast.setString(2, event);
				psDelLast.execute();
			}
			
			// delete start_event which are already expired
			ResultSet deletedStarts = stat.executeQuery("select caseid, event_label from start_event where delete_time <= " + currenttime);
			
			String delStartSQL = "delete from start_event where caseid = ? and event_label = ?";
			PreparedStatement psDelStart = conn.prepareStatement(delStartSQL);
			
			while(deletedStarts.next()) {
				String caseid = deletedStarts.getString(1);
				String event = deletedStarts.getString(2);
				psDelStart.setString(1, caseid);
				psDelStart.setString(2, event);
				psDelStart.execute();
			}
			
			
			// check
			/*ResultSet rs = stat.executeQuery("select * from to_delete_event");
			while(rs.next()) {
				System.out.println("to_delete_event now: " + rs.getString(1) + " " + rs.getString(2));
			}
			rs = stat.executeQuery("select * from dfr");
			while(rs.next()) {
				System.out.println("dfr now: " + rs.getString(1) + " " + rs.getString(2) + " " + rs.getInt(3));
			}
			rs = stat.executeQuery("select * from last_event");
			while(rs.next()) {
				System.out.println("last_event now: " + rs.getString(1) + " " + rs.getString(2));
			}*/
		} catch (SQLException e) {			
			e.printStackTrace();
		} 		
		
		printTimeTaken();
	}

	@Override
	public void interrupt() throws UnableToInterruptJobException {
		// TODO Auto-generated method stub
		
	}

}
