package org.h2.util;

import static org.quartz.JobBuilder.newJob;
import static org.quartz.TriggerBuilder.newTrigger;
import static org.quartz.SimpleScheduleBuilder.*;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

import org.quartz.JobDataMap;
import org.quartz.JobDetail;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.SchedulerFactory;
import org.quartz.Trigger;
import org.quartz.impl.StdSchedulerFactory;

public class DeleteScheduler {
	Connection conn;
	
	public DeleteScheduler(Connection conn) {
		this.conn = conn;
	}
	
	public void run() {
		SchedulerFactory sf = new StdSchedulerFactory();
	    Scheduler sched;
		try {
			sched = sf.getScheduler();
	    
		    JobDataMap jdm = new JobDataMap();
		    Map<String,Connection> map = new HashMap<String,Connection>();
		    map.put("connection", conn);
		    jdm.putAll(map);
		    	        
		    JobDetail job = newJob(DeleteJob.class)
		    							.withIdentity("deleteJob", "group1")
		    							.usingJobData(jdm)
		    							.build();
		    Trigger trigger = newTrigger().withIdentity("deleteTrigger", "group1")
		    								.startNow()
		    								.withSchedule(simpleSchedule()
		    										.withIntervalInSeconds(5)
		    										.repeatForever())          
		    								.build();
		    
		    sched.scheduleJob(job, trigger);
		    
		    sched.start();
		} catch (SchedulerException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	    			    
	    /*try {
			conn.wait();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}*/

		
	}
	
	/*public static void main(String[] args) throws Exception {
		
		Class.forName("org.h2.Driver");
		Connection conne = DriverManager.getConnection("jdbc:h2:mem:", "sa", "");

		DeleteScheduler example = new DeleteScheduler(conne, 3);
	    example.run();

	  }*/
}
