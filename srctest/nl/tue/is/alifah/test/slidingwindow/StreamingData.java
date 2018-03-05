package nl.tue.is.alifah.test.slidingwindow;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Timer;
import java.util.TimerTask;

import com.mchange.v2.sql.filter.SynchronizedFilterStatement;

import nl.tue.is.alifah.test.slidingwindow.Event;

public class StreamingData extends TimerTask {
	
	static Connection conn;
	static Statement stat;
	static String filename;
	static Integer index = 0;
	static ArrayList<Event> events = new ArrayList<Event>();
	
	public static void main(String[] args) throws IOException, ParseException, ClassNotFoundException, SQLException {
		// create connection
		Class.forName("org.h2.Driver");
		conn = DriverManager.getConnection("jdbc:h2:mem:;DB_CLOSE_ON_EXIT=FALSE", "sa", "");
		stat = conn.createStatement();
				
		// read and sort events
		filename = "bpi";
		events = Data.sort(Data.read("C:\\Users\\asyamsiy\\workspace\\h2processmining-master\\resources\\"+filename+".csv"));	
		
		// create table
		createTable(filename);		
		
		// activate trigger and delete schedule
		stat.executeQuery("SELECT * FROM autofollows('caseid,activity,completetimestamp','"+filename+"','','2000-01-01','2000-01-02',7000)"); // interval is 7 days (= 7 ms)	
		
		// stream the data
		Timer timer = new Timer();
		for(int i = 0; i< events.size(); i++) {
			timer.schedule(new StreamingData(), events.get(i).scaledEpoch);
		}		
	}

	@Override
	public void run() {
		String insertSQL = "insert into " + filename + " values (?,?,?)";
		Event e = events.get(index);
		
		try {
			PreparedStatement ps = conn.prepareStatement(insertSQL);
			ps.setString(1, e.traceID);
			ps.setString(2, e.task);
			java.util.Date date= new java.util.Date();
			ps.setTimestamp(3, new Timestamp(date.getTime())); // set the event time as the current time
			ps.execute();
		} catch (SQLException ex) {
			ex.printStackTrace();
		}
		System.out.println("insert: " + index + " " + e.time);
		index++;
		
		if(index == 28) {
			try {
				BufferedWriter out = new BufferedWriter(new FileWriter("C:\\Users\\asyamsiy\\Documents\\Experiment\\h2\\result\\H2"+filename+".txt", false));
				ResultSet rs = stat.executeQuery("select * from dfr");
				
				while(rs.next()) {
					out.write(rs.getString(1) + "\t" + rs.getString(2) + "\t" + rs.getString(3) + "\n");
				}
				out.close();
				System.out.println("sukses");
			} catch (SQLException  e1) {
				e1.printStackTrace();
			} catch (IOException e1) {
				e1.printStackTrace();
			}
		}
		
	}
	
	public static void createTable(String logName) throws SQLException {
		stat.execute("CREATE TABLE IF NOT EXISTS " + logName + " (CaseID varchar(255), Activity varchar(255), CompleteTimestamp timestamp);");
	}
}
