package nl.tue.is.alifah.test;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.processmining.mixedparadigm.controller.H2DatabaseDeclareController;

public class DeclareTest {
	
	static Connection conn;
	static Statement stat;
	static ResultSet rs;
	static long startTime;
	static BufferedWriter out;
	
	public static void loadLog(String logName) throws SQLException, IOException{
		stat.execute("DROP TABLE IF EXISTS " + logName);
		
		startTimeMeasurement();		
		stat.execute("CREATE TABLE IF NOT EXISTS " + logName
				+ " AS SELECT "
				+ "cases as CaseID,"
				+ "event as Activity,"
				+ "convert(parseDateTime(startTime,'yyyy/MM/dd HH:mm:ss.SSS'),TIMESTAMP) AS CompleteTimestamp"
				+ " FROM CSVREAD('C:/Users/asyamsiy/workspace/h2processmining-master/resources/"+logName+".csv', null, 'fieldSeparator=,');");
		printTimeTaken();
	}
	
	public static void startTimeMeasurement(){
		startTime = System.nanoTime();
	}
	
	public static void printTimeTaken() throws IOException{
		long endTime = System.nanoTime();
		double timeTaken =  (endTime - startTime)/1000000000.0; 
		System.out.println(timeTaken);
	}
	
	public static H2DatabaseDeclareController controllerOperator(String logName) throws SQLException, IOException {
		startTimeMeasurement();		
		rs = stat.executeQuery("SELECT * FROM CONTROLLER(SELECT caseid,activity,completetimestamp FROM " + logName + ")");
		printTimeTaken();
		
		startTimeMeasurement();	
		H2DatabaseDeclareController cont = new H2DatabaseDeclareController(rs);
		printTimeTaken();
		
		return cont;
	}
	

	public static void main(String[] args) throws ClassNotFoundException, SQLException, IOException {
		String fileName = "t500Ka30";
		out = new BufferedWriter(new FileWriter("C:\\Users\\asyamsiy\\Documents\\Experiment\\h2\\result\\H2-D"+fileName+".txt", false));
		
		Class.forName("org.h2.Driver");
		conn = DriverManager.getConnection("jdbc:h2:tcp://ais-hadoop-1.win.tue.nl/~/test2;cache_size=60000000;multi_threaded=1", "sa", "");
		//conn = DriverManager.getConnection("jdbc:h2:tcp://localhost/~/test", "sa", "");
		//conn = DriverManager.getConnection("jdbc:h2:mem:", "sa", "");
        
		stat = conn.createStatement();
		
		stat.execute("SET MULTI_THREADED 1");
		
		//loadLog(fileName);
		
		H2DatabaseDeclareController cont = controllerOperator(fileName);
		
		cont.printProcessModel(out);
		
		out.close();
	}

}
