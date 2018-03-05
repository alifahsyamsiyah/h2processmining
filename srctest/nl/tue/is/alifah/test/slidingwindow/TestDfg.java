package nl.tue.is.alifah.test.slidingwindow;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;

import org.deckfour.xes.classification.XEventClass;
import org.processmining.plugins.InductiveMiner.dfgOnly.Dfg;
import org.processmining.plugins.InductiveMiner.dfgOnly.DfgImpl;
import org.processmining.plugins.InductiveMiner.dfgOnly.DfgMiningParameters;
import org.processmining.plugins.InductiveMiner.dfgOnly.DfgMiningParametersIMd;
import org.processmining.plugins.InductiveMiner.dfgOnly.plugins.IMdProcessTree;
import org.processmining.processtree.ProcessTree;
import org.processmining.processtree.ptml.Ptml;

public class TestDfg {
	public static void main(String[] args) throws SQLException, IOException {
		Dfg dfg = createDfg();
		discover(dfg);
	}
	
	public static Dfg createDfg() throws SQLException, IOException {
		Dfg dfg = new DfgImpl();
		HashMap<String,Integer> eventClassIndex = new HashMap<String, Integer>();
		int index = 0;
		
		/*String[] log = {"start,a,1", "a,b,1", "b,d,1", "d,e,1", "e,f,1", "f,g,1", "g,end,1",
								"start,a,1", "a,c,1", "c,d,1", "d,f,1", "f,e,1", "e,g,1", "g,end,1",
								"b,d,1", "d,e,1", "e,f,1", "f,g,1", "g,end,1",
								"c,d,1", "d,e,1", "e,f,1", "f,g,1", "g,end,1",
								"d,e,1", "e,f,1", "f,g,1", "g,end,1",
								"e,f,1", "f,g,1", "g,end,1",
								"f,g,1", "g,end,1",
								"start,b,1",
								"start,c,1",
								"start,d,1"};*/
		
		String[] log = {"a,c,1" , "a,b,1" , "b,a,1" , "start,a,2" , "c,end,1"};
		
		// add pair of activities to DFG
		for(int i = 0; i < log.length; i++) {
			String[] arr = log[i].split(",");
			String elem1 = arr[0];
			String elem2 = arr[1];
			int freq = Integer.parseInt(arr[2]);
						
			if(!eventClassIndex.containsKey(elem1)) {
				eventClassIndex.put(elem1, index);
				index++;
			}				
			if(!eventClassIndex.containsKey(elem2)) {
				eventClassIndex.put(elem2, index);
				index++;
			}
			
			XEventClass ec1 = new XEventClass(elem1, eventClassIndex.get(elem1));
			XEventClass ec2 = new XEventClass(elem2, eventClassIndex.get(elem2));		
			
			if(elem1.equalsIgnoreCase("start")) {
				dfg.addActivity(ec2);
				dfg.addStartActivity(ec2, freq); 
			} else if(elem2.equalsIgnoreCase("end")) {
				dfg.addActivity(ec1);
				dfg.addEndActivity(ec1, freq);
			} else {
				dfg.addActivity(ec1);
				dfg.addActivity(ec2);
				dfg.addDirectlyFollowsEdge(ec1, ec2, freq);
			}
		}		
		return dfg;
	}
	
	public static void discover(Dfg dfg) throws IOException {
		DfgMiningParameters param = new DfgMiningParametersIMd();
		ProcessTree pt = IMdProcessTree.mineProcessTree(dfg, param);
		
		Ptml ptml = new Ptml().marshall(pt);
 		String text = "<?xml version=\"1.0\" encoding=\"ISO-8859-1\"?>\n" + ptml.exportElement(ptml);

 		BufferedWriter bw = null;
		try {
			bw = new BufferedWriter(new FileWriter("C:\\Users\\asyamsiy\\Documents\\Experiment\\h2-slidingwindow\\processtree\\" + "test" + "-ProcessTree.ptml", false));
			bw.write(text);
			bw.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
