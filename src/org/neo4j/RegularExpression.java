package org.neo4j;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.ws.rs.core.MediaType;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.neo4j.graphdb.ConstraintViolationException;
import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.IndexHits;
import org.neo4j.graphdb.index.IndexManager;
import org.neo4j.graphdb.index.RelationshipIndex;
import org.neo4j.graphdb.schema.Schema;
import org.neo4j.helpers.collection.MapUtil;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;


public class RegularExpression {

	GraphDatabaseService service;
	private static final String path="D:\\Check159.graphdb";
	
	public RegularExpression() {
	

	}
	public Map<JSONObject,JSONArray> query(String originalQuery){
		WebResource resource = Client.create().resource( "http://localhost:7474/db/data/cypher" );
		String query = JSONObject.escape(originalQuery);
		ClientResponse cypherResponse = resource.accept( MediaType.APPLICATION_JSON ).type( MediaType.TEXT_PLAIN )
				.entity( "{\"query\" : \""+query+"\", \"params\" : {}}" )
				.post( ClientResponse.class );
		String cypherResult = cypherResponse.getEntity( String.class );
		//    System.out.println(cypherResult);
		// System.out.println();
		cypherResponse.close();
		//JSONObject send = new JSONObject();   
		ArrayList<JSONObject[]> rows= new ArrayList<JSONObject[]>();
		JSONObject obj = (JSONObject)JSONValue.parse(cypherResult);
		//System.out.println(cypherResult);
		JSONArray data = (JSONArray)obj.get("data");
		//System.out.println(data);
		//	    ArrayList<JSONObject> fields= new ArrayList<JSONObject>();	
		//	    System.out.println(data+" json data row size");
		//	    ArrayList<JSONObject> jsonField = new ArrayList<JSONObject>();
		JSONArray send  = new JSONArray();
		Map<JSONObject, JSONArray> toSend = new HashMap<JSONObject, JSONArray>();
		System.out.println(data.size());
		JSONObject jsonOriginal = new JSONObject();
		for(int x=0; x < data.size(); x++){
			JSONArray fieldData = (JSONArray) data.get(x);
			//	    	System.out.println(fieldData);
			//System.out.println(fieldData.size());
			//for(int i = 0;i<fieldData.size() ;i++){
			jsonOriginal = (JSONObject) fieldData.get(0);
			//System.out.println(jsonOriginal);
			JSONObject objSingle = (JSONObject) fieldData.get(1);
			//jsonField.add((JSONObject) objSinigle.get("data"));
			//System.out.println(i);
			//System.out.println((JSONObject) objSinigle.get("data"));
			send.add((JSONObject) objSingle.get("data"));
			//}
			// JSONObject field = (JSONObject)row.get(y);
			/*  JSONObject fieldData = (JSONObject)data.get("data");
	        System.out.println(fieldData);
	        fields.add(fieldData);
	        //System.out.println(fieldData);
	        //send.put(y,fieldData);
	     System.out.println(send);
	      //System.out.println(fields);
	      JSONObject[] fieldsArray = new JSONObject[fields.size()];  
	      fields.toArray(fieldsArray);
	      //System.out.println(fieldsArray);
	      rows.add(fieldsArray);*/
		}
		//System.out.println(send);
		/* int index =0 ;
	    for(JSONObject json : jsonField ){
	    	send.put(index, json);
	    	index++;
	    }*/
		//System.out.println(send);
		//for(JSONObject json : field)
		//System.out.println(send);
		toSend.put((JSONObject) jsonOriginal.get("data"), send);
		return toSend;
	}
	public JSONArray getJsonFromMessageList(List<Long> messgIDList){

		//Map<Long, JSONObject> JsonResultList = new HashMap<>();
		Map<String, JSONObject> dependentNodesMap = new HashMap<String, JSONObject>();
		JSONArray arraySend = new JSONArray();
		JSONObject tweetnode = null;
		for (Iterator<Long> iterator = messgIDList.iterator(); iterator.hasNext();) {
			Long singleMessgID = (Long) iterator.next();

			//			String cypherQuery = "START n=node(*) WHERE n.MessageID=" + singleMessgID + 
			//					" MATCH n-[:" + relationship + "]-m RETURN DISTINCT m";
			String cypherQuery = "START n=node(*) WHERE n.MessageID=" + singleMessgID + 
					" MATCH n<-[r]->m RETURN DISTINCT n,m";
			//			String cypherQuery = "START n=node(*) WHERE n.MessageID=" + singleMessgID + " RETURN n";
			//			System.out.println(query);
			Map<JSONObject, JSONArray> finalData = query(cypherQuery);
			
			//JSONObject nodeData = new JSONObject();
			JSONArray depends = new JSONArray();
			for (Map.Entry<JSONObject, JSONArray> m : finalData.entrySet()) {
				tweetnode  = m.getKey();
				tweetnode.put("type", "tweetNode");
				depends = m.getValue();
				/*nodeData.put("node", m.getKey());
				nodeData.put("depends", m.getValue());
*/			}
			List<String> dependsData = new ArrayList<String>();
			//put the dppends in map
			for(int i=0;i<depends.size();i++){
				
				JSONObject element = (JSONObject) depends.get(i);
				if (element.containsKey("Message")) {
					element.put("type", "tweet");
				} else if (element.containsKey("UserNameKey")) {
					element.put("type", "username");
				} else if (element.containsKey("HashTagKey")){
					element.put("type", "hashtag");
				}
				dependentNodesMap.put(element.get("id")+"",element);
				dependsData.add(element.get("id")+"");
			}
			tweetnode.put("depends", dependsData);
			//JsonResultList.put(singleMessgID, nodeData);		
			arraySend.add(tweetnode);
		}
		
		//JSONObject send = new JSONObject();
		for(Map.Entry<String, JSONObject> entry : dependentNodesMap.entrySet()) {
			arraySend.add( entry.getValue());
		}
		return arraySend;

	}

	public void writeJsonToFile(JSONArray jObj) {
		File file = new File("C:\\Users\\Rohit\\Desktop\\data.txt");

		try {
			if (!file.exists()) {
				file.createNewFile();
			}

			FileWriter fw = new FileWriter(file.getAbsoluteFile());
			BufferedWriter bw = new BufferedWriter(fw);
			bw.write(jObj.toJSONString());
			bw.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) {
		String dataSetPath;
		RegularExpression schema=new RegularExpression();

		RegexDB initiate = new RegexDB();
		initiate.parseAndCreateDatabase();
		/*String query = "START n=node(*) RETURN n limit 10";
		//System.out.println(schema.query(query));
		//schema.query(query);
		List<Long> messageIds = new ArrayList<Long>();
		messageIds.add(267416374350053376);
		messageIds.add(267416391370551296l);
		messageIds.add(267416714092871680L);
		messageIds.add(267416441786073088L);

		messageIds.add(267416370327736321L);
		messageIds.add(267416429236740096L);
		messageIds.add(267416533851062274L);
		messageIds.add(267416592680361984L);
		messageIds.add(267416387029454848L);
		messageIds.add(267419566446100480L);

		JSONArray obj = schema.getJsonFromMessageList(messageIds);
		//		System.out.println(obj.toJSONString());
		//JSONObject demo = new  JSONObject();
		schema.writeJsonToFile(obj);*/

	}
	@Override
	protected void finalize() throws Throwable {
		try {
			// close open files
		} finally {
			super.finalize();
		}
	}
}

