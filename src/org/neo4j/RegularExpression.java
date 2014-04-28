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
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
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
	private static final String path="/var/lib/neo4j/data/test153.db";
//	IndexManager indexManager;
//	Index<Node> userIndex, tweetIndex, hashTagIndex;
//	RelationshipIndex relIndex;
//	Transaction tx ;
//	Map<String, Long> createdNodeMap ;
	/**
	 * @param args
	 */
	/*private static enum RelationType implements RelationshipType{
		TWEETS, RETWEETS, CONTAINS, IS_A_RETWEETOF, MENTIONS, IS_A_REPLYTOTWEET,REPLIES;
	}*/
	public RegularExpression() {
	
	}
	public JSONArray query(String originalQuery){
		WebResource resource = Client.create().resource( "http://localhost:7474/db/data/cypher" );
		String query = JSONObject.escape(originalQuery);
		ClientResponse cypherResponse = resource.accept( MediaType.APPLICATION_JSON ).type( MediaType.TEXT_PLAIN )
				.entity( "{\"query\" : \""+query+"\", \"params\" : {}}" )
				.post( ClientResponse.class );
		String cypherResult = cypherResponse.getEntity( String.class );
//		System.out.println(cypherResult);
		// System.out.println();
		cypherResponse.close();
		//JSONObject send = new JSONObject();   
//		ArrayList<JSONObject[]> rows = new ArrayList<JSONObject[]>();

		// convert cypherResult to JSONObject
		JSONObject obj = (JSONObject) JSONValue.parse(cypherResult);
//		System.out.println(obj);

		// only get the "data" field  which contains more "data" - original node & connecting nodes
		JSONArray data = (JSONArray)obj.get("data");
//		System.out.println(data);
//		System.out.println(data.size());
//		JSONObject OuterData = (JSONObject) data.get(0);
		//	    ArrayList<JSONObject> fields= new ArrayList<JSONObject>();	
		//	    System.out.println(data+" json data row size");
		//	    ArrayList<JSONObject> jsonField = new ArrayList<JSONObject>();
//		System.out.println(data.size());

		// extract all the "data" fields from the original "OuterData"
		JSONObject tweetNodeData = new JSONObject();
		JSONObject filterTweetNodeData = new JSONObject();
		String filterMessage = "";
//		LinkedHashMap<String, Object> finalNodesDataMap = new LinkedHashMap<>();
		JSONArray finalNodesDataArray = new JSONArray();
		JSONObject finalNodesJobj1 = new JSONObject();
		JSONObject finalNodesJobj2 = new JSONObject();
		JSONObject finalNodesJobj3 = new JSONObject();

		finalNodesJobj1.put("type", "tweet");
		finalNodesDataArray.add(finalNodesJobj1);
//		System.out.println(finalNodesDataArray);
//		JSONObject finalNodesData = new JSONObject();
//		finalNodesData.put("type", "node");
		JSONObject relNodeData = new JSONObject();
		JSONObject singleRelNodeData = new JSONObject();
		JSONArray allRelNodeData = new JSONArray();
		JSONArray scannedJarray = new JSONArray();
		JSONArray tempScannedJarray = new JSONArray();
		for (int i = 0; i < data.size(); i++) {
			JSONArray fieldData = (JSONArray) data.get(i);
//			System.out.println(fieldData);
//			System.out.println(fieldData.size());
			tweetNodeData = (JSONObject) fieldData.get(0);
//			System.out.println(tweetNodeData);
			filterTweetNodeData = (JSONObject) tweetNodeData.get("data");
			// filter further by extracting "Message" text only
			filterMessage = (String) filterTweetNodeData.get("Message");
//			System.out.println(filterTweetNodeData);
//			System.out.println(finalNodesData);

			relNodeData = (JSONObject) fieldData.get(1);
//			System.out.println(relNodeData);
			singleRelNodeData = (JSONObject) relNodeData.get("data");
//			System.out.println(singleRelNodeData);

			// add "type": "username".. Json information
			scannedJarray = scanForType(singleRelNodeData);
			tempScannedJarray.add(scannedJarray);

//			JSONObject collectScannedJobjs = (JSONObject) scannedJobj.get(0);
			allRelNodeData.add(scannedJarray);
		}

		finalNodesJobj2.put("Message", filterMessage);
		finalNodesDataArray.add(finalNodesJobj2);
		finalNodesJobj3.put("depends", allRelNodeData);
		finalNodesDataArray.add(finalNodesJobj3);

		// iterate over tempScannedJarray to add the rest of the related nodes info
		for (int i = 0; i < tempScannedJarray.size(); i++) {
			finalNodesDataArray.add(tempScannedJarray.get(i));
		}

/*		JSONArray send  = new JSONArray();
		Map<JSONObject, JSONArray> toSend = new HashMap<JSONObject, JSONArray>();
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
			  JSONObject fieldData = (JSONObject)data.get("data");
	        System.out.println(fieldData);
	        fields.add(fieldData);
	        //System.out.println(fieldData);
	        //send.put(y,fieldData);
	     System.out.println(send);
	      //System.out.println(fields);
	      JSONObject[] fieldsArray = new JSONObject[fields.size()];  
	      fields.toArray(fieldsArray);
	      //System.out.println(fieldsArray);
	      rows.add(fieldsArray);
		}
		//System.out.println(send);
		 int index =0 ;
	    for(JSONObject json : jsonField ){
	    	send.put(index, json);
	    	index++;
	    }
		//System.out.println(send);
		//for(JSONObject json : field)	
		//System.out.println(send);
		toSend.put((JSONObject) jsonOriginal.get("data"), send);
		return toSend;*/
		return finalNodesDataArray;
	}

	private JSONArray scanForType(JSONObject singleRelNodeData) {
		JSONArray nodeJsonArray = new JSONArray();

		// for tweet nodes
		if (singleRelNodeData.containsKey("Message")) {
//			System.out.println("true: contains Message");
			JSONObject jobj1 = new JSONObject();
			jobj1.put("type", "tweet");
			nodeJsonArray.add(jobj1);
			String message = (String) singleRelNodeData.get("Message");
			JSONObject jobj2 = new JSONObject();
			jobj2.put("Message", message);
			nodeJsonArray.add(jobj2);
		}

		// for user nodes
		if (singleRelNodeData.containsKey("UserNameKey")) {
//			System.out.println("true: contains hashtag");
			JSONObject jobj1 = new JSONObject();
			jobj1.put("type", "username");
			nodeJsonArray.add(jobj1);
			String username = (String) singleRelNodeData.get("UserNameKey");
			JSONObject jobj2 = new JSONObject();
			jobj2.put("name", username);
			nodeJsonArray.add(jobj2);
		}

		// for hashtag nodes
		if (singleRelNodeData.containsKey("HashTagKey")) {
//			System.out.println("true: contains hashtag");
			JSONObject jobj1 = new JSONObject();
			jobj1.put("type", "hashtag");
			nodeJsonArray.add(jobj1);
			String hashtag = (String) singleRelNodeData.get("HashTagKey");
			JSONObject jobj2 = new JSONObject();
			jobj2.put("name", hashtag);
			nodeJsonArray.add(jobj2);
		}

		return nodeJsonArray;
	}

/*	private JSONObject breakRelNodeData(JSONArray allRelNodeData, int i) {
		JSONObject toSendJObj = new JSONObject();
		
		JSONArray singleJarr = (JSONArray) allRelNodeData.get(i);
		JSONObject jobj1 = (JSONObject) singleJarr.get(0);
		JSONObject jobj2 = (JSONObject) singleJarr.get(1);
		toSendJObj.put("name", jobj2.get("name"));
		toSendJObj.put("type", jobj1.get("type"));
		System.out.println(toSendJObj);
		
		return toSendJObj;
	}*/

	public JSONArray getJsonFromMessageList(List<Long> messgIDList){
		JSONArray jsonDataArray = new JSONArray();
		JSONArray sendJsonDataArray = new JSONArray();

		Map<Long, JSONObject> JsonResultList = new HashMap<>();
		for (Iterator<Long> iterator = messgIDList.iterator(); iterator.hasNext();) {
			Long singleMessgID = (Long) iterator.next();

			//			String cypherQuery = "START n=node(*) WHERE n.MessageID=" + singleMessgID + 
			//					" MATCH n-[:" + relationship + "]-m RETURN DISTINCT m";
			String cypherQuery = "START n=node(*) WHERE n.MessageID=" + singleMessgID + 
					" MATCH n<-[r]->m RETURN DISTINCT n,m";
			//			String cypherQuery = "START n=node(*) WHERE n.MessageID=" + singleMessgID + " RETURN n";
			//			System.out.println(query);
//			Map<JSONArray, JSONArray> finalData = query(cypherQuery);
			jsonDataArray = query(cypherQuery);
//			System.out.println(finalData);

//			JSONObject nodeData = new JSONObject();
	/*		for (Map.Entry<JSONObject, JSONArray> m : finalData.entrySet()) {
				nodeData.put("node", m.getKey());
				nodeData.put("depends", m.getValue());
			}
			*/
//			JsonResultList.put(singleMessgID, nodeData);		

			// append each jsonDataArray to final sendJsonDataArray
			sendJsonDataArray.add(jsonDataArray);
		}

/*		JSONObject send = new JSONObject();
		for(Map.Entry<Long, JSONObject> entry : JsonResultList.entrySet()) {
			send.put(entry.getKey(), entry.getValue());
		}*/

		return sendJsonDataArray;
	}

	public void writeJsonToFile(JSONArray jArr) {
		File file = new File("/home/abdul/Desktop/data.txt");

		try {
			if (!file.exists()) {
				file.createNewFile();
			}

			FileWriter fw = new FileWriter(file.getAbsoluteFile());
			BufferedWriter bw = new BufferedWriter(fw);
			bw.write(jArr.toJSONString());
			bw.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) {
		SearchQuery schema=new SearchQuery();
		RegexDB initiate = new RegexDB();
		//initiate.parseAndCreateDatabase();
		//String query = "START n=node(*) RETURN n limit 10";

		//System.out.println(schema.query(query));
//		schema.query(query);
		List<Long> messageIds = new ArrayList<Long>();

		messageIds.add(267416374350053376L);
		messageIds.add(267416403819233280L);
/*		messageIds.add(267416714092871680L);
		messageIds.add(267416441786073088L);

=======
		messageIds.add(267416374350053376l);
		messageIds.add(267416391370551296l);
		messageIds.add(267416714092871680L);
		messageIds.add(267416441786073088L);
>>>>>>> 10eba4e10080b7b1b9e1196cc5e983e6e255000c
		messageIds.add(267416370327736321L);
		messageIds.add(267416429236740096L);*/
/*		messageIds.add(267416533851062274L);
		messageIds.add(267416592680361984L);
<<<<<<< HEAD
		messageIds.add(267416387029454848L);*/

/*		JSONArray jArray = schema.getJsonFromMessageList(messageIds);
		System.out.println(jArray);
//		System.out.println(obj.toJSONString());
//		JSONObject demo = new  JSONObject();
		schema.writeJsonToFile(jArray);

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
