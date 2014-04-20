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
	IndexManager indexManager;
	Index<Node> userIndex, tweetIndex, hashTagIndex;
	RelationshipIndex relIndex;
	Transaction tx ;
	Map<String, Long> createdNodeMap ;
	/**
	 * @param args
	 */
	private static enum RelationType implements RelationshipType{
		TWEETS, RETWEETS, CONTAINS, IS_A_RETWEETOF, MENTIONS, IS_A_REPLYTOTWEET,REPLIES;
	}
	public RegularExpression() {
		// TODO Auto-generated constructor stub
		service =new GraphDatabaseFactory().newEmbeddedDatabase(path);
		registerShutdownHook(service);
		indexManager = service.index();
		createdNodeMap = new HashMap<String, Long>();
		configureDatabase();
		tx = service.beginTx();

	}
	private void configureDatabase() {
		Transaction tx_label = service.beginTx();
            Schema schema = service.schema();
            try {
                schema.indexFor(DynamicLabel.label("User")).on("id").create();
            } catch (ConstraintViolationException e) {
            }
            try {
                schema.indexFor(DynamicLabel.label("Tweet")).on("id").create();
            } catch (ConstraintViolationException e) {
            }
            try {
                schema.indexFor(DynamicLabel.label("HashTag")).on("id").create();
            } catch (ConstraintViolationException e) {
            }
            tx_label.success();
            tx_label.close();
    }
	@SuppressWarnings("deprecation")
	public Node createTweetNode(long messageID,String message, long timeStamp, String location){//, String[] links){
		//Transaction tx= service.beginTx();

		Node tweetNode = null;
		try {
			Label tweetLabel = DynamicLabel.label("Tweet");
			tweetIndex = indexManager.forNodes("TweetFullText", MapUtil.stringMap(IndexManager.PROVIDER, "lucene", "type", "fulltext"));
			if(createdNodeMap.get("Tweet:"+messageID)!= null)
				tweetNode = checkNode(tweetLabel,"MessageID", messageID,"TweetFullText");
			if(tweetNode==null)
			{
				//String[] links_hardcoded = {"Rohit", "Zawar"};
				//System.out.println("not found..hence created new tweet");
				tweetNode = service.createNode(tweetLabel);
				tweetNode.setProperty("MessageID",messageID);
				tweetNode.setProperty("id",messageID);
				tweetNode.setProperty("Message", message);
				tweetNode.setProperty("TimeStamp",timeStamp);
				tweetNode.setProperty("Location", location);
				createdNodeMap.put("Tweet:"+messageID, tweetNode.getId());
				//tweetNode.setProperty("Links", links);

				//tweetIndex = indexManager.forNodes("TweetFullText", MapUtil.stringMap(IndexManager.PROVIDER, "lucene", "type", "fulltext"));
				tweetIndex.add(tweetNode, "Message", tweetNode.getProperty("Message"));

				tweetIndex.add(tweetNode,"MessageID",tweetNode.getProperty("MessageID"));
			}
			else;
			//System.out.println();

			tx.success();

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}	
		finally
		{
			//tx.finish();
		}
		return tweetNode;
	}


	public Node checkNode( Label label , String key,Object value,String indexName ){
		/*//Transaction tx=service.beginTx();
		ResourceIterator<Node> s =service.findNodesByLabelAndProperty(label,key,value).iterator();
		//tx.success();
		if(s.hasNext()){
			return s.next();
		}*/
//		System.out.println(indexName);
		Index<Node> indexGeneric = indexManager.forNodes( indexName );
		System.out.println(key + " "+value);
		IndexHits<Node> hits = indexGeneric.get( key, value );
		Node node =null;
		try {	
			node = hits.getSingle();
			//System.out.println(key+" "+value);
			/*if(node == null)
				System.out.println("here node found null "+indexName);
			else
				System.out.println("Found man!!!");*/
		} catch (Exception e) {
			// TODO Auto-generated catch block
			System.out.println("get single exception for "+key +" "+value+" "+value+" "+indexName+" ");
			e.printStackTrace();
			System.exit(0);

		}
		return node;
	}
	public Node createUserNode(String username){
		//Transaction tx=service.beginTx();
		//System.out.println(username);
		Node user=null;
		try {
			Label userLabel=DynamicLabel.label("User");
			userIndex = indexManager.forNodes("UserIndex");
			if(createdNodeMap.get("User:"+username)!= null)
				user=checkNode(userLabel, "UserNameKey", username,"UserIndex");
			if(user==null){
				user=service.createNode(userLabel);
				user.setProperty("UserNameKey", username);
				user.setProperty("id", username);
				createdNodeMap.put("User:"+username, user.getId());
				//userIndex=indexManager.forNodes("UserIndex");
				userIndex.add(user, "UserNameKey", user.getProperty("UserNameKey"));
			}
			else;
			//System.out.println();
			tx.success();
		}
		catch (Exception e) {
			e.printStackTrace();
		}
		finally{
			//tx.close();
		}
		return user;

	}
	@SuppressWarnings("deprecation")
	public Node createHashTag(String hashTag){
		//Transaction tx=service.beginTx();
		Node hashTagNode=null;
		try{
			Label hashTagLabel=DynamicLabel.label("HashTag");
			hashTagIndex = indexManager.forNodes("HashTag-FullText", MapUtil.stringMap(IndexManager.PROVIDER, "lucene", "type", "fulltext"));
			if(createdNodeMap.get("HashTag:"+hashTag)!= null){
				hashTagNode = checkNode(hashTagLabel, "HashTagKey", hashTag,"HashTag-FullText");
//				System.out.println("same");
			}
			if(hashTagNode==null){
				hashTagNode=service.createNode(hashTagLabel);
				hashTagNode.setProperty("HashTagKey", hashTag);
				hashTagNode.setProperty("id", hashTag);
				createdNodeMap.put("HashTag:"+hashTag, hashTagNode.getId());
				//hashTagIndex=indexManager.forNodes("HashTa-FullText", MapUtil.stringMap(IndexManager.PROVIDER, "lucene", "type", "fulltext"));
				hashTagIndex.add(hashTagNode, "HashTagKey", hashTagNode.getProperty("HashTagKey"));

			}
			else;
			//System.out.println();
			tx.success();
		}
		catch(Exception e){
			e.printStackTrace();
		}
		finally{
			//tx.close();
		}
		return hashTagNode;
	}
	@SuppressWarnings("deprecation")
	public void createRelationShip(Node node1, Node node2, String relationshipname){
		Relationship relation;
		//Transaction tx=service.beginTx();
		relIndex=indexManager.forRelationships("RelationshipIndex");
		try {
			if(relationshipname.equalsIgnoreCase("Tweets")){
				relation=node1.createRelationshipTo(node2, RelationType.TWEETS);
				relation.setProperty("Tweets", "Tweets");
				relIndex.add(relation, "Tweets", relation.getProperty("Tweets"));
			}
			else if (relationshipname.equalsIgnoreCase("Retweets")){
				relation=node1.createRelationshipTo(node2,RelationType.RETWEETS);
				relation.setProperty("Retweets", "Retweets");
				relIndex.add(relation, "Retweets", relation.getProperty("Retweets"));
			}
			else if(relationshipname.equalsIgnoreCase("Contains")){
				relation=node1.createRelationshipTo(node2,RelationType.CONTAINS);
				relation.setProperty("Contains", "Contains");
				relIndex.add(relation, "Contains", relation.getProperty("Contains"));
			}
			else if(relationshipname.equalsIgnoreCase("IsARetweetOf")){
				relation=node1.createRelationshipTo(node2, RelationType.IS_A_RETWEETOF);
				relation.setProperty("IsARetweetOf", "IsARetweetOf");
				relIndex.add(relation,"IsARetweetOf",relation.getProperty("IsARetweetOf"));
			}
			else if(relationshipname.equalsIgnoreCase("Mentions")){
				relation=node1.createRelationshipTo(node2, RelationType.MENTIONS);
				relation.setProperty("Mentions", "Mentions");
				relIndex.add(relation,"Mentions",relation.getProperty("Mentions"));
			}
			else if(relationshipname.equalsIgnoreCase("Replies")){
				relation=node1.createRelationshipTo(node2, RelationType.REPLIES);
				relation.setProperty("Replies", "Replies");
				relIndex.add(relation, "Replies", relation.getProperty("Replies"));
			}
			else if(relationshipname.equalsIgnoreCase("RepliesToTweet")){
				relation=node1.createRelationshipTo(node2, RelationType.IS_A_REPLYTOTWEET);
				relation.setProperty("RepliesToTweet", "RepliesToTweet");
				relIndex.add(relation,"RepliesToTweet",relation.getProperty("RepliesToTweet"));
			}
			tx.success();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		finally{

			//tx.finish();
		}
	}
	public void connectTweets(long retweet_original_message_id, long tweet_id, String string) {
		//Transaction tx = service.beginTx();
		Node originalNode = null;
		Node tweetNode = null;
		try {
			ResourceIterator<Node> nodes = service.findNodesByLabelAndProperty(DynamicLabel.label("Tweet"), "MessageID", retweet_original_message_id).iterator();
			if(nodes.hasNext()){

				originalNode = nodes.next();
				nodes = service.findNodesByLabelAndProperty(DynamicLabel.label("Tweet"), "MessageID", tweet_id).iterator();
				if(nodes.hasNext()){
					tweetNode = nodes.next();
					if(string.equalsIgnoreCase("isretweetof"))
						createRelationShip(tweetNode, originalNode, "IsARetweetOf");
					else if(string.equalsIgnoreCase("RepliesToTweet"))
						createRelationShip(tweetNode, originalNode, "RepliesToTweet");
				}
			}
			tx.success();
		} catch (Exception e) {
			// TODO: handle exception
		}
		finally{
			//tx.finish();
		}
		// TODO Auto-generated method stub

	}
	private static void registerShutdownHook( final GraphDatabaseService graphDb )
	{
		Runtime.getRuntime().addShutdownHook( new Thread()
		{
			@Override
			public void run()
			{
				graphDb.shutdown();
			}
		} );
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
		//String dataSetPath;
		RegularExpression schema = new RegularExpression();
		
		//dataset parsing and DB creation functionality put in a different file for modularity 
		RegexDB initiate = new RegexDB();
		initiate.parseAndCreateDatabase();
		System.exit(0);
		
		//		String query = "START n=node(*) RETURN n limit 10";
		//System.out.println(schema.query(query));
//		schema.query(query);
		List<Long> messageIds = new ArrayList<Long>();
		messageIds.add(267416374350053376L);
		messageIds.add(267416403819233280L);
/*		messageIds.add(267416714092871680L);
		messageIds.add(267416441786073088L);

		messageIds.add(267416370327736321L);
		messageIds.add(267416429236740096L);*/
/*		messageIds.add(267416533851062274L);
		messageIds.add(267416592680361984L);
		messageIds.add(267416387029454848L);*/

		JSONArray jArray = schema.getJsonFromMessageList(messageIds);
		System.out.println(jArray);
//		System.out.println(obj.toJSONString());
//		JSONObject demo = new  JSONObject();
		schema.writeJsonToFile(jArray);

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