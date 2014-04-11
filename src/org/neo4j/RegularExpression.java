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
	private static final String path="D:\\Check109.graphdb";
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
		tx = service.beginTx();
		createdNodeMap = new HashMap<String, Long>();
		//configureDatabase();

	}
	/*private void configureDatabase() {
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
            tx.success();
    }*/
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
		Index<Node> indexGeneric = indexManager.forNodes( indexName );
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
			System.out.println("get single exception for "+key +" "+value);
			e.printStackTrace();
			System.exit(0);

		}
		return node;
	}
	@SuppressWarnings("deprecation")
	public Node createUserNode(String username){
		//Transaction tx=service.beginTx();
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
			//tx.finish();
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
			if(createdNodeMap.get("HashTag:"+hashTag)!= null)
				hashTagNode = checkNode(hashTagLabel, "HashTagKey", hashTag,"HashTag-FullText");
			if(hashTagNode==null){
				hashTagNode=service.createNode(hashTagLabel);
				hashTagNode.setProperty("HashTagKey", hashTag);
				hashTagNode.setProperty("id", hashTag);
				createdNodeMap.put("Tweet:"+hashTag, hashTagNode.getId());
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
			//tx.finish();
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
	public JSONObject getJsonFromMessageList(List<Long> messgIDList){

		Map<Long, JSONObject> JsonResultList = new HashMap<>();
		for (Iterator<Long> iterator = messgIDList.iterator(); iterator.hasNext();) {
			Long singleMessgID = (Long) iterator.next();

			//			String cypherQuery = "START n=node(*) WHERE n.MessageID=" + singleMessgID + 
			//					" MATCH n-[:" + relationship + "]-m RETURN DISTINCT m";
			String cypherQuery = "START n=node(*) WHERE n.MessageID=" + singleMessgID + 
					" MATCH n<-[r]->m RETURN DISTINCT n,m";
			//			String cypherQuery = "START n=node(*) WHERE n.MessageID=" + singleMessgID + " RETURN n";
			//			System.out.println(query);
			Map<JSONObject, JSONArray> finalData = query(cypherQuery);
			JSONObject nodeData = new JSONObject();
			for (Map.Entry<JSONObject, JSONArray> m : finalData.entrySet()) {
				nodeData.put("node", m.getKey());
				nodeData.put("depends", m.getValue());
			}

			JsonResultList.put(singleMessgID, nodeData);		

		}
		JSONObject send = new JSONObject();
		for(Map.Entry<Long, JSONObject> entry : JsonResultList.entrySet()) {
			send.put(entry.getKey(), entry.getValue());
		}
		return send;

	}

	public void writeJsonToFile(JSONObject jObj) {
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

		try {
			dataSetPath = "C:\\Users\\Rohit\\Desktop\\Neo4j\\obama_20121015_20121115.txt";
			dataSetPath.replace('\\', '/');
			BufferedReader file = new BufferedReader(new FileReader(dataSetPath));
			int i = 0;
			String fileLine;
			boolean isRetweet = false;
			while((fileLine =file.readLine()) != null && i<10000 && !fileLine.equals("")){
				String[] temp = fileLine.split("\\|");
				if((temp.length == 9 || temp.length == 8)){
					if(fileLine.contains("RT @")){
						isRetweet = true;
					}
					long tweet_id = Long.parseLong(temp[0]);

					//converting the timestamp to unix time format
					DateFormat formatter;
					Date date = null;
					long unix_time;
					formatter = new SimpleDateFormat("yyyy-mm-dd hh:mm:ss");
					date = formatter.parse(temp[1]);
					unix_time = date.getTime() / 1000L;

					//CODE TO RETREIVE THE DATE FROM UNIX DATE
					Date date1 = new Date(unix_time*1000L); // *1000 is to convert seconds to milliseconds
					SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss z"); // the format of your date
					String formattedDate = sdf.format(date1);
					long retweet_original_message_id =  Long.parseLong(temp[5]);
					long replyto_message_id = Long.parseLong(temp[7]);
					String username = temp[2];
					String tweet = temp[3];
					String Location= temp[4];
					String retweet_username = temp[6];
					String reply_username;

					if(temp.length == 8)
					{
						reply_username = "";
					}
					else
					{
						reply_username = temp[8];
					}
					ArrayList<StringBuffer> hashtags_list = new ArrayList<StringBuffer>();
					ArrayList<StringBuffer> username_list = new ArrayList<StringBuffer>();
					//ArrayList<String> links_list = new ArrayList<String>();
					int isHashTag = 0;
					int isUserName = 0;
					StringBuffer tempHashTag = new StringBuffer(); 
					StringBuffer tempUserName = new StringBuffer();
					int d = 0;
					String fileLineSub;
					/*while( linkIndex < tweet.length())
					{			        	
						fileLineSub = tweet.substring(linkIndex);
						if(fileLineSub.contains("http://")  == true)
						{

							linkIndex = fileLineSub.indexOf("http://");
							fileLineSub = fileLineSub.substring(linkIndex);
							//System.out.println(fileLineSub);
							String link;
							if(fileLineSub.contains(" "))
							{
								//if(fileLineSub.indexOf(" ") < fileLineSub.indexOf("|"))
								link = fileLineSub.substring(0, fileLineSub.indexOf(" "));
								//else
								//link = fileLineSub.substring(0, fileLineSub.indexOf("|"));
								links_list.add(link);
								linkIndex += link.length();
							}
							else
							{
								link = fileLineSub.substring(0);			    
								links_list.add(link);
								linkIndex += link.length();
								break;
							}	        		

						}
						else
						{
							break;
						}			        	
					}*/




					for(char ch: tweet.toCharArray())
					{
						if(ch == ' ' || ch== '.' || ch == ',' || ch == '&')
						{
							if(isHashTag ==1)
							{
								hashtags_list.add(tempHashTag);			        		
								tempHashTag = new StringBuffer();
								isHashTag = 0;
							}			        		
							if(isUserName == 1)
							{
								if( reply_username.equals(tempUserName.toString()) == false && retweet_username.equals(tempUserName.toString())==false )
								{
									username_list.add(tempUserName);
								}		
								tempUserName = new StringBuffer();
								isUserName = 0;
							}
							continue;
						}
						else if(ch == '#')
						{
							isHashTag = 1;
							continue;
						}
						else if(ch == '@')
						{
							isUserName = 1;
							continue;
						}
						else if(isHashTag == 1)
						{
							tempHashTag.append(ch);
						}
						else if(isUserName == 1)
						{
							if(ch != ':'){
								tempUserName.append(ch);
							}
						}
					}			
					if(isHashTag ==1)
					{
						hashtags_list.add(tempHashTag);			        		
						tempHashTag = new StringBuffer();
						isHashTag = 0;
					}			        		
					if(isUserName == 1)
					{

						if( reply_username.equals(tempUserName.toString()) == false && retweet_username.equals(tempUserName.toString())==false )
						{
							username_list.add(tempUserName);
						}			        							        		
						tempUserName = new StringBuffer();
						isUserName = 0;
					}
					//create the nodes and relationships

					// create username node
					Node Username=schema.createUserNode(username);


					// create tweet node
					/*String[] links =  new String[links_list.size()];
					int k = 0;
					for(String item: links_list){
						links[k] = item;
						k++;
					}*/
					Node Tweet=schema.createTweetNode(tweet_id, tweet, unix_time, Location);//,links);
					boolean isTweet = true;
					if(isRetweet)
					{	isTweet = false;
					schema.createRelationShip(Username, Tweet, "Retweets");
					}
					if(!reply_username.equals(""))
					{
						isTweet = false;
						schema.createRelationShip(Username, Tweet, "Replies");
					}

					if(isTweet == true)
					{

						schema.createRelationShip(Username, Tweet, "Tweets");
					}


					if(hashtags_list.size() > 0)
					{
						Node HashTag;
						for(StringBuffer item:hashtags_list)
						{
							HashTag=schema.createHashTag(item.toString());
							schema.createRelationShip(Tweet, HashTag, "Contains");
						}			        	
					}
					if(username_list.size() > 0)
					{
						Node MentionedUser;
						for(StringBuffer temp_username:username_list)
						{
							MentionedUser = schema.createUserNode(temp_username.toString());
							schema.createRelationShip(Tweet, MentionedUser, "Mentions");
						}			        	
					}
					if(isRetweet && retweet_original_message_id!=0){
						schema.connectTweets(retweet_original_message_id,tweet_id,"isretweetof");
					}

					if(!reply_username.equalsIgnoreCase(""))
					{
						schema.connectTweets(replyto_message_id, tweet_id, "RepliesToTweet");
					}

					//links_list.clear();
					//links_list = null;
					//links = null;
					System.gc();
					schema.finalize();

				}
				else
				{
					continue;
				}

				i++;
				isRetweet = false;

				System.out.println(i);
			}
			schema.tx.close();
			//System.out.println("The number of lines is:" + i);
			file.close();
			schema.service.shutdown();
		}
		catch (Exception e) {
			e.printStackTrace();
			// TODO: handle exception
		} catch (Throwable e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.out.println("here in finally exception");
		}
		String query = "START n=node(*) RETURN n limit 10";
		//System.out.println(schema.query(query));
		//schema.query(query);
		List<Long> messageIds = new ArrayList<Long>();
		/*messageIds.add(267416374350053376);
		messageIds.add(267416391370551296l);*/
		/*messageIds.add(267416714092871680L);
		messageIds.add(267416441786073088L);*/

		messageIds.add(267416370327736321L);
		messageIds.add(267416429236740096L);
		messageIds.add(267416533851062274L);
		messageIds.add(267416592680361984L);
		messageIds.add(267416387029454848L);

		//JSONObject obj = schema.getJsonFromMessageList(messageIds);
		//		System.out.println(obj.toJSONString());
		//JSONObject demo = new  JSONObject();
		//schema.writeJsonToFile(obj);

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

