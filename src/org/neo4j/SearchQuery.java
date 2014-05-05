
/*********************************************************************************************************************************************************************
 * @Author: 	Neo4J Team
 * @Course: 	z517 - Web Programming
 * @Date: 		20th April, 2014
 * @Description:Contains the implementation for querying the DB for required data. 	
 ***********************************************************************************************************************************************************************/
package org.neo4j;



import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.ws.rs.core.MediaType;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;

public class SearchQuery {

	private static List<Long>tweetIdList;

	public SearchQuery() {
		//constructor for the d3.js team to instantiate the object of search query to invoke the Search function.
	}
	@SuppressWarnings("static-access")
	public SearchQuery(List<Long> tweetId) {
		//constructor for lucene team to create the object with the tweeID's in order use it across all the objects instantiated.
		this.tweetIdList = new ArrayList<Long>();
		this.tweetIdList = tweetId;
	}





	/**
	 * This method directs a cypher query to a REST service. The REST service responds with the matching nodes' information.
	 * The response is filtered to construct a JSON format that can be parsed by the d3.js team.
	 * @param originalQuery The cypher query constructed to locate each tweet and its related nodes
	 * @return a map of the tweet JSONObject mapped to its dependent nodes (JSONArray)
	 * @SuppressWarnings("unchecked")
	 */
	@SuppressWarnings("unchecked")
	public JSONArray getJsonFromMessageList( String relType, long startTime, long endTime){
		Map<String, JSONObject> dependentNodesMap = new HashMap<String, JSONObject>();
		JSONArray arraySend = new JSONArray();
		String cypherQuery = "MATCH (n:Tweet) where n.MessageID IN [";
		for(int i=0; i<tweetIdList.size();i++){
			//cypherQuery += "\""+ tweetIdList.get(i)+"\"";
			cypherQuery +=tweetIdList.get(i);
			if(i!=tweetIdList.size()-1) cypherQuery +=",";
		}
		cypherQuery += "] MATCH (n)<-[:"+relType+"*1..5]-(x:Tweet) where x.TimeStamp>="+ startTime+" and x.TimeStamp<="+endTime+" return distinct n,x";
		Map<JSONObject, JSONArray> dataFromTweets =  query_new(cypherQuery);
		try{
			for(Map.Entry<JSONObject, JSONArray> entry : dataFromTweets.entrySet()){

				JSONObject seedTweet = entry.getKey();
				JSONArray depends = entry.getValue();
				seedTweet.put("type","tweet");
				List<String> dependsData = new ArrayList<String>();
				for(int i=0;i<depends.size();i++){

					JSONObject element = (JSONObject) depends.get(i);
					if (element.containsKey("Message")) {
						element.put("type", "tweet");

						element.remove("id");
						element.put("name", element.get("MessageID")+"");

					} else if (element.containsKey("UserNameKey")) {
						element.put("type", "username");

						// replace "id" field in 'element' by "name" field
						element.remove("id");
						element.put("name", element.get("UserNameKey"));
					} else if (element.containsKey("HashTagKey")){
						String hashTagKey= "#"+element.get("HashTagKey");
						element.put("type", "hashtag");
						element.put("HashTagKey", hashTagKey);

						// replace "id" field in 'element' by "name" field
						element.remove("id");
						element.put("name", hashTagKey);
					}

					// map each dependent node "name" (MessageID, hashtag name, username) to its own node
					dependentNodesMap.put(element.get("name")+"",element);

					// store each dependent node's "name" in order to append it to its respective parent tweet node 
					dependsData.add(element.get("name")+"");
				}
				//put depends data
				seedTweet.put("depends", dependsData);
				arraySend.add(seedTweet);

			}
		}
		catch (NullPointerException e) {
			//ignore the data if null pointer exception
		}
		for(Map.Entry<String, JSONObject> entry : dependentNodesMap.entrySet()) {
			arraySend.add( entry.getValue());
		}
		return arraySend;

	}

	/**
	 * This method writes a JSONArray to a text file. The JSONArray is the one that is constructed after a cypher query 
	 * is forwarded to the REST service. The cypher query asks the graph database for a set of nodes that match 
	 * the specified conditions. The response from the REST service is then converted to a JSONArray format.  
	 * @param jObj the argument is the final JSONArray that needs to be written to a text file
	 * @return void
	 */

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

	/**
	 * This method takes a list of messageIDs as input. The messageIDs are generated and forwarded to this method by 
	 * the Lucene API. Each messageID corresponds to the tweet that matches the search parameters selected by the user 
	 * in the Lucene user interface. 
	 * @param messgIDList List of message IDs generated by the Lucene API
	 * @return a JSONArray that contains all relevant node information in the form of JSONObjects
	 * @SuppressWarnings("unchecked")
	 */
	@SuppressWarnings("unchecked")
	public Map<JSONObject,JSONArray> query_new(String originalQuery){

		WebResource resource = Client.create().resource( "http://localhost:7474/db/data/cypher" );
		String query = JSONObject.escape(originalQuery);
		ClientResponse cypherResponse = resource.accept( MediaType.APPLICATION_JSON ).type( MediaType.TEXT_PLAIN )
				.entity( "{\"query\" : \""+query+"\", \"params\" : {}}" )
				.post( ClientResponse.class );
		String cypherResult = cypherResponse.getEntity( String.class );
		cypherResponse.close();
		JSONObject obj = (JSONObject)JSONValue.parse(cypherResult);
		JSONArray data = (JSONArray)obj.get("data");
		Map<JSONObject, JSONArray> toSend = new HashMap<JSONObject, JSONArray>();
		JSONArray depends = new JSONArray();
		for(int i = 0 ;i <tweetIdList.size();i++){
			depends = new JSONArray();
			JSONObject seedNode = new JSONObject();
			for(int x=0; x < data.size(); x++){
				JSONArray fieldData = (JSONArray) data.get(x);
				JSONObject seedNodeRaw = (JSONObject) fieldData.get(0);
				JSONObject seedNodeTemp = (JSONObject) seedNodeRaw.get("data");
				Long seedTweet = (Long) seedNodeTemp.get("MessageID");

				if(seedTweet.equals(tweetIdList.get(i)))
				{
					JSONObject tempnode = (JSONObject) fieldData.get(0);
					seedNode =  (JSONObject) tempnode.get("data");
					JSONObject dependObject = (JSONObject)fieldData.get(1);
					depends.add(dependObject.get("data"));

				}
				else
					continue;
			}
			toSend.put(seedNode,depends);
		}
		return toSend;
	}
}

