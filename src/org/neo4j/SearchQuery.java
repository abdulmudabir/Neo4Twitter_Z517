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
import java.util.Iterator;
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

	public SearchQuery() {
		// TODO Auto-generated constructor stub
	}
	@SuppressWarnings("unchecked")
	public Map<JSONObject,JSONArray> query(String originalQuery){
		WebResource resource = Client.create().resource( "http://localhost:7474/db/data/cypher" );
		String query = JSONObject.escape(originalQuery);
		ClientResponse cypherResponse = resource.accept( MediaType.APPLICATION_JSON ).type( MediaType.TEXT_PLAIN )
				.entity( "{\"query\" : \""+query+"\", \"params\" : {}}" )
				.post( ClientResponse.class );
		String cypherResult = cypherResponse.getEntity( String.class );
		cypherResponse.close();
		JSONObject obj = (JSONObject)JSONValue.parse(cypherResult);
		JSONArray data = (JSONArray)obj.get("data");
		JSONArray send  = new JSONArray();
		Map<JSONObject, JSONArray> toSend = new HashMap<JSONObject, JSONArray>();
		System.out.println(data.size());
		JSONObject jsonOriginal = new JSONObject();
		for(int x=0; x < data.size(); x++){
			JSONArray fieldData = (JSONArray) data.get(x);
			
			jsonOriginal = (JSONObject) fieldData.get(0);
			JSONObject objSingle = (JSONObject) fieldData.get(1);
			send.add((JSONObject) objSingle.get("data"));
			
		}
		toSend.put((JSONObject) jsonOriginal.get("data"), send);
		return toSend;
	}
	public JSONArray getJsonFromMessageList(List<Long> messgIDList){
		Map<String, JSONObject> dependentNodesMap = new HashMap<String, JSONObject>();
		JSONArray arraySend = new JSONArray();
		JSONObject tweetnode = null;
		for (Iterator<Long> iterator = messgIDList.iterator(); iterator.hasNext();) {
			Long singleMessgID = (Long) iterator.next();
			String cypherQuery = "START n=node(*) WHERE n.MessageID=" + singleMessgID + 
					" MATCH n<-[r]->m RETURN DISTINCT n,m";
			Map<JSONObject, JSONArray> finalData = query(cypherQuery);
			JSONArray depends = new JSONArray();
			for (Map.Entry<JSONObject, JSONArray> m : finalData.entrySet()) {
				tweetnode  = m.getKey();
				tweetnode.put("type", "tweetNode");
				depends = m.getValue();
			}
			List<String> dependsData = new ArrayList<String>();
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
			arraySend.add(tweetnode);
		}
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
}
