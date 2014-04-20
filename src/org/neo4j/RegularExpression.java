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

	//GraphDatabaseService service;
	//private static final String path="D:\\Check170.graphdb";
	
	public RegularExpression() {
	

	}
	public static void main(String[] args) {
		SearchQuery schema=new SearchQuery();
		RegexDB initiate = new RegexDB();
		//initiate.parseAndCreateDatabase();
		//String query = "START n=node(*) RETURN n limit 10";
		//System.out.println(schema.query(query));
		//schema.query(query);
		List<Long> messageIds = new ArrayList<Long>();
		messageIds.add(267416374350053376l);
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
		schema.writeJsonToFile(obj);

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

