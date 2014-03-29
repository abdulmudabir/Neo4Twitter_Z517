package org.neo4j;

import java.util.ArrayList;
import java.util.List;

import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ResourceIterable;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;

public class Schema {
	GraphDatabaseService service;
	private static final String path="D:\\iub\\sem2\\web programming\\neo4j\\data1221";
	private static enum RelationType implements RelationshipType{
		TWEETS, RETWEETS, CONTAINS;
	}
	public Schema() {
		// TODO Auto-generated constructor stub
		service =new GraphDatabaseFactory().newEmbeddedDatabase(path);
	}

	@SuppressWarnings("deprecation")
	public Node createTweetNode(long messageID,String message, long timeStamp, String location, String[] links){
		Transaction tx= service.beginTx();

		Node tweetNode=null;
		try {
			Label tweetLabel = DynamicLabel.label("Tweet");
			tweetNode = checkNode(tweetLabel,"MessageID", messageID);
			if(tweetNode==null)
			{
				System.out.println("not found..hence created new tweet");
				tweetNode = service.createNode(tweetLabel);
				tweetNode.setProperty("MessageID",messageID);
				tweetNode.setProperty("Message", message);
				tweetNode.setProperty("TimeStamp",timeStamp);
				tweetNode.setProperty("Location", location);
				tweetNode.setProperty("Links", links);
			}
			else
				System.out.println("found..hence not created new tweet");

			tx.success();

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}	
		finally
		{
			tx.finish();
		}
		return tweetNode;
	}


	public Node checkNode( Label label , String id,Object value ){
		Transaction tx=service.beginTx();
		ResourceIterator<Node> s =service.findNodesByLabelAndProperty(label,id,value).iterator();
		tx.success();
		if(s.hasNext())
			return s.next();
		//		else
		//			System.out.println("not found");
		return null;

	}


	@SuppressWarnings("deprecation")
	public Node createUserNode(String username){

		Transaction tx=service.beginTx();
		Node user=null;
		try {
			Label userLabel=DynamicLabel.label("User");
			user=checkNode(userLabel, "UserNameKey", username);
			if(user==null)
			{
				System.out.println("not found. hence created user");
				user=service.createNode(userLabel);
				user.setProperty("UserNameKey", username);
			}
			else
				System.out.println("found..so not created new user");
			tx.success();
		}
		catch (Exception e) {
			e.printStackTrace();
		}
		finally{
			tx.finish();
		}
		return user;

	}

	@SuppressWarnings("deprecation")
	public Node createHashTag(String hashTag){
		Transaction tx=service.beginTx();
		Node hashTagNode=null;
		try{
			Label hashTagLabel=DynamicLabel.label("HashTag");
			hashTagNode = checkNode(hashTagLabel, "HashTagKey", hashTag);
			if(hashTagNode==null)
			{
				System.out.println("not found..so created new hash");
				hashTagNode=service.createNode(hashTagLabel);
				hashTagNode.setProperty("HashTagKey", hashTag);
			}
			else
				System.out.println("found..so not created new hash");
			tx.success();
		}
		catch(Exception e){
			e.printStackTrace();
		}
		finally{
			tx.finish();
		}
		return hashTagNode;
	}

	@SuppressWarnings("deprecation")
	public void createRelationShip(Node node1, Node node2, String relationshipname){
		Relationship relation;
		Transaction tx=service.beginTx();

		try {
			if(relationshipname.equalsIgnoreCase("Tweets")){
				relation=node1.createRelationshipTo(node2, RelationType.TWEETS);
			}
			else if (relationshipname.equalsIgnoreCase("Retweets")){
				relation=node1.createRelationshipTo(node2,RelationType.RETWEETS);
			}
			else if(relationshipname.equalsIgnoreCase("Contains")){
				relation=node1.createRelationshipTo(node2,RelationType.CONTAINS);
			}


			tx.success();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		finally{

			tx.finish();
		}
	}


	public static void main(String args[]){
		Schema schema=new Schema();
		String[] links={"http://www.google.com","http://www.facebook.com"};

		Node nodetweet1=schema.createTweetNode(12,"I am awesome", 1, "California", links );
		Node nodetweet2=schema.createTweetNode(13,"Chicha rocks!", 2, "Bloomington",links);
		Node nodetweet3=schema.createTweetNode(12,"Chintan hurrr", 1, "California", links );
		Node nodetweet4=schema.createTweetNode(12,"Rohit get lost!", 3, "Sunny Vale", links );


		Node nodeuser1=schema.createUserNode("aish");
		Node nodeuser2=schema.createUserNode("rohit");
		Node nodeuser3=schema.createUserNode("aish");

		Node nodehashtag1=schema.createHashTag("chicha_isbest!");
		Node nodehashtag2=schema.createHashTag("aish_isbest!");
		Node nodehashtag3=schema.createHashTag("chicha_isbest!");

		schema.createRelationShip(nodeuser1, nodetweet1, "Tweets");
		schema.createRelationShip(nodeuser1, nodetweet2, "tweets");
		schema.createRelationShip(nodeuser2, nodetweet3, "Tweets");
		schema.createRelationShip(nodeuser2, nodetweet4, "tweets");

		schema.createRelationShip(nodetweet1, nodehashtag1, "contains")	;
		schema.createRelationShip(nodetweet2, nodehashtag2, "contains")	;

		Node nodehashtagcheck=schema.createHashTag("demo");
		//Node nodehashtagcheck1=schema.createHashTag("chicha_isbest!");
		//System.out.println(schema.checkNode(DynamicLabel.label("HashTag"),"HashTagKey","chich_isbest!"));


		schema.service.shutdown();
		System.out.println("ends");


	}


}
