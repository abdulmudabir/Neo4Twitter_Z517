package org.neo4j;

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
import org.neo4j.graphdb.index.IndexManager;
import org.neo4j.graphdb.index.RelationshipIndex;
import org.neo4j.helpers.collection.MapUtil;

public class Schema {
	GraphDatabaseService service;
	private static final String path="D:\\iub\\sem2\\web programming\\neo4j\\data015210";
	IndexManager indexManager;
	Index<Node> userIndex, tweetIndex, hashTagIndex;
	RelationshipIndex relIndex;

	private static enum RelationType implements RelationshipType{
		TWEETS, RETWEETS, CONTAINS, ISARETWEETOF, MENTIONS, ISAREPLYTOTWEET,REPLIES;
	}
	public Schema() {
		// TODO Auto-generated constructor stub
		service =new GraphDatabaseFactory().newEmbeddedDatabase(path);
		indexManager = service.index();

	}

	@SuppressWarnings("deprecation")
	public Node createTweetNode(long messageID,String message, long timeStamp, String location, Object[] links){
		Transaction tx= service.beginTx();

		Node tweetNode = null;

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

				tweetIndex = indexManager.forNodes("TweetFullText", MapUtil.stringMap(IndexManager.PROVIDER, "lucene", "type", "fulltext"));
				tweetIndex.add(tweetNode, "Message", tweetNode.getProperty("Message"));

				tweetIndex.add(tweetNode,"MessageID",tweetNode.getProperty("MessageID"));
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


	public Node checkNode( Label label , String key,Object value ){
		Transaction tx=service.beginTx();
		ResourceIterator<Node> s =service.findNodesByLabelAndProperty(label,key,value).iterator();
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

				userIndex=indexManager.forNodes("UserIndex");
				userIndex.add(user, "UserNameKey", user.getProperty("UserNameKey"));
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


/*	@SuppressWarnings("deprecation")
	public Node findOriginalTweetNode(long retweet_original_message_id){
		Transaction tx=service.beginTx();;
		Node found=null;
		try {
			
			Index<Node> foundTweetIndex = indexManager.forNodes("TweetFullText");
			 found= foundTweetIndex.query("MessageID",retweet_original_message_id ).getSingle();
			 tx.success();
		
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		finally{
			tx.finish();
		}
		return found;
	} */

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

				hashTagIndex=indexManager.forNodes("HashTag-FullText", MapUtil.stringMap(IndexManager.PROVIDER, "lucene", "type", "fulltext"));
				hashTagIndex.add(hashTagNode, "HashTagKey", hashTagNode.getProperty("HashTagKey"));

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


	/*@SuppressWarnings("deprecation")
	public Node findMentionedUserNode(String mentionedUser){
		Transaction tx=service.beginTx();
		Node mentionedUserNode=null;
		try{
			Index<Node> foundUserNode=indexManager.forNodes("UserIndex");
			mentionedUserNode=foundUserNode.query("UserNameKey", mentionedUser).getSingle();
			tx.success();
		}
		catch(Exception e){
			e.printStackTrace();
		}
		finally{
			tx.finish();
		}
		return mentionedUserNode;
	} */


	@SuppressWarnings("deprecation")
	public void createRelationShip(Node node1, Node node2, String relationshipname){
		Relationship relation;

		Transaction tx=service.beginTx();
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
				relation=node1.createRelationshipTo(node2, RelationType.ISARETWEETOF);
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
				relation=node1.createRelationshipTo(node2, RelationType.ISAREPLYTOTWEET);
				relation.setProperty("RepliesToTweet", "RepliesToTweet");
				relIndex.add(relation,"RepliesToTweet",relation.getProperty("RepliesToTweet"));
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

		Node nodetweet1=schema.createTweetNode(127,"I am awesome", 1, "California", links );
		Node nodetweet2=schema.createTweetNode(13,"Chicha rocks!", 2, "Bloomington",links);
		Node nodetweet3=schema.createTweetNode(12,"Chintan hurrr", 1, "California", links );
		Node nodetweet4=schema.createTweetNode(12,"Rohit get lost!", 3, "Sunny Vale", links );

		Node nodeuser1=schema.createUserNode("aish");
		Node nodeuser2=schema.createUserNode("rohit");
		Node nodeuser3=schema.createUserNode("aish");

		Node nodehashtag1=schema.createHashTag("chicha_isbest1");
		Node nodehashtag2=schema.createHashTag("aish_isbest!");
		Node nodehashtag3=schema.createHashTag("chicha_isbest");

		schema.createRelationShip(nodeuser1, nodetweet1, "Tweets");
		schema.createRelationShip(nodeuser1, nodetweet2, "tweets");
		schema.createRelationShip(nodeuser2, nodetweet3, "Tweets");
		schema.createRelationShip(nodeuser2, nodetweet4, "tweets");

		schema.createRelationShip(nodetweet1, nodehashtag1, "contains")	;
		schema.createRelationShip(nodetweet2, nodehashtag2, "contains")	;

		Node nodehashtagcheck=schema.createHashTag("demo");
		//Node nodehashtagcheck1=schema.createHashTag("chicha_isbest!");
		//System.out.println(schema.checkNode(DynamicLabel.label("HashTag"),"HashTagKey","chich_isbest!"));

		Node retweetNode=schema.createTweetNode(1, "I am awesome", 2, "USA", links);


		if(schema.checkNode(DynamicLabel.label("Tweet"), "MessageID",129)!=null)
			System.out.println("found aish");
		else
			System.out.println("not found aish");



//	if(schema.findOriginalTweetNode(127)!=null)
//		System.out.println("original node present  " );
//	else
//		System.out.println("not present original!");

//	if
//	{
//		Transaction tx = schema.service.beginTx();
//		Node found = null;
//		
//		try {
////			Index<Node> foundTweetIndex = schema.indexManager.forNodes("TweetFullText");
////			found = foundTweetIndex.query("Message", "am Rohit").getSingle();
//			
////			Index<Node> foundHashTagIndex = schema.indexManager.forNodes("HashTag-FullText");
////			found=foundHashTagIndex.query("HashTagKey","chiCha_IsBest").getSingle();
//			
//			tx.success();
//		} catch (Exception e) {
//			e.printStackTrace();
//		} finally {
//			tx.finish();
//		}
//		
//		if (found != null) {
//			System.out.println("Found index search item");
//		} else 
//			System.out.println("cannot find index search item !");
//		
		schema.service.shutdown();
		System.out.println("ends");

	}
}