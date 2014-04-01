package org.neo4j;

import java.io.*;
import java.text.*;
import java.util.*;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.IndexManager;
import org.neo4j.graphdb.index.RelationshipIndex;
import org.neo4j.helpers.collection.MapUtil;

public class RegularExpression {

	GraphDatabaseService service;
	private static final String path="D:\\Check73.graphdb";
	IndexManager indexManager;
	Index<Node> userIndex, tweetIndex, hashTagIndex;
	RelationshipIndex relIndex;
	/**
	 * @param args
	 */
	private static enum RelationType implements RelationshipType{
		TWEETS, RETWEETS, CONTAINS, ISARETWEETOF, MENTIONS, ISAREPLYTOTWEET,REPLIES;
	}
	public RegularExpression() {
		// TODO Auto-generated constructor stub
		service =new GraphDatabaseFactory().newEmbeddedDatabase(path);
		registerShutdownHook(service);
		indexManager = service.index();

	}
	@SuppressWarnings("deprecation")
	public Node createTweetNode(long messageID,String message, long timeStamp, String location){//, String[] links){
		Transaction tx= service.beginTx();

		Node tweetNode = null;
		try {
			Label tweetLabel = DynamicLabel.label("Tweet");
			tweetNode = checkNode(tweetLabel,"MessageID", messageID);
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
				//tweetNode.setProperty("Links", links);

				tweetIndex = indexManager.forNodes("TweetFullText", MapUtil.stringMap(IndexManager.PROVIDER, "lucene", "type", "fulltext"));
				tweetIndex.add(tweetNode, "Message", tweetNode.getProperty("Message"));

				tweetIndex.add(tweetNode,"MessageID",tweetNode.getProperty("MessageID"));
			}
			else
				System.out.println();

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
		return null;
	}
	@SuppressWarnings("deprecation")
	public Node createUserNode(String username){
		Transaction tx=service.beginTx();
		Node user=null;
		try {
			Label userLabel=DynamicLabel.label("User");
			user=checkNode(userLabel, "UserNameKey", username);
			if(user==null){
				user=service.createNode(userLabel);
				user.setProperty("UserNameKey", username);
				user.setProperty("id", username);
				userIndex=indexManager.forNodes("UserIndex");
				userIndex.add(user, "UserNameKey", user.getProperty("UserNameKey"));
			}
			else
				System.out.println();
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
			if(hashTagNode==null){
				hashTagNode=service.createNode(hashTagLabel);
				hashTagNode.setProperty("HashTagKey", hashTag);
				hashTagNode.setProperty("id", hashTag);
				hashTagIndex=indexManager.forNodes("HashTag-FullText", MapUtil.stringMap(IndexManager.PROVIDER, "lucene", "type", "fulltext"));
				hashTagIndex.add(hashTagNode, "HashTagKey", hashTagNode.getProperty("HashTagKey"));

			}
			else
				System.out.println();
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
	public void connectTweets(long retweet_original_message_id, long tweet_id, String string) {
		Transaction tx = service.beginTx();
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
			tx.finish();
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
			while((fileLine =file.readLine()) != null && i<10000  && !fileLine.equals("")){
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
			System.out.println("The number of lines is:" + i);
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

