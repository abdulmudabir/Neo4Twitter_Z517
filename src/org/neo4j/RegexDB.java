package org.neo4j;
/*********************************************************************************************************************************************************************
 * @Author: 	Neo4J Team
 * @Course: 	z517 - Web Programming
 * @Date: 		18th April, 2014
 * @Description:The file contains code to parse the given twitter dataset and create a graph database. 	
 ***********************************************************************************************************************************************************************/

import java.io.*;
import java.text.*;
import java.util.*;

//Importing Neo4J binaries
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.factory.*;
import org.neo4j.graphdb.schema.Schema;


/**
 * The object of this class is used to store the tweet information while parsing the dataset.
 * An ArrayList of the class is passed to every thread to facilitate sequential batch processing.
 * @category: Class
 */
class Dataset
{

	/**
	 * Contains the tweet id of the original message. It has a valid value in cases where a tweet A is retweet of tweet B. This variable in that case
	 * will contain the tweet id of tweet B. Otherwise, its value will be 0.
	 */
	long retweet_original_message_id;
	/**
	 * Contains the tweet id of the original message. It has a valid value in cases where a tweet A is reply to tweet B. This variable in that case
	 * will contain the tweet id of tweet B. Otherwise, its value will be 0.
	 */
	long replyto_message_id;
	/**
	 * Contains the tweet id of the tweet.
	 */
	long tweet_id;
	/**
	 * Contains the username of the user who has tweeted the tweet.
	 */
	String username;
	/**
	 * Contains the actual tweet.
	 */
	String tweet;
	/**
	 * Contains the location from where the user has tweeted the tweets.
	 */
	String Location;
	/**
	 * Contains the username of the original message. It has a valid value in cases where a tweet A is retweet to tweet B. This variable in that case
	 * will contain the username of tweet B. Otherwise, it will be empty.
	 */
	String retweet_username;
	/**
	 * Contains the username of the original message. It has a valid value in cases where a tweet A is reply to tweet B. This variable in that case
	 * will contain the username of tweet B. Otherwise, it will be empty.
	 */
	String reply_username;
	/**
	 * Contains a list of ArrayList of hashtags in a tweet.
	 */
	ArrayList<StringBuffer> hashtags_list;
	/**
	 * Contains a list of ArrayList of usernames mentioned in a tweet. It does not contain the name of the user to whom you are replying. Neither
	 * does it contain the username of the original tweet in case of retweets.
	 */
	ArrayList<StringBuffer> username_list;
	/**
	 * Stores the timestamp in UNIX format
	 */
	long unix_time;
	/** 
	 * Specifies whether the tweet is a retweet or Not. Its a boolean variable.
	 */
	boolean isRetweet = false;
	
}

class RegexDB extends Thread 
{
	boolean callThread = false;
	
	private static enum RelationType implements RelationshipType
	{
		TWEETS, RETWEETS, CONTAINS, IS_A_RETWEETOF, MENTIONS, IS_A_REPLYTOTWEET,REPLIES;
	}
	static GraphDatabaseService service;
	private static final String path="C:\\Users\\Rohit\\Desktop\\Neo4j\\obama_20121015_20121115.txt";
	static Transaction tx ;
	static Map<String, Long> createdNodeMap  = new HashMap<String, Long>();;
	static Map<String, Long> createdTweetNodeMap = new HashMap<String, Long>();;
	
	static ArrayList<Dataset> dataset = new ArrayList<Dataset>();

	static int datasetTweetCount = 0;
	static int numberofBatches = 0;
	static int batchsize = 1000;
		
	/**
	 * @param args
	 */
	
	private static void configureDatabase() 
	{
		service = new GraphDatabaseFactory().
				newEmbeddedDatabaseBuilder(path).
			    setConfig( GraphDatabaseSettings.relationship_keys_indexable, "Tweets,Retweets,Contains,IsARetweetOf,Mentions,Replies,RepliesToTweet" ).
			    setConfig( GraphDatabaseSettings.relationship_auto_indexing, "true" ).
			    newGraphDatabase();
		registerShutdownHook(service);
		Transaction tx_label = service.beginTx();
        Schema schema = service.schema();
        try 
        {
        	schema.indexFor(DynamicLabel.label("User")).on("id").create();
        	schema.indexFor(DynamicLabel.label("Tweet")).on("TimeStamp").create();
            schema.indexFor(DynamicLabel.label("Tweet")).on("id").create();
            schema.indexFor(DynamicLabel.label("Tweet")).on("MessageID").create();
            schema.indexFor(DynamicLabel.label("HashTag")).on("id").create();
        }
        catch (ConstraintViolationException e) 
        {
        	System.out.println("in configure database: " + e);
        }            
        tx_label.success();
        tx_label.close();
    }
	
	public static void AnalyzeDataset(String dataSetPath)
	{
		try 
		{
			BufferedReader br = new BufferedReader(new FileReader(dataSetPath));
			/*while(br.readLine()!=null)
			{
				datasetTweetCount++;
			}*/
			
			System.out.println("The result of Analysis: ");
			System.out.println("Total Tweets:" + datasetTweetCount);
			numberofBatches = datasetTweetCount/batchsize; 
			if(datasetTweetCount % batchsize > 0)
			{
				numberofBatches += 1;
			}
			System.out.println("The total number of batches : " + numberofBatches);
			System.out.println("The size of each of the batch:" + batchsize);
			br.close();
		} 
		catch (Exception e) 
		{
			e.printStackTrace();
		}
		
	}
/*	public static void main(String[] args) 
	{
		
		parseAndCreateDatabase();		
		
		
	}*/
	
	/**
	 * 
	 */
	public void parseAndCreateDatabase()
	{
			Calendar cal = Calendar.getInstance();
    		cal.getTime();
    		SimpleDateFormat sdf = new SimpleDateFormat("HH:mm:ss");
    		System.out.println("Start Time: " + sdf.format(cal.getTime()));
    	
			int batchesCompleted = 0;
			
			configureDatabase();
			System.out.println("Configuring Database schema complete. Now parsing the Dataset initiated...");
			String dataSetPath;

			Dataset entry = new Dataset();

			try {
				dataSetPath = "C:\\Users\\Chintan Gosalia\\Desktop\\web programming project\\obama_20121015_20121115.txt";
				dataSetPath.replace('\\', '/');
				System.out.println("Analyzing the Dataset. Please wait...");
				System.out.println();
				
				AnalyzeDataset(dataSetPath);				
				
				BufferedReader file = new BufferedReader(new FileReader(dataSetPath));
				int i = 0;
				String fileLine;
				tx = service.beginTx();
				while((fileLine =file.readLine()) != null && i < datasetTweetCount)
				{
					try
					{
					String[] temp = fileLine.split("\\|");
					if((temp.length == 9 || temp.length == 8))
					{
						
							if(fileLine.contains("RT @"))
							{
								entry.isRetweet = true;
							}
							entry.tweet_id = Long.parseLong(temp[0]);
							
							//converting the timestamp to unix time format
							DateFormat formatter;
							Date date = null;
							formatter = new SimpleDateFormat("yyyy-mm-dd hh:mm:ss");
							date = formatter.parse(temp[1]);
							entry.unix_time = date.getTime() / 1000L;

							//CODE TO RETREIVE THE DATE FROM UNIX DATE
							/*Date date1 = new Date(unix_time*1000L); // *1000 is to convert seconds to milliseconds
							SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss z"); // the format of your date
							String formattedDate = sdf.format(date1);*/
						
							entry.retweet_original_message_id =  Long.parseLong(temp[5]);
							entry.replyto_message_id = Long.parseLong(temp[7]);
							entry.username = temp[2];
							entry.tweet = temp[3];
							entry.Location= temp[4];
							entry.retweet_username = temp[6];

							if(temp.length == 8)
							{
								entry.reply_username = "";
							}
							else
							{
								entry.reply_username = temp[8];
							}	
							entry.hashtags_list = new ArrayList<StringBuffer>();
							entry.username_list = new ArrayList<StringBuffer>();
							int isHashTag = 0;
							int isUserName = 0;
							StringBuffer tempHashTag = new StringBuffer(); 
							StringBuffer tempUserName = new StringBuffer();
							
							
							for(char ch: entry.tweet.toCharArray())
							{
								if(ch == ' ' || ch== '.' || ch == ',' || ch == '&')
								{
									if(isHashTag ==1)
									{
										entry.hashtags_list.add(tempHashTag);			        		
										tempHashTag = new StringBuffer();
										isHashTag = 0;
									}			        		
									if(isUserName == 1)
									{
										if( entry.reply_username.equals(tempUserName.toString()) == false && entry.retweet_username.equals(tempUserName.toString())==false )
										{
											entry.username_list.add(tempUserName);
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
									if(ch != ':')
									{
										tempUserName.append(ch);
									}
								}
							}				
							if(isHashTag ==1)
							{
								entry.hashtags_list.add(tempHashTag);			        		
								tempHashTag = new StringBuffer();
								isHashTag = 0;
							}			        		
							if(isUserName == 1)
							{

								if( entry.reply_username.equals(tempUserName.toString()) == false && entry.retweet_username.equals(tempUserName.toString())==false)
								{
									entry.username_list.add(tempUserName);
								}			        							        		
								tempUserName = new StringBuffer();
								isUserName = 0;
							}
							dataset.add(entry);
							//create the nodes and relationships
						/*************************************************************/
							if(i==0)
							{
								System.out.println("Batch processing initiated..");
							}
						 						
							if( (i!=0 && i%batchsize==0 && batchesCompleted<=numberofBatches) || (batchesCompleted == numberofBatches-1 && (datasetTweetCount%batchsize != 0) && i == datasetTweetCount))
							{
								batchesCompleted++;
								System.out.println("Please wait. Batch "+ batchesCompleted+" processing in progress...");
								RegexDB dbCreateThread = new RegexDB();
								dbCreateThread.setPriority(Thread.MAX_PRIORITY);
								dbCreateThread.run();
								dataset = new ArrayList<Dataset>();
								System.gc();
							}
							else
							{						
								entry = new Dataset();
							}
							/*************************************************************/
						}					
					}
					catch(Exception e)
					{
						continue;
					}
					
					i++;
					entry.isRetweet = false;
					System.out.println(i);
				}
				/*if(batchesCompleted == 40)
				{
					System.out.println("Stopped at: " + i);
					System.out.println(entry.tweet_id);
					System.out.println(entry.tweet);
				}*/
				file.close();
				tx.close();
				service.shutdown();
			}
			catch (Exception e) {
				e.printStackTrace();
				System.out.println("in parse and create DB: " + e);
			}
			
			Calendar cal1 = Calendar.getInstance();
	    	cal.getTime();
	    	SimpleDateFormat sdf1 = new SimpleDateFormat("HH:mm:ss");
	    	System.out.println("End Time: "+sdf1.format(cal1.getTime()) );
	}
	
	
	public void run()
	{
	    
		for(Dataset entry: dataset)
		{			
			Node Username=createUserNode(entry.username);
			Node Tweet= createTweetNode_New(entry.tweet_id, entry.tweet, entry.unix_time, entry.Location);
			
			boolean isTweet = true;
			if(entry.isRetweet)
			{	isTweet = false;
			createRelationShip(Username, Tweet, "Retweets");
			}
			
			if(!entry.reply_username.equals(""))
			{
				isTweet = false;
				createRelationShip(Username, Tweet, "Replies");
			}

			if(isTweet == true)
			{
				createRelationShip(Username, Tweet, "Tweets");
			}


			if(entry.hashtags_list.size() > 0)
			{
				Node HashTag;
				for(StringBuffer item:entry.hashtags_list)
				{
					HashTag=createHashTag(item.toString());
					createRelationShip(Tweet, HashTag, "Contains");
				}			        	
			}
			
			if(entry.username_list.size() > 0)
			{
				Node MentionedUser;
				for(StringBuffer temp_username:entry.username_list)
				{
					MentionedUser = createUserNode(temp_username.toString());
					createRelationShip(Tweet, MentionedUser, "Mentions");
				}			        	
			}
			
			if(entry.isRetweet && entry.retweet_original_message_id!=0)
			{
				connectTweets(entry.retweet_original_message_id,Tweet,"isretweetof");
			}

			if(!entry.reply_username.equalsIgnoreCase(""))
			{
				connectTweets(entry.replyto_message_id, Tweet, "RepliesToTweet");
			}			
		}
	}
		
	public Node createTweetNode(long messageID,String message, long timeStamp, String location)
	{
		Node tweetNode = null;
		try 
		{
			//Label tweetLabel = DynamicLabel.label("Tweet");
			Long value = createdTweetNodeMap.get("Tweet:"+messageID);
			if(value != null)
				tweetNode = checkNode(value);
			if(tweetNode==null)
			{
				return null;
			}
	
			tx.success();
		} 
		catch (Exception e) 
		{
			System.out.println("in create tweet node: " + e);	
		}	
		return tweetNode;
	}
	
	public Node createTweetNode_New(long messageID,String message, long timeStamp, String location)
	{
		Node tweetNode = null;
		try 
		{
				Label tweetLabel = DynamicLabel.label("Tweet");				
				tweetNode = service.createNode(tweetLabel);
				tweetNode.setProperty("MessageID",messageID);
				tweetNode.setProperty("id",messageID);
				tweetNode.setProperty("Message", message);
				tweetNode.setProperty("TimeStamp",timeStamp);
				tweetNode.setProperty("Location", location);
				createdTweetNodeMap.put("Tweet:"+messageID, tweetNode.getId());
				tx.success();
		} 
		catch (Exception e)
		{			
			System.out.println("In create tweet node new" + e);
		}	
		return tweetNode;
	}

	
	public Node checkNode(Long value)
	{
		Node node =null;
		try 
		{	
			node = service.getNodeById(value);	
		} 
		catch (Exception e) 
		{
			System.out.println("in check node:" + e);
		}
		return node;
	}
	
	
	public Node createUserNode(String username)
	{
		Node user=null;
		try 
		{
			Label userLabel=DynamicLabel.label("User");
			Long value = createdNodeMap.get("User:"+username);
			if(value != null)
				user=checkNode(value);
			if(user==null){
				user=service.createNode(userLabel);
				user.setProperty("UserNameKey", username);
				createdNodeMap.put("User:"+username, user.getId());
			}			
			tx.success();
		}
		catch (Exception e) {
			System.out.println("in create user node:" + e);
		}		
		return user;
	}
	
	
	
	public Node createHashTag(String hashTag)
	{
		Node hashTagNode=null;
		try
		{
			Label hashTagLabel=DynamicLabel.label("HashTag");
			Long value = createdNodeMap.get("HashTag:"+hashTag.toLowerCase());
			if(value!= null)
			{
				hashTagNode = checkNode(value);
			}
			if(hashTagNode==null)
			{
				hashTagNode=service.createNode(hashTagLabel);
				hashTagNode.setProperty("HashTagKey", hashTag.toLowerCase());
				createdNodeMap.put("HashTag:"+hashTag, hashTagNode.getId());
			}
			tx.success();
		}
		catch(Exception e){
			System.out.println("in create hashtag:" + e);
		}
		return hashTagNode;
	}
	
	
	public void createRelationShip(Node node1, Node node2, String relationshipname)
	{
		Relationship relation;
		try
		{
			if(relationshipname.equalsIgnoreCase("Tweets")){
				relation=node1.createRelationshipTo(node2, RelationType.TWEETS);
				relation.setProperty("Tweets", "Tweets");
			}
			else if (relationshipname.equalsIgnoreCase("Retweets")){
				relation=node1.createRelationshipTo(node2,RelationType.RETWEETS);
				relation.setProperty("Retweets", "Retweets");
			}
			else if(relationshipname.equalsIgnoreCase("Contains")){
				relation=node1.createRelationshipTo(node2,RelationType.CONTAINS);
				relation.setProperty("Contains", "Contains");
			}
			else if(relationshipname.equalsIgnoreCase("IsARetweetOf")){
				relation=node1.createRelationshipTo(node2, RelationType.IS_A_RETWEETOF);
				relation.setProperty("IsARetweetOf", "IsARetweetOf");
			}
			else if(relationshipname.equalsIgnoreCase("Mentions")){
				relation=node1.createRelationshipTo(node2, RelationType.MENTIONS);
				relation.setProperty("Mentions", "Mentions");
			}
			else if(relationshipname.equalsIgnoreCase("Replies")){
				relation=node1.createRelationshipTo(node2, RelationType.REPLIES);
				relation.setProperty("Replies", "Replies");
			}
			else if(relationshipname.equalsIgnoreCase("RepliesToTweet")){
				relation=node1.createRelationshipTo(node2, RelationType.IS_A_REPLYTOTWEET);
				relation.setProperty("RepliesToTweet", "RepliesToTweet");
			}
			tx.success();
		} 
		catch (Exception e) 
		{
			System.out.println("in create Relationship: " + e);
		}
	}
	public void connectTweets(long retweet_original_message_id, Node Tweet, String string) 
	{
		Node originalNode = null;
		Node tweetNode = Tweet;
		try 
		{
			ResourceIterator<Node> nodes = service.findNodesByLabelAndProperty(DynamicLabel.label("Tweet"), "MessageID", retweet_original_message_id).iterator();
			if(nodes.hasNext())
			{
				originalNode = nodes.next();
				//nodes = service.findNodesByLabelAndProperty(DynamicLabel.label("Tweet"), "MessageID", tweet_id).iterator();
				if(Tweet != null){
					//tweetNode = nodes.next();
					if(string.equalsIgnoreCase("isretweetof"))
						createRelationShip(tweetNode, originalNode, "IsARetweetOf");
					else if(string.equalsIgnoreCase("RepliesToTweet"))
						createRelationShip(tweetNode, originalNode, "RepliesToTweet");
				}
			}
			tx.success();
		}
		catch (Exception e) 
		{
			System.out.println("in connect tweets:" + e);
		}

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


}