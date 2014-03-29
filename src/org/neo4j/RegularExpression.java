package org.neo4j;

import java.io.*;
import java.text.*;
import java.util.*;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;

public class RegularExpression {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
		System.out.println("Enter the data set path:");
		String dataSetPath;
		Schema schema=new Schema();



		try {
			dataSetPath = br.readLine();
			//		dataSetPath = "C:\\Users\\Chintan Gosalia\\Desktop\\web programming project\\obama_20121015_20121115.txt";
			dataSetPath.replace('\\', '/');
			BufferedReader file = new BufferedReader(new FileReader(dataSetPath));
			int i = 0;
			String fileLine;
			//return;
			boolean isRetweet = false;


			while((fileLine =file.readLine()) != null && i < 10 && !fileLine.equals(""))
			{
				System.out.println(fileLine);
				String[] temp = fileLine.split("\\|");
				if((temp.length == 9 || temp.length == 8))
				{
					//determining if it is a retweet
					if(fileLine.contains("RT @"))
					{
						isRetweet = true;
					}

					//the tweet id
					long tweet_id = Long.parseLong(temp[0]);

					//converting the timestamp to unix time format
					DateFormat formatter;
					Date date = null;
					long unix_time;
					formatter = new SimpleDateFormat("yyyy-mm-dd hh:mm:ss");
					date = formatter.parse(temp[1]);
					unix_time = date.getTime() / 1000L;

					//CODE TO RETREIVE THE DATE FROM UNIX DATE
					/*Date date1 = new Date(unix_time*1000L); // *1000 is to convert seconds to milliseconds
			        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss z"); // the format of your date
			        String formattedDate = sdf.format(date1);
			        System.out.println(formattedDate);*/	

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
					ArrayList<String> links_list = new ArrayList<String>();
					int isHashTag = 0;
					int isUserName = 0;
					StringBuffer tempHashTag = new StringBuffer(); 
					StringBuffer tempUserName = new StringBuffer();
					int linkIndex = 0;
					String fileLineSub;


					while( linkIndex < tweet.length())
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
					}




					for(char ch: tweet.toCharArray())
					{
						if(ch == ' ' || ch== '.' || ch == ',')
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
							if(ch != ':')
								tempUserName.append(ch);
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

					/*
			        System.out.println();
			        System.out.println();
			        //create the nodes
			        System.out.println("Create Node for User:" + username);
			        System.out.println("Properties:");
			        System.out.println("Tweet_id:" +tweet_id);
			        System.out.println("TimeStamp:" + unix_time);
			        System.out.println("Username:" + username);
			        System.out.println("Tweet:" + tweet);
			        System.out.println("Location:" + Location);

			        System.out.println("The hash tags:");
			        System.out.println(hashtags_list.size());
			        for(StringBuffer temp1:hashtags_list)
			        {
			        	System.out.println(temp1);
			        }

			        System.out.println("The users mentioned in the tweet:");
			        System.out.println(username_list.size());
			        for(StringBuffer temp1:username_list)
			        {
			        	System.out.println(temp1);
			        }

			        System.out.println("The retweet username:" + retweet_username);
			        System.out.println("The retweet id:" + retweet_original_message_id);

			        System.out.println("The reply username:" + reply_username);
			        System.out.println("The reply id:" + replyto_message_id);

			        System.out.println("The links are:" + links_list.size());
			        for(String temp1:links_list)
			        {
			        	System.out.println(temp1);
			        }
					 */

					//create the nodes and relationships

					// create username node
					Node Username=schema.createUserNode(username);


					// create tweet node
					Node Tweet=schema.createTweetNode(tweet_id, tweet, unix_time, Location, links_list.toArray());


					//create relationship between username node and tweet node : tweets 
					if(!reply_username.equals(""))
					{
						schema.createRelationShip(Username, Tweet, "Replies");
						//retrieve or create the original tweet
						Node replyToNode= schema.checkNode(DynamicLabel.label("Tweet"), "MessageID", replyto_message_id);
						//establish relationship between tweet and the ReplyTweet : is_a_reply
						if(replyToNode!=null)
							schema.createRelationShip(Tweet, replyToNode, "RepliesToTweet");

					}
					else if(!isRetweet)
						schema.createRelationShip(Username, Tweet, "Tweets");

					else if(retweet_original_message_id!=0){
						Node original_message_node=schema.checkNode(DynamicLabel.label("Tweet"), "MessageID", retweet_original_message_id);
						if(original_message_node!=null){
							schema.createRelationShip(Tweet, original_message_node, "IsARetweetOf");
						}

					}

					//create hashtag node
					if(hashtags_list.size() > 0)
					{
						Node HashTag;
						for(StringBuffer item:hashtags_list)
						{
							//create or retrieve existing node for each of the hashtags
							HashTag=schema.createHashTag(item.toString());

							//create a relationship between hashtag and the tweet node: contains
							schema.createRelationShip(Tweet, HashTag, "Contains");
						}			        	
					}

					//create mentions relationship 
					if(username_list.size() > 0)
					{
						Node MentionedUser;
						for(StringBuffer temp_username:username_list)
						{
							//create or retrieve existing node for each of the username


							//create a relationship between username and the tweet node: mentions
							Node original_user_node=schema.checkNode(DynamicLabel.label("User"), "UserNameKey", temp_username.toString());
							if(original_user_node!=null){
								schema.createRelationShip(Tweet, original_user_node, "Mentions");
							}

						}			        	
					}












				}
				else
				{
					continue;
				}

				i++;


			}
			System.out.println("The number of lines is:" + i);
			file.close();
		}
		catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}


	}

}

