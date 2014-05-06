/*********************************************************************************************************************************************************************
 * @Author: 	Neo4J Team
 * @Course: 	z517 - Web Programming
 * @Date: 		20th April, 2014
 * @Description:Contains the implementation for querying the DB for required data. 	
 ***********************************************************************************************************************************************************************/

package org.neo4j;



import java.io.IOException;
import java.net.URL;
import java.security.CodeSource;
import java.security.ProtectionDomain;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;



/**
 * Servlet implementation class ServletForD3
 */


/**
 * @author Neo4J Team	
 */
//This class serves a point of integration with the D3.js Team.
public class ServletForD3 extends HttpServlet {
	private static final long serialVersionUID = 1L;

    /**
     * Default constructor. 
     */
    public ServletForD3() {
        // TODO Auto-generated constructor stub
    }

	/**
	 * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse response)
	 */
	/* (non-Javadoc)
	 * @see javax.servlet.http.HttpServlet#doGet(javax.servlet.http.HttpServletRequest, javax.servlet.http.HttpServletResponse)
	 */
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		// TODO Auto-generated method stub
		// TODO Auto-generated method stub
		
				try {
				response.setContentType("text/html");
				java.io.PrintWriter out = response.getWriter();
		
				//retriving the parameters from the url
				String relation = request.getParameter("mydropdown");
				String startTimeStamp = request.getParameter("startDate") + " " + request.getParameter("startTime");
				String endTimeStamp = request.getParameter("endDate") + " " + request.getParameter("endTime");
				
				startTimeStamp = startTimeStamp.replaceAll("/","-");
				endTimeStamp = endTimeStamp.replaceAll("/","-");

//				out.println("Consolidated timestamp");
//				out.println(startTimeStamp);
//				out.println(endTimeStamp);
//				out.println("<br>");
				 
				//converting the time stamp into UNIX time stamp				
				DateFormat formatter;
				Date date = null;
				formatter = new SimpleDateFormat("mm-dd-yyyy HH:mm:ss");		
				date = formatter.parse(startTimeStamp);
				
				long startTS= date.getTime()/1000;
				
				date = formatter.parse(endTimeStamp);
				
				long endTS = date.getTime()/1000;
				out.println("Unix TimeStamp:");
				long st = Long.parseLong(startTimeStamp);
				 long et = Long.parseLong(endTimeStamp);
				//calling the search function to retrieve the result in JSON object
				 //the jSON object will be written on location that the D3.js team will already know
				System.out.println("JSON Started....");
				SearchQuery searchObject = new SearchQuery();
				searchObject.getJsonFromMessageList(relation, st, et);
				System.out.println("JSON ended....");
				/*
				
				
				
				Date date1 = new Date(startTS*1000L); // *1000 is to convert seconds to milliseconds
				SimpleDateFormat sdf = new SimpleDateFormat("mm-dd-yyyy HH:mm:ss"); // the format of your date
				String formattedDate = sdf.format(date1);
				
				out.println(formattedDate);
				
				Date date2 = new Date(endTS*1000L); // *1000 is to convert seconds to milliseconds
				SimpleDateFormat sdf2 = new SimpleDateFormat("mm-dd-yyyy HH:mm:ss"); // the format of your date
				String formattedDate2 = sdf.format(date2);
				out.println(formattedDate2);
				*/
				 
				
				//out.println("</body></html>");
				
				
				
				
				
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				System.out.println("Request received.");
	}

	/**
	 * @see HttpServlet#doPost(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		// TODO Auto-generated method stub
		//sending a Status OK response when the JSON is don so that the D3.js team can visualize it
		System.out.println("Response sending initiated...");
		response.setStatus(200);
		response.getWriter().write("OK");
		System.out.println("Response sent...");
	}

}
