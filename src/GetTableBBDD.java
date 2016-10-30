package bkBBDD;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.InputStream;
import java.io.Reader;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

public class GetTableBBDD {
	 static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";  
	   static final String DB_URL = "jdbc:oracle:thin:@//IP:PORT/NAME_BBDD";
	   static final String DIR="C:\\Temp\\backup\\";
	   static final String USER = "BASIC_USER_ORACLE";
	   static final String PASS = "BASIC_PASS_ORACLE";
	   
	   public void getTable(String table) {
	   Connection conn = null;
	   Statement stmt = null;
	    int REC_START = 0;
	    int REC_END = 0;
	    int lastE =0;
	    String separator = System.getProperty("line.separator");
		String separatorCVS="§";
	    System.out.println("NEXT table : "+ table);
	    DateFormat df = new SimpleDateFormat("dd/MM/yyyy");
	   try{
		  Calendar c = Calendar.getInstance();

		  int dayOfWeek = (c.get(Calendar.DAY_OF_WEEK)-1);
	      String driver = "oracle.jdbc.driver.OracleDriver";
	      FileWriter fw = new FileWriter(DIR+dayOfWeek+"\\"+table+".csv");
          Class.forName(driver);

	      //STEP 3: Open a connection
	      System.out.println("Connecting to database...");
	      conn = DriverManager.getConnection(DB_URL,USER,PASS);

	      //STEP 4: Execute a query
	      System.out.println("Creating statement...");
	      stmt = conn.createStatement(
                  ResultSet.TYPE_SCROLL_INSENSITIVE, 
                  ResultSet.CONCUR_READ_ONLY);
	      String sql;
	      List<String> campos= new ArrayList<String>();
	      for (int e = 1; e <= 180; e++) {
	    	  
	    	  lastE =e;
	          long t11 = System.currentTimeMillis();
	          REC_START=REC_END;
	          REC_END = 500000 * e;
			  sql = "SELECT * FROM "+table+" where ROWNUM <5";
		      stmt.setFetchSize(1500);
		      ResultSet rs = stmt.executeQuery(sql);
		      ResultSetMetaData rmd = rs.getMetaData();
		      int total=rmd.getColumnCount();
		      List<Integer> dondeCLOB = new ArrayList<Integer>();
		      List<Integer> dondeBLOB = new ArrayList<Integer>();
		      List<Integer> dondeFecha= new ArrayList<Integer>();
		      campos.clear();
		      for(int i=1;i<=total;i++){
		    	  campos.add(rmd.getColumnName(i));
		    	  if(rmd.getColumnTypeName(i).equals("CLOB"))
		    		  dondeCLOB.add(i);
		    	  if(rmd.getColumnTypeName(i).equals("BLOB"))
		    		  dondeBLOB.add(i);
		    	  if(rmd.getColumnTypeName(i).equals("DATE"))
		    		  dondeFecha.add(i);
		    	  if(rmd.getColumnName(i).toLowerCase().equals("id"))
		    		  sql = "SELECT * FROM "+table+" where id >=  " + REC_START + " and id <" + REC_END +" ORDER BY ID";
		      }
		      
		      if(!dondeCLOB.isEmpty()){
		    	  
		    	  StringBuilder camposComa = new StringBuilder();
		    	    for (int i = 0; i < campos.size() - 1; i++) {
		    	    //data.length - 1 => to not add separator at the end
		    	        if (!dondeCLOB.contains(new Integer(i+1))) {
		    	        	camposComa.append(campos.get(i));
		    	        	camposComa.append(" , ");
		    	        }else {
		    	        	camposComa.append("(CASE WHEN "+campos.get(i)+" is null or DBMS_LOB.getlength("+campos.get(i)+")>3999 THEN null ELSE DBMS_LOB.substr("+campos.get(i)+",DBMS_LOB.getlength("+campos.get(i)+"),1) END) as "+campos.get(i)+" , "); 
		    	        	
		    	        }
		    	    }
		    	    camposComa.append(campos.get(campos.size() - 1).trim());
		    	    
		    	  
		    	  
		    	  sql = "SELECT "+camposComa.toString()+" FROM "+table+" where id >=  " + REC_START + " and id <" + REC_END +" ORDER BY ID";
		      }
		      System.out.println(sql);
		      
		      if(sql.contains("ROWNUM")){
		    	  sql = "SELECT * FROM "+table;
		    	  if(e>1){
		    		  e=200;
		    		  sql = "SELECT * FROM "+table+" where ROWNUM >121321312321321";
		    	  }
		    	 
		      }
		      
		      rs = stmt.executeQuery(sql);
		      StringBuilder sb = new StringBuilder();
		      int inicioFile=0;
		      int a=0;
		      if (!rs.isBeforeFirst() ) {    
		    	  e=200;
		    	 } 
		      while(rs.next()){
			      if(inicioFile==0){
			    	  StringBuilder out = new StringBuilder();
			    	  for (String o : campos)
			    	  {
			    		out.append("§");
			    	    out.append(o.toString());
			    	  }
			    	  fw.write("·"+out.toString()+separator+"·");
			      }
		    	  a++;
		    	  inicioFile++;
		    	      for (int i=1; i<=total; i++) {
		    	    		  if(dondeCLOB.contains(new Integer(i))){
		    	    			  sb.append("§"+rs.getString(i));
								  
		    	      		  }else if(dondeBLOB.contains(new Integer(i))){
		    	    			  Blob clobObject = rs.getBlob(i);
		    	    			  if(clobObject!=null){
		    	    				  try {
		    	    					  InputStream bin = clobObject.getBinaryStream();
		    	    					  
		    	    					  sb.append("§"+convertStreamToString(bin));
										} catch (Exception e2) {
											System.out.println(rmd.getColumnTypeName(i)+e2.getMessage());
						    	    		  sb.append("§NULL");
										}
			    	    		  }else{
		    	    				  sb.append("§null");
		    	    				  }
		    	      		  } else if(dondeFecha.contains(new Integer(i))){
		    	    			  Date dateObject = rs.getDate(i);
		    	    			  if(dateObject !=null){
		    	    				  try {
		    	    					  sb.append("§"+df.format(dateObject));
										} catch (Exception e2) {
											System.out.println(rmd.getColumnTypeName(i)+e2.getMessage());
						    	    		  sb.append("§NULL");
										}
			    	    		  }else{
		    	    				  sb.append("§null");
		    	    				  }
		    	      		  }else{
		    	    			 try {
		    	    				  sb.append("§"+rs.getString(i));
								} catch (Exception e2) {
									System.out.println(rmd.getColumnTypeName(i)+e2.getMessage()+"-id:"+rs.getString("id"));
									
				    	    		  sb.append("§NULL");
								}
		    	    		  }
		    	    	 
		    	      }
		    	      sb.append(separator+"·");
		    	      if(a>2000 || rs.isLast() ){
		    	    	  if(a<1000)
		    	    		  e=200;
		    	    	  fw.write(sb.toString() );
		    	    	  fw.flush();
		    	    	  sb.setLength(0);
		    	    	  a=0;
		    	      }
		    	   
		      }
		      if(e<200){
			      fw.close();
			      fw=new FileWriter(DIR+dayOfWeek+"\\"+table+e+".csv");
		      }
		      long t22 = System.currentTimeMillis();
	            System.out.println("e:" + e + "-"+ (t22 - t11) / 1000 + "s .-" +sql);
	            rs.close();
	      }
		      
		      
		      fw.close();
		      File fileExits = new File(DIR+dayOfWeek+"\\"+table+lastE+".csv");
	          if (fileExits.exists()){
			      BufferedReader br = new BufferedReader(new FileReader(DIR+dayOfWeek+"\\"+table+lastE+".csv"));     
			      if (br.readLine() == null) {
			    	  br.close();
			          System.out.println(DIR+dayOfWeek+"\\"+table+"200.csv and file empty");
			          try{
			              File fileTemp = new File(DIR+dayOfWeek+"\\"+table+lastE+".csv");
			                	 System.out.println(DIR+dayOfWeek+"\\"+table+lastE+".csv and file is empty"+ fileTemp.delete());
			                   //fileTemp.delete();
	
			            }catch(Exception e){
			               // if any error occurs
			               e.printStackTrace();
			            }
			      }
	          }
	          
	          fileExits = new File(DIR+dayOfWeek+"\\"+table+(lastE-1)+".csv");
	          if (fileExits.exists()){
			      BufferedReader br = new BufferedReader(new FileReader(DIR+dayOfWeek+"\\"+table+(lastE-1)+".csv"));     
			      if (br.readLine() == null) {
			    	  br.close();
			          try{
			              File fileTemp = new File(DIR+dayOfWeek+"\\"+table+(lastE-1)+".csv");
			                	 System.out.println(DIR+dayOfWeek+"\\"+table+(lastE-1)+".csv and file is empty"+ fileTemp.delete());
			                   //fileTemp.delete();
	
			            }catch(Exception e){
			               // if any error occurs
			               e.printStackTrace();
			            }
			      }
	          }
		      
		      stmt.close();
		      conn.close();
	   }catch(SQLException se){
	      //Handle errors for JDBC
	      se.printStackTrace();
	   }catch(Exception e){
	      //Handle errors for Class.forName
	      e.printStackTrace();
	   }finally{
	      //finally block used to close resources
	      try{
	         if(stmt!=null)
	            stmt.close();
	      }catch(SQLException se2){
	      }// nothing we can do
	      try{
	         if(conn!=null)
	            conn.close();
	      }catch(SQLException se){
	         se.printStackTrace();
	      }//end finally try
	   }//end try
	  
	}//end function
	   static String convertStreamToString(java.io.InputStream is) {

		    java.util.Scanner s = new java.util.Scanner(is).useDelimiter("\\A");
		    return s.hasNext() ? s.next() : "";
		}
}
