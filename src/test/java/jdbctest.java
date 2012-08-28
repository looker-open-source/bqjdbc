/**
 * NO LONGER IN USE
 */

import java.net.URLEncoder;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;

import net.starschema.clouddb.jdbc.BQConnection;



public class jdbctest {

	/**
	 * @param args
	 * @throws SQLException 
	 */
	public static void main(String[] args) throws SQLException {

		System.out.println("Testing JDBC driver");
		
		BQConnection con = null;
		try 
		{
		Class.forName("net.starschema.clouddb.jdbc.BQDriver");
		//con = (BQConnection)DriverManager.getConnection("jdbc:BQDriver:518060107194-lg3mgs32ej9l45kk4uo501rjkm7b5e10@developer.gserviceaccount.com:ce3619c27c0d0fd28e6981b59d9b1e9c09ff2a4e-privatekey.p12:dezzsokebqproject1sttry");
		con = (BQConnection) DriverManager.getConnection("jdbc:BQDriver:"+
				URLEncoder.encode("serviceacc230262422504-8hee1pe5017iq1i48jehgnj3vj517694@developer.gserviceaccount.com","UTF-8")+":"+
				URLEncoder.encode("C:\\key.p12","UTF-8")+":"+
				URLEncoder.encode("starschema.net:clouddb","UTF-8"));
		System.out.println("jdbc:BQDriver:"+
				URLEncoder.encode("serviceacc230262422504-8hee1pe5017iq1i48jehgnj3vj517694@developer.gserviceaccount.com","UTF-8")+":"+
				URLEncoder.encode("C:\\key.p12","UTF-8")+":"+
				URLEncoder.encode("starschema.net:clouddb","UTF-8"));
		System.out.println(con.getURLPART());
		} 
		catch (Exception e) 
		{
		System.out.println("Error in connection" + e);
		}	
		//final String Query = "Select STRFTIME_UTC_USEC(NOW(),'%x-%X%Z') AS day, FORMAT_UTC_USEC(NOW()) as d";
		//con.createStatement().executeQuery(Query);
		/*
		ResultSet res = con.getMetaData().getSchemas();
		res.first();
		while(!res.isAfterLast())
		{
			System.out.println(res.getString(1));
			res.next();
		}*/
		
		ResultSet res2 = con.getMetaData().getCatalogs();
		res2.first();
		while(!res2.isAfterLast())
		{
			System.out.println(res2.getString(1));
			//System.out.println(res2.getString(2));
			res2.next();
		}

		
		
		
		
		/*
		ResultSet res = con.getMetaData().getColumns(null,null,null,"");
		res.first();
		while (res.isAfterLast()!= true)
		{
			for(int i=1;i<=23;i++)
			{
				String myres = res.getString(i);
				if(myres == null) System.out.print("null ");
				else
				System.out.print(res.getString(i)+" ");
			}
			System.out.print("\n");
			res.next();
		}
		/*Res.next();
		System.out.println(Res.getString(1));
		System.out.println(Res.getString(2));*/
		//System.out.println(Res.getMetaData().getColumnType(1));
		//System.out.println(Res.getString(2));
		/*
		String xmlString = "<?xml version=\"1.0\" encoding=\"utf-8\"?><soap:Envelope xmlns:soap=\"http://schemas.xmlsoap.org/soap/envelope/\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xmlns:xsd=\"http://www.w3.org/2001/XMLSchema\"></soap:Envelope>"; 
		net.starschema.clouddb.jdbc.BQSQLXML sqlx = null;
		try
		{
		sqlx = new net.starschema.clouddb.jdbc.BQSQLXML(xmlString);
		}
		catch(SQLException e)
		{
			e.printStackTrace();
		}
		System.out.println(xmlString);
		System.out.println(sqlx.getString());*/
	}
}
