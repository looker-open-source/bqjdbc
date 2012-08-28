import java.io.IOException;
import java.net.URLEncoder;
import java.sql.SQLException;
import java.util.List;

import net.starschema.clouddb.cmdlineverification.Oauth2Bigquery;

import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.model.DatasetList.Datasets;


public class authtest {

	/**
	 * @param args
	 * @throws InterruptedException 
	 * @throws IOException 
	 */

	public static void main(String[] args) throws InterruptedException, IOException {
		System.out.println(URLEncoder.encode("230262422504.apps.googleusercontent.com","UTF-8"));	
		System.out.println(URLEncoder.encode("nvioI6U4DvmRnXJJLKz1TmBQ","UTF-8"));
		Bigquery bigquery = null;
		try {
			bigquery = Oauth2Bigquery.authorizeviainstalled("230262422504.apps.googleusercontent.com", "nvioI6U4DvmRnXJJLKz1TmBQ");
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}	
	
		 try {
			 List<Datasets> data = bigquery.datasets().list("starschema.net:clouddb").execute().getDatasets();
			System.out.println(data.get(0).getId());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		 
		 boolean waiting = true;
		 int Sleeping = 0;
		 int left = 3600+100;
		 while(waiting)
		 {
			 Thread.sleep(1000);
			 System.out.println("Thread is sleeping for: "+Sleeping+" Seconds");
			 left --;
			 System.out.println("Left from sleeping: "+ left+ " Seconds");
			 if(left <=0)
			 {
				 waiting = false;
			 }
			 Sleeping++;
		 }
		 
		 try {
			 List<Datasets> data = bigquery.datasets().list("starschema.net:clouddb").execute().getDatasets();
			System.out.println(data.get(0).getId());
		} catch (IOException e) {
			System.out.println("Token expired");
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}

