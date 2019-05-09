package loadCSVtoDatabase.examples;

import java.io.FileReader;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;

import com.opencsv.CSVReader;

public class ImportCsv
{
		public static void main(String[] args)
		{
				readCsv();
				readCsvUsingLoad();
		}

		private static void readCsv()
		{

				try (CSVReader reader = new CSVReader(new FileReader("/home/costrategix/Desktop/Upload.csv"), ',');Connection connection = DBConnection.getConnection();)
				{
						String insertQuery = "Insert into user (name, phone_no, email_id) values (?,?,?)";
						PreparedStatement pstmt = connection.prepareStatement(insertQuery);
						String[] rowData = null;
						int i=0; ;
						while((rowData = reader.readNext()) != null){
						for (String data : rowData)
						{
							    //sets name,phone_no and email_id
								pstmt.setString((i % 3) + 1, data);
                                
								// add batch
								if (++i % 3 == 0)
								pstmt.addBatch();
								//	pstmt.execute();

								// insert when the batch size is 10
								if (i % 30 == 0)
										pstmt.executeBatch();
								
						}}
						System.out.println("Data Successfully Uploaded");
				}
				catch (Exception e)
				{
						e.printStackTrace();
				}

		}

		private static void readCsvUsingLoad()
		{
				try (Connection connection = DBConnection.getConnection())
				{

						String loadQuery = "LOAD DATA LOCAL INFILE '" + "/home/costrategix/Desktop/Upload.csv" + "' INTO TABLE user FIELDS TERMINATED BY ','"
										+ " LINES TERMINATED BY '\n' (name, phone_no, email_id) ";
					//	System.out.println(loadQuery);
						Statement stmt = connection.createStatement();
						stmt.execute(loadQuery);
						System.out.println("Data Successfully Uploaded");
						
				}
				catch (Exception e)
				{
						e.printStackTrace();
				}
		}

}
