package dwh_project;
import java.sql.*;
import java.util.LinkedList;
import java.util.Queue;
import java.util.ArrayList;
import java.util.Iterator;
public class MeshJoin {
	
	

	public static void main(String args[]) {
		try {
				
				Queue<ArrayList<Master>> queue = new LinkedList<ArrayList<Master>>();
				
				Queue<ArrayList<Transactions> > Streams_Queue = new LinkedList<ArrayList<Transactions>>();
				
				ArrayList<FactTable> FactTableStyleArray = new ArrayList<FactTable>();
			
				Class.forName("com.mysql.cj.jdbc.Driver");
				
				Connection con=DriverManager.getConnection(
						"jdbc:mysql://localhost:3306/dw_project","root","1122");
				Statement stmt=con.createStatement();
				//creating queue for master Data 
				
				ResultSet m1 = stmt.executeQuery("select * from masterdata");
				//System.out.println(m1.getString(1));
//				 m1.next();
//				 String foundType = m1.getString(1);
//				 System.out.println(foundType);
				
				ArrayList<Master> masterDataList = new ArrayList<Master>(); // Create an ArrayList object
				int count=0;
				for(int i=0;i<100;i++)
				{
					m1.next();
					Master masterData = new Master(m1.getString(1),m1.getString(2),m1.getString(3),m1.getString(4),m1.getDouble(5));
					masterDataList.add(masterData);
					if(count>8)
					{
						queue.add(new ArrayList<Master>(masterDataList));
						masterDataList.clear();
						count=0;
					}
					else
					{
						count++;
					}
				}
				
				
				
//				
				//creating Queue for Transaction Data
				int Stream_count = 1;
				ArrayList<Transactions> Transaction_Data_Bucket = new ArrayList<Transactions>(); // Create an ArrayList object
				int numberOfStreams=0;
				while(true)
				{
					if(numberOfStreams >= 200)
					{
						break;
					}
					else
					{
						ResultSet t1 = stmt.executeQuery("select * from transactions limit 0,50");
						while(t1.next())
						{
							//System.out.println(t1.getString(1) + " " + t1.getString(2)  + " " + t1.getString(3)+ " " + t1.getString(4)+ t1.getString(5)+ " " + t1.getString(6));
							Transactions streamData = new Transactions(Integer.parseInt(t1.getString(1)),t1.getString(2),t1.getString(3),t1.getString(4),t1.getString(5),t1.getString(6),t1.getDate(7),Integer.parseInt(t1.getString(8)));
							Transaction_Data_Bucket.add(streamData);
							Stream_count++;			    
						}
					}
					Streams_Queue.add(new ArrayList<Transactions>(Transaction_Data_Bucket));
					Transaction_Data_Bucket.clear();
					Stream_count = 1;
					numberOfStreams++;
				}
				
				Queue<ArrayList<Transactions> > MainQ = new LinkedList<ArrayList<Transactions>>();
				int cnt=0;
				int QsizeCount=0;
				for(int i=0;i<200;i++)
				{
					if(QsizeCount<10)
					{
						ArrayList<Transactions>  queueObjectTrans = Streams_Queue.remove();
						MainQ.add(new ArrayList<Transactions>(queueObjectTrans));
						QsizeCount++;
					}
					else
					{
						ArrayList<Transactions>  temp = MainQ.remove();
						Queue<ArrayList<Master>> newQueue = copyQueue(queue);
						for(int l=0;l<10;l++)
						{
							ArrayList<Master>  queueObject =newQueue.remove();
							for(int j=0;j<10;j++)
							{
								for (int k=0;k<50;k++)
								{
										if (queueObject.get(j).PRODUCT_ID.equals(temp.get(k).PRODUCT_ID))
										{
											FactTable FactData = new FactTable(
													queueObject.get(j).PRODUCT_ID,
													temp.get(k).CUSTOMER_ID,
													temp.get(k).CUSTOMER_NAME, 
													temp.get(k).STORE_ID,
													temp.get(k).STORE_NAME,
													temp.get(k).T_DATE,  
													queueObject.get(j).PRODUCT_NAME, 
													queueObject.get(j).SUPPLIER_ID,  
													queueObject.get(j).SUPPLIER_NAME, 
													queueObject.get(j).PRICE,
													temp.get(k).QUANTITY
													);
											FactTableStyleArray.add(FactData);
//											System.out.println(temp.get(k).CUSTOMER_NAME + cnt);
										}
								}
							}
						}
						ArrayList<Transactions>  queueObjectTrans = Streams_Queue.remove();
						MainQ.add(new ArrayList<Transactions>(queueObjectTrans));
					}
					
					if(i==199)
					{
					
						for(int s=0;s<10;s++)
						{
							ArrayList<Transactions>  temp1 = MainQ.remove();
							Queue<ArrayList<Master>> newQueue1 = copyQueue(queue);
							for(int l=0;l<10;l++)
							{
								ArrayList<Master>  queueObject =newQueue1.remove();
								for(int j=0;j<10;j++)
								{
									for (int k=0;k<50;k++)
									{
											if (queueObject.get(j).PRODUCT_ID.equals(temp1.get(k).PRODUCT_ID))
											{
												FactTable FactData = new FactTable(
														queueObject.get(j).PRODUCT_ID,
														temp1.get(k).CUSTOMER_ID,
														temp1.get(k).CUSTOMER_NAME, 
														temp1.get(k).STORE_ID,
														temp1.get(k).STORE_NAME,
														temp1.get(k).T_DATE,  
														queueObject.get(j).PRODUCT_NAME, 
														queueObject.get(j).SUPPLIER_ID,  
														queueObject.get(j).SUPPLIER_NAME, 
														queueObject.get(j).PRICE,
														temp1.get(k).QUANTITY
														);
												FactTableStyleArray.add(FactData);
//												System.out.println(temp.get(k).CUSTOMER_NAME + cnt);
											}
									}
								}
							}
							
						}
						
					}
				}
				int cnt1=0;
				for (int i=0;i<FactTableStyleArray.size();i++)
				{
					cnt1++;
					System.out.println(cnt1);
					String Query = "INSERT IGNORE into metro.CUSTOMER values(\""+FactTableStyleArray.get(i).customer_id+ "\", \""+FactTableStyleArray.get(i).customer_name+"\");" ;
					int Q = stmt.executeUpdate(Query);
					
					Query = "INSERT IGNORE into metro.SUPPLIER values(\""+FactTableStyleArray.get(i).supplier_id+ "\", \""+FactTableStyleArray.get(i).supplier_name+"\");" ;
					Q = stmt.executeUpdate(Query);
					
					Query = "INSERT IGNORE into metro.STORE values(\""+FactTableStyleArray.get(i).store_id+ "\", \""+FactTableStyleArray.get(i).store_name+"\");" ;
					Q = stmt.executeUpdate(Query);
					
					Query = "INSERT IGNORE into metro.PRODUCT values(\""+FactTableStyleArray.get(i).product_id+ "\", \""+FactTableStyleArray.get(i).product_name+"\", \""+FactTableStyleArray.get(i).price+"\");" ;
					Q = stmt.executeUpdate(Query);
					
					Query = "INSERT IGNORE into metro.DATE values("+"\""+ FactTableStyleArray.get(i).t_date + "\", "+ "year(\"" +FactTableStyleArray.get(i).t_date+ "\"), "+ "quarter(\"" +FactTableStyleArray.get(i).t_date+ "\"), "
							+ "monthname(\"" +FactTableStyleArray.get(i).t_date+ "\"), "+ "dayname(\"" +FactTableStyleArray.get(i).t_date+ "\"), "+ "day(\"" +FactTableStyleArray.get(i).t_date+ "\") "+ ");" ;
					Q = stmt.executeUpdate(Query);
					
					Query = "INSERT IGNORE into metro.FACT_TABLE values("+ "\""+FactTableStyleArray.get(i).customer_id+"\", "+ "\""+FactTableStyleArray.get(i).store_id+"\", "+ "\""+FactTableStyleArray.get(i).supplier_id+"\", "
							+ "\""+FactTableStyleArray.get(i).product_id+"\", "+ "\""+FactTableStyleArray.get(i).t_date+"\", "+ "\""+FactTableStyleArray.get(i).quantity+"\", "+ "\""+FactTableStyleArray.get(i).sales+"\");" ;
					Q = stmt.executeUpdate(Query);
					
				}
				con.close();
		}
		catch(Exception e)
		{
			System.out.println(e);
		}
	}
	

	private static Queue<ArrayList<Master>> copyQueue(Queue<ArrayList<Master>> queue) {
		
		Queue<ArrayList<Master>> newQueue = new LinkedList<ArrayList<Master>>();
		
		for(ArrayList<Master> item : queue){
			
			// make new array of masters 
			ArrayList<Master> arrayForNewQueue = new ArrayList<Master>(); 
			
			for(int i = 0 ; i < item.size(); i++) {
				
				// filling the Master Object 
				Master m = new Master(item.get(i).PRODUCT_ID,item.get(i).PRODUCT_NAME, item.get(i).SUPPLIER_ID,item.get(i).SUPPLIER_NAME, item.get(i).PRICE);
				
				// adding the object in the array
				arrayForNewQueue.add(m);
				
				
			}
			
			// add the filled array in the queue
			newQueue.add(arrayForNewQueue); 
		}
		
		return newQueue;
					
	}

	
	
}
