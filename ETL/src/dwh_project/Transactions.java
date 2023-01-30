package dwh_project;
import java.sql.Date;
public class Transactions {
	public int TRANSACTION_ID;
	public String PRODUCT_ID;
	public String CUSTOMER_ID;
	public String CUSTOMER_NAME;
	public String STORE_ID;
	public String STORE_NAME;
	public Date   T_DATE;
	public int QUANTITY;
	public Transactions(int tRANSACTION_ID, String pRODUCT_ID, String cUSTOMER_ID, String cUSTOMER_NAME,
			String sTORE_ID, String sTORE_NAME, Date t_DATE, int qUANTITY) {
		super();
		TRANSACTION_ID = tRANSACTION_ID;
		PRODUCT_ID = pRODUCT_ID;
		CUSTOMER_ID = cUSTOMER_ID;
		CUSTOMER_NAME = cUSTOMER_NAME;
		STORE_ID = sTORE_ID;
		STORE_NAME = sTORE_NAME;
		T_DATE = t_DATE;
		QUANTITY = qUANTITY;
	}
	
}
