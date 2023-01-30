package dwh_project;
import java.sql.Date;
public class FactTable {

	String product_id;
	String customer_id;
	String customer_name;
	String store_id;
	String store_name;
	Date t_date;
	String product_name;
	String supplier_id;
	String supplier_name;
	double price;
	int quantity;
	double sales;
	public FactTable(String product_id, String customer_id, String customer_name, String store_id, String store_name,
			Date t_date, String product_name, String supplier_id, String supplier_name, double price, int quantity) {
		super();
		this.product_id = product_id;
		this.customer_id = customer_id;
		this.customer_name = customer_name;
		this.store_id = store_id;
		this.store_name = store_name;
		this.t_date = t_date;
		this.product_name = product_name;
		this.supplier_id = supplier_id;
		this.supplier_name = supplier_name;
		this.price = price;
		this.quantity = quantity;
		this.sales = price*quantity;
	}
	
}

