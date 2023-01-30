#Query 1
select s.supplier_name as Supplier,p.product_name as Product,
d.t_quarter as Quarter,d.t_month as month, sum(sf.sales) as Total_Sales
from metro.fact_table sf join metro.supplier s join metro.date d
join metro.product p 
on (s.supplier_id = sf.supplier_id AND d.t_date = sf.t_date AND sf.product_id=p.product_id)
group by Supplier,product, Quarter , month
order by Supplier,product, Quarter , month;


#Query 2
select s.Store_name as Store_Name,p.Product_name as Product_Name,sum(ft.Sales) as Total_Sale
From metro.fact_table ft join metro.product p join metro.store s
on (s.Store_id = ft.Store_id AND ft.Product_id=p.Product_id)
group by Product_Name,Store_Name
order by Store_Name,Product_Name;



#Query 3
select p.product_name as product, sum(ft.quantity) as units_sold 
		from metro.product p join metro.fact_table ft join metro.date d
		on(p.product_id = ft.product_id AND d.t_date = ft.t_date)
		where (d.t_dayname = "Saturday" OR d.t_dayname = "Sunday") 
		group by p.product_name
        order by units_sold DESC Limit 5;
        
        



#Query 4

select q1.product_name as Product, q1.sales as First_Quarter, q2.sales as second_Quarter, q3.sales as Third_Quarter, q4.sales as Four_Quarter,(q1.sales+q2.sales+q3.sales+q4.sales) as Yearly_Sale
from 
	(select p.product_id,p.product_name, sum(sf.sales) sales
		from metro.product p join metro.fact_table sf join metro.date d
		on (p.product_id = sf.product_id AND d.t_date = sf.t_date)
		where d.t_quarter = 1
		group by p.product_id) q1 join    
	(select p.product_id,p.product_name, sum(sf.sales) sales 
		from metro.product p join metro.fact_table sf join metro.date d
		on (p.product_id = sf.product_id AND d.t_date = sf.t_date)
		where d.t_quarter = 2
		group by p.product_id) q2 join
	(select p.product_id,p.product_name, sum(sf.sales) sales
		from metro.product p join metro.fact_table sf join metro.date d
		on (p.product_id = sf.product_id AND d.t_date = sf.t_date)
		where d.t_quarter = 3
		group by p.product_id) q3 join
	(select p.product_id,p.product_name, sum(sf.sales) sales
		from metro.product p join metro.fact_table sf join metro.date d
		on (p.product_id = sf.product_id AND d.t_date = sf.t_date)
		where d.t_quarter = 4
		group by p.product_id) q4
		on (q1.product_id = q2.product_id AND q2.product_id = q3.product_id AND q3.product_id = q4.product_id )
        order by q1.product_id;



#Query 5

select p.Product_id as ID,p.Product_name as Product_Name,sum(sf.Sales) as Total_Sale,d.T_quarter as quarter
From metro.fact_table sf join metro.product p join metro.date d
on (d.T_date = sf.T_date AND sf.Product_id=p.Product_id)
group by Product_Name,quarter
order by quarter;

#Query 7

Create view metro.STOREANALYSIS_MV as 
select s.store_id, p.product_id, sum(sf.sales) as Store_Total
from metro.store s join metro.product p join metro.fact_table sf 
on (s.store_id=sf.store_id and p.product_id=sf.product_id)
group by s.store_id,p.product_id ;
