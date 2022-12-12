#define _CRT_SECURE_NO_WARNINGS
#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <string.h>
#include "mysql.h"

#pragma comment(lib, "libmysql.lib")

#define T1(X)	printf("\n----------------------------- TYPE %d -----------------------------\n",X)
#define T2(X,Y)	printf("\n---------------------------- TYPE %d-%d ---------------------------\n",X,Y)
#define SUB(X)	printf("\n----------------------- Subtypes in TYPE %d -----------------------\n\n",X)
#define LIST_SUB(X,Y)	printf("\t%d. TYPE %d-%d \n",Y,X,Y)
#define SEL()	printf("select >> ")
#define MAX_QUERY_LEN 2000
#define BUF_LEN 1000

bool init_DB();
void deinit_DB();
void showTypes();
int getInput();

void type1();
void type1_1(const char* shipper, const int order_id);
void type2();
void type2_1(char* c_id, char* c_name);
void type3();
void type3_1();
void type3_2();
void type4();
void type4_1();
void type4_2();
void type5();
void type6();
void type7();

const char* host = "localhost";
const char* user = "root";
const char* pw = "mysql";
const char* db = "project";

MYSQL* connection = NULL;
MYSQL conn;
FILE* fp;

int main(void) {

	if (!init_DB()) return 1;
	srand(time(NULL));

	while (1) {
		showTypes();	// print 7 types to choose
		int option = getInput();

		if (option == 0) break;

		switch (option) {
		case 1:
			type1(); break;
		case 2:
			type2(); break;
		case 3:
			type3(); break;
		case 4:
			type4(); break;
		case 5:
			type5(); break;
		case 6:
			type6(); break;
		case 7:
			type7(); break;
		}
	}
	deinit_DB();

	return 0;
}

void type1() {

	MYSQL_RES* sql_result;
	MYSQL_ROW sql_row;
	int option, num_row, order_id;
	const char* shipper = "USPS";
	const char* ship_query = "select tracking_no from shipment where delivered_date is null";
	char trackSelect[100];
	char query[MAX_QUERY_LEN];

	if (mysql_query(connection, ship_query)) return;

	sql_result = mysql_store_result(connection);
	num_row = mysql_num_rows(sql_result);

	if (num_row == 0) {		// when all the packages are properly delivered
		printf("\tAll packages delivered. \n\n");
		return;
	}

	int tIdx = rand() % num_row, i = 0;
	while ((sql_row = mysql_fetch_row(sql_result)) != NULL) {	// randomly select one shipment among them whose delivered date is NULL
		if (i++ == tIdx) {
			strcpy(trackSelect, sql_row[0]);
			break;
		}
	}

	// query for getting customer info for shipment with tracking number gained above
	sprintf(query, "select C.name, C.phone_number, O.order_id, S.ship_comp, S.tracking_no from shipment as S join orders as O on S.order_id = O.order_id join customer as C on O.customer_id = C.customer_id where S.tracking_no = '%s' and S.ship_comp = '%s' and S.delivered_date is null", trackSelect, shipper);

	if (mysql_query(connection, query)) return;

	T1(1);
	printf("\n** Assume the package shipped by '%s' with tracking number '%s' is reported to have been destroyed in an accident. **\n", shipper, trackSelect);
	printf("** Find the contact information for the customer. **\n\n");

	sql_result = mysql_store_result(connection);
	printf("\t===============================================================================\n");
	printf("\t| Name	         Phone no.            Order_ID	     Shipper      Tracking No.|\n");
	printf("\t===============================================================================\n");
	while ((sql_row = mysql_fetch_row(sql_result)) != NULL) {
		printf("\t| %-15s%-21s%-15s%-12s%12s | \n", sql_row[0], sql_row[1], sql_row[2], sql_row[3], sql_row[4]);
		order_id = atoi(sql_row[2]);
	}
	printf("\t===============================================================================\n");
	mysql_free_result(sql_result);

	while (1) {
		SUB(1);
		LIST_SUB(1, 1);
		option = getInput();

		if (option == 0) break;
		if (option == 1) {
			type1_1(shipper, order_id);
		}
	}
}

void type1_1(const char* shipper, const int order_id) {
	// shipper is USPS
	// order_id is the value gained in function type1

	MYSQL_RES* sql_result;
	MYSQL_ROW sql_row;
	const char* shipment_query = "select S.tracking_no from shipment as S";	// get all existing tracking_no from shipment table
	char query[MAX_QUERY_LEN];

	sprintf(query, "select S.tracking_no from shipment as S where S.order_id = '%d'", order_id);	// get track_no corresponding to order_id
	mysql_query(connection, query);
	sql_result = mysql_store_result(connection);
	sql_row = mysql_fetch_row(sql_result);

	T2(1, 1);
	printf("\n** Then find the contents of the shipment for tracking number '%s' **\n\n", sql_row[0]);
	mysql_free_result(sql_result);

	memset(query, 0, sizeof(query));
	// query for getting the packages for the lost shipment
	sprintf(query, "select C.product_name, sum(I.quantity) from shipment as S join itemList as I on S.order_id = I.order_id join package as P on P.package_id = I.package_id join consist as C on C.package_id = P.package_id where S.order_id = '%d' and S.ship_comp = '%s' and S.delivered_date is null group by C.product_name", order_id, shipper);

	if (mysql_query(connection, query)) {
		printf("Query Failed. \n");
		return; 
	}

	sql_result = mysql_store_result(connection);

	printf("\t================================ \n");
	printf("\t| Product\t      Quantity |\n");
	printf("\t================================ \n");

	while ((sql_row = mysql_fetch_row(sql_result)) != NULL) {
		printf("\t| %-20s %7s |\n", sql_row[0], sql_row[1]);
	}
	printf("\t================================ \n");
	mysql_free_result(sql_result);

	if (mysql_query(connection, shipment_query)) {	// get all track_no from shipment table
		printf("Failed to check shipment table. \n");
		return;
	}

	sql_result = mysql_store_result(connection);
	int num_row = mysql_num_rows(sql_result);
	int* tracks = (int*)malloc(sizeof(int) * num_row);
	int idx = 0;

	while ((sql_row = mysql_fetch_row(sql_result)) != NULL) {
		tracks[idx++] = atoi(sql_row[0]);
	}

	int new_track_no = 70000000 + rand() % 100;

	while (1) {			// get new track_no that does not overlap with existing tracking numbers
		bool flag = true;
		for (int i = 0; i < num_row; i++) {
			if (tracks[i] == new_track_no) {
				flag = false; break;
			}
		}
		if (flag == true) break;
		new_track_no = 70000000 + rand() % 100;
	}

	char delQuery[MAX_QUERY_LEN];
	char insertQuery[MAX_QUERY_LEN];

	sprintf(delQuery, "delete from shipment where shipment.order_id = '%d'", order_id);		// delete the lost shipment
	sprintf(insertQuery, "insert into shipment values('%d','%d','%s',curdate(),null)", order_id, new_track_no, shipper);		// insert new shipment

	if (mysql_query(connection, delQuery)) {
		printf("Failed to delete shipment. \n");
		free(tracks);
		return;
	}
	if (mysql_query(connection, insertQuery)) {
		printf("Failed to insert new shipment. \n");
		free(tracks);
		return;
	}

	printf("\n\t<< New Shipment with Order_ID: %d, Tracking_No: %d inserted. >>\n", order_id, new_track_no);

	free(tracks);
}

void type2() {
	
	T1(2);
	printf("\n** Find the customer who has bought the most (by price) in the past year. ** \n\n");

	// query for type 2
	const char* query = "with X as (with A as (select CS1.customer_id as 'acid', CS1.name as 'acus', sum(I1.quantity * PD1.cost) as 'asum' from customer as CS1 join orders as O on CS1.customer_id = O.customer_id join itemList as I1 on O.order_id = I1.order_id join consist as C1 on I1.package_id = C1.package_id join product as PD1 on PD1.product_name = C1.product_name where O.order_date >= DATE_SUB(NOW(),INTERVAL 1 YEAR) group by acid),B as (select CS2.customer_id as 'bcid', CS2.name as 'bcus', sum(I2.quantity * PD2.cost) as 'bsum' from customer as CS2 join purchase as P on CS2.customer_id = P.customer_id join itemList as I2 on P.purchase_id = I2.purchase_id join consist as C2 on I2.package_id = C2.package_id join product as PD2 on PD2.product_name = C2.product_name where P.purchase_date >= DATE_SUB(NOW(),INTERVAL 1 YEAR) group by bcid) select * from (select * from A left join B on A.acid = B.bcid) as T union (select * from A right join B on A.acid = B.bcid)) select coalesce(acid,bcid), coalesce(acus, bcus), coalesce(asum,0)+coalesce(bsum,0) as 'total' from X order by total desc limit 1;";
	char c_id[20];
	char c_name[40];
	int option, limit = 1;
	MYSQL_RES* sql_result;
	MYSQL_ROW sql_row;

	if (mysql_query(connection, query) == 0) {
		sql_result = mysql_store_result(connection);

		printf("\t==========================================\n");
		printf("\t| Customer_ID\t  Name\t        Amount($)|\n");
		printf("\t==========================================\n");

		while ((sql_row = mysql_fetch_row(sql_result)) != NULL && limit--) {
			printf("\t| %-15s %-15s%7s |\n", sql_row[0], sql_row[1], sql_row[2]);
			strcpy(c_id, sql_row[0]);
			strcpy(c_name, sql_row[1]);
		}
		printf("\t==========================================\n");
		mysql_free_result(sql_result);
	}

	while (1) {	// choose subtypes for TYPE 2
		SUB(2);
		LIST_SUB(2, 1);
		option = getInput();

		if (option == 0) break;
		if (option == 1) 
			type2_1(c_id, c_name);
	}
}

void type2_1(char* c_id, char* c_name) { // customer_id and customer_name gained from function type2

	MYSQL_RES* sql_result;
	MYSQL_ROW sql_row;
	int limit = 1;
	char query[MAX_QUERY_LEN];

	// set query for TYPE 2-1
	sprintf(query, "with X as (with A as (select C1.product_name as 'aprod', sum(I1.quantity * PD1.cost) as 'asum' from orders as O join itemList as I1 on O.order_id = I1.order_id join consist as C1 on I1.package_id = C1.package_id join product as PD1 on C1.product_name = PD1.product_name where O.order_date >= DATE_SUB(NOW(),INTERVAL 1 YEAR) and O.customer_id = '%s' group by C1.product_name),B as (select C2.product_name as 'bprod', sum(I2.quantity * PD2.cost) as 'bsum' from purchase as P join itemList as I2 on P.purchase_id = I2.purchase_id join consist as C2 on I2.package_id = C2.package_id join product as PD2 on C2.product_name = PD2.product_name where P.purchase_date >= DATE_SUB(NOW(),INTERVAL 1 YEAR)and p.customer_id = '%s' group by C2.product_name)select * from(select * from A left join B on A.aprod = B.bprod) as T union(select * from A right join B on A.aprod = B.bprod))select coalesce(aprod,bprod), coalesce(asum,0)+coalesce(bsum,0) as 'total' from X order by total desc limit 1;", c_id, c_id);


	T2(2, 1);
	printf("\n** The product that %s had bought the most in the past year. **\n\n", c_name);

	if (mysql_query(connection, query)) return;

	printf("\t================================\n");
	printf("\t| Product \t      Amount($)|\n");
	printf("\t================================\n");

	sql_result = mysql_store_result(connection);
	while ((sql_row = mysql_fetch_row(sql_result)) != NULL && limit--) {
		printf("\t| %-20s %7s |\n", sql_row[0], sql_row[1]);
	}
	printf("\t================================\n");
	mysql_free_result(sql_result);
}

void type3() {

	T1(3);
	printf("\n** Find all products sold in the past year. ** \n\n");

	MYSQL_RES* sql_result;
	MYSQL_ROW sql_row;
	int option;

	// query for TYPE 3
	const char* query = "select * from(select distinct C1.product_name from orders as O join itemList as I1 on O.order_id = I1.order_id join consist as C1 on I1.package_id = C1.package_id where O.order_date >= DATE_SUB(NOW(),INTERVAL 1 YEAR)) as X union distinct (select distinct C2.product_name from purchase as P join itemList as I2 on P.purchase_id = I2.purchase_id join consist as C2 on I2.package_id = C2.package_id where P.purchase_date >= DATE_SUB(NOW(),INTERVAL 1 YEAR));";

	if (mysql_query(connection, query) == 0) {
		printf("\t========================\n");
		printf("\t| Product              | \n");
		printf("\t========================\n");

		sql_result = mysql_store_result(connection);
		while ((sql_row = mysql_fetch_row(sql_result)) != NULL) {
			printf("\t| %-20s |\n", sql_row[0]);
		}
		printf("\t========================\n");
		mysql_free_result(sql_result);
	}

	while (1) {	// choose subtypes for type 3
		SUB(3);
		LIST_SUB(3, 1);
		LIST_SUB(3, 2);

		option = getInput();

		if (option == 0) break;
		if (option == 1) {
			type3_1();
		}
		else if (option == 2) {
			type3_2();
		}
	}
}

void type3_1() {

	MYSQL_RES* sql_result;
	MYSQL_ROW sql_row;
	int k;
	char query[MAX_QUERY_LEN];

	T2(3, 1);
	printf("\n** Then find the top k products by dollar-amount sold. ** \n");
	printf("\n\tWhich K? : ");
	scanf("%d", &k);	// get k
	getchar();

	// set query for type 3-1 with "limit k"
	sprintf(query, "with X as (with A as (select C1.product_name as 'aprod', sum(I1.quantity * PD1.cost) as 'asum' from orders as O join itemList as I1 on O.order_id = I1.order_id join consist as C1 on I1.package_id = C1.package_id join product as PD1 on PD1.product_name = C1.product_name where O.order_date >= DATE_SUB(NOW(),INTERVAL 1 YEAR) group by PD1.product_name),B as (select C2.product_name as 'bprod', sum(I2.quantity * PD2.cost) as 'bsum' from purchase as P join itemList as I2 on P.purchase_id = I2.purchase_id join consist as C2 on I2.package_id = C2.package_id join product as PD2 on PD2.product_name = C2.product_name where P.purchase_date >= DATE_SUB(NOW(),INTERVAL 1 YEAR)group by C2.product_name)select * from(select * from A left join B on A.aprod = B.bprod) as T union(select * from A right join B on A.aprod = B.bprod))select coalesce(aprod,bprod), coalesce(asum,0)+coalesce(bsum,0) as total from X order by total desc limit %d;", k);

	if (mysql_query(connection, query)) return;

	printf("\n\t====================================\n");
	printf("\t| #  Product\t\t  Amount($)|\n");
	printf("\t====================================\n");

	int i = 1;
	sql_result = mysql_store_result(connection);
	while ((sql_row = mysql_fetch_row(sql_result)) != NULL) {
		printf("\t|%2d. %-20s %8s |\n", i++, sql_row[0], sql_row[1]);
	}
	printf("\t====================================\n");
	mysql_free_result(sql_result);
}

void type3_2() {

	MYSQL_RES* sql_result;
	MYSQL_ROW sql_row;
	int  k, top_ten;
	const char* query = "with X as (with A as (select C1.product_name as 'aprod', sum(I1.quantity * PD1.cost) as 'asum' from orders as O join itemList as I1 on O.order_id = I1.order_id join consist as C1 on I1.package_id = C1.package_id join product as PD1 on PD1.product_name = C1.product_name where O.order_date >= DATE_SUB(NOW(),INTERVAL 1 YEAR) group by PD1.product_name),B as (select C2.product_name as 'bprod', sum(I2.quantity * PD2.cost) as 'bsum' from purchase as P join itemList as I2 on P.purchase_id = I2.purchase_id join consist as C2 on I2.package_id = C2.package_id join product as PD2 on PD2.product_name = C2.product_name where P.purchase_date >= DATE_SUB(NOW(),INTERVAL 1 YEAR)group by C2.product_name)select * from(select * from A left join B on A.aprod = B.bprod) as T union(select * from A right join B on A.aprod = B.bprod))select coalesce(aprod,bprod), coalesce(asum,0)+coalesce(bsum,0) as total from X order by total desc;";

	if (mysql_query(connection, query)) return;

	sql_result = mysql_store_result(connection);
	int num_row = mysql_num_rows(sql_result);	// get the number of tuples returned as query result

	T2(3, 2);
	printf("\n** And then find the top 10%% products by dollar-amount sold. ** \n\n");
	printf("\tTotal number of products sold: %d \n\n", num_row);
	top_ten = num_row / 10;	// get the number of products for top 10% by dollor-amount sold

	printf("\n\t====================================\n");
	printf("\t| #  Product\t\t  Amount($)|\n");
	printf("\t====================================\n");
	int i = 1;
	while ((sql_row = mysql_fetch_row(sql_result)) != NULL && top_ten--) {
		printf("\t|%2d. %-20s %8s |\n", i++, sql_row[0], sql_row[1]);
	}
	printf("\t====================================\n");
	mysql_free_result(sql_result);
}

void type4() {

	T1(4);
	printf("\n** Find all products by unit sales in the past year. ** \n\n");

	MYSQL_RES* sql_result;
	MYSQL_ROW sql_row;
	int option;

	// query for TYPE 4
	const char* query = "with X as (with A as (select C1.product_name as 'aprod', sum(I1.quantity) as 'asum' from orders as O join itemList as I1 on O.order_id = I1.order_id join consist as C1 on I1.package_id = C1.package_id where O.order_date >= DATE_SUB(NOW(),INTERVAL 1 YEAR)group by C1.product_name),B as (select C2.product_name as 'bprod', sum(I2.quantity) as 'bsum' from purchase as P join itemList as I2 on P.purchase_id = I2.purchase_id join consist as C2 on I2.package_id = C2.package_id where P.purchase_date >= DATE_SUB(NOW(),INTERVAL 1 YEAR) group by C2.product_name)select * from(select * from A left join B on A.aprod = B.bprod) as T union(select * from A right join B on A.aprod = B.bprod))select coalesce(aprod,bprod), coalesce(asum,0)+coalesce(bsum,0) from X";

	printf("\t================================ \n");
	printf("\t| Product\t      Quantity |\n");
	printf("\t================================ \n");

	if (mysql_query(connection, query) == 0) {
		sql_result = mysql_store_result(connection);
		while ((sql_row = mysql_fetch_row(sql_result)) != NULL) {
			printf("\t| %-20s %7s |\n", sql_row[0], sql_row[1]);
		}
		printf("\t================================ \n");
		mysql_free_result(sql_result);
	}

	while (1) {	// choose subtypes for TYPE 4
		SUB(4);
		LIST_SUB(4, 1);
		LIST_SUB(4, 2);
		option = getInput();

		if (option == 0) break;
		if (option == 1) {
			type4_1();
		}
		else if (option == 2) {
			type4_2();
		}
	}
}

void type4_1() {

	T2(4, 1);
	MYSQL_RES* sql_result;
	MYSQL_ROW sql_row;
	int k;
	char query[MAX_QUERY_LEN];
	
	printf("\n** Then find the top k products by unit sales. ** \n\n");
	printf("\tWhich k? : ");
	scanf("%d", &k);	// get k
	getchar();

	// set query for TYPE 4-1 with "limit k"
	sprintf(query, "with X as (with A as (select C1.product_name as 'aprod', sum(I1.quantity) as 'asum' from orders as O join itemList as I1 on O.order_id = I1.order_id join consist as C1 on I1.package_id = C1.package_id where O.order_date >= DATE_SUB(NOW(),INTERVAL 1 YEAR)group by C1.product_name),B as (select C2.product_name as 'bprod', sum(I2.quantity) as 'bsum' from purchase as P join itemList as I2 on P.purchase_id = I2.purchase_id join consist as C2 on I2.package_id = C2.package_id where P.purchase_date >= DATE_SUB(NOW(),INTERVAL 1 YEAR) group by C2.product_name)select * from(select * from A left join B on A.aprod = B.bprod) as T union(select * from A right join B on A.aprod = B.bprod))select coalesce(aprod,bprod), coalesce(asum,0)+coalesce(bsum,0) as total from X order by total desc limit %d", k);

	if (mysql_query(connection, query)) return;

	printf("\n\t=====================================\n");
	printf("\t| #  Product\t\t   Quantity |\n");
	printf("\t=====================================\n");
	int i = 1;
	sql_result = mysql_store_result(connection);
	while ((sql_row = mysql_fetch_row(sql_result)) != NULL) {
		printf("\t|%2d. %-20s %9s |\n", i++, sql_row[0], sql_row[1]);
	}
	printf("\t=====================================\n");
	mysql_free_result(sql_result);
}

void type4_2() {

	T2(4, 2);
	MYSQL_RES* sql_result;
	MYSQL_ROW sql_row;
	int top_ten;
	const char* query = "with X as (with A as (select C1.product_name as 'aprod', sum(I1.quantity) as 'asum' from orders as O join itemList as I1 on O.order_id = I1.order_id join consist as C1 on I1.package_id = C1.package_id where O.order_date >= DATE_SUB(NOW(),INTERVAL 1 YEAR)group by C1.product_name),B as (select C2.product_name as 'bprod', sum(I2.quantity) as 'bsum' from purchase as P join itemList as I2 on P.purchase_id = I2.purchase_id join consist as C2 on I2.package_id = C2.package_id where P.purchase_date >= DATE_SUB(NOW(),INTERVAL 1 YEAR) group by C2.product_name)select * from(select * from A left join B on A.aprod = B.bprod) as T union(select * from A right join B on A.aprod = B.bprod))select coalesce(aprod,bprod), coalesce(asum,0)+coalesce(bsum,0) as total from X order by total desc";
	
	if (mysql_query(connection, query)) return;

	sql_result = mysql_store_result(connection);
	int num_row = mysql_num_rows(sql_result);	// get the number of tuples returned as query result


	printf("\n** And then find the top 10%% products by unit sales. ** \n\n");
	printf("\tNumber of products sold: %d \n", num_row);
	top_ten = num_row / 10;		// get the number of products for top 10% by the number of unit sold

	printf("\n\t=====================================\n");
	printf("\t| #  Product\t\t   Quantity |\n");
	printf("\t=====================================\n");
	int i = 1;
	while ((sql_row = mysql_fetch_row(sql_result)) != NULL && top_ten--) {
		printf("\t|%2d. %-20s %9s |\n", i++, sql_row[0], sql_row[1]);
	}
	printf("\t=====================================\n");
	mysql_free_result(sql_result);
}

void type5() {

	T1(5);
	MYSQL_RES* sql_result;
	MYSQL_ROW sql_row;

	// query for TYPE 5
	const char* query = "select * from store as A natural join store_stock as B where A.region = 'California' and B.quantity = 0";

	printf("\n** Find those products that are out-of-stock at every store in California. ** \n\n");

	if (mysql_query(connection, query)) return;
	sql_result = mysql_store_result(connection);

	printf("\t=======================================================\n");
	printf("\t| Store_ID     State\t       Product\t        Stock |\n");
	printf("\t=======================================================\n");

	while ((sql_row = mysql_fetch_row(sql_result)) != NULL) {
		printf("\t| %-12s %-15s %-20s %s |\n", sql_row[0], sql_row[1], sql_row[2], sql_row[3]);
	}
	printf("\t=======================================================\n");
	mysql_free_result(sql_result);
}

void type6() {

	T1(6);
	MYSQL_RES* sql_result;
	MYSQL_ROW sql_row;

	// query for TYPE 6
	const char* query = "select S.order_id, S.tracking_no, S.ship_comp, S.promised_date, S.delivered_date, P.name from shipment as S join itemList as I on I.order_id = S.order_id join package as P on I.package_id = P.package_id where  S.delivered_date is not null and S.delivered_date > S.promised_date";
	
	printf("\n** Find those packages that were not delivered within the promised time. ** \n\n");

	if (mysql_query(connection, query)) return;
	sql_result = mysql_store_result(connection);

	printf("\t=============================================================================================\n");
	printf("\t| Order_ID    Tracking no.   Shipper     Promised_Date     Delivered_Date           Package |\n");
	printf("\t=============================================================================================\n");
	while ((sql_row = mysql_fetch_row(sql_result)) != NULL) {
		printf("\t| %-12s%-15s%-12s%-18s%-16s %15s |\n", sql_row[0], sql_row[1], sql_row[2], sql_row[3], sql_row[4], sql_row[5]);
	}
	printf("\t=============================================================================================\n");
	mysql_free_result(sql_result);
}

void type7() {

	T1(7);
	MYSQL_RES* sql_result;
	MYSQL_ROW sql_row;

	// query for TYPE 7
	const char* query = "with X as (with A as (select CS1.customer_id as 'acid', CS1.name as 'acus', sum(I1.quantity * PD1.cost) as 'asum', CS1.account_no as 'Aacc' from customer as CS1 join orders as O on CS1.customer_id = O.customer_id join itemList as I1 on O.order_id = I1.order_id join consist as C1 on I1.package_id = C1.package_id join product as PD1 on PD1.product_name = C1.product_name where O.order_date >= DATE_SUB(NOW(),INTERVAL 1 MONTH) and CS1.account_no is not null group by acid),B as (select CS2.customer_id as 'bcid', CS2.name as 'bcus', sum(I2.quantity * PD2.cost) as 'bsum', CS2.account_no as 'Bacc' from customer as CS2 join purchase as P on CS2.customer_id = P.customer_id join itemList as I2 on P.purchase_id = I2.purchase_id join consist as C2 on I2.package_id = C2.package_id join product as PD2 on PD2.product_name = C2.product_name where P.purchase_date >= DATE_SUB(NOW(),INTERVAL 1 MONTH) and CS2.account_no is not null group by bcid)select * from(select * from A left join B on A.acid = B.bcid) as T union(select * from A right join B on A.acid = B.bcid)) select coalesce(acid,bcid), coalesce(acus, bcus), coalesce(Aacc, Bacc) as acc_no, coalesce(asum,0)+coalesce(bsum,0) as 'total' from X";
	
	printf("\n** Generate the bill for each customer for the past month. ** \n\n");

	if (mysql_query(connection, query)) return;
	sql_result = mysql_store_result(connection);

	printf("\t===============================================================\n");
	printf("\t|                          <<BILL>>                           | \n");
	printf("\t===============================================================\n");
	printf("\t| Customer_ID       Name           Account NO.\t        Price |\n");
	printf("\t===============================================================\n");
	while ((sql_row = mysql_fetch_row(sql_result)) != NULL) {
		printf("\t| %-18s%-15s%-20s %5s |\n", sql_row[0], sql_row[1], sql_row[2], sql_row[3]);
	}
	printf("\t===============================================================\n");
	mysql_free_result(sql_result);
}

void showTypes() {

	printf("\n\n------- SELECT QUERY TYPES -------\n\n");
	printf("\t1. TYPE 1\n");
	printf("\t2. TYPE 2\n");
	printf("\t3. TYPE 3\n");
	printf("\t4. TYPE 4\n");
	printf("\t5. TYPE 5\n");
	printf("\t6. TYPE 6\n");
	printf("\t7. TYPE 7\n");
	printf("\t0. QUIT\n");
	printf("----------------------------------\n");
}

bool init_DB() {

	if (mysql_init(&conn) == NULL)
		printf("mysql_init() error!");

	connection = mysql_real_connect(&conn, host, user, pw, db, 3306, (const char*)NULL, 0);
	if (connection == NULL)
	{
		printf("%d ERROR : %s\n", mysql_errno(&conn), mysql_error(&conn));
		return false;
	}

	printf("Connection Succeed\n");

	if (mysql_select_db(&conn, db)) {
		printf("%d ERROR : %s\n", mysql_errno(&conn), mysql_error(&conn));
		return false;
	}

	fp = fopen("20160169.txt", "r");

	char buffer[BUF_LEN];
	
	fgets(buffer, BUF_LEN, fp);
	int lineNum = atoi(buffer);

	for (int i = 0; i < lineNum; i++) {
		memset(buffer, 0, sizeof(buffer));
		fgets(buffer, BUF_LEN, fp);
		int state = mysql_query(connection, buffer);

		if (state) {
			printf("ERROR: %s \n", buffer);
			fclose(fp);
			return false;
		}
	}

	return true;
}

void deinit_DB() {

	while (feof(fp) == 0) {
		char buffer[BUF_LEN];
		fgets(buffer, BUF_LEN, fp);
		int state = mysql_query(connection, buffer);

		if (state) {
			printf("ERROR: %s \n", buffer);
			fclose(fp);
			return;
		}
	}

	fclose(fp);

	mysql_close(connection);
	printf("BYE! \n");
}

int getInput() {		// prompt ����
	int ret = -1;
	printf("\n");

	while (ret < 0 || ret > 7) {
		char cmd[10] = { 0 };
		SEL();
		fgets(cmd, 10, stdin);
		if (cmd[0] == '\n') continue;
		ret = atoi(cmd);
	}
	return ret;
}

