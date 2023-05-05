from airflow.providers.sqlite.hooks.sqlite import SqliteHook
import csv
import pandas as pd


def extract_orders():
    """
    Extracts all data from the "Order" table in the "sqlite" SQLite database and writes it to a CSV file named 
    "output_orders.csv".
    """
    # Create a hook for the Sqlite database connection
    sqlite_hook = SqliteHook(sqlite_conn_id="sqlite")
    conn = sqlite_hook.get_conn()
    cursor = conn.cursor()

    # Select all data from the "Order" table
    select_query = "SELECT * FROM 'Order'"
    cursor.execute(select_query)
    data = cursor.fetchall()

    # Write data to a CSV file
    csv_data = []
    csv_data.append([i[0] for i in cursor.description])
    for row in data:
        csv_data.append(row)
    with open('output_orders.csv', 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerows(csv_data)


def calculate_count():
    """
    Reads the "output_orders.csv" file and the "OrderDetail" table from the Northwind SQLite database, performs a JOIN
    operation on the two datasets and calculates the sum of the "Quantity" column for all rows where "ShipCity" is "Rio de
    Janeiro". The result is saved to a text file named "count.txt".
    """
    # Read "output_orders.csv" file
    orders_data = pd.read_csv(file path)

    # Read "OrderDetail" table from the Northwind SQLite database
    sqlite_hook = SqliteHook(sqlite_conn_id="sqlite")
    conn = sqlite_hook.get_conn()
    order_detail_data = pd.read_sql_query("SELECT * FROM OrderDetail", conn)

    # Perform JOIN operation on the two datasets
    joined_orders = pd.merge(orders_data, order_detail_data, left_on="Id", right_on="OrderId", how="inner")

    # Calculate sum of "Quantity" column where "ShipCity" is "Rio de Janeiro"
    rio_orders_sum = joined_orders.loc[joined_orders["ShipCity"] == "Rio de Janeiro", "Quantity"].sum()

    # Save result to a text file
    with open("count.txt", "w") as f:
        f.write(str(rio_orders_sum))