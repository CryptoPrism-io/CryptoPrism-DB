import mysql.connector

# Establish a connection to the MySQL database
try:
    con = mysql.connector.connect(
        host="dbcp.cry66wamma47.ap-south-1.rds.amazonaws.com",
        port=3306,
        user="yogass09",
        password="jaimaakamakhya",
        database="dbcp"
    )

    # Create a cursor object
    cursor = con.cursor()

    # Execute a query to show active connections
    cursor.execute("SHOW PROCESSLIST")

    # Fetch all results
    processlist = cursor.fetchall()

    # Iterate through the results and identify MySQL connections
    mysql_connections = []
    for process in processlist:
        if process[1] == 'yogass09':  # Replace 'your_mysql_user' with your actual username
            mysql_connections.append(process)


    if mysql_connections:
        print("Active MySQL connections:")
        for connection in mysql_connections:
            print(f"  ID: {connection[0]}, User: {connection[1]}, Host: {connection[3]}, Command: {connection[4]}")

        # Kill the connections (be cautious with this)
        for connection in mysql_connections:
            kill_query = f"KILL {connection[0]}"
            cursor.execute(kill_query)
            print(f"Killed connection with ID: {connection[0]}")

        con.commit()  # Commit the changes
        print("All identified MySQL connections have been terminated.")
    else:
        print("No active MySQL connections found.")
except mysql.connector.Error as err:
    print(f"Error: {err}")
finally:
    if con.is_connected():
        cursor.close()
        con.close()
        print("MySQL connection closed.")
