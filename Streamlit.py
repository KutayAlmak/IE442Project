import streamlit as st
import sqlite3
from prettytable import PrettyTable

def fetch_mrp_data(part_id):
    conn = sqlite3.connect('MRP_database.db')
    cursor = conn.cursor()

    try:
        # Execute SQL to fetch MRP for the selected part
        cursor.execute("SELECT * FROM MRP WHERE PartID = ? ORDER BY PeriodID", (part_id,))
        mrp_results = cursor.fetchall()
        return mrp_results

    finally:
        # Close connection
        conn.close()

def print_mrp_table(mrp_data):
    # Create a pretty table
    table = PrettyTable()
    table.field_names = ["PartID", "PeriodID", "GrossRequirements", "ScheduledReceipts", "EndingInventory",
                         "NetRequirements", "PlannedOrderRelease", "PlannedOrderReceipts"]

    for result in mrp_data:
        table.add_row(result)

    # Print the table
    st.write(table)

def main():
    # Create tables and insert sample data (if not already done)
    create_tables()
    insert_sample_data()

    st.title("Material Requirements Planning (MRP) Viewer")

    # Get the list of available parts
    conn = sqlite3.connect('MRP_database.db')
    cursor = conn.cursor()
    cursor.execute("SELECT PartID FROM Part")
    part_ids = [str(part[0]) for part in cursor.fetchall()]
    conn.close()

    # Create a dropdown to select the part
    selected_part = st.selectbox("Select Part ID:", part_ids)

    # Convert selected part to integer
    selected_part_id = int(selected_part)

    # Fetch and display MRP data for the selected part
    mrp_data = fetch_mrp_data(selected_part_id)

    if mrp_data:
        print_mrp_table(mrp_data)
    else:
        st.warning("No MRP data found for the selected part.")

if __name__ == "__main__":
    main()

streamlit run mrp_app.py
