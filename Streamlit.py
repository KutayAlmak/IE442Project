import streamlit as st
from MRP import create_tables, insert_sample_data, calculate_mrp_values_for_periods, print_mrp_for_part_a
st.title("MRP Streamlit App")

# You can add more Streamlit UI components here

if st.button("Generate MRP"):
    create_tables()
    insert_sample_data()
    calculate_mrp_values_for_periods()
    print_mrp_for_part_a()

