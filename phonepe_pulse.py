import json
import os
import pandas as pd
import streamlit as st
import mysql.connector
import plotly.express as px
import matplotlib.pyplot as plt
import plotly.graph_objects as go
dir_path = "C:/GitHub/pulse/data/aggregated/user/country/india"
st.title("Phonepe pulse Data Visualization and Exploration")
bgcolor = "#F0F2F6"
#with st.sidebar:

st.markdown(f"""
    <style>
    body {{
        background-color: {bgcolor};
    }}
    </style>
    """, unsafe_allow_html=True)

tab1, tab2, tab3, tab4 = st.tabs(["Load Data", "Explore Data", "Insert Data","Visualization"])
#load_data_slider_key = 'load_data_slider'



def load_data(dir_path):
    json_files = []
    for root, dirs, files in os.walk(dir_path):
        for file in files:
            if file.endswith('.json'):
                json_files.append(os.path.join(root, file))
    dfs = []
    for json_file in json_files:
        with open(json_file, 'r') as f:
            data = json.load(f)
            usersByDevice = data['data']['usersByDevice']
            df = pd.DataFrame(usersByDevice)
            df['filename'] = os.path.basename(json_file)
            dfs.append(df)
    final_df = pd.concat(dfs, ignore_index=True)

    return final_df
with tab1:
    st.header("Data Loaded")
    df = load_data(dir_path)


def explore_data(dir_path):
    df = load_data(dir_path)
    st.sidebar.subheader('Select a page number:')
    PAGE_SIZE = 100
    page = st.sidebar.slider('Page', 1, len(df) // PAGE_SIZE + 1, 1, key='load_data_slider_key_2')
    start_idx = (page - 1) * PAGE_SIZE
    end_idx = start_idx + PAGE_SIZE
    st.write(f'Displaying rows {start_idx + 1} to {min(end_idx, len(df))} of {len(df)}')
    st.write(df.iloc[start_idx:end_idx])

    st.write('Original Dataframe:')
    st.dataframe(df)

    df = df.dropna()
    num_nulls = df.isnull().sum().sum()
    st.write(f'Number of null values removed: {num_nulls}')

    # Drop duplicate rows based on 'usersByDevice'
    df = df.drop_duplicates(subset=['count', 'percentage'], keep='first')
    num_dupes = len(df) - len(df.drop_duplicates(subset=['count', 'percentage']))
    st.write(f'Number of duplicate rows removed: {num_dupes}')

    # Display the final dataframe
    st.write('Cleaned Dataframe:')
    st.dataframe(df)


    return df


with tab2:
    st.header("Data Explored")
    df = explore_data(dir_path)
def sqlcon(df):
    # Connect to the MySQL database
    mydb = mysql.connector.connect(
        host="database-phonepe-pulse.cbgdu1nd11gm.ap-south-1.rds.amazonaws.com",
        user="admin",
        password="Vanathis",
        database="database-phonepe-pulse"
    )
    mycursor = mydb.cursor()

    # Create a table if it does not already exist
    mycursor.execute(
        "CREATE TABLE IF NOT EXISTS usersByDevice (id INT AUTO_INCREMENT PRIMARY KEY, brand VARCHAR(255), count INT, percentage FLOAT, filename VARCHAR(255))"
    )
    rows_to_insert = []
    # Insert data from the dataframe into the table
    for index, row in df.iterrows():
        rows_to_insert.append((row['brand'], row['count'], row['percentage'], row['filename']))
        if len(rows_to_insert) == 1000:  # Insert in batches of 1000 rows
            sql = "INSERT INTO usersByDevice (brand, count, percentage, filename) VALUES (%s, %s, %s, %s)"
            mycursor.executemany(sql, rows_to_insert)
            rows_to_insert = []

    # Insert any remaining rows
    if len(rows_to_insert) > 0:
        sql = "INSERT INTO usersByDevice (brand, count, percentage, filename) VALUES (%s, %s, %s, %s)"
        mycursor.executemany(sql, rows_to_insert)
    # Commit changes to the database
    mydb.commit()

    # Print the number of rows inserted
    st.write(f"{mycursor.rowcount} rows inserted into the 'usersByDevice' table.")

    # Check if the table exists
    mycursor.execute(
        "SELECT * FROM information_schema.tables WHERE table_schema = 'database-phonepe-pulse' AND table_name = 'usersByDevice'"
    )
    result = mycursor.fetchone()

    if result:
        st.write("Table 'usersByDevice' exists in the database.")
    else:
        st.write("Error: Table 'usersByDevice' does not exist in the database.")
with tab3:
    st.header("Data inserted into sql")
    st.image("https://i1.wp.com/technopoints.co.in/wp-content/uploads/2017/09/data-insert.png?fit=750%2C400&ssl=1", width = 500)
    sqlcon(df)