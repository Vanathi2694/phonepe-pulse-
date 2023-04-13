import os
import json
import pandas as pd
import streamlit as st
import mysql.connector
import plotly.express as px


st.title("Phonepe Pulse Data Visualization and Exploration")

path = "C:/GitHub/pulse/data/top/user/country/india/state"

dict1 = {'State': [], 'Year': [], 'Quarter': [], 'Districts': [], 'Users': []}

tab1, tab2, tab3,tab4 = st.tabs(["Extract Data", "Transform Data", "Insert Data", "Visualize Data"])



def load_data(path):
    for state in os.listdir(path):
        state_dir = os.path.join(path, state)
        for year in os.listdir(state_dir):
            year_dir = os.path.join(state_dir, year)
            for quarter_file in os.listdir(year_dir):
                quarter_path = os.path.join(year_dir, quarter_file)
                with open(quarter_path, 'r') as f:
                    data = json.load(f)
                    quarter = quarter_file.split('.')[0]
                    districts = data['data']['districts']
                    for district in districts:
                        district_name = district['name']
                        district_users = district['registeredUsers']
                        dict1['State'].append(state)
                        dict1['Year'].append(year)
                        dict1['Quarter'].append(quarter)
                        dict1['Districts'].append(district_name)
                        dict1['Users'].append(district_users)
    st.dataframe(dict1)
with tab1:
    load_data(path)

df = pd.DataFrame(dict1)

df.to_csv('users_data.csv', index=False)

def explore_data():

    df.dropna(inplace=True)

    df['Year'] = pd.to_datetime(df['Year'], format='%Y')

    grouped = df.groupby(['State', pd.Grouper(key='Year', freq='Y')])['Users'].sum()

    grouped_df = grouped.to_frame().reset_index()

    pivoted = grouped_df.pivot(index='State', columns='Year', values='Users')

    pivoted.fillna(0, inplace=True)

    st.write(pivoted)

    pivoted.to_csv('pivoted_data.csv', index=False)

with tab2:
    explore_data()


def sql_con():

    pivoted = pd.read_csv('pivoted_data.csv')

    conn = mysql.connector.connect(host="database-phonepe-pulse.cbgdu1nd11gm.ap-south-1.rds.amazonaws.com",
                                   user="admin",
                                   password="Vanathis",
                                   database="database-phonepe-pulse")

    cursor = conn.cursor()

    pivoted = pivoted.astype(int)

    for state, row in pivoted.iterrows():
        year_2018 = int(row['2018-12-31'])
        year_2019 = int(row['2019-12-31'])
        year_2020 = int(row['2020-12-31'])
        year_2021 = int(row['2021-12-31'])
        sql = "INSERT INTO users (state, year_2018, year_2019, year_2020, year_2021) VALUES (%s, %s, %s, %s, %s)"
        values = (state, year_2018, year_2019, year_2020, year_2021)
        cursor.execute(sql, values)

    st.write("Data Inserted Successfully")

    conn.commit()

    conn.close()

with tab3:
    sql_con()


@st.cache_data
def visualization():
    with open('C:/GitHub/pulse/india_state_geo.json') as f:
        data = json.load(f)

    fig = px.choropleth_mapbox(df, geojson=data, locations='State', color='Users',
                               color_continuous_scale='Viridis', range_color=(0, df['Users'].max()),
                               mapbox_style='carto-positron', zoom=3, center={'lat': 20.5937, 'lon': 78.9629},
                               opacity=0.5, labels={'Users': 'Number of Users'})
    fig.update_layout(margin={'l': 0, 'r': 0, 't': 0, 'b': 0})
    st.plotly_chart(fig)

    #year = st.selectbox('Select a year', df['Year'].unique())

    #filtered_data = df[df['Year'] == year]

    #fig = px.bar(filtered_data, x='State', y='Users', color='Districts', title=f'Number of Users by State for {year}')
    #st.plotly_chart(fig)

with tab4:
    visualization()


def visualization1():
    year = st.selectbox('Select a year', df['Year'].unique())

    filtered_data = df[df['Year'] == year]

    fig = px.bar(filtered_data, x='State', y='Users', color='Districts', title=f'Number of Users by State for {year}')

    st.plotly_chart(fig)

visualization1()
