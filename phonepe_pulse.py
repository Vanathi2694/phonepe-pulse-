import os
import json
import pandas as pd
import streamlit as st
import mysql.connector
import plotly.express as px
import geopandas as gpd
import numpy as np

st.set_page_config(page_title="Phonepe",page_icon="👋",)
#add_selectbox = st.sidebar.selectbox("Select any one",("Aggregated", "Map", "Top"))

#with st.sidebar:
  # if add_selectbox == "Top":

st.title("Phonepe Pulse Data Visualization and Exploration")
st.sidebar.success("Select a page")

path = "C:/GitHub/pulse/data/top/transaction/country/india/state"

dict2 = {'State': [], 'Year': [], 'Quarter': [], 'Districts': [], 'Count': [], 'Amount': []}

tab1, tab2, tab3, tab4, tab5 = st.tabs(["Extract Data", "Transform Data", "Insert Data", "Visualize Data in Map", "Visualize Bar Chart"])


def extract(path):
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
                        district_name = district['entityName']
                        district_count = district['metric']['count']
                        district_amount = district['metric']['amount']
                        dict2['State'].append(state)
                        dict2['Year'].append(year)
                        dict2['Quarter'].append(quarter)
                        dict2['Districts'].append(district_name)
                        dict2['Count'].append(district_count)
                        dict2['Amount'].append(district_amount)
    st.dataframe(dict2)
    df1 = pd.DataFrame(dict2)
    df1.to_csv('top_transaction_data.csv', index=False)


def transform():
    df1 = pd.read_csv('../top_transaction_data.csv')
    df1.drop_duplicates(inplace=True)
    df1.replace('', np.nan, inplace=True)
    df1.dropna(inplace=True)
    df1['State'] = df1['State'].str.title()
    df1['State'] = df1['State'].apply(lambda x: x.replace("-", " "))
    df1['Year'] = pd.to_datetime(df1['Year'], format='%Y')
    # df1['TotalAmount'] = df1['Count'] * df1['Amount']
    df_grouped = df1.groupby(['State', 'Year']).agg({'Count': 'sum', 'Amount': 'mean'})
    df_grouped = df_grouped.reset_index()
    df_grouped.columns = ['State', 'Year', 'TotalCount', 'AverageAmount']
    df_grouped.to_csv('aggregatedtoptrans_data.csv', index=False)
    st.write(df_grouped)
    return df_grouped


def sqlcon1():
    df_grouped = transform()
    conn = mysql.connector.connect(host="database-phonepe-pulse.cbgdu1nd11gm.ap-south-1.rds.amazonaws.com",
                                       user="admin",
                                       password="Vanathis",
                                       database="database-phonepe-pulse")

    cursor = conn.cursor()

    for i, row in df_grouped.iterrows():
        sql = "INSERT INTO top_transaction (state, year, total_count, average_amount) VALUES (%s, %s, %s, %s)"
            # values = (state, year_2018, year_2019, year_2020, year_2021)
        cursor.execute(sql, tuple(row))

    query = """
                   SELECT * from top_transaction
               """
    cursor.execute(query)

    data = cursor.fetchall()

    df = pd.DataFrame(data, columns=['ID', 'State', 'year', 'total_count', 'average_amount'])
    # df = st.dataframe(data)
    st.write("Data Inserted Successfully")

    conn.commit()

    conn.close()

        # st.write(pivoted)

    return df



path1 = "C:/GitHub/pulse/data/top/user/country/india/state"

dict1 = {'State': [], 'Year': [], 'Quarter': [], 'Districts': [], 'Users': []}


def load_data(path1):
    for state in os.listdir(path1):
        state_dir = os.path.join(path1, state)
        for year in os.listdir(state_dir):
            year_dir = os.path.join(state_dir, year)
            for quarter_file in os.listdir(year_dir):
                quarter_path = os.path.join(year_dir, quarter_file)
                with open(quarter_path, 'r') as f:
                    data = json.load(f)
                    quarter = quarter_file.split('.')[0]
                    districts = data['data']['districts']
                    for district in districts:
                        district_name = district['name']#.format(lambda x: district['name'] if x.isalpha() else x.remove())
                        district_users = district['registeredUsers']
                        dict1['State'].append(state)
                        dict1['Year'].append(year)
                        dict1['Quarter'].append(quarter)
                        dict1['Districts'].append(district_name)
                        dict1['Users'].append(district_users)

    st.dataframe(dict1)
with tab1:
    extract(path)
    load_data(path1)

df = pd.DataFrame(dict1)

df.to_csv('users_data.csv', index=False)

def explore_data():

    df.dropna(inplace=True)

    df['Year'] = pd.to_datetime(df['Year'], format='%Y')

    grouped = df.groupby(['State', pd.Grouper(key='Year', freq='Y')])['Users'].sum()

    grouped_df = grouped.to_frame().reset_index()

    pivoted = grouped_df.pivot(index='State', columns='Year', values='Users')

    pivoted.fillna(0, inplace=True)

    df['State'] = df['State'].str.title()

    df['State'] = df['State'].apply(lambda x: x.replace("-"," "))

    #df['State'] = df['State'].astype(str)

    st.write(pivoted)

    pivoted.to_csv('pivoted_data.csv', index=False)

    return pivoted

with tab2:
    transform()
    explore_data()


def sql_con():

    pivoted = explore_data()

    conn = mysql.connector.connect(host="database-phonepe-pulse.cbgdu1nd11gm.ap-south-1.rds.amazonaws.com",
                                       user="admin",
                                       password="Vanathis",
                                       database="database-phonepe-pulse")

    cursor = conn.cursor()


    pivoted = pivoted.astype(int)

    for state,row in pivoted.iterrows():
        year_2018 = int(row['2018-12-31'])
        year_2019 = int(row['2019-12-31'])
        year_2020 = int(row['2020-12-31'])
        year_2021 = int(row['2021-12-31'])
        sql = "INSERT INTO users (state, year_2018, year_2019, year_2020, year_2021) VALUES (%s, %s, %s, %s, %s)"
        values = (state, year_2018, year_2019, year_2020, year_2021)
        cursor.execute(sql, values)

    query = """
               SELECT * from users
           """
    cursor.execute(query)

    data = cursor.fetchall()

    df = pd.DataFrame(data, columns=['ID','State', 'year_2018', 'year_2019', 'year_2020', 'year_2021'])
       # df = st.dataframe(data)
    st.write("Data Inserted Successfully")

    conn.commit()

    conn.close()

        #st.write(pivoted)

    return df
with tab3:
    sqlcon1()
    sql_con()
        # @st.cache_data()


def visualization():
        # with open('C:/GitHub/pulse/india_state_geo.json') as f:
        #   data = json.load(f)
    df = sql_con()
    df1 = sqlcon1()

    if option == 'Users':
        if option1 == '2018':
            fig = px.choropleth(
                    df,
                    geojson="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson",
                    featureidkey='properties.ST_NM',
                    locations='State',
                    hover_data=['year_2018'],
                    color='State',
                    color_continuous_scale='Viridis'
                )
            fig.update_geos(fitbounds="locations")

            st.plotly_chart(fig)
        elif option1 == '2019':
            fig = px.choropleth(
                    df,
                    geojson="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson",
                    featureidkey='properties.ST_NM',
                    locations='State',
                    hover_data=['year_2019'],
                    color='State',
                    color_continuous_scale='Viridis'
                )
            fig.update_geos(fitbounds="locations")

            st.plotly_chart(fig)
        elif option1 == '2020':
            fig = px.choropleth(
                    df,
                    geojson="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson",
                    featureidkey='properties.ST_NM',
                    locations='State',
                    hover_data=['year_2020'],
                    color='State',
                    color_continuous_scale='Viridis'
                )
            fig.update_geos(fitbounds="locations")

            st.plotly_chart(fig)
        elif option1 == '2021':
            fig = px.choropleth(
                    df,
                    geojson="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson",
                    featureidkey='properties.ST_NM',
                    locations='State',
                    hover_data=['year_2021'],
                    color='State',
                    color_continuous_scale='Viridis'
                )
            fig.update_geos(fitbounds="locations")

            st.plotly_chart(fig)
        else:
            st.write("Select any year")
    elif option == 'Transaction':
        fig = px.choropleth(
                df1,
                geojson="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson",
                featureidkey='properties.ST_NM',
                locations='State',
                hover_data=['average_amount'],
                color='State',
                color_continuous_scale='Viridis'
            )
        fig.update_geos(fitbounds="locations")

        st.plotly_chart(fig)
        # fig = px.choropleth_mapbox(df, geojson=data, locations='State', color='Users',
        # color_continuous_scale='Viridis', range_color=(0, df['Users'].max()),
        # mapbox_style='open-street-map', zoom=3, center={'lat': 20.5937, 'lon': 78.9629},
        # opacity=0.5, labels={'Users': 'Number of Users'}, featureidkey='properties.ID_1')

        # else:


with tab4:
    option = st.selectbox("Select any one", ('Users', 'Transaction'))
    option1 = st.selectbox("Select the year", ('2018', '2019', '2020', '2021'))
    visualization()


def visualization1():
    df = sql_con()

    fig = px.bar(df, x="State", y=["year_2018", "year_2019", "year_2020", "year_2021"],
                     barmode='group', title="User Growth by State")
    fig.update_layout(xaxis_title="State", yaxis_title="Number of Users")

    st.plotly_chart(fig)


with tab5:
    visualization1()

