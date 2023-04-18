import os
import json
import pandas as pd
import streamlit as st
import mysql.connector
import plotly.express as px
import numpy as np

st.set_page_config(page_title="Phonepe",page_icon="👋",)
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
    df1 = pd.read_csv('top_transaction_data.csv')
    df1.drop_duplicates(inplace=True)
    df1.replace('', np.nan, inplace=True)
    df1.dropna(inplace=True)
    df1['Year'] = pd.to_datetime(df1['Year'], format='%Y')
    # df1['TotalAmount'] = df1['Count'] * df1['Amount']
    df_grouped = df1.groupby(['State', 'Year']).agg({'Count': 'sum', 'Amount': 'mean'})
    df_grouped = df_grouped.reset_index()
    df_grouped.columns = ['State', 'Year', 'TotalCount', 'AverageAmount']
    #df_grouped.to_csv('aggregatedtoptrans_data.csv', index=False)
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
    df['State'] = df['State'].str.title()
    df['State'] = df['State'].apply(lambda x: x.replace("-", " "))
    #df['State'] = df['State'].apply(lambda x: x.replace("&", "and"))
    df['State'].loc[df["State"] == 'Andaman & Nicobar Islands'] = df['State'].loc[
        df["State"] == 'Andaman & Nicobar Islands'].apply(
        lambda x: x.replace("Andaman & Nicobar Islands", "Andaman & Nicobar"))
    df['State'].loc[df["State"] == 'Dadra & Nagar Haveli & Daman & Diu'] = df['State'].loc[
        df["State"] == 'Dadra & Nagar Haveli & Daman & Diu'].apply(lambda x: x.replace("&", "and"))
    st.write("Data Inserted Successfully")
    conn.commit()
    conn.close()
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
                        district_name = district['name']
                        district_users = district['registeredUsers']
                        dict1['State'].append(state)
                        dict1['Year'].append(year)
                        dict1['Quarter'].append(quarter)
                        dict1['Districts'].append(district_name)
                        dict1['Users'].append(district_users)

    st.dataframe(dict1)
    df = pd.DataFrame(dict1)
    df.to_csv('users_data.csv', index=False)


def explore_data():
    df = pd.read_csv('map_user_data.csv')
    df.dropna(inplace=True)
    df['Year'] = pd.to_datetime(df['Year'], format='%Y')
    grouped = df.groupby(['State', pd.Grouper(key='Year', freq='Y')])['Users'].sum()
    grouped_df = grouped.to_frame().reset_index()
    pivoted = grouped_df.pivot(index='State', columns='Year', values='Users')
    pivoted.fillna(0, inplace=True)
    # df['State'] = df['State'].astype(str)
    st.write(pivoted)
    return pivoted


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
    df['State'] = df['State'].str.title()
    df['State'] = df['State'].apply(lambda x: x.replace("-", " "))
   # df['State'] = df['State'].apply(lambda x: x.replace("&", "and"))
    df['State'].loc[df["State"] == 'Andaman & Nicobar Islands'] = df['State'].loc[df["State"] == 'Andaman & Nicobar Islands'].apply(lambda x: x.replace("Andaman & Nicobar Islands", "Andaman & Nicobar"))
    df['State'].loc[df["State"] =='Dadra & Nagar Haveli & Daman & Diu'] = df['State'].loc[df["State"] =='Dadra & Nagar Haveli & Daman & Diu'].apply(lambda x: x.replace("&", "and"))
    st.write("Data Inserted Successfully")
    conn.commit()
    conn.close()
        #st.write(pivoted)
    return df
        # @st.cache_data()


def visualization():
    if option == 'Visualize Users':
        if option1 == '2018':
            df = sql_con()
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
            df = sql_con()
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
            df = sql_con()
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
            df = sql_con()
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
    elif option == 'Visualize Transaction':
        df1 = sqlcon1()
        st.write("Average Amount")
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
        st.write("Total Count")
        fig = px.choropleth(
            df1,
            geojson="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson",
            featureidkey='properties.ST_NM',
            locations='State',
            hover_data=['total_count'],
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


def visualization1():
    df = sql_con()

    fig = px.bar(df, x="State", y=["year_2018", "year_2019", "year_2020", "year_2021"],
                     barmode='group', title="User Growth by State")
    fig.update_layout(xaxis_title="State", yaxis_title="Number of Users")

    st.plotly_chart(fig)


with tab1:
    option2 = st.selectbox("Select any one", ('Extract Users', 'Extract Transaction'))
    if option2 == 'Extract Transaction':
        st.write("Top Transaction")
        extract(path)
    elif option2 == 'Extract Users':
        st.write("Top User")
        load_data(path1)

with tab2:
    option3 = st.selectbox("Select any one", ('Transform Users', 'Transform Transaction'))
    if option3 == 'Transform Transaction':
        st.write("Top Transaction")
        transform()
    elif option3 == 'Transform Users':
        st.write("Top User")
        explore_data()

with tab3:
    option4 = st.selectbox("Select any one", ('Insert Users', 'Insert Transaction'))
    if option4 == 'Insert Transaction':
        st.write("Top Transaction")
        sqlcon1()
    elif option4 == 'Insert Users':
        st.write("Top User")
        sql_con()

with tab4:
    option = st.selectbox("Select any one", ('Visualize Users', 'Visualize Transaction'))
    option1 = st.selectbox("Select the year", ('2018', '2019', '2020', '2021'))
    visualization()

with tab5:
    visualization1()


#################################################################################################################################################################

import os
import json
import pandas as pd
import streamlit as st
import mysql.connector
import plotly.express as px
import geopandas as gpd
import numpy as np
import matplotlib.pyplot as plt


st.title("Phonepe Pulse Data Visualization and Exploration")
path = "C:/GitHub/pulse/data/map/transaction/hover/country/india/state"
dict1 = {'State': [], 'Year': [], 'Quarter': [], 'Districts': [], 'Count': [], 'Amount': []}

tab1, tab2, tab3, tab4 = st.tabs(["Extract Data", "Transform Data", "Insert Data", "Visualization"])


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
                    hoverDataList = data['data']['hoverDataList']
                    for hoverD in hoverDataList:
                        district_name = hoverD['name']
                        district_metric = hoverD['metric']
                        for districts in district_metric:
                            district_count = districts['count']
                            district_amount = districts['amount']
                            dict1['State'].append(state)
                            dict1['Year'].append(year)
                            dict1['Quarter'].append(quarter)
                            dict1['Districts'].append(district_name)
                            dict1['Count'].append(district_count)
                            dict1['Amount'].append(district_amount)
    st.dataframe(dict1)
    df1 = pd.DataFrame(dict1)
    df1.to_csv('map_transaction_data.csv', index=False)


path1 = "C:/GitHub/pulse/data/map/user/hover/country/india/state"
dict2 = {'State': [], 'Year': [], 'Quarter': [], 'Districts': [], 'Users': []}


def extract1(path1):
    for state in os.listdir(path1):
        state_dir = os.path.join(path1, state)
        for year in os.listdir(state_dir):
            year_dir = os.path.join(state_dir, year)
            for quarter_file in os.listdir(year_dir):
                quarter_path = os.path.join(year_dir, quarter_file)
                with open(quarter_path, 'r') as f:
                    data = json.load(f)
                    quarter = quarter_file.split('.')[0]
                    districts = data['data']['hoverData']

                    for district,values in districts.items():
                        district_users = values['registeredUsers']
                        districtname = district
                        dict2['State'].append(state)
                        dict2['Year'].append(year)
                        dict2['Quarter'].append(quarter)
                        dict2['Districts'].append(districtname)
                        dict2['Users'].append(district_users)



    st.dataframe(dict2)
    df = pd.DataFrame(dict2)
    df.to_csv('map_user_data.csv', index=False)


def transform():
    df1 = pd.read_csv('map_transaction_data.csv')
    df1.drop_duplicates(inplace=True)
    df1.replace('', np.nan, inplace=True)
    df1.dropna(inplace=True)
    df1['Year'] = pd.to_datetime(df1['Year'], format='%Y')
    # df1['TotalAmount'] = df1['Count'] * df1['Amount']
    df_grouped = df1.groupby(['State', 'Year']).agg({'Count': 'sum', 'Amount': 'mean'})
    df_grouped = df_grouped.reset_index()
    df_grouped.columns = ['State', 'Year', 'TotalCount', 'AverageAmount']
    #df_grouped.to_csv('aggregatedtoptrans_data.csv', index=False)
    st.write(df_grouped)
    return df_grouped



def transform1():
    df = pd.read_csv('map_user_data.csv')
    df.dropna(inplace=True)
    df['State'] = df['State'].str.title()
    df['State'] = df['State'].apply(lambda x: x.replace("-", " "))
    df['State'] = df['State'].apply(lambda x: x.replace("&", "and"))
    df['Year'] = pd.to_datetime(df['Year'], format='%Y')
    grouped = df.groupby(['State', pd.Grouper(key='Year', freq='Y')])['Users'].sum()
    grouped_df = grouped.to_frame().reset_index()
    pivoted = grouped_df.pivot(index='State', columns='Year', values='Users')
    pivoted.fillna(0, inplace=True)
    #df['State'] = df['State'].astype(str)
    st.write(pivoted)
    return pivoted


def sqlcon1():
    df_grouped = transform()
    conn = mysql.connector.connect(host="database-phonepe-pulse.cbgdu1nd11gm.ap-south-1.rds.amazonaws.com",
                                       user="admin",
                                       password="Vanathis",
                                       database="database-phonepe-pulse")
    cursor = conn.cursor()
    for i, row in df_grouped.iterrows():
        sql = "INSERT INTO map_transaction (state, year, total_count, average_amount) VALUES (%s, %s, %s, %s)"
            # values = (state, year_2018, year_2019, year_2020, year_2021)
        cursor.execute(sql, tuple(row))
    query = """
                   SELECT * from map_transaction
               """
    cursor.execute(query)
    data = cursor.fetchall()
    df = pd.DataFrame(data, columns=['ID', 'State', 'year', 'total_count', 'average_amount'])
    # df = st.dataframe(data)
    df['State'] = df['State'].str.title()
    df['State'] = df['State'].apply(lambda x: x.replace("-", " "))
    st.write("Data Inserted Successfully")
    conn.commit()
    conn.close()
    return df

def sql_con():

    pivoted = transform1()
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
        sql = "INSERT INTO map_users (state, year_2018, year_2019, year_2020, year_2021) VALUES (%s, %s, %s, %s, %s)"
        values = (state, year_2018, year_2019, year_2020, year_2021)
        cursor.execute(sql, values)
    query = """
               SELECT * from map_users
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


def visualization():
    df = sqlcon1()
    grouped = df.groupby(['State', 'year']).sum().reset_index()
    fig = px.pie(grouped, values='total_count', names='State', hole=.3)
    st.plotly_chart(fig)

with tab1:
    st.write("Map Transaction")
    extract(path)
    st.write("Map User")
    extract1(path1)


with tab2:
    st.write("Map Transaction")
    transform()
    st.write("Map User")
    transform1()


with tab3:
    st.write("Map Transaction")
    sql_con()
    st.write("Map User")
    sqlcon1()

with tab4:
    visualization()
######################################################################################################################################################################

import os
import json
import pandas as pd
import streamlit as st
import mysql.connector
import plotly.express as px
import geopandas as gpd
import numpy as np

st.title("Phonepe Pulse Data Visualization and Exploration")
path = "C:/GitHub/pulse/data/aggregated/transaction/country/india/state"
dict1 = {'State': [], 'Year': [], 'Quarter': [], 'Type of payment': [], 'Count': [], 'Amount': []}

tab1, tab2, tab3, tab4 = st.tabs(["Extract Data", "Transform Data", "Insert Data", "Visualization"])

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
                    transactionData = data['data']['transactionData']
                    for trans in transactionData:
                        district_name = trans['name']
                        district_payIns = trans['paymentInstruments']
                        for districts in district_payIns:
                            district_count = districts['count']
                            district_amount = districts['amount']
                            dict1['State'].append(state)
                            dict1['Year'].append(year)
                            dict1['Quarter'].append(quarter)
                            dict1['Type of payment'].append(district_name)
                            dict1['Count'].append(district_count)
                            dict1['Amount'].append(district_amount)
    st.dataframe(dict1)
    df1 = pd.DataFrame(dict1)
    df1.to_csv('aggregated_transaction_data.csv', index=False)


path1 = "C:/GitHub/pulse/data/aggregated/user/country/india/state"
dict2 = {'State': [], 'Year': [], 'Quarter': [], 'Brand': [], 'Count':[], 'Percentage':[]}


def extract1(path1):
    for state in os.listdir(path1):
        state_dir = os.path.join(path1, state)
        for year in os.listdir(state_dir):
            year_dir = os.path.join(state_dir, year)
            for quarter_file in os.listdir(year_dir):
                quarter_path = os.path.join(year_dir, quarter_file)
                with open(quarter_path, 'r') as f:
                    data = json.load(f)
                    quarter = quarter_file.split('.')[0]
                    try:
                        for districts in data['data']['usersByDevice']:
                            dict2['Brand'].append(districts['brand'])
                            dict2['Count'].append(districts['count'])
                            dict2['Percentage'].append(districts['percentage'])
                            dict2['State'].append(state)
                            dict2['Year'].append(year)
                            dict2['Quarter'].append(quarter)
                    except:
                        pass

    st.dataframe(dict2)
    df = pd.DataFrame(dict2)
    df.to_csv('aggregated_user_data.csv', index=False)



def transform():
    df1 = pd.read_csv('aggregated_transaction_data.csv')
    df1.drop_duplicates(inplace=True)
    df1.replace('', np.nan, inplace=True)
    df1.dropna(inplace=True)
    df1['Year'] = pd.to_datetime(df1['Year'], format='%Y')
    # df1['TotalAmount'] = df1['Count'] * df1['Amount']
    df_grouped = df1.groupby(['State', 'Year','Type of payment']).agg({'Count': 'sum', 'Amount': 'mean'})
    df_grouped = df_grouped.reset_index()
    df_grouped.columns = ['State', 'Year', 'Type of payment', 'TotalCount', 'Amount']
    #df_grouped.to_csv('aggregatedtoptrans_data.csv', index=False)
    st.write(df_grouped)
    return df_grouped



def transform1():
    df = pd.read_csv('aggregated_user_data.csv')
    df.dropna(inplace=True)
    df['State'] = df['State'].str.title()
    df['State'] = df['State'].apply(lambda x: x.replace("-", " "))
    df['State'] = df['State'].apply(lambda x: x.replace("&", "and"))
    df['Year'] = pd.to_datetime(df['Year'], format='%Y')
    grouped = df.groupby(['State', 'Year', 'Brand']).agg({'Count': 'sum', 'Percentage': 'mean'})
    grouped_df = grouped.reset_index()
    grouped_df.columns = ['State', 'Year', 'Brand', 'TotalCount', 'Percentage']
    grouped_df.fillna(0, inplace=True)
    #df['State'] = df['State'].astype(str)
    st.write(grouped_df)
    return grouped_df


@st.cache_data
def sqlcon1():
    df_grouped = transform()
    conn = mysql.connector.connect(host="database-phonepe-pulse.cbgdu1nd11gm.ap-south-1.rds.amazonaws.com",
                                       user="admin",
                                       password="Vanathis",
                                       database="database-phonepe-pulse")
    cursor = conn.cursor()
    for i, row in df_grouped.iterrows():
        sql = "INSERT INTO aggregated_transaction (state, year, type_of_payment, total_count, amount) VALUES (%s, %s, %s, %s, %s)"
        cursor.execute(sql, tuple(row))
    query = """
                   SELECT * from aggregated_transaction
               """
    cursor.execute(query)
    data = cursor.fetchall()
    df = pd.DataFrame(data, columns=['ID', 'State', 'year', 'type_of_payment', 'total_count', 'amount'])
    # df = st.dataframe(data)
    df['State'] = df['State'].str.title()
    df['State'] = df['State'].apply(lambda x: x.replace("-", " "))
    st.write("Data Inserted Successfully")
    conn.commit()
    conn.close()
    return df

@st.cache_data
def sqlcon():
    df_grouped = transform1()
    conn = mysql.connector.connect(host="database-phonepe-pulse.cbgdu1nd11gm.ap-south-1.rds.amazonaws.com",
                                       user="admin",
                                       password="Vanathis",
                                       database="database-phonepe-pulse")
    cursor = conn.cursor()
    for i, row in df_grouped.iterrows():
        sql = "INSERT INTO aggregated_users (state, year, brand, total_count, percentage) VALUES (%s, %s, %s, %s, %s)"
        cursor.execute(sql, tuple(row))
    query = """
                   SELECT * from aggregated_users
               """
    cursor.execute(query)
    data = cursor.fetchall()
    df = pd.DataFrame(data, columns=['ID', 'state', 'year', 'brand', 'total_count', 'percentage'])
    # df = st.dataframe(data)
    st.write("Data Inserted Successfully")
    conn.commit()
    conn.close()
    return df



with tab1:

    extract(path)
    extract1(path1)


with tab2:

    transform()
    transform1()

with tab3:

    sqlcon()
    sqlcon1()

