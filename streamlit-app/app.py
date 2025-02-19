import streamlit as st
import pandas as pd
import pandas_gbq
import numpy as np
import google.auth
from streamlit_autorefresh import st_autorefresh

count = st_autorefresh(interval=5000)
st.header("Data Visualized from BQ Database")
credentials, project = google.auth.default()  # From google-auth or pydata-google-auth library.

pandas_gbq.context.credentials = credentials
pandas_gbq.context.project = "mythical-temple-451205-j6"

st.subheader("Cars Purchased Per Month")

df = pandas_gbq.read_gbq("SELECT date FROM `testdataset1.car_buyers_table`")
df['date'] = pd.to_datetime(df['date'])
monthly_sales = df.groupby(df['date'].dt.month).count()
monthly_sales = monthly_sales.rename(columns={'date':'sales'})
monthly_sales.style.format(precision=0)

# Replace the default generated index from the groupby() function with the month name
months = pd.Series(pd.date_range(start='2024-01', freq='MS', periods=12), name='month')
monthly_sales.index+=-1
monthly_sales = pd.concat([months.to_frame(), monthly_sales], axis=1).fillna(0)
monthly_sales = monthly_sales.sort_values(by='month')
monthly_sales = monthly_sales.set_index('month')
monthly_sales.index = monthly_sales.index.strftime('%m-%B')

st.text("")
st.line_chart(monthly_sales, y='sales', x_label='Month', y_label='Total Sales')
st.dataframe(monthly_sales.style.format(precision=0), use_container_width=True)
st.text("")
st.text("")

st.subheader("Average Price of Car Purchased by Gender")

df = pandas_gbq.read_gbq("SELECT gender, car_price FROM `testdataset1.car_buyers_table`")
sales_by_gender = df.groupby(df['gender']).mean()
sales_by_gender['car_price'] = sales_by_gender['car_price'].round(2)

st.bar_chart(sales_by_gender)
st.dataframe(sales_by_gender.style.format(precision=2), use_container_width=True)

st.subheader("Average Price of Car Purchased by Age Group")

df_age_total = pandas_gbq.read_gbq("SELECT age_num, car_price FROM `testdataset1.car_buyers_table`")
df_age_total['car_price'] = df_age_total['car_price'].astype(float).round(2)
df_age_total.sort_values(by='car_price',ascending=False)
st.scatter_chart(df_age_total.style.format(precision=2), x='age_num', y='car_price', x_label='Age', y_label = 'Car Price')

df_age_group_means = df_age_total
df_age_group_means['Age Groups'] = pd.cut(x=df_age_group_means['age_num'], bins=[0,25,45,100], labels=["18 to 25","25 to 45","45+"])
df_age_group_means = df_age_group_means.groupby(df_age_group_means['Age Groups']).mean()
df_age_group_means = df_age_group_means.rename(columns={'age_num':'Average Age', 'car_price':'Average Car Price'})
st.dataframe(df_age_group_means.style.format(precision=2), use_container_width=True)