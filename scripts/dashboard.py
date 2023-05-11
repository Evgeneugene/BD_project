import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
import matplotlib.pyplot as plt

asset_details = pd.read_csv('data/asset_details.csv') 
st.markdown('---')

st.markdown("""<style>body {     background-color: #eee; }
.fullScreenFrame > div {     display: flex;     justify-content: center; }
</style>""", unsafe_allow_html=True)

st.write("# Big Data Project  \n _Crypto forecast_ :sunglasses:  \n", "*Year*: **2023**   \n", "*Name*: **Evgenii Evlampev**")


st.markdown('---')

st.header("Data Characteristics")

st.markdown('1. Two tables: `train.csv`, `asset_details.csv`')
st.markdown('2. `train.csv` features: \n `id`, `timestamp`, `asset_id`, `count`, `open`, `high`, `low`, `close`, `volume`, `VWAP`, `target`')
st.markdown('3. `asset_details.csv` features: `asset_id`, `weight`, `asset_name`')
st.markdown('4. Number of entries in `train.csv`: 23486459 ')
st.markdown('5. Number of entries in `assets_details.csv`: 14')

st.header("Data Insights")

st.markdown('1. The distribution of cryptocurrencies in the dataset')

q1 = pd.read_csv("output/q1.csv")
st.write(q1)
fig = px.bar(x = asset_details["Asset_Name"],
	     y = asset_details["Asset_ID"])
fig.update_xaxes(title="Assets")
fig.update_yaxes(title = "Number of Rows")
fig.update_layout(showlegend = True,
	title = {
		'text': 'Data Distribution ',
		'y':0.95,
		'x':0.5,
		'xanchor': 'center',
		'yanchor': 'top'} ,
		template="plotly_white")
fig.show()
st.markdown('2. Average for all records volume weighted average price for the minute')

q2 = pd.read_csv("output/q2.csv")
st.write(q2)

st.markdown('3. The maximum difference between open and close moments for a given minute')

q3 = pd.read_csv("output/q3.csv")
st.write(q3)

st.markdown('4. Minute by minute display of change in price for Bitcoin')

q4 = pd.read_csv("output/q4.csv")
st.write(q4)


st.markdown('5. Timestamps when the price for Bitcoin is increasing')
q5 = pd.read_csv("output/q5.csv")
st.write(q5)
