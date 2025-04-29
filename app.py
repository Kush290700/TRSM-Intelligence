import streamlit as st
from database import fetch_raw_tables
from data_preparation import prepare_full_data
from filters import apply_filters
from dashboard_ui import dashboard
from datetime import datetime

st.set_page_config(page_title="TRSM Intelligence", layout="wide")

@st.cache_data
def load_data(start=None,end=None):
    raw=fetch_raw_tables(start,end)
    return prepare_full_data(raw)

def main():
    st.title("📊 TRSM Advanced Analytics")
    view=st.sidebar.radio("View",["Sales Dashboard","Labor Analysis"])
    if view=="Sales Dashboard":
        min_d=st.sidebar.date_input("Start Date",value=datetime(2020,1,1))
        max_d=st.sidebar.date_input("End Date",value=datetime.today())
        df_all=load_data(min_d.strftime('%Y-%m-%d'),max_d.strftime('%Y-%m-%d'))
        df=apply_filters(df_all)
        cmap=df_all.set_index('CustomerId')['CustomerName'].to_dict()
        pmap=df_all.set_index('ProductId')['ProductName'].to_dict()
        if df.empty: st.warning("No data"); return
        dashboard(df_all,df,cmap,pmap)
    else:
        st.info("Labor Analysis TBD")

if __name__=='__main__': main()