import streamlit as st

from scrape import scrap_all


def scrape():
    st.title("downloading, it will take a while")
    data = scrap_all(typ_stavby="byty")
    csv = data.to_csv().encode('utf-8')
    st.title("downloaded")
    st.download_button(
        label="Download data as CSV",
        data=csv,
        file_name='byty.csv',
        mime='text/csv',
    )


st.button("prepare data", on_click=scrape)
