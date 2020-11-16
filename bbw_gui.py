import pandas as pd
import streamlit as st
from bbw.bbw import annotate
import base64
from io import StringIO

st.set_page_config(page_title="bbw", page_icon=None, layout='centered', initial_sidebar_state='auto')


def settings():
    st.set_option('client.caching', False)
    st.sidebar.title("bbw: Match CSV to Wikidata")


def get_table_download_link(df, fname):
    csv = df.to_csv(index=False)
    b64 = base64.b64encode(
        csv.encode()
    ).decode()
    return f'<br><a href="data:file/csv;base64,{b64}" download="bbw.{fname}">CSV</a>'


def process_data(uploaded_file):
    rawtable = st.empty()
    with rawtable.beta_container():
        bytes_data = uploaded_file.read()
        uploaded_file.seek(0)
        filename = uploaded_file.name
        s=str(bytes_data, 'utf-8')
        data = StringIO(s) 
        csvfile = pd.read_csv(data, dtype=str, header=None)
        rawcsv = csvfile[1:]
        rawcsv.columns = csvfile.iloc[0]
        st.subheader("INPUT")
        st.table(rawcsv)
    return [csvfile, filename, rawtable]


def annotate_data(csvfile, filename):
    bbwtable = st.empty()
    with bbwtable.beta_container():
        [webtable, urltable, labeltable, cpa_sub, cea_sub, cta_sub] = annotate(csvfile,filename)
        st.subheader("OUTPUT: Semantically annotated web table")
        st.write(webtable.to_html(render_links=True, escape=False), unsafe_allow_html=True)
        st.markdown(get_table_download_link(webtable, filename), unsafe_allow_html=True)
        st.subheader("OUTPUT: Table with up-to-date URLs in Wikidata")
        st.write(urltable.to_html(render_links=True, escape=False), unsafe_allow_html=True)
        st.markdown(get_table_download_link(urltable, 'url_'+filename), unsafe_allow_html=True)
        st.subheader("OUTPUT: Table with up-to-date labels in Wikidata")
        st.write(labeltable.to_html(render_links=True, escape=False), unsafe_allow_html=True)
        st.markdown(get_table_download_link(labeltable, 'label_'+filename), unsafe_allow_html=True)
        st.subheader("CPA")
        st.write(cpa_sub.to_html(render_links=True, escape=False), unsafe_allow_html=True)
        st.markdown(get_table_download_link(cpa_sub, 'cpa_'+filename), unsafe_allow_html=True)
        st.subheader("CTA")
        st.write(cta_sub.to_html(render_links=True, escape=False), unsafe_allow_html=True)
        st.markdown(get_table_download_link(cpa_sub, 'cta_'+filename), unsafe_allow_html=True)
        st.subheader("CEA")
        st.write(cea_sub.to_html(render_links=True, escape=False), unsafe_allow_html=True)
        st.markdown(get_table_download_link(cpa_sub, 'cea_'+filename), unsafe_allow_html=True)
    return bbwtable


if __name__ == "__main__":
    settings()
    filebox = st.empty()
    with filebox.beta_container():
        uploaded_file = st.sidebar.file_uploader("Choose a raw CSV-file", type=['csv'])
        if uploaded_file:
            try:
                [csvfile, filename, rawtable] = process_data(uploaded_file)
            except Exception:
                st.info('Something went wrong: bbw is unable to process the input '+filename)
            try:
                bbwtable = annotate_data(csvfile, filename)
            except Exception:
                st.info('Something went wrong: bbw is unable to annotate the input '+filename)        
