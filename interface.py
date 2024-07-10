    
import streamlit as st
from vcis.cdr.format2.preprocess import CDR_Preprocess
from vcis.bts.integration import CDR_Integration
from vcis.utils.utils import CDR_Utils, CDR_Properties
from vcis.databases.cassandra.cassandra_tools import CassandraTools
from vcis.databases.cassandra_spark.cassandra_spark_tools import CassandraSparkTools
from vcis.script.cdr import trace2_preprocess
from vcis.script.cdr import trace1_preprocess

import os
import pandas as pd

import os

# Set environment variable
os.environ['STREAMLIT_BROWSER_GATHER_USAGE_STATS'] = 'false'
utils = CDR_Utils()
properties = CDR_Properties()
preprocess_format2 = CDR_Preprocess()
from vcis.cdr.format1.preprocess import CDR_Preprocess
preprocess_format1 = CDR_Preprocess()
from vcis.cdr.format3.preprocess import CDR_Preprocess
preprocess_format3 = CDR_Preprocess()

integration = CDR_Integration()
cassandra_tools = CassandraTools()
cassandra_spark_tools = CassandraSparkTools()

offset = True
def main():
    # Navigation menu
    page = st.sidebar.selectbox("Choose a page", ["Trace", "LH",'NBI'])
    if page == "LH":
        st.title('LH')
        st.write('Note: CSV files - Only same ID data accepted!')
        # Display the file uploader
        uploaded_files = st.file_uploader("Upload CSV files", type=['csv'], accept_multiple_files=True)
        if uploaded_files is not None:
            # Update the session state with the new files
            st.session_state["uploaded_files"] = uploaded_files
        
        # Use the session state to access the uploaded files for processing
        if st.session_state["uploaded_files"]:
            all_files = st.session_state["uploaded_files"]
            df = utils.read_cdr_records(all_files)
            df = preprocess_format2.select_columns(df)
            
            # Display the first 5 rows of the DataFrame
            st.write(df.head(5))

            processing_message = st.empty()
            # Conditionally display the process or insert button based on the processing status
            session =cassandra_tools.get_cassandra_connection(server=properties.cassandra_server)
            if st.button("Process Data"):
                progress_bar = st.progress(0)
                step_progress = 1.0 / 4
                processing_message.text("Processing...")
                df = trace2_preprocess.process_data(df, progress_bar, step_progress,offset,session)
                processing_message.text("Processing completed.")
                trace2_preprocess.insert_data(df)
                processing_message.text("Insert completed.")
                st.session_state["imsi"] = df[properties.imsi].iloc[0] 
                st.session_state["imei"] = df[properties.imei].iloc[0] 
                st.session_state["phone_number"] = df[properties.phone_number].iloc[0]
                
                st.session_state["imsi"] = st.text_input("Enter IMSI", value=st.session_state["imsi"])
                st.session_state["imei"] = st.text_input("Enter IMEI", value=st.session_state["imei"])
                st.session_state["phone_number"] = st.text_input("Enter Phone Number", value=st.session_state["phone_number"])

    elif page == "Trace":
        st.title('Trace')
        st.write('Note: XLSX files - Only one file is accepted!')
        uploaded_files = st.file_uploader("Upload Excel file", type=['xlsx'], accept_multiple_files=False)
        if uploaded_files is not None:
            # Update the session state with the new files
            st.session_state["uploaded_files"] = uploaded_files
            if st.session_state["uploaded_files"]:
                file_path = st.session_state["uploaded_files"]

                processing_message = st.empty()
                session =cassandra_tools.get_cassandra_connection(server=properties.cassandra_server)
                if st.button("Process Data"):
                    progress_bar = st.progress(0)
                    step_progress = 1.0 / 10
                    processing_message.text("Processing...")
                    df = trace1_preprocess.process_data(file_path, progress_bar, step_progress,offset,session)
                    processing_message.text("Processing completed.")
                    st.write(df.head(5))
                    trace1_preprocess.insert_data(df,session)
                    processing_message.text("Insert completed.")
                    st.session_state["imsi"] = df[properties.imsi].iloc[0] 
                    st.session_state["imei"] = df[properties.imei].iloc[0] 
                    st.session_state["phone_number"] = df[properties.phone_number].iloc[0]
                    
                    # Display the values in input fields
                    st.session_state["imsi"] = st.text_input("Enter IMSI", value=st.session_state["imsi"])
                    st.session_state["imei"] = st.text_input("Enter IMEI", value=st.session_state["imei"])
                    st.session_state["phone_number"] = st.text_input("Enter Phone Number", value=st.session_state["phone_number"])

    elif page == "NBI":
        st.title('NBI')
        st.write('Note: CSV files - Only one file is accepted!')
        uploaded_files = st.file_uploader("Upload CSV file", type=['csv'], accept_multiple_files=False)
        if uploaded_files is not None:
            st.session_state["uploaded_files"] = uploaded_files
            if st.session_state["uploaded_files"]:
                file_path = st.session_state["uploaded_files"]

                processing_message = st.empty()
                data_type = st.radio("Select Data Type", ('CDR', 'SDR'))
                process_enabled = st.checkbox("Enable Processing", value=False)
                session =cassandra_tools.get_cassandra_connection(server=properties.cassandra_server)
                if st.button("Process Data", disabled=not process_enabled):
                    progress_bar = st.progress(0)
                    step_progress = 1.0 / 5
                    processing_message.text("Processing...")
                    df = preprocess_format3.process_data(file_path, progress_bar, step_progress, offset, data_type)
                    processing_message.text("Processing completed.")
                    st.write(df.head(5))
                    preprocess_format3.insert_data(df,session)
                    processing_message.text("Insert completed.")
                    st.session_state["imsi"] = df[properties.imsi].iloc[0] 
                    st.session_state["imei"] = df[properties.imei].iloc[0] 
                    st.session_state["phone_number"] = df[properties.phone_number].iloc[0]
                    
                    st.session_state["imsi"] = st.text_input("Enter IMSI", value=st.session_state["imsi"])
                    st.session_state["imei"] = st.text_input("Enter IMEI", value=st.session_state["imei"])
                    st.session_state["phone_number"] = st.text_input("Enter Phone Number", value=st.session_state["phone_number"])

if __name__ == "__main__":
    main()