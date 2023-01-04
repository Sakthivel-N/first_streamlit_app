import streamlit
import pandas as pd
import requests
import snowflake.connector
from urllib.error import URLError
import re
from io import StringIO


streamlit.title("Data Lineage");
@streamlit.cache(allow_output_mutation=True)
def get_results(query,s):
    my_cnx = snowflake.connector.connect(**streamlit.secrets["snowflake"])
    with my_cnx.cursor() as my_cur:
        try:
            my_cur.execute('use warehouse COMPUTE_WH;')
            
            for q in query:
                my_cur.execute(q)
            result= my_cur.fetchall()
            
            if(s != 'NO'):
                inputval = streamlit.selectbox("Enter your "+s+" : ", list(i[0] for i in result)) 
                return(inputval)
            else:
                streamlit.dataframe(result);
            
        finally:
            my_cur.close()
        
        my_cnx.close()


    
# ##db
streamlit.header('Select your Database')
dbval = get_results([f"Select Database_name from SNOWFLAKE.INFORMATION_SCHEMA.DATABASES;"],"Database")


streamlit.header('Select your Schema')
# ##schema
schemaval = get_results([f"select DISTINCT(table_schema )from SNOWFLAKE.INFORMATION_SCHEMA.TABLE_STORAGE_METRICS where table_catalog ='"+dbval+"';"],"schema")


streamlit.header('Select your Table')
# ##Table
tableval = get_results([f"select DISTINCT(table_name) from SNOWFLAKE.INFORMATION_SCHEMA.TABLE_STORAGE_METRICS where table_catalog = '"+dbval+"' and table_schema ='"+schemaval+"';"],"Table")


streamlit.header('All DML Changes')
# ##all Dml changes
get_results([f"call Data_lineage.PUBLIC.sp_dl_histroy('"+dbval+"."+schemaval+"."+tableval+"',1);",f"call Data_lineage.PUBLIC.sp_dl();",f"select * from Data_lineage.PUBLIC.employee_changes order by start_time ;"],'NO')
streamlit.header('All Inserts')
# ##inser all
get_results([f"select * from Data_lineage.PUBLIC.employee_changes where metadata$action ='INSERT' and metadata$isupdate='false' order by start_time;"],'NO')

streamlit.header('All Updates')
##update all
get_results([f"select * from Data_lineage.PUBLIC.employee_changes where (metadata$action ='INSERT' or metadata$action ='DELETE' )and metadata$isupdate='true' order by start_time;"],'NO')

streamlit.header('All Deletes')
##delete all
get_results([f"select * from Data_lineage.PUBLIC.employee_changes where metadata$action ='DELETE' and metadata$isupdate='false' order by  start_time;"],'NO')



streamlit.header('Enter your Column')
##column
COLUMNVAL = get_results([f"select column_name from "+dbval+".information_schema.columns where table_catalog = '"+dbval+"' and table_schema = '"+schemaval+"' and table_name = '" + tableval+"';"],"Column")


##columnval
SEARCHVAL  = streamlit.text_input("Enter "+COLUMNVAL+" value : " )


## FOR COLUMN 
# all dml
if SEARCHVAL:
    streamlit.header('All DML on column')
    get_results([f" select * from Data_lineage.PUBLIC.employee_changes  WHERE "+COLUMNVAL+" = "+ SEARCHVAL+" order by start_time;"],'NO')

    streamlit.header('All Insert on column')
    #insert only
    get_results([f"select * from Data_lineage.PUBLIC.employee_changes where metadata$action ='INSERT' and metadata$isupdate='false' AND "+COLUMNVAL+" = "+ SEARCHVAL+" order by start_time;"],'NO')

    streamlit.header('All Updates on column')
    #update only
    get_results([f"select * from Data_lineage.PUBLIC.employee_changes where (metadata$action ='INSERT' or metadata$action ='DELETE' )and metadata$isupdate='true' AND "+COLUMNVAL+" = "+ SEARCHVAL+" order by start_time;"],'NO')

    streamlit.header('All Deletes on column')
    #delete only
    get_results([f"select * from Data_lineage.PUBLIC.employee_changes where metadata$action ='DELETE' and metadata$isupdate='false'  AND "+COLUMNVAL+" = "+ SEARCHVAL+" order by  start_time;"],'NO')


