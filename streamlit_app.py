import streamlit
import pandas
import requests
import snowflake.connector
from urllib.error import URLError
import re



streamlit.title("Data Lineage");

#streamlit.header('Select your Database')

def get_db():
    with my_cnx.cursor() as my_cur:
        my_cur.execute("Select Database_name from SNOWFLAKE.INFORMATION_SCHEMA.DATABASES;")
        return my_cur.fetchall();

my_cnx = snowflake.connector.connect(**streamlit.secrets["snowflake"])
db_row = get_db()
my_cnx.close()
#streamlit.dataframe(db_row)

db_option = streamlit.selectbox(
'select db',
 (db_row))
dbname = re.findall(r"'(.*?)'", str(db_option), re.DOTALL)

schemaquery = "select DISTINCT table_schema from SNOWFLAKE.INFORMATION_SCHEMA.TABLE_STORAGE_METRICS where table_catalog = '"+str(dbname[0])+"';"

def get_results(query):
    with my_cnx.cursor() as my_cur:
        my_cur.execute(query)

        return my_cur.fetchall();

my_cnx = snowflake.connector.connect(**streamlit.secrets["snowflake"])

schema_row = get_results(schemaquery)

my_cnx.close()

#streamlit.dataframe(schema_row)


schema_option = streamlit.selectbox(
'select schema',
 (schema_row))
schemaname = re.findall(r"'(.*?)'", str(schema_option), re.DOTALL)



tablequery = "select DISTINCT table_name from SNOWFLAKE.INFORMATION_SCHEMA.TABLE_STORAGE_METRICS where table_catalog = '"+str(dbname[0])+"' and table_schema ='"+str(schemaname[0])+"';"

my_cnx = snowflake.connector.connect(**streamlit.secrets["snowflake"])

table_row = get_results(tablequery)

my_cnx.close()

#streamlit.dataframe(table_row)


table_option = streamlit.selectbox(
'select table',
 (table_row))
tablename = re.findall(r"'(.*?)'", str(table_option), re.DOTALL)

#streamlit.write(tablename)
columnquery = "select column_name from "+str(dbname[0])+".information_schema.columns where table_catalog = '"+str(dbname[0])+"' and table_schema = '"+str(schemaname[0])+"' and table_name = '" + str(tablename[0])+"';"

my_cnx = snowflake.connector.connect(**streamlit.secrets["snowflake"])

column_row = get_results(columnquery)

my_cnx.close()

#streamlit.dataframe(column_row)

column_option = streamlit.selectbox(
'select column',
 (column_row))
columnname = re.findall(r"'(.*?)'", str(column_option), re.DOTALL)

#streamlit.write(columnname[0])


val = streamlit.text_input("Enter column values" )
check_update_query = "SELECT t.query_start_time,t.USER_NAME,t.objects_modified,upper(q.query_text) as query_string from SNOWFLAKE.ACCOUNT_USAGE.ACCESS_HISTORY t LEFT JOIN SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY q on q.query_id = t.query_id,LATERAL FLATTEN(CASE WHEN ARRAY_SIZE(t.base_objects_accessed)>0 then t.base_objects_accessed ELSE t.objects_modified END) b WHERE b.value:\"objectName\""+" = '"+str(dbname[0]+"."+schemaname[0]+"."+tablename[0])+"' and startswith(query_string, 'UPDATE') and contains(query_string,'"+str("WHERE "+columnname[0]+" = "+val)+"') ORDER BY query_start_time desc;"

streamlit.write(check_update_query)




# streamlit.text('Omega 3 & Blueberry Oatmeal')
# streamlit.text('Kale, Spinach & Rocket Smoothie')
# streamlit.text('Hard-Boiled Free-Range Egg')

# my_fruit_list = pandas.read_csv("https://uni-lab-files.s3.us-west-2.amazonaws.com/dabw/fruit_macros.txt")
# my_fruit_list = my_fruit_list.set_index('Fruit')

# fruits_selected = streamlit.multiselect("Pick some fruits:", list(my_fruit_list.index),['Avocado','Strawberries'])
# fruits_to_show = my_fruit_list.loc[fruits_selected]

# streamlit.dataframe(fruits_to_show)

# def get_fruityvice_data(this_fruit_choice):
#     fruityvice_response = requests.get("https://fruityvice.com/api/fruit/"+this_fruit_choice)
#     fruityvice_normalized = pandas.json_normalize(fruityvice_response.json())
#     return fruityvice_normalized

# streamlit.header("Fruityvice Fruit Advice!")
# try:
    
#     fruit_choice = streamlit.text_input('What fruit would you like information about?')
#     if not fruit_choice:
#         streamlit.error("Please select a fruit to get information.")
#     else:
        
#         # streamlit.write('The user entered ', fruit_choice)
#         back_from_function = get_fruityvice_data(fruit_choice)
#         # write your own comment - what does this do?
#         streamlit.dataframe(back_from_function)
# except URLError as e:
#     streamlit.error()







# streamlit.text("The fruit load list contains:")
# def get_fruit_load_list():
#     with my_cnx.cursor() as my_cur:
#         my_cur.execute("SELECT * from fruit_load_list")
#         return my_cur.fetchall();
# if streamlit.button('Get Fruit Load List'):
#     my_cnx = snowflake.connector.connect(**streamlit.secrets["snowflake"])
#     my_data_row = get_fruit_load_list()
#     my_cnx.close()
#     streamlit.dataframe(my_data_row)


# def insert_row_snowflake(new_fruit):
#     with my_cnx.cursor() as my_cur:
#         my_cur.execute("insert into fruit_load_list values ('"+new_fruit+"')")
#         return "Thanks for adding "+new_fruit

# add_my_fruit = streamlit.text_input('What fruit would you like to add')
# if streamlit.button('Add a fruit to the list'):
#     my_cnx = snowflake.connector.connect(**streamlit.secrets["snowflake"])
#     back_from_function = insert_row_snowflake(add_my_fruit)
#     streamlit.text(back_from_function)
   
    