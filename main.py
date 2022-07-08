import streamlit as st
import pandas as pd
import numpy as np
import streamlit as st
from snowflake.snowpark import Session
from st_aggrid import AgGrid

SIMULATION_TABLE = 'STG_SIMULATION'

## Initialize connection.
# Uses st.experimental_singleton to only run once.

session = Session.builder.configs(st.secrets["snowflake"]).create()
df_simul = session.table('STG_SIMULATION')
df_simul_pd= df_simul.toPandas()

def update_table(df, table_name):
    new_simul = session.create_dataframe(df)
    new_simul.write.mode("overwrite").save_as_table(table_name)

def add_row():
    dict = df_simul_pd.to_dict()
    #df_simul_pd.append( df_simul_pd.iloc[-1], ignore_index=True )
    new_table = pd.concat([df_simul_pd, df_simul_pd.iloc[-1:]])
    new_table.iloc[-1][0] = new_table.iloc[-1][0] +1
    print(new_table)
    update_table(new_table, SIMULATION_TABLE)


#session.sql("select current_warehouse(), current_database(), current_schema()").collect()


st.button("add row", key=None, help=None, on_click=add_row, args=None, kwargs=None, disabled=False)
grid_return = AgGrid(df_simul_pd, editable=True)
update_table(grid_return['data'], SIMULATION_TABLE)


# Print results.
#for row in rows:
#    st.write(f"{row[0]} has a :{row[1]}:")
