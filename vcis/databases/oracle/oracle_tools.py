import oracledb
import pandas as pd, geopandas as gpd
import json
from vcis.utils.properties import CDR_Properties
from shapely.geometry import shape
import shapely.wkt
import psycopg2


class OracleTools:
    def __init__(self, verbose=False):
        self.properties = CDR_Properties()
        self.table_name = self.properties.co_traveler_table
        self.verbose = verbose

    # def get_oracle_connection(self,clob:bool = False):
    #     # Initialization
    #     try:
    #         oracledb.init_oracle_client(lib_dir=self.properties.oracle_lib_dir)    
    #     except:
    #         pass
    #     # Create a connection
    #     if clob:
    #         oracledb.defaults.fetch_lobs = False
    #     connection = oracledb.connect(user = self.properties.oracle_username,
    #                                    password = self.properties.oracle_password, 
    #                                    dsn = f"{self.properties.oracle_server}:{self.properties.oracle_port}/{self.properties.oracle_key_space}")
    #     cursor = connection.cursor()
    #     return cursor, connection
    
    def get_oracle_connection(self,clob:bool = False,postgres:bool = False):
        if postgres:
            connection = psycopg2.connect(host=self.properties.postgres_host,
                                          database=self.properties.postgres_database,
                                          user=self.properties.postgres_username,
                                          password=self.properties.postgres_password,
                                          port=self.properties.postgres_port)
            cursor = connection.cursor()
        # Initialization
        else:
            try:
                oracledb.init_oracle_client(lib_dir=self.properties.oracle_lib_dir)    
            except:
                pass
            # Create a connection
            if clob:
                oracledb.defaults.fetch_lobs = False
            connection = oracledb.connect(user = self.properties.oracle_username,
                                        password = self.properties.oracle_password,
                                        dsn = f"{self.properties.oracle_server}:{self.properties.oracle_port}/{self.properties.oracle_key_space}")
            cursor = connection.cursor()
        return cursor, connection
    
    def get_table (self,key_space:str = None,table_name:str = None,table_id:int = 0  , id : bool = False):
        cursor, connection = self.get_oracle_connection()
        if id:
            query = f"SELECT * FROM {key_space}.{table_name}_{table_id}"
        else:
            query = f"SELECT * FROM {key_space}.{table_name}"
        print(query)
        cursor.execute(query)
        result = cursor.fetchall()
        if self.verbose:
            print("table fetched successfully")
        # Convert result to pandas DataFrame
        columns = [desc[0] for desc in cursor.description]
        result = pd.DataFrame(result, columns=columns)
        connection.commit()
        connection.close()
        return result

    
    def drop_create_table(self, table_name, table_id, table_schema_query, drop : bool = True):
        cursor, connection = self.get_oracle_connection()

        table_name = '{}_{}'.format(table_name, table_id)
        if drop:
            try:
                cursor.execute(f"DROP TABLE SSDX_TMP.{table_name}")
                print("Table dropped successfully",table_name)
            except:
                print("Table does not exist",table_name)
                pass
        # Create table for AOI in SQL
        cursor.execute(table_schema_query.format(table_name))
        connection.commit()
        connection.close()
        if self.verbose:
            print("Table created successfully", table_name)
        

    def insert_into_oracle(self, data, table_name, table_id):
        table_name= '{}_{}'.format(table_name, table_id)
        columns = data.columns.tolist()
        placeholders = ', '.join(':' + str(i+1) for i in range(len(columns)))
        co_sql = f'INSERT INTO SSDX_TMP.{table_name} ({", ".join(columns)}) VALUES ({placeholders})'
        cursor, connection = self.get_oracle_connection()

        try: 
            for _, row in data.iterrows():
                cursor.execute(co_sql, tuple(row))
        except Exception as e :
            print(' INFO: Problem with the following Query')
            print(e)
            # print(tuple(row))
            # print(co_sql)

        connection.commit()
        connection.close()

        if self.verbose:
            print(len(data), "Rows inserted successfully into Oracle table SSDX_TMP.{}".format(table_name))

    def drop_create_insert(self, data, table_name, table_id, table_schema_query):
        self.drop_create_table(table_name, table_id, table_schema_query)
        if data is None:
            print("No data to insert")
        else:
            self.insert_into_oracle(data, table_name, table_id)

    def execute_oracle_procedure(self, procedure_query, procedure_id):
        cursor, connection = self.get_oracle_connection()
        print(procedure_query.format(procedure_id))
        cursor.execute(procedure_query.format(procedure_id))
        connection.commit()
        connection.close()
        if self.verbose:
            print("Procedure {} executed successfully".format(procedure_id))

    def get_oracle_result(self, table_name, table_id):
        cursor, connection = self.get_oracle_connection()
        result_table_name = '{}_{}'.format(table_name, table_id)
        cursor.execute(f"SELECT NAME_ID, LAT, LNG, NBR_HITS, dbms_lob.substr(SHAPE) AS SHAPE FROM SSDX_TMP.{result_table_name}")
        result = cursor.fetchall()
        if self.verbose:
            print("Procedure result fetched successfully")
        
        # Convert result to pandas DataFrame
        columns = [desc[0] for desc in cursor.description]
        result = pd.DataFrame(result, columns=columns)
        connection.close()
        return result
    
    def get_execute_oracle_procedure_result(self, procedure_query, procedure_id, result_table_name):
        self.execute_oracle_procedure(procedure_query, procedure_id)
        return self.get_oracle_result(result_table_name, procedure_id)

    
    # def get_oracle_query(self,query:str=None,):
    #     cursor, connection = self.get_oracle_connection()
    #     cursor.execute(query)
    #     result = cursor.fetchall()
    #     columns = [desc[0] for desc in cursor.description]
    #     result = pd.DataFrame(result, columns=columns)
    #     connection.commit()
    #     connection.close()
    #     return result
    
    def get_oracle_query(self,query:str=None, postgres:bool = False, clob:bool=False):
        cursor, connection = self.get_oracle_connection(postgres=postgres,clob=clob)
        cursor.execute(query)
        result = cursor.fetchall()
        # print(result)
        columns = [desc[0] for desc in cursor.description]
        result = pd.DataFrame(result, columns=columns)
        connection.commit()
        connection.close()
        return result
    
    def execute_sql_query(self,Simulation_id):

        cursor, connection = self.get_oracle_connection()

        try:
            # Create a cursor
            cursor = connection.cursor()

            # Define the SQL query
            sql_query = self.properties.oracle_timeline_query.format(Simulation_id=Simulation_id)
            # Execute the SQL query
            cursor.execute(sql_query)

            # Fetch the result
            result = cursor.fetchall()

            clob_content = result[0][0].read()  # Read the entire content of the CLOB
            data = clob_content
            # Now you can return the marker_positions list or use it as needed
            return data

        finally:
            # Close the cursor and connection
            cursor.close()
            connection.close()

    def get_simulation_shapes(self,table_id):
        radius_list = []
        center_list = []
        shapes = []
        query = "SELECT dbms_lob.substr(execution_param) AS SHAPE FROM web_api_method_log_details where method_log_id={}"
        query = query.format(table_id)

        cursor, connection = self.get_oracle_connection()
        cursor.execute(query)
        result = cursor.fetchall()

        coordinates = json.loads(result[0][0])

        for shape in coordinates['Coordinates'] :
            print(shape)
            shape_type = shape['Type']

            if shape_type =='Circle':
                radius = shape['radius']
                center = shape['center']
                shapes.append((center,radius))
                # center_list.append(center)
        # shapes = {'center':center_list,
        #            'radius':radius_list}
        return shapes
    
    
    def get_cities_geopandas(self,country_name:str):

        query = f"select * from SSDX_ENG.CITIES_BORDERS_123 where SHAPEGROUP='{country_name}'"
        # query = f"select SHAPENAME, SHAPEISO, SHAPEID, SHAPEGROUP, SHAPETYPE, SUBSTR(GEOMETRY,1,100000000) AS GEOMETRY from SSDX_TMP.CITIES_BORDERS_123 where SHAPEGROUP='{country_name}'"
        oracle_table = self.get_oracle_query(query=query,clob=True)
        oracle_table.columns = oracle_table.columns.str.lower()
        oracle_table['geometry'] = oracle_table['geometry'].apply(lambda wkt: shapely.wkt.loads(wkt))
        gdf = gpd.GeoDataFrame(oracle_table,geometry='geometry',crs="EPSG:4326")
        return gdf
    
    
    def get_all_cities_geopandas(self):

        query = f"select * from SSDX_ENG.CITIES_BORDERS_123"
        # query = f"select SHAPENAME, SHAPEISO, SHAPEID, SHAPEGROUP, SHAPETYPE, SUBSTR(GEOMETRY,1,100000000) AS GEOMETRY from SSDX_TMP.CITIES_BORDERS_123 where SHAPEGROUP='{country_name}'"
        oracle_table = self.get_oracle_query(query=query,clob=True)
        oracle_table.columns = oracle_table.columns.str.lower()
        oracle_table['geometry'] = oracle_table['geometry'].apply(lambda wkt: shapely.wkt.loads(wkt))
        gdf = gpd.GeoDataFrame(oracle_table,geometry='geometry',crs="EPSG:4326")
        return gdf