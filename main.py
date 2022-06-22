from pyspark import SparkContext, SparkConf, SQLContext

path_jar_driver = 'C:\Workspaces\mysql-connector-java-8.0.29.jar'

#Configuración de la sesión
conf=SparkConf() \
    .set('spark.driver.extraClassPath', path_jar_driver)

spark_context = SparkContext(conf=conf)
sql_context = SQLContext(spark_context)
spark = sql_context.sparkSession

def execute_query(db_connection_string, sql, db_user, db_psswd):
    df_bd = spark.read.format('jdbc')\
        .option('url', db_connection_string) \
        .option('dbtable', sql) \
        .option('user', db_user) \
        .option('password', db_psswd) \
        .option('driver', 'com.mysql.cj.jdbc.Driver') \
        .load()
    return df_bd

db_user = 'Estudiante_14'
db_psswd = 'KGRE7I6YIZ'
db_connection_string = 'jdbc:mysql://157.253.236.116:8080/WWImportersTransactional'

sql_count_data = execute_query(db_connection_string, '(select count(*) from movimientosCopia) AS Compatible', db_user, db_psswd)
sql_count_data.show()

sql_count_columns = execute_query(db_connection_string, '(SELECT TABLE_NAME, count( COLUMN_NAME ) FROM INFORMATION_SCHEMA.COLUMNS WHERE table_name = "movimientosCopia" AND table_schema = "WWImportersTransactional") AS Compatible', db_user, db_psswd)
sql_count_columns.show()

sql_transaction_id = execute_query(db_connection_string, '(SELECT COUNT(*) FROM (SELECT COUNT(*) AS num FROM movimientosCopia AS a GROUP BY a.TransaccionProductoID) t WHERE num > 1) AS Compatible', db_user, db_psswd)
sql_transaction_id.show()

# Completitud
sql_completitud = execute_query(db_connection_string, '(select count(*) from movimientosCopia where ProveedorID = "") AS Compatible', db_user, db_psswd)
sql_completitud.show()

# Unicidad
sql_unicidad = execute_query(db_connection_string, '(SELECT COUNT(*) FROM (SELECT COUNT(*) AS num FROM movimientosCopia AS a GROUP BY a.TransaccionProductoID) t WHERE num > 1) AS Compatible', db_user, db_psswd)
sql_unicidad.show()

# Validez
sql_validez = execute_query(db_connection_string, '(SELECT * FROM movimientosCopia WHERE FechaTransaccion LIKE "%.%" OR FechaTransaccion LIKE "% %") AS Compatible', db_user, db_psswd)
sql_validez.show()