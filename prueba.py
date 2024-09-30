import pandas as pd
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from utils import format_date  # Importamos la función desde utils.py

# Ruta del archivo CSV
ruta_csv = 'C:/Users/hermann.benda/DATA/BASETOTAL/game.csv'

# Cargar el archivo CSV como DataFrame
df = pd.read_csv(ruta_csv)

# Mostrar las primeras 5 filas del DataFrame
#print(df.head())

# Iniciar sesión de Spark
#spark = SparkSession.builder.appName("FormatoFecha").getOrCreate()

# Convertir el DataFrame de pandas a Spark DataFrame
#df_spark = spark.createDataFrame(df)

# Definir la columna a formatear y el formato de fecha
cols = ['game_date']  # La columna que deseas formatear
#date_format = 'yyyy-MM-dd hh:mm:ss'  # El formato en el que está la fecha
date_format = '%Y-%m-%d %H:%M:%S'  # Formato de fecha y hora


# Aplicar la función format_date a la columna game_date
df2 = format_date(df, cols, date_format)

# Mostrar el DataFrame resultante
#df_spark.show()
print(df2.head())

