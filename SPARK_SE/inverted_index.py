from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
import re

# Configuración de Spark
conf = SparkConf().setAppName("InvertedIndex").setMaster("yarn")
sc = SparkContext(conf=conf)
spark = SparkSession(sc)

# Función para procesar documentos
def preprocess(text):
    return re.findall(r'\w+', text.lower())

# Leer los datos desde S3
data = sc.textFile("s3://labt-bg01/input_ii/data.txt")

# Construir el índice invertido
inverted_index = (data.flatMap(lambda line: [(word, doc_id) for word in preprocess(line) for doc_id in re.findall(r'^(\d+)', line)])
                       .groupByKey()
                       .mapValues(lambda docs: list(set(docs)))  # Convertir a lista única de documentos
                       .sortByKey())

# Guardar el índice invertido en S3
inverted_index.saveAsTextFile("s3://labt-bg01/output_ii/")

# Detener la sesión de Spark
spark.stop()
