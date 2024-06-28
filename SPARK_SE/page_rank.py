from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.sql import SparkSession

# Configuración de Spark
conf = SparkConf().setAppName("PageRank").setMaster("yarn")
sc = SparkContext(conf=conf)
spark = SparkSession(sc)

# Leer los datos del grafo desde S3
lines = sc.textFile("s3://labt-bg01/input_pr_s/pagerank.txt")

# Parsear datos para construir los enlaces (página, lista de enlaces salientes)
links = lines.map(lambda line: line.split("\t")) \
             .map(lambda parts: (parts[0], parts[1].split(","))) \
             .cache()

# Número de iteraciones para el cálculo de PageRank
iterations = 10

# Inicialización de PageRank
ranks = links.mapValues(lambda urls: 1.0)

# Función para calcular contribuciones de PageRank
def computeContributions(urls, rank):
    num_urls = len(urls)
    for url in urls:
        yield (url, rank / num_urls)

# Iterativamente calcular PageRank
for iteration in range(iterations):
    # Calcula contribuciones de PageRank de cada página
    contributions = links.join(ranks).flatMap(lambda page_urls_rank: computeContributions(page_urls_rank[1][0], page_urls_rank[1][1]))
    
    # Calcula el nuevo PageRank basado en las contribuciones
    ranks = contributions.reduceByKey(lambda x, y: x + y).mapValues(lambda rank: rank * 0.85 + 0.15)

# Guardar en el bucket
ranks.saveAsTextFile("s3://labt-bg01/output_pr/")

# Detener el contexto de Spark
sc.stop()