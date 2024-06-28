from flask import Flask, render_template, request
import boto3


app = Flask(__name__)

# Amazon S3
s3 = boto3.client(
    's3',
    aws_access_key_id='',
    aws_secret_access_key='',
    region_name='us-east-2'
)

# Nombre del bucket y nombre del access point
bucket_name = 'labt-bg01'

# Función para listar los archivos en el directorio de salida en S3
def list_s3_files(bucket, prefix):
    files = []
    response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
    for content in response.get('Contents', []):
        files.append(content['Key'])
    return files

# Función para cargar y combinar todos los archivos JSON del índice invertido desde S3
def load_inverted_index(output_prefix):
	files = list_s3_files(bucket_name, output_prefix)
	files.remove(output_prefix + '_SUCCESS')
	inverted_index = {}  # Usaremos un diccionario para almacenar las palabras y sus documentos
	
	for file in files:
		response = s3.get_object(Bucket=bucket_name, Key=file)
		file_content = response['Body'].read().decode('utf-8').strip()
		
		# Dividir el contenido del archivo por líneas
		lines = file_content.splitlines()
		
		for line in lines:
			
			line = line.replace('(', '').replace(')', '').replace('\'','').replace('[','').replace(']','').split(', ')
			# Extraer la palabra y los documentos
			word = line[0].strip("'")
			line[1] = line[1].replace('\'','').replace('[','').replace(']','')
			documents = [int(doc) for doc in line[1:]]
			
			# Agregar a la lista de documentos de la palabra (usando un conjunto para evitar duplicados)
			if word not in inverted_index:
				inverted_index[word] = set(documents)
			else:
				inverted_index[word].update(documents)
			
	# Convertir el diccionario a una lista de tuplas (palabra, lista de documentos)
	inverted_index_list = [(word, sorted(list(documents))) for word, documents in inverted_index.items()]
	
	return inverted_index_list

# Función para cargar y combinar todos los archivos JSON del índice invertido desde S3
def load_page_rank(output_prefix):
	files = list_s3_files(bucket_name, output_prefix)
	page_rank = []  # Usaremos un diccionario para almacenar las palabras y sus documentos
	
	for file in files:
		response = s3.get_object(Bucket=bucket_name, Key=file)
		file_content = response['Body'].read().decode('utf-8').strip()
		
		# Dividir el contenido del archivo por líneas
		lines = file_content.splitlines()
		
		for line in lines:
			
			line = line.replace('(', '').replace(')', '').replace('\'','').split(', ')
			# Extraer la palabra y los documentos
			docID = line[0]
			score = line[1]
			
			page_rank.append([docID, float(score)])
			
	return sorted(page_rank, key=lambda x: x[1], reverse=True)

def buscar_palabra(diccionario, palabra):
    resultados = []
    for tupla in diccionario:
        if palabra == tupla[0]:
            resultados.append((tupla[0], tupla[1]))
    return resultados
#print(inverted_index)
#print(page_rank)

# Página de inicio de búsqueda
@app.route('/')
def index():
    return render_template('index.html')

# Ruta para manejar la búsqueda
@app.route('/search', methods=['GET', 'POST'])
def search():
    if request.method == 'POST':
        query = request.form['search_term']
        
        # Ejemplo de carga de índice invertido y PageRank
        inverted_index = load_inverted_index('output_ii/')
        pagerank = load_page_rank('output_pr/')

        # Implementa la lógica de búsqueda usando el índice invertido
        # Aquí obtienes los resultados de búsqueda, por ejemplo:
        results = buscar_palabra(inverted_index, query)
        
        pagerank_dict = {item[0]: item[1] for item in pagerank}

		# Filtrar y ordenar los resultados del índice invertido según el PageRank
        results_sorted = []
        for palabra, documentos in results:
            for documento in documentos:
                if str(documento) in pagerank_dict:
                    results_sorted.append((documento, pagerank_dict[str(documento)]))

		# Ordenar resultados por PageRank (en orden descendente)
        results_sorted.sort(key=lambda x: x[1], reverse=True)

        # Mostrar los resultados ordenados por PageRank
        for documento, score in results_sorted:
            print(f"Documento {documento}: PageRank {score}")       

        return render_template('results.html', query=query, results=results_sorted)

    return render_template('index.html')

if __name__ == '__main__':
    app.run(debug=True)