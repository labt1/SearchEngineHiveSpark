from flask import Flask, request, render_template
from pyhive import hive

app = Flask(__name__)

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/search', methods=['POST'])
def search():
    search_term = request.form['search_term']
    print(search_term)
    conn = hive.Connection(host='ec2-3-128-254-98.us-east-2.compute.amazonaws.com', port=10000, username='hadoop')
    cursor = conn.cursor()

    query = "SELECT DISTINCT ii.page_id, pr.rank FROM inverted_index ii JOIN pagerank pr ON ii.page_id = pr.page_id WHERE ii.word LIKE '" + str(search_term) + "' ORDER BY pr.rank DESC"

    print(query)
    
    #print(f"Executing query: {query} with parameter: {search_term_param}")

    cursor.execute(query)

    results = cursor.fetchall()
    print(results)
    return render_template('results.html', results=results)

if __name__ == '__main__':
    app.run(debug=True)
