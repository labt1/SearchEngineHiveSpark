aws s3 cp s3://labt-bg01/scripts . --recursive

spark-submit inverted_index.py
spark-submit page_rank.py