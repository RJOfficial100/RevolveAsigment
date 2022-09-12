import findspark
findspark.init('C:\\Apps\\spark-3.0.3-bin-hadoop2.7')

import logging
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from datetime import datetime

spark=SparkSession.builder.master("local[2]").appName("RevolveAssignment").getOrCreate()

def main():

    try:
        #reading input files and create dataframes
        customerDF=spark.read.options(header=True,delimiter=',').csv('..\\..\\input_data\\starter\\customers.csv')
        productDF=spark.read.options(header=True,delimiter=',').csv('..\\..\\input_data\\starter\\products.csv')
        transactionDF=spark.read.json('..\\..\\input_data\\starter\\transactions')

        #converts the Array of structure Columns to row.
        transactionExplodeDF=transactionDF.select(transactionDF.customer_id,F.explode(transactionDF.basket).alias('product'))

        #fetched product_id from structure and stored in new column.
        transactionExplodeDF=transactionExplodeDF.withColumn('product_id', transactionExplodeDF.product.product_id).drop(transactionExplodeDF.product)

        #aggregate records and found count of purchases named purchase_count.
        trancationFinalDF=transactionExplodeDF.groupby('customer_id','product_id').count().withColumnRenamed('count','purchase_count')

        #joined the product and customer dataframes with product_id and customer_id resp.
        ResultDF=trancationFinalDF.join(productDF,['product_id']).join(customerDF,['customer_id'])

        #selected required columns
        ResultDF.select('customer_id', 'loyalty_score', 'product_id', 'product_category', 'purchase_count').show()

    except pyspark.sql.utils.AnalysisException as ae:
        logging.error('Input files are missing')
        print(ae)
    except Exception as e:
        logging.error('Exception occured')
        print(e)
    
    

if __name__ == "__main__":
    main()
