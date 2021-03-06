#!/usr/bin/python3
"""
Extra Requirements:
    pandas
"""
from __future__ import print_function

import sys
import argparse
from operator import add
from pyspark.sql import Row
# $example on$
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
# $example off$
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StringType, StructType, DoubleType, ArrayType, StructField
from pyspark.ml.linalg import Vectors, VectorUDT
from pyspark.ml.feature import VectorAssembler
from pyspark.sql.functions import pandas_udf, udf, collect_list
import pandas as pd

reload(sys)
sys.setdefaultencoding('utf8')

parser = argparse.ArgumentParser()
parser.add_argument(
    "--data_file", help="Data file to read in must be specified using --data_file")
parser.add_argument(
    "--data_output", help="Data output path must be specified using --data_output", action="store_const", const=None)

#  SHOT_DIST, CLOSEST_DEF_DIST, SHOT_CLOCK
init_k_zones = [
    [8, 12, 20],
    [15, 9, 15],
    [22, 6, 10],
    [30, 3, 5]
]


def deb_print(outstr):
    print("{}{}{}".format("#"*20, outstr, "#"*20))


def assign_nearest_group(in_row):
    return in_row[0]


def find_k_clusters(zone_list):  # (panda_df, ncluster=4):

    kmeanifier = KMeans().setK(ncluster).setSeed(10)
    model = kmeanifier.fit(panda_df['features'])
    # return pd.Dataframe(model.clusterCenters())
    return model.clusterCenters()


def main():
    print("*"*50)
    print("*"*50)
    print("*"*50)
    spark = SparkSession.builder.appName("MostComfortableZones").getOrCreate()
    columns_of_interest = ['player_name',
                           'SHOT_DIST', 'CLOSE_DEF_DIST', 'SHOT_CLOCK']

    # used in find_k_clusters to defined returned dataframe column schema
    columns_schema_ddl = ','.join(["{} {}".format(col, typ)
                                   for col, typ in zip(columns_of_interest,
                                                       [StringType(), DoubleType(), DoubleType(), DoubleType()])
                                   ])

    args = parser.parse_args()
    deb_print("using data file {}".format(args.data_file))

    zone_data = spark.read.csv(args.data_file, header=True).select(
        *columns_of_interest).dropna()  # .fillna(2) # https://hackingandslacking.com/cleaning-pyspark-dataframes-1a3f5fdcedd1
    zone_data.show()
    print("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
    converted_zone_data = zone_data.withColumn('SHOT_DIST', zone_data['SHOT_DIST'].cast(DoubleType())) \
        .withColumn('CLOSE_DEF_DIST', zone_data['CLOSE_DEF_DIST'].cast(DoubleType())) \
        .withColumn('SHOT_CLOCK', zone_data['SHOT_CLOCK'].cast(
            DoubleType()))  # https://www.datasciencemadesimple.com/typecast-integer-to-string-and-string-to-integer-in-pyspark/
    assembler = VectorAssembler(
        inputCols=columns_of_interest[1:], outputCol='features')

    trainingData = assembler.transform(converted_zone_data)
    trainingData.show()
    print(">>>>>>>>>>>>>>>>>>> {}".format(
        trainingData.select('features').dtypes))
    print(">>>>>>>>>>>>>>>>>>> {}".format(type(trainingData)))
    player_zone_schema = ArrayType(StructType(
        [StructField("SHOT_DIST", DoubleType(), False),
         StructField("CLOSE_DEF_DIST", DoubleType(), False),
         StructField("SHOT_CLOCK", DoubleType(), False)
         ]))

    def __find_k_clusters(zone_list):  # (panda_df, ncluster=4):

        return [1.0, 2.0, 3.0]  # model.clusterCenters()
    # ArrayType(VectorUDT()))
    find_clusters_udf = udf(__find_k_clusters, player_zone_schema)

    trainingData.groupby("player_name").agg(
        collect_list("features").alias("features")).withColumn("features", find_clusters_udf("features")).show()
        
    print("*"*50)
    deb_print("END PROGRAM")
    spark.stop()


if __name__ == "__main__":
    main()
