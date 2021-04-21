#!/usr/bin/python3

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
from pyspark.sql.types import *
from pyspark.ml.linalg import Vectors, VectorUDT
from pyspark.ml.feature import VectorAssembler

reload(sys)
sys.setdefaultencoding('utf8')

parser = argparse.ArgumentParser()
parser.add_argument(
    "--data_file", help="Data file to read in must be specified using --data_file")
parser.add_argument(
    "--data_output", help="Data output path must be specified using --data_output", action="store_const", const=None)


def deb_print(outstr):
    print("{}{}{}".format("#"*20, outstr, "#"*20))


def main():
    print("*"*50)
    print("*"*50)
    print("*"*50)
    spark = SparkSession.builder.appName("MostComfortableZones").getOrCreate()
    columns_of_interest = ['player_name',
                           'SHOT_DIST', 'CLOSE_DEF_DIST', 'SHOT_CLOCK']

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

    # Convert zone data into VectorRow data cells
    # Exclude player_name as strings are supported
    assembler = VectorAssembler(
        inputCols=columns_of_interest[1:], outputCol='features')

    trainingData = assembler.transform(converted_zone_data)

    trainingData.show()

    all_player_names = trainingData.select("player_name").distinct().collect()
    result = list()
    evaluator = ClusteringEvaluator()

    for pname in all_player_names:
        players_features = trainingData.where(
            trainingData["player_name"] == pname[0]).select("features")
        kmeanifier = KMeans().setK(4).setSeed(10)
        model = kmeanifier.fit(players_features)

        predictions = model.transform(players_features)
        silhouette = evaluator.evaluate(predictions)

        result.append([pname[0], "[{}]".format(
            model.clusterCenters()), silhouette])

    print("player_name | zones | silouette")
    for row in result:
        print("{}\t{}\t{}".format(*row))

    print("*"*50)
    deb_print("END PROGRAM")
    spark.stop()


if __name__ == "__main__":
    main()
