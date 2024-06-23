from argparse import ArgumentParser, Namespace
from typing import List

import matplotlib.pyplot as plt
from pyspark.ml import Pipeline
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.feature import OneHotEncoder, StringIndexer, VectorAssembler
from pyspark.sql.dataframe import DataFrame as spDataFrame
from pyspark.sql.functions import col
from pyspark.sql.types import FloatType, IntegerType


def parse_arguments() -> Namespace:
    parser = ArgumentParser()

    parser.add_argument('--data-path', '-d')
    parser.add_argument('--num-nodes', '-n', choices=['1', '3'], default = '1')
    parser.add_argument('--optimized', '-o', action='store_true')
    parser.add_argument('--iters', '-i', default = 100, type=int)
    return parser.parse_args()


def encode(dataset):
    int_columns = ['age', 'hypertension', 'heart_disease', 'blood_glucose_level', 'diabetes']
    for column in int_columns:
        dataset = dataset.withColumn(column, dataset[column].cast(IntegerType()))

    float_columns = ['bmi', 'HbA1c_level']
    for column in float_columns:
        dataset = dataset.withColumn(column, dataset[column].cast(FloatType()))

    cat_columns = ['gender', 'smoking_history']
    indexers = [StringIndexer(inputCol=column, outputCol=column + '_index') for column in cat_columns]
    encoders = [OneHotEncoder(inputCol=indexer.getOutputCol(), outputCol=column + '_encoded') for indexer, column in zip(indexers, cat_columns)]
    pipeline = Pipeline(stages=indexers + encoders)
    model = pipeline.fit(dataset)
    dataset = model.transform(dataset)
    for column in cat_columns:
        dataset = dataset.drop(column)
        dataset = dataset.drop(column + '_index')
    return dataset


def train(dataset: spDataFrame) -> float:
    target_column_name = 'diabetes'
    vector_column_name = 'features'

    dataset = dataset.na.fill(0)
    dataset = encode(dataset)
    dataset = dataset.filter(col(target_column_name).isNotNull())
    feature_columns = [el for el in dataset.columns if el != target_column_name]
    assembler = VectorAssembler(inputCols=feature_columns, outputCol=vector_column_name)
    dataset = assembler.transform(dataset)
    train_data, test_data = dataset.randomSplit([0.7, 0.3], seed=42)
    rf = RandomForestClassifier(featuresCol=vector_column_name, labelCol=target_column_name)
    model = rf.fit(train_data)
    predictions = model.transform(test_data)
    evaluator = BinaryClassificationEvaluator(labelCol=target_column_name)
    accuracy = evaluator.evaluate(predictions)
    return accuracy


def get_executor_memory(sc):
    executor_memory_status = sc._jsc.sc().getExecutorMemoryStatus()
    executor_memory_status_dict = sc._jvm.scala.collection.JavaConverters.mapAsJavaMapConverter(executor_memory_status).asJava()
    total_used_memory = 0
    for values in executor_memory_status_dict.values():
        total_memory = values._1() / (1024 * 1024)
        free_memory = values._2() / (1024 * 1024)
        used_memory = total_memory - free_memory
        total_used_memory += used_memory
    return total_used_memory


def dump_chart(total_time: List[int], total_RAM: List[int], optimized: str, num_nodes: int) -> None:
    plt.figure(figsize=(14, 6))

    str_optimized = 'optimized' if optimized else 'not_optimized'

    plt.subplot(1, 2, 1)
    plt.hist(total_time, bins=30, edgecolor='k', alpha=0.7)
    plt.xlabel('Time(c)')
    plt.ylabel('Frequency')
    plt.title(f'Histogram of time distribution of {str_optimized} way for {num_nodes} data nodes')
    plt.grid(True)

    plt.subplot(1, 2, 2)
    plt.hist(total_RAM, bins=30, edgecolor='k', alpha=0.7)
    plt.xlabel('RAM(MB)')
    plt.ylabel('Frequency')
    plt.title(f'Histogram of RAM distribution of {str_optimized} way for {num_nodes} data nodes')
    plt.grid(True)

    plt.tight_layout()
    plt.savefig(f'./{str_optimized}_num_nodes_{num_nodes}.png')
