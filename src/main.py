import time

import numpy as np
from pyspark.sql import SparkSession
from tqdm import tqdm

from utils import dump_chart, get_executor_memory, parse_arguments, train


def main(data_path: str, num_nodes: int, optimized: bool, iters: int) -> None:
    spark: SparkSession = (
        SparkSession.builder
        .appName('SparkApp')
        .getOrCreate()
    )

    total_time = []
    total_RAM = []
    total_accuracy = []

    for _ in tqdm(range(iters)):
        start_time = time.time()

        dataset = spark.read.csv(data_path, header=True)

        accuracy = spark.sparkContext.parallelize(range(100)).map(train) if optimized \
            else train(dataset)

        end_time = time.time()
        total_accuracy.append(accuracy)
        total_time.append(end_time - start_time)
        total_RAM.append(get_executor_memory(spark.sparkContext))

    dump_chart(total_time, total_RAM, optimized, num_nodes)

    print('Average accuracy:', np.mean(total_accuracy), flush=True)
    print('Average memory(MB):', np.mean(total_RAM), flush=True)
    print('Average time(c):', np.mean(total_time), flush=True)
    spark.stop()


if __name__ == '__main__':
    args = parse_arguments()
    main(args.data_path, args.num_nodes, args.optimized, args.iters)
