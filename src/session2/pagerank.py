from typing import List
from typing import Tuple

import pandas as pd
import streamlit as st
from pyspark import SparkContext
from pyspark.rdd import RDD


def ungroup_input(sc: SparkContext, system: RDD) -> RDD:
    """Generate the websystem as an RDD of tuples (page, neighbor page)
    from a RDD of tuples (page name, list of all neighbors)
    """
    # YOUR CODE HERE
    return system.map(
        lambda grouped: (
            list(map(lambda destination: (grouped[0], destination), grouped[1])),
        )
    ).flatMap(lambda x: x[0])
    raise NotImplementedError()


def group_input(sc: SparkContext, system: RDD) -> RDD:
    """Generate the websystem as an RDD of tuples (page, list of neighbors)
    from an RDD of tuples (page, neighbor page)
    """
    # YOUR CODE HERE
    return (
        system.map(lambda x: (x[0], list(x[1])))
        .reduceByKey(lambda x, y: x + y)
        .sortByKey()
    )
    raise NotImplementedError()


def compute_contributions(urls: List[str], rank: float) -> List[Tuple[str, float]]:
    """Calculates URL contributions to the rank of other URLs."""
    # YOUR CODE HERE
    # If you're lost on the input/output
    # consult method test_compute_contributions in src/tests/test_session2.py
    each = rank / len(urls)
    return [(x, each) for x in urls]
    raise NotImplementedError()


def generate_contributions(sc: SparkContext, links: RDD, ranks: RDD) -> RDD:
    """Calculates URL contributions to the rank of other URLs."""
    # YOUR CODE HERE
    # Let me suggest you to peek into test_generate_contributions() in src/tests/test_session2.py
    # if you need a concrete example of the expected output.

    # links (in wide form):
    # [
    #     ("a", ["b", "c", "d"])
    #     ("c", ["b"]),
    #     ("b", ["c", "d"]),
    #     ("d", ["a", "c"])
    # ]

    # ranks:
    # [
    #     ("a", 1.0),
    #     ("c", 3.0),
    #     ("b", 2.0),
    #     ("d", 4.0)
    # ]

    joined_links_ranks = links.join(ranks)
    # joined_links_ranks:
    # [
    #     ("a", (["b", "c", "d"], 1))
    #     ("c", (["b"], 3)),
    #     ("b", (["c", "d"], 2)),
    #     ("d", (["a", "c"], 4))
    # ]

    contributions = joined_links_ranks.map(
        lambda x: (
            x[0],
            compute_contributions(x[1][0], x[1][1]),
        )
    )

    # contributions:
    # [
    #     ("a", [("b", 1 / 3), ("c", 1 / 3), ("d", 1 / 3)]),
    #     ("c", [("b", 3)]),
    #     ("b", [("c", 1), ("d", 1)]),
    #     ("d", [("a", 2), ("c", 2)]),
    # ]

    output = contributions.flatMap(lambda x: x[1])
    # expected output:
    # [
    #     ("b", 3.0),  # contribution from c
    #     ("c", 1.0),  # contribution from b
    #     ("d", 1.0),  # contribution from b
    #     ("a", 2.0),  # contribution from d
    #     ("c", 2.0),  # contribution from d
    #     ("b", 1 / 3),  # contribution from a
    #     ("c", 1 / 3),  # contribution from a
    #     ("d", 1 / 3),  # contribution from a
    # ]

    return output
    raise NotImplementedError()


def generate_ranks(sc: SparkContext, contributions: RDD, damping: float) -> RDD:
    """Calculates URL contributions to the rank of other URLs."""
    # YOUR CODE HERE
    raise NotImplementedError()


def main(
    sc: SparkContext, iterations: int, damping: float, links: RDD, ranks: RDD
) -> pd.DataFrame:
    """Main logic.

    Return pandas dataframe with appended pageranks for each node in order of iterations.

    Example:
    Index A B C D
    1     1 2 3 4   <-- iteration 1
    2     2 3 4 5   <-- iteration 2
    ...
    """
    columns = ["a", "b", "c", "d"]
    pageranks = {"a": [0.25], "b": [0.25], "c": [0.25], "d": [0.25]}
    for iteration in range(iterations):
        print("At iteration %s" % (iteration + 1))
        # YOUR CODE HERE
        raise NotImplementedError()
    return pd.DataFrame(pageranks, columns=columns)
