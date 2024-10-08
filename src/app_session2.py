import findspark

findspark.init()
import sys

import streamlit as st
from pyspark import SparkConf, SparkContext

sys.path.append("C:\\Users\\rishi\\Desktop\\git\\pyspark-streamlit-tutorial")
# $env:PYSPARK_DRIVER_PYTHON='python'
# $env:PYSPARK_PYTHON='python'
from src.tests.test_session2 import *
from src.utils import (
    display_exercise_solved,
    display_goto_next_section,
    display_goto_next_session,
)


def _initialize_spark() -> SparkContext:
    """Create a Spark Context for Streamlit app"""
    conf = SparkConf().setAppName("lecture-lyon2").setMaster("local")
    sc = SparkContext.getOrCreate(conf=conf)
    return sc


def display_about(sc: SparkContext):
    st.title("Introduction to Spark Resilient Distributed Datasets (RDD)")
    st.markdown(
        """
    ![Spark logo](http://spark.apache.org/images/spark-logo-trademark.png)

    Apache Spark is a cluster computing engine designed to be fast and general-purpose, 
    making it the ideal choice for processing of large datasets. 
    It answers those two points with efficient data sharing accross computations.

    The past years have seen a major changes in computing systems, 
    as growing data volumes required more and more applications to scale out to large clusters. 
    To solve this problem, a wide range of new programming models have been designed 
    to manage multiple types of computations in a distributed fashion, without having people learn too much about distributed systems. 
    Those programming models would need to deal with parallelism, fault-tolerance and resource sharing for us.

    Google's MapReduce presented a simple and general model for batch processing, which handles faults and parallelism easily. 
    Unfortunately the programming model is not adapted for other types of workloads, 
    and multiple specialized systems were born to answer a specific need in a distributed way.

    * Iterative : Giraph
    * Interactive : Impala, Piccolo, Greenplum
    * Streaming : Storm, Millwheel

    The initial goal of Apache Spark is to try and unify all of the workloads for generality purposes. 
    Matei Zaharia in his PhD dissertation suggests that most of the data flow models that required a 
    specialized system needed efficient data sharing accross computations:

    * Iterative algorithms like PageRank or K-Means need to make multiple passes over the same dataset
    * Interactive data mining often requires running multiple ad-hoc queries on the same subset of data
    * Streaming applications need to maintain and share state over time.

    He then proposes to create a new abstraction that gives its users direct control over data sharing, 
    something that other specialized systems would have built-in for their specific needs. 
    The abstraction is implemented inside a new engine that is today called Apache Spark. 
    The engine makes it possible to support more types of computations than with the original MapReduce in a more efficient way, 
    including interactive queries and stream processing.
    """
    )


def display_prerequisites(sc: SparkContext):
    st.title("Initializing Spark")
    st.markdown(
        """
    Before running Spark code, we need to start a SparkContext instance, 
    which connects to a Spark cluster. 
    The following code block starts a local Spark cluster and its associated SparkContext. 
    This is done at the beginning of the Streamlit app so you don't have to think about it.

    ```python
    conf = SparkConf()
        .setAppName("lecture-lyon2")  # name of the Spark application
        .setMaster("local")  # which Spark cluster to connect to. If it's a remote cluster you will need to add additional info there
    sc = SparkContext.getOrCreate(conf=conf)
    ```

    While your SparkContext is running, you can hit http://localhost:4040 
    to get an overview of your Spark local cluster and all operations ongoing. Try it now :tada:!
    """
    )


def display_q1(sc: SparkContext):
    st.title("Building your first RDDs")
    with st.expander("Introduction"):
        st.markdown(
            """
        In this section, we are going to introduce Spark's core abstraction for working with data 
        in a distributed and resilient way: the **Resilient Distributed Dataset**, or RDD. 
        Under the hood, Spark automatically performs the distribution of RDDs and its processing around 
        the cluster, so we can focus on our code and not on distributed processing problems, 
        such as the handling of data locality or resiliency in case of node failure.

        A RDD consists of a collection of elements partitioned accross the nodes of a cluster of machines 
        that can be operated on in parallel. 
        In Spark, work is expressed by the creation and transformation of RDDs using Spark operators.
        """
        )
        st.image("./img/spark-rdd.png", use_column_width=True)
        st.markdown(
            """
        _Note_: RDD is the core data structure to Spark, but the style of programming we are studying 
        in this lesson is considered the _lowest-level API_ for Spark. 
        The Spark community is pushing the use of Structured programming with Dataframes/Datasets instead, 
        an optimized interface for working with structured and semi-structured data, 
        which we will learn later. 
        Understanding RDDs is still important because it teaches you how Spark works under the hood 
        and will serve you to understand and optimize your application when deployed into production.

        There are two ways to create RDDs: parallelizing an existing collection in your driver program, 
        or referencing a dataset in an external storage system, 
        such as a shared filesystem, HDFS, HBase, or any data source offering a Hadoop InputFormat.
        """
        )

    st.subheader("Question 1 - From Python collection to RDD")
    st.markdown(
        """
    Edit the `rdd_from_list` method in `src/session2/rdd.py` 
    to generate a Python list and transform it into a Spark RDD.

    Ex:
    ```python
    rdd_from_list([1, 2, 3]) should be a RDD with values [1, 2, 3]
    ```
    """
    )
    test_rdd_from_list(sc)
    display_exercise_solved()

    st.subheader("Question 2 - From text file to RDD")
    st.markdown(
        """
    Edit the `load_file_to_rdd` method in `src/session2/rdd.py` 
    to generate a Spark RDD from a text file. 
    
    Each line of the file will be an element of the RDD.

    Ex:
    ```python
    load_file_to_rdd("./data/FL_insurance_sample.csv") should be a RDD with each line an element of the RDD
    ```
    """
    )
    test_rdd_from_list(sc)
    display_exercise_solved()
    display_goto_next_section()


def display_q2(sc: SparkContext):
    st.title("Operations on RDDs")
    with st.expander("Introduction"):
        st.markdown(
            """
        RDDs have two sets of parallel operations:

        * transformations : which return pointers to new RDDs without computing them, it rather waits for an action to compute itself.
        * actions : which return values to the driver after running the computation. The `collect()` funcion is an operation which retrieves all elements of the distributed RDD to the driver.

        RDD transformations are _lazy_ in a sense they do not compute their results immediately.

        The following exercises study the usage of the most common Spark RDD operations.
        """
        )
    st.subheader("Question 1 - Map")
    with st.expander(".map() and flatMap() transformation"):
        st.markdown(
            """
        The `.map(function)` transformation applies the function given in argument to each of the elements 
        inside the RDD. 

        The following sums every number in the RDD by one, in a distributed manner.
        ```python
        sc.parallelize([1,2,3]).map(lambda num: num+1)
        ```

        The `.flatMap(function)` transformation applies the function given in argument to each of the elements 
        inside the RDD, then flattens the list so that there are no more nested elements inside it. 
        
        The following splits each line of the RDD by the comma and returns all numbers in a unique RDD.
        ```python
        sc.parallelize(["1,2,3", "2,3,4", "4,5,3"]).flatMap(lambda csv_line: csv_line.split(","))
        ```
        ---
        """
        )
        st.markdown(
            """
        What would be the result of:
        ```python
        sc.parallelize(["1,2,3", "2,3,4", "4,5,3"]).map(lambda csv_line: csv_line.split(","))
        ```
        ?

        ---
        """
        )
    st.markdown(
        """
    Suppose we have a RDD containing only lists of 2 elements :

    ```python
    matrix = [[1,3], [2,5], [8,9]]
    matrix_rdd = sc.parallelize(matrix)
    ```

    This data structure is reminiscent of a matrix.

    Edit the method `op1()` to  multiply the first column (or first coordinate of each element) 
    of the matrix by 2, and removes 3 to the second column (second coordinate).
    """
    )
    test_op1(sc)
    display_exercise_solved()

    st.subheader("Question 2 - Extracting words from sentences")
    st.markdown(
        """
    Suppose we have a RDD containing sentences :

    ```python
    sentences_rdd = sc.parallelize(
        ['Hi everybody', 'My name is Fanilo', 'and your name is Antoine everybody'
    ])
    ```

    Edit `op2()` which returns all the words in the rdd, after splitting each sentence by the whitespace character.
        
    """
    )
    test_op2(sc)
    display_exercise_solved()

    st.subheader("Question 3 - Filtering")
    st.markdown(
        """
    The `.filter(function)` transformation let's us filter elements verify a certain function.

    Suppose we have a RDD containing numbers.

    Edit `op3()` to returns all the odd numbers.
    """
    )
    test_op3(sc)
    display_exercise_solved()

    st.subheader("Question 4 - Reduce")
    with st.expander("About reduce"):
        st.markdown(
            """
        The `.reduce(function)` transformation reduces all elements of the RDD into one 
        using a specific method.

        This next example sums elements 2 by 2 in a distributed manner, 
        which will produce the sum of all elements in the RDD.
        ```python
        sc.parallelize([1,2,3,4,5]).map(lambda x,y: x + y)
        ```

        Do take note that, as in the Hadoop ecosystem, the function used to reduce the dataset 
        should be associative and commutative.

        ---
        """
        )

    st.markdown(
        """
    Suppose we have a RDD containing numbers.

    Create an operation `.op4()` which returns the sum of 
    all squared odd numbers in the RDD, using the `.reduce()` operation.
    """
    )
    test_op4(sc)
    display_goto_next_section()


def display_q3(sc: SparkContext):
    st.title("Using Key-value RDDs")
    st.markdown(
        """
    If you recall the classic MapReduce paradigm, you were dealing with key/value pairs 
    to reduce your data in a distributed manner. 
    We define a pair as a tuple of two elements, 
    the first element being the key and the second the value.

    Key/value pairs are good for solving many problems efficiently in a parallel fashion 
    so let us delve into them.
    ```python
    pairs = [('b', 3), ('d', 4), ('a', 6), ('f', 1), ('e', 2)]
    pairs_rdd = sc.parallelize(pairs)
    ```

    ### reduceByKey

    The `.reduceByKey()` method works in a similar way to the `.reduce()`, 
    but it performs a reduction on a key-by-key basis.
    The following counts the sum of all values for each key.
    ```python
    pairs = [('b', 3), ('d', 4), ('a', 6), ('f', 1), ('e', 2)]
    pairs_rdd = sc.parallelize(pairs).reduceByKey(lambda x,y: x+y)
    ```
    """
    )

    st.header("Time for the classic Hello world question !")
    st.markdown(
        "You know the drill. Edit `wordcount()` to count the number of occurences of each word."
    )
    test_wordcount(sc)
    display_exercise_solved()

    st.subheader("Question 2 - Joins")
    with st.expander("About joins"):
        st.markdown(
            """
        The `.join()` method joins two RDD of pairs together on their key element.
        
        ```python
        genders_rdd = sc.parallelize([('1', 'M'), ('2', 'M'), ('3', 'F'), ('4', 'F'), ('5', 'F'), ('6', 'M')])
        grades_rdd = sc.parallelize([('1', 5), ('2', 12), ('3', 7), ('4', 18), ('5', 9), ('6', 5)])

        genders_rdd.join(grades_rdd)
        ```
        """
        )
    st.markdown(
        """
    Let's give ourselves a `student-gender` RDD and a `student-grade` RDD. 
    Compute the mean grade for each gender.
    """
    )

    with st.expander("Hint ?"):
        st.markdown(
            """
        _This is a long exercise._
        Remember that the mean for a gender equals the sum of all grades 
        divided by the count of the number of grades. 

        You already know how to sum by key, 
        and you can use the `countByKey()` function for returning a hashmap of gender to count of grades, 
        then use that hashmap inside a map function to divide. 
        
        Good luck !
        """
        )
    test_mean_grade_per_gender(sc)
    display_goto_next_section()


def display_q4(sc: SparkContext):
    st.title("Manipulating a CSV file")
    st.markdown(
        """
    We provide a `FL_insurance_sample.csv` file inside the `data` folder to use in our computations, 
    it will be loaded through  `load_file_to_rdd()` you have previously implemented.

    The first line of the CSV is the header, and it is annoying to have it mixed with the data. 
    In the lower-level RDD API we need to write code to specifically filter that first line.

    Edit the `filter_header` method to remove the first element of a RDD.
    """
    )
    with st.expander("Hint ?"):
        st.markdown(
            """
        **Hint** : `rdd.zipwithindex()` is a useful function when you need to filter by position 
        in a file _(though it is computationally expensive)_.
        """
        )
    test_filter_header(sc)
    display_exercise_solved()

    st.markdown(
        """
    Let's try some statistics on the `county` variable, which is the second column of the dataset.

    Edit `test_county_count` to return the number of times each county appears
    """
    )
    test_county_count(sc)
    display_exercise_solved()

    st.markdown(
        """
    A little bonus. Streamlit can display plots directly:
    * Matplotlib: `st.pyplot`
    * Plotly: `st.plotly_chart`
    * Bokeh: `st.bokeh_chart`
    * Altair: `st.altair_chart`

    So as a bonus question, display a bar chart of number of occurrences 
    for each county directly in the app by editing the `bar_chart_county` method.
    """
    )
    bar_chart_county(sc)
    display_goto_next_section()


def display_pagerank(sc: SparkContext):
    st.title("Application - Computing Pagerank")
    st.warning(
        """
        This final bit is algorithmically heavy :skull:
        
        Take your time, and good luck!
    """
    )
    st.markdown(
        """
    PageRank is a function that assigns a real number to each page in the Web 
    (or at least to that portion of the Web that has been crawled and its links discovered). 
    The intent is that the higher the PageRank of a page, the more "important" it is.

    Hadoop has its origins in Apache Nutch, an open source web search engine, 
    and one of the first use cases for Big Data technologies and MapReduce was the indexing 
    of millions of webpages. 

    > In the following application, we will delve into an implementation of an iterative algorithm, 
    the PageRank, which is well suited to Spark. You have to imagine your Pagerank implementation
    will run on Petabytes of web crawler logs to compute the ranking of the web URLs!

    We will deal with the following simplified web system :
    """
    )
    st.image("./img/pagerank.png", width=400)
    st.markdown(
        """
    We have four web pages (a, b, c, and d) in our system :

    * Web page A has outbound links to pages B, C, D
    * Web page B has outbound links to C, D
    * Web page C has outbound link to B
    * Web page D has outbound link to A, C

    We will implement a [simpler version of PageRank](https://en.wikipedia.org/wiki/PageRank#Simplified_algorithm) 
    in PySpark, in the `src/session2/pagerank.py` file.

    ## Input Data

    When crawling the Web for URLs and their outbound links, 
    web crawlers will append all the new data in the same file. 
    As such we expect the format to be the following when reading from the filesystem, 
    which is easier to append on. Let's call it the **long form**.

    ```
    URL1         neighbor1
    URL1         neighbor2
    URL2         neighbor1
    ...
    ```

    We could also work on a data structure where all neighbors of the same URL are grouped on one line.
    Let's call this the **wide form**.

    ```
    URL1   [neighbor1, neighbor2]
    URL2   [neighbor1]
    ...
    ```

    Let's build two functions to alternate between both representations.
    """
    )

    st.subheader("Question 1.1 - From wide to long")
    st.markdown(
        """
        Generate a web system as a RDD of tuples (page name, neighbor page name) 
        from a RDD of tuples (page name, list of all neighbors), by editing the `ungroup_input` method.

        _Hint: we are working with PairedRDDs here, don't hesitate to check the_
        _[API reference](https://spark.apache.org/docs/2.2.0/api/python/pyspark.html#pyspark.RDD)_
        _of RDDs for functions like `mapValues` or `flatMapValues`_
    """
    )
    test_ungroup_input(sc)
    display_exercise_solved()

    st.subheader("Question 1.2 - From long to wide")
    st.markdown(
        """
        Generate a web system as a RDD of tuples (page name, list of all neighbors)
        from a RDD of tuples (page name, neighbor page name), by editing the `group_input` method.

        _Hint: Never hesitate to collect and print your RDD in the method using Streamlit._
        _You may most notably find out that `pyspark.resultiterable.ResultIterable` is not the result we want to return._
    """
    )
    test_group_input(sc)
    display_exercise_solved()

    st.header("Question 2 - Page Contributions")
    st.image("./img/pagerank.png", width=400)
    st.markdown(
        """
    PageRank is an iterative program, for each iteration a page gets empowered 
    by every other page that links to it.

    At each iteration, we need to compute what a given URL contributes to the rank of other URLs. 
    The PageRank transferred from a given page to the targets of its outbound links 
    upon the next iteration is divided equally among all outbound links.
    """
    )
    st.latex(
        r"Contribution\ to\ a\ page = \frac{pagerank\ of\ contributing\ page}{number\ outbound\ links\ from\ contributing\ page}"
    )
    st.markdown(
        """
    > So for example, if page A has pagerank 3, it will contribute 1 to B, 1 to C and 1 to D.

    Then, for a page, we will sum the contributions of every page linking to it.

    > So B should receive contributions from A anc C's pageranks.

    Finally, the update pagerank for a page, given a damping factor $s$ will be :
    """
    )
    st.latex(
        r"pagerank(u) = 1 - s + s \times \sum_{v \in B_u} \frac{pagerank(v)}{L(v)}"
    )
    st.markdown(
        """
    i.e. the PageRank value for a page $u$ is dependent on the PageRank values for each page $v$ 
    contained in the set $B_u$ (the set containing all pages linking to page $u$), 
    divided by the number $L(v)$ of links from page $v$.
    """
    )

    st.subheader("Question 2.1 - Split a pagerank into contributions to outbound links")
    st.markdown(
        """
    First, we are going to build the page contribution of a page to a set of outbound urls 
    in `compute_contribs`. 
    
    The first arugment gives the outbound links of a page, 
    the second argument gives the rank of the page, 
    and we return a list of tuples (outbound url, contribution to the outbound url).
    """
    )
    st.warning(
        """:balloon: We will **NOT** be using Spark for this question.
    We assume a Python worker is capable of working this out.
    """
    )
    st.markdown(
        """
    Ex:
    ```python
    assert compute_contributions(['b', 'c', 'd'], 1) == [('b', 1/3), ('c', 1/3), ('d', 1/3)]
    ```
    """
    )
    test_compute_contributions()
    display_exercise_solved()

    st.subheader("Question 2.2 - Generate a RDD of all contributions")
    st.markdown(
        """
    Let's assume we maintain the following two key-value RDD structures:

    * links RDD

    ```
    page1    [list of neighbors to page1]
    page2    [list of neighbors to page2]
    ...
    ```

    * ranks RDD

    ```
    page1    rank1
    page2    rank2
    ...
    ```

    With the help from `compute_contributions`, 
    build the `generate_contributions` method, 
    which returns a key-value RDD of all generated contributions of the form 
    (URL, contributed rank by one of its parent page) using the links and ranks RDD.

    ```
    A    contribution_to_B
    A    contribution_to_C
    A    contribution_to_D
    B    contribution_to_C
    B    contribution_to_D
    ...
    ```
    """
    )
    test_generate_contributions(sc)
    display_exercise_solved()

    st.subheader("Question 2.3 - Apply damping and generate the new pagerank")
    st.markdown(
        """
    In `generate_ranks`, compute new ranks for each URL by summing all contributions from outbound URL, 
    and applying the damping factor.
    """
    )
    test_generate_ranks(sc)
    display_exercise_solved()

    st.subheader("Question 3 - Building the main algorithm")
    st.markdown(
        """
    Let's build the iteration. At each step :

    1. Generate all contributions with `generate_contributions`
    2. Update ranks RDD with `generate_ranks`

    We provide a function to initialize the web system from the image and initial ranls:
    """
    )

    with st.echo("below"):

        def initialize(sc):
            """
            Initialize links and ranks RDDs
            """
            # Loads all URLs from input file and initialize their neighbors.
            links = sc.parallelize(
                [
                    ("a", "b"),
                    ("a", "c"),
                    ("a", "d"),
                    ("c", "b"),
                    ("b", "c"),
                    ("b", "d"),
                    ("d", "a"),
                    ("d", "c"),
                ]
            )
            links = group_input(
                sc, links
            ).cache()  # put the links RDD in cache because it will be reused a lot

            # Initialize all ranks to 0.25
            ranks = links.keys().map(lambda url: (url, 0.25))

            return (links, ranks)

    st.markdown(
        """
    Let's create a Pandas dataframe with columns \[a, b, c, d\]. 
    At each iteration, add a row with the Pagerank value for each node in the corresponding column. 
    Then return the Pandas dataframe as a result so we can investigate it.
    """
    )
    test_main(sc)
    display_exercise_solved()

    st.markdown(
        """So do you want to visualize the convergence of pagerank ? Let's do this !"""
    )
    if st.button("Run Pagerank algorithm"):
        links, ranks = initialize(sc)
        result = main(sc, 25, 0.85, links, ranks)
        st.write(result.plot())

    display_goto_next_session()


def display_resources(sc: SparkContext):
    st.title("Resources")
    st.markdown(
        """
    List of resources to help you:

    - [Pyspark lecture](https://andfanilo.github.io/pyspark-interactive-lecture/#/)
    - [Spark docs page](https://spark.apache.org/docs/latest/)
    - [PySpark API](https://spark.apache.org/docs/latest/api/python/index.html)

    """
    )


def main():
    pages = {
        "Introduction to RDDs": display_about,
        "Prerequisites - Spark Initialization": display_prerequisites,
        "1 - Building your first RDDs": display_q1,
        "2 - Operations on RDDs": display_q2,
        "3 - Using Key-value RDDs": display_q3,
        "4 - Manipulating a CSV file": display_q4,
        "5 - Application to Pagerank": display_pagerank,
        "Resources": display_resources,
    }
    st.sidebar.header("Questions")
    sc = _initialize_spark()
    # sc.stop()  #! COMMENT OUT LATER.

    page = st.sidebar.selectbox("Select your question", tuple(pages.keys()))
    pages[page](sc)


if __name__ == "__main__":
    main()
