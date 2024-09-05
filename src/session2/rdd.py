import streamlit as st
from pyspark import SparkContext
from pyspark.rdd import RDD


def rdd_from_list(sc: SparkContext, n: int) -> RDD:
    """Return a RDD consisting of elements from 1 to n.
    For now we assume we will always get n > 1, no need to test for the exception nor raise an Exception.
    """
    # YOUR CODE HERE
    # st.info("_Don't forget you can print any value in the Streamlit app_")
    # st.write(range(10))
    return sc.parallelize(list(range(1, n + 1)))
    raise NotImplementedError()


def load_file_to_rdd(sc: SparkContext, path: str) -> RDD:
    """Create a RDD by loading an external file. We don't expect any formatting nor processing here.
    You don't need to raise an exception if the file does not exist.
    """
    # YOUR CODE HERE
    return sc.textFile(path)
    raise NotImplementedError()


def op1(sc: SparkContext, mat: RDD) -> RDD:
    """Multiply the first coordinate by 2, remove 3 to the second"""

    # YOUR CODE HERE
    # mat = [[1,3], [2,9]]
    # st.write(sc.parallelize(mat).map(lambda row: row[0]).collect())
    def f(x: list):
        return [x[0] * 2, x[1] - 3]

    rdd = mat.map(f)
    return rdd
    raise NotImplementedError()


def op2(sc: SparkContext, sentences: RDD) -> RDD:
    """Return all words contained in the sentences."""
    # YOUR CODE HERE
    return sentences.flatMap(lambda x: x.split(" "))
    raise NotImplementedError()


def op3(sc: SparkContext, numbers: RDD) -> RDD:
    """Return all numbers contained in the RDD that are odd."""
    # YOUR CODE HERE
    # st.write(sc.parallelize(range(20)).filter(lambda num: num > 5).collect())
    return numbers.filter(lambda x: x % 2)
    raise NotImplementedError()


def op4(sc: SparkContext, numbers: RDD) -> RDD:
    """Return the sum of all squared odd numbers"""
    # YOUR CODE HERE
    st.info(
        "_Now's a good time to tell you that chaining RDD transformations is possible..._"
    )
    return numbers.map(lambda x: x**2 if x % 2 else 0).reduce(lambda x, y: x + y)
    raise NotImplementedError()


def wordcount(sc: SparkContext, sentences: RDD) -> RDD:
    """Given a RDD of sentences, return the wordcount, after splitting sentences per whitespace."""
    # YOUR CODE HERE
    # st.write(sc.parallelize(range(10)).map(lambda num: (num % 2, num)).reduceByKey(lambda x,y: x+y).collect())
    rdd = (
        sentences.flatMap(lambda x: x.split(" "))
        .map(lambda x: (x, 1))
        .reduceByKey(
            lambda x, y: x + y
        )  # since each value will be 1 (1 occurence), this will effectively give us countByKey()
    )
    return rdd
    raise NotImplementedError()


def mean_grade_per_gender(sc: SparkContext, genders: RDD, grades: RDD) -> RDD:
    """Given a RDD of studentID to grades and studentID to gender, compute mean grade for each gender returned as paired RDD.
    Assume all studentIDs are present in both RDDs, making inner join possible, no need to check that.
    """
    # YOUR CODE HERE
    joined = genders.join(grades)

    # data is like: (studentId, (gender, grade))

    # collected_joined = joined.collect()
    # print(f"collected_joined: {collected_joined}")
    # collected_joined: [('6', ('M', 5)), ('2', ('M', 12)), ('4', ('F', 18)), ('3', ('F', 7)), ('5', ('F', 9)), ('1', ('M', 5))]

    removed_student_id = joined.map(lambda x: x[1])
    # removed_student_id: RDD [('M', 5), ('M', 12), ('F', 18), ('F', 7), ('F', 9), ('M', 5)]

    # group by gender
    grouped_by_gender = removed_student_id.groupByKey()
    # grouped_by_key: RDD [('M', <pyspark.resultiterable.ResultIterable at 0x2edae2d5190>),
    #                      ('F', <pyspark.resultiterable.ResultIterable at 0x2edae2bf5d0>)]

    # now need to convert the value part to List (it is currently Iterable)
    grouped_by_gender = grouped_by_gender.mapValues(list)

    # collected_grouped_by_gender = grouped_by_gender.collect()
    # print(f"collected_grouped_by_gender: {collected_grouped_by_gender}")
    # collected_grouped_by_gender: [('M', [5, 12, 5]), ('F', [18, 7, 9])]

    sum_by_gender = grouped_by_gender.map(lambda x: (x[0], sum(x[1])))
    # collected_sum_by_gender: [('M', 22), ('F', 34)]

    count_by_key = removed_student_id.countByKey()
    # print(f"count_by_key: {count_by_key}")
    # count_by_key: defaultdict(<class 'int'>, {'M': 3, 'F': 3})

    # sum_by_gender / count_by_key
    mean_by_gender = sum_by_gender.map(lambda x: (x[0], x[1] / count_by_key[x[0]]))
    # collected_mean_by_gender = mean_by_gender.collectAsMap()
    # print(f"collected_mean_by_gender: {collected_mean_by_gender}")
    # collected_mean_by_gender: {"M": 7.333333333333333, "F": 11.333333333333334}

    return mean_by_gender
    raise NotImplementedError()


def filter_header(sc: SparkContext, rdd: RDD) -> RDD:
    """From a RDD of lines from a text file, remove the first line."""
    # YOUR CODE HERE
    file_lines_with_index = rdd.zipWithIndex()
    return file_lines_with_index.filter(lambda x: x[1] != 0).map(lambda x: x[0])
    raise NotImplementedError()


def county_count(sc: SparkContext, rdd: RDD) -> RDD:
    """Return a RDD of key,value with county as key, count as values"""
    # YOUR CODE HERE
    return rdd.map(lambda x: (x.split(",")[2], 1)).reduceByKey(lambda x, y: x + y)
    raise NotImplementedError()


def bar_chart_county(sc: SparkContext) -> None:
    """Display a bar chart for the number of occurences for each county
    with Matplotlib, Bokeh, Plotly or Altair...

    Don't return anything, just display in the function directly.

    Load and process the data by using the methods you defined previously.
    """
    # YOUR CODE HERE
    county_rdd = county_count(
        sc, filter_header(sc, load_file_to_rdd(sc, "./data/FL_insurance_sample.csv"))
    )

    data_dict = county_rdd.map(lambda x: {"County": x[0], "Count": int(x[1])})
    collected_data_dict = data_dict.collect()
    st.bar_chart(collected_data_dict, x="County", y="Count", y_label="# of occurrences")

    return
    raise NotImplementedError()
