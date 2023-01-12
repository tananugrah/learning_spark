# learning_spark
learning basic operation of spark using python


#Create DataFrame from a list of data
1. Generate a sample dictionary list with toy data:

```python
data = [{"Category": 'A', "ID": 1, "Value": 121.44, "Truth": True},
        {"Category": 'B', "ID": 2, "Value": 300.01, "Truth": False},
        {"Category": 'C', "ID": 3, "Value": 10.99, "Truth": None},
        {"Category": 'E', "ID": 4, "Value": 33.87, "Truth": True}
        ]
```
2. Import and create a SparkSession:

```python
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
```

3. Create a DataFrame using the createDataFrame method. Check the data type to confirm the variable is a DataFrame:
```python
df = spark.createDataFrame(data)
type(df)
```

you can see the code on Learning-Spark-2.ipynb

#Load data into a DataFrame from files

You can load data from many supported file formats. The following example uses a dataset available in the /databricks-datasets directory, accessible from most workspaces. See Sample datasets.

```python
df = (spark.read
  .format("csv")
  .option("header", "true")
  .option("inferSchema", "true")
  .load("/databricks-datasets/samples/population-vs-price/data_geo.csv")
)
```

PySpark Where Filter Function | Multiple Conditions

1. PySpark DataFrame filter() Syntax
Below is syntax of the filter function. condition would be an expression you wanted to filter
```python
filter(condition)
```
2. DataFrame filter() with Column Condition.

Using SQL Expression.

exp:
```python
df.filter(df.State == "Arizona").select(df.City,df.State).show()
```
3. Filter Based on List Values

If you have a list of elements and you wanted to filter that is not in the list or in the list, 
use isin() function of Column class and it doesn’t have isnotin() function but you do the same 
using not operator (~).

a. Filter IS IN List values and is not in value.
```python
df.filter(df.State.isin("Arizona","Alaska","California")).select(df.City,df.State).show()
```
b. Filter is not in value.
```python
df.filter(~df.State.isin("Arizona","Alaska","California")).select(df.City,df.State).show()
```
```python
df.filter(df.State.isin("Arizona","Alaska","California")==False).select(df.City,df.State).show()
```
4. Filter Based on Starts With, Ends With, Contains
You can also filter DataFrame rows by using startswith(), endswith() and contains() methods of Column class.

a. Using startswith
```python
df.filter(df.City.startswith("A")).select("City","State","State Code","2014 Population estimate").show()
```
b. using endswith
```python
df.filter(df.City.endswith("e")).show()
```
c. contains
```python
df.filter(df.City.contains("e")).show()
```
5. PySpark Filter like and rlike

If you have SQL background you must be familiar with like and rlike (regex like), 
PySpark also provides similar methods in Column class to filter similar values using wildcard characters.

a. PySpark Filter like and rlike
```python
df.filter(df.City.like("%le%")).show()
```
b. This check case insensitive
```python
df.filter(df.City.rlike("(?i)^*les$")).show()
```
# PySpark Aggregate Functions

create a DataFrame to work with PySpark aggregate functions.

```python
sampledata = [("James", "Sales", 3000),
    ("Michael", "Sales", 4600),
    ("Robert", "Sales", 4100),
    ("Maria", "Finance", 3000),
    ("James", "Sales", 3000),
    ("Scott", "Finance", 3300),
    ("Jen", "Finance", 3900),
    ("Jeff", "Marketing", 3000),
    ("Kumar", "Marketing", 2000),
    ("Saif", "Sales", 4100)
  ]
schema = ["employee_name", "department", "salary"]
df = spark.createDataFrame(data=sampledata, schema = schema)
df.printSchema()
df.show(truncate=False)
```

1. approx_count_distinct Aggregate Function
In PySpark approx_count_distinct() function returns the count of distinct items in a group.

```python
print("approx_count_distinct: " + \
      str(df.select(approx_count_distinct("salary")).collect()[0][0]))
```
or
```python
df.select(approx_count_distinct("salary")).show()
```

2. avg (average) Aggregate Function

avg() function returns the average of values in the input column.

```python
print("avg: " + str(df.select(avg("salary")).collect()[0][0]))
```
3. collect_set Aggregate Function

collect_list() function returns all values from an input column with duplicates.
```python
df.select(collect_list("salary")).show(truncate=False)
```
4. countDistinct Aggregate Function

countDistinct() function returns the number of distinct elements in a columns.
```python
df = df.select(countDistinct("department", "salary"))
df.show(truncate=False)
print("Distinct Count of Department & Salary: "+str(df.collect()[0][0]))
```
5. count function

count() function returns number of elements in a column.
```python
print("count: "+str(df.select(count("salary")).collect()[0][0]))
```
6. sum function
sum() function Returns the sum of all values in a column.
```python
df.groupBy('department').sum("salary").show()
```
or
```python
print("sum: "+str(df.select(sum("salary")).collect()[0][0]))
```
```python
df.select(sum("salary")).show()
```

7. min function
min() function returns the minimum value in a column.
```python
df.select(min("salary")).show()
```
8. max function
max() function returns the maximum value in a column.
```python
df.select(max("salary")).show()
```
```python
print("max salary : ",df.select(max("salary")).collect()[0][0])
```
9. skewness() function
skewness() function returns the skewness of the values in a group
```python
df.select(skewness("salary")).show(truncate=False)
```
10. stddev(), stddev_samp() and stddev_pop()
stddev() alias for stddev_samp.

stddev_samp() function returns the sample standard deviation of values in a column.

stddev_pop() function returns the population standard deviation of the values in a column.
```python
df.select(stddev("salary"), stddev_samp("salary"),stddev_pop("salary")).show(truncate=False)
```
11. variance(), var_samp(), var_pop()
variance() alias for var_samp

var_samp() function returns the unbiased variance of the values in a column.

var_pop() function returns the population variance of the values in a column.
```python
df.select(variance("salary"),var_samp("salary"),var_pop("salary")).show(truncate=False)
 ```
source:
https://phoenixnap.com/kb/spark-create-dataframe.
https://docs.databricks.com/getting-started/dataframes-python.html.
https://sparkbyexamples.com/pyspark/pyspark-where-filter/.
https://sparkbyexamples.com/pyspark/pyspark-aggregate-functions/.
