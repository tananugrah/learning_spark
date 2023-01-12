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

# PySpark StructType & StructField Explained.

code you can see on Learning-Spark-StructType.ipynb.

PySpark StructType & StructField classes are used to programmatically specify the schema to the DataFrame and create complex columns like nested struct, array, and map columns. that defines column name, column data type, boolean to specify if the field can be nullable or not and metadata.

1. StructType – Defines the structure of the Dataframe
```python
from pyspark.sql.types import StructType
```
StructType is a collection or list of StructField objects.

PySpark printSchema() method on the DataFrame shows StructType columns as struct.

2. StructField – Defines the metadata of the DataFrame column
```python
from pyspark.sql.types import StructField
```
to define the columns which include column name(String), column type (DataType), nullable column (Boolean) and metadata (MetaData).

3. Using PySpark StructType & StructField with DataFrame.

StructType is a collection of StructField’s which is used to define the column name, data type, and a flag for nullable or not. Using StructField we can also add nested struct schema, ArrayType for arrays, and MapType for key-value pairs which we will discuss in detail in later sections.

```python
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType

spark = SparkSession.builder.master("local[1]") \
                    .appName('SparkStructExample.com') \
                    .getOrCreate()
```
create a StructType & StructField on DataFrame.
```python
data = [
    ("Agus","","Rohmawan","36636","M",3000),
    ("Dilan","Cepmek","","40288","M",4000),
    ("Fajar","","Sadboy","42114","M",4000),
    ("Kekeyi","Doll","Jones","39192","F",4000),
    ("Jenjen","Maryam","Pink","","F",-1),
    ("Jeni","Jeno","Jojo","","F",-1),
    ("Tan","Tin","Tun","42113","M",10000)
  ]

schema = StructType([ \
    StructField("firstname",StringType(),True), \
    StructField("middlename",StringType(),True), \
    StructField("lastname",StringType(),True), \
    StructField("id", StringType(), True), \
    StructField("gender", StringType(), True), \
    StructField("salary", IntegerType(), True) \
  ])
 
df = spark.createDataFrame(data=data,schema=schema)
df.printSchema()
df.show(truncate=False)
```
4. Defining Nested StructType object struct.

While working on DataFrame we often need to work with the nested struct column and this can be defined using StructType.
```python
#Defining Nested StructType object struct
data2 = [
    (("Agus","","Rohmawan"),"36636","M",7000),
    (("Dilan","Cepmek",""),"40288","M",8000),
    (("Fajar","","Sadboy"),"42114","M",7000),
    (("Kekeyi","Doll","Jones"),"39192","F",7000),
    (("Jenjen","Maryam","Pink"),"","F",-1),
    (("Jeni","Jeno","Jojo"),"","F",-1),
    (("Tan","Tin","Tun"),"42113","M",10000)
  ]
structureSchema = StructType([
        StructField('name', StructType([
             StructField('firstname', StringType(), True),
             StructField('middlename', StringType(), True),
             StructField('lastname', StringType(), True)
             ])),
         StructField('id', StringType(), True),
         StructField('gender', StringType(), True),
         StructField('salary', IntegerType(), True)
         ])
#define data what you use for data=(variable data name)
df2 = spark.createDataFrame(data=data2,schema=structureSchema)
df2.printSchema()
df2.show(truncate=False)
```
5. Adding & Changing struct of the DataFrame.
Using PySpark SQL function struct(), we can change the struct of the existing DataFrame and add a new StructType to it. 
example how to copy the columns from one structure to another and adding a new column.
```python
#Adding & Changing struct of the DataFrame
from pyspark.sql.functions import col,struct,when
updatedDF = df2.withColumn("OtherInfo", #add column other info with nasted column id,gender,salary
    struct(
        col("id").alias("identifier"),
        col("gender").alias("gender"),
        col("salary").alias("salary"),
    when(
        col("salary").cast(IntegerType()) < 2000,"Low")
      .when(
          col("salary").cast(IntegerType()) < 7000,"Medium")
      .otherwise("High").alias("Salary_Grade")
      )).drop("id","gender","salary")

updatedDF.printSchema()
updatedDF.show(truncate=False)
```
Here, it copies “gender“, “salary” and “id” to the new struct “otherInfo” and add’s a new column “Salary_Grade“.

source : https://sparkbyexamples.com/pyspark/pyspark-structtype-and-structfield/

# PySpark Distinct to Drop Duplicate Rows

PySpark distinct() function is used to drop/remove the duplicate rows (all columns) from DataFrame and dropDuplicates() is used to drop rows based on selected (one or multiple) columns.

1. create a DataFrame with some duplicate rows and values on a few columns.
```link
https://github.com/tananugrah/learning_spark/blob/main/Learning-Spark-Pyspark-distinct-Drop-Duplicate.ipynb
```
2. Get Distinct Rows (By Comparing All Columns).

On the above DataFrame, we have a total of 10 rows with 2 rows having all values duplicated, performing distinct on this DataFrame should get us 9 after removing 1 duplicate row.

```python
#Get Distinct Rows (By Comparing All Columns)
distinctDF = df.distinct()
print("Distinct count: "+str(distinctDF.count()))
distinctDF.show(truncate=False)
```
or
```python
#Alternatively, you can also run dropDuplicates() function which returns a new DataFrame after removing duplicate rows.
#this using more resource
df2 = df.dropDuplicates()
print("Distinct count: "+str(df2.count()))
df2.show(truncate=False)
```
3. PySpark Distinct of Selected Multiple Columns.

PySpark doesn’t have a distinct method that takes columns that should run distinct on (drop duplicate rows on selected multiple columns) however, it provides another signature of dropDuplicates() function which takes multiple columns to eliminate duplicates.
```python
#PySpark Distinct of Selected Multiple Columns
dropDisDF = df.dropDuplicates(["department","salary"])
print("Distinct count of department & salary : "+str(dropDisDF.count()))
dropDisDF.show(truncate=False)
```
source : https://sparkbyexamples.com/pyspark/pyspark-distinct-to-drop-duplicates/
