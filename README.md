# Spark File Explorer
When developing spark applications I came across the growing number of data files that I create. 

![pe03](https://github.com/user-attachments/assets/e7d51949-2868-4b1c-ac4a-3807d0f4a41a)

![pe04](https://github.com/user-attachments/assets/442d70e5-8098-4bbf-87db-a9cddbeaf223)

## CSVs are fine but what about JSON and complex PARQUET files?

To open and explore a file I used Excel to view CSV files, text editors with plugins to view JSON files, 
but there was nothing handy to view PARQUETs. Event formatted JSONs were not always readable. What about viewing schemas? 

Each time I had to use spark and write simple apps which was not a problem itself but was tedious and boring.

## Why not a database?

Well, for tabular data there problems is already solved - just use your preferred database.
Quite often we can load text files or even parquets directly to the database. 

So what's the big deal?

## Hierarchical data sets

Unfortunately the files I often deal with have hierarchical structure. They cannot be simply visualized as tables
or rather some fields contain tables of other structures. Each of these structures is a table itself but how to load 
and explore such embedded tables in a database?

## For Spark files use... Spark! 

Hold on - since I generate files using Apache Spark, why can't I use it to explore them?
I can easily handle complex structures and file types using built-in features. So all I need is to build a use interface 
to display directories, files and their contents.

## Why console?

I use Kubernetes in production environment, I develop Spark applications locally or in VM. 
In all environments I would like to have _one tool to rule them all_.  

I like console tools a lot, they require some sort of simplicity. They can run locally or over SSH connection on 
the remote cluster. Sounds perfect. All I needed was a console UI library, so I wouldn't have to reinvent the wheel.

## Textual

What a great project [_textual_](https://textual.textualize.io/) is! 

Years ago I used [_curses_](https://docs.python.org/3/library/curses.html) but 
[_textual_](https://textual.textualize.io/) is so superior to what I used back then. It has so many features packed in
a friendly form of simple to use components. Highly recommended.

# Usage

Install package with pip:
    
    pip install pyspark-explorer

Run:

    pyspark-explorer

You may wish to provide a base path upfront. It can be changed at any time (press _o_ for _Options_).

For local files that could be for example:

    # Linux
    pyspark-explorer file:///home/myuser/datafiles/base_path
    # Windows
    pyspark-explorer file:/c:/datafiles/base_path

For remote location:

    # Remote hdfs cluster
    pyspark-explorer hdfs://somecluster/datafiles/base_path

Default path is set to /, which represents local root filesystem and works fine even in Windows thanks to Spark logics.

Configuration files are saved to your home directory (_.pyspark-explorer_ subdirectory). 
These are json files so you are free to edit them.

# Spark limitations

Note that you will not be able to open any JSON file - only those with _correct_ structure can be viewed. If you try to open a file which has a unacceptable structure, Spark will throw an error like this:

    Since Spark 2.3, the queries from raw JSON/CSV files are disallowed when the
    referenced columns only include the internal corrupt record column
    (named _corrupt_record by default). For example:
    spark.read.schema(schema).csv(file).filter($"_corrupt_record".isNotNull).count()
    and spark.read.schema(schema).csv(file).select("_corrupt_record").show().
    Instead, you can cache or save the parsed results and then send the same query.
    For example, val df = spark.read.schema(schema).csv(file).cache() and then
    df.filter($"_corrupt_record".isNotNull).count().

You can find the log file in your home directory (_.pyspark-explorer_ subdirectory).