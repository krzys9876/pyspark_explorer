# Spark File Explorer
When developing spark applications I came across the growing number of data files that I create. 

## CSV, JSON, PARQUET?

To open and explore a file I used Excel to view CSV files, text editors with plugins to view JSON files, 
but there was nothing handy to view PARQUETs. Event formatted JSONs were not always readable. What about viewing schemas? 

Each time I had to use spark and write simple apps which was not a problem itself but was tedious and boring.

## Database?

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
In all environments I would and I would like to have _one tool to rule them all_.  

I like console tools a lot, they require some sort of simplicity. They can run locally or over SSH connection on 
the remote cluster. Sounds perfect. All I needed was a console UI library, so I wouldn't have to reinvent the wheel.

## Textual

What a great project! Years ago I used [_courses_](https://docs.python.org/3/library/curses.html) but 
[_textual_](https://textual.textualize.io/) is so superior to what I remember.

# Usage

TBD