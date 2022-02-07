# OT Platform. Additional commands for dataframe. OT plugin.

Additional OTL commands for transforming dataframe in **Dispatcher App**.

## Command List

### assert
Verifies some assertion. Returns intact dataframe if assertion is true or an exception otherwise.
#### Syntax
```
| assert expression
```
#### Example
```
| assert x is not null
```

### coalesce
Returns a new Dataset that has exactly '__partitions_number__' partitions, when the fewer partitions are requested.

num - must be positive integer.
#### Syntax
```
| coalesce num=__partitions_number__
```
#### Example
```
| coalesce num=8
```

### dropna
Returns a new DataFrame that drops rows containing null or NaN values in the specified columns.

If how is "any", then drop rows containing any null or NaN values in the specified columns. If how is "all", then drop rows only if every specified column is null or NaN for that row.

#### Syntax
```
| dropna subset=__columns__ how=__drop_method_
```
#### Example
```
| dropna subset=cpu_util,mem_used how=all
```

### latest
Gets last not-null elements.
#### Syntax
```
| latest __columns__ [by __columns__]
```
#### Example
```
| latest mod2, mod3 by cat
```
```
Source dataset:
+---+----+----+-----+   
| id|mod3|mod2|  cat|
+---+----+----+-----+
|  0|null|null|small|
|  1|   1|   1|small|
|  2|   2|null|small|
|  3|null|   3|small|
|  4|   4|null|small|
|  5|   5|   5|  big|
|  6|null|null|  big|
|  7|   7|   7|  big|
|  8|   8|null|  big|
|  9|null|   9|  big|
+---+----+----+-----+
```
```
Result dataset:
+-----+----+----+
|  cat|mod2|mod3|
+-----+----+----+
|  big|   9|   8|
|small|   3|   4|
+-----+----+----+
```

### latestrow | lastrow
Gets latest row.

time - time column. Default value - _time.

engine = join | window - specifies type of aggregation.

Caution - 'window' has poor performance if no 'by' columns are specified (empty window - ExchangeSinglePartition).

Default value - join.

#### Syntax
```
| lastrow [time=__time_column__] [engine=_agg_engine__] [by __columns__]
```
#### Example
```
| lastrow _time=id engine=window by mod2
```

### pivot
Pivots dataset.
#### Syntax
```
| pivot __value_column__ __category_column__ [__fixed_columns__] [groups=__categories_clumns__] 
```
#### Example
```
| pivot time, well, metric, value groups=m1,m2,m3
```
```
Source dataset:
+----+----+------+-----+-------+
|time|well|metric|value|useless|
+----+----+------+-----+-------+
|   1|   a|    m1|   10|      1|
|   1|   a|    m2|   11|      2|
|   1|   a|    m3|   12|      2|
|   1|   b|    m1|   20|      2|
|   1|   b|    m2|   21|      1|
|   1|   b|    m3|   22|      4|
|   2|   a|    m1|  100|      9|
|   2|   a|    m2|  101|      3|
|   2|   a|    m3|  102|      2|
|   2|   b|    m1|  200|      5|
|   2|   b|    m2|  201|      5|
|   2|   b|    m3|  202|      2|
+----+----+------+-----+-------+
```
```
Result dataset:
+----+----+---+---+---+
|time|well| m1| m2| m3|
+----+----+---+---+---+
|   1|   a| 10| 11| 12|
|   1|   b| 20| 21| 22|
|   2|   a|100|101|102|
|   2|   b|200|201|202|
+----+----+---+---+---+
```

### repartition
Returns a new Dataset that has exactly '__partitions_number__' partitions.
num - must be positive integer.
#### Syntax
```
| repartition num=__partitions_number__
```
#### Example
```
| repartition num=8
```

### split
Splits '__column_to_split__' into '__splitted_columns__' by '__separator__'
sep - separator character. Default value - #.
#### Syntax
```
| split __column_to_split__ cols=__splitted_columns__ sep=[__separator__]
```
#### Example
```
| split text cols=col1,col2,col3 sep=-
```
```
Source dataset:
+---+-----------------+
| id|             text|
+---+-----------------+
|  0|   col1-col2-col3|
|  1|col11-col22-col33|
+---+-----------------+
```
```
Result dataset:
+---+-----------------+-----+-----+-----+
| id|             text| col1| col2| col3|
+---+-----------------+-----+-----+-----+
|  0|   col1-col2-col3| col1| col2| col3|
|  1|col11-col22-col33|col11|col22|col33|
+---+-----------------+-----+-----+-----+
```

### superjoin
Joins with dataset on specified path.
#### Syntax
```
| superjoin type=__join_type__ path=__path_to_right_dataset__ format=__format_right_dataset__ join_on_columns 
```
#### Example
```
| superjoin type=inner path=src/test/resources/superJoin format=parquet name
```

### unpivot
Unpivots dataset.
#### Syntax
```
| unpivot __columns__
```
#### Example
```
| unpivot time, well, metric, value
```
```
Source dataset:
+----+----+---+---+---+
|time|well| m1| m2| m3|
+----+----+---+---+---+
|   1|   a| 10| 11| 12|
|   1|   b| 20| 21| 22|
|   2|   a|100|101|102|
|   2|   b|200|201|202|
+----+----+---+---+---+
```
```
Result dataset:
+----+----+------+-----+
|time|well|metric|value|
+----+----+------+-----+
|   1|   a|    m1|   10|
|   1|   a|    m2|   11|
|   1|   a|    m3|   12|
|   1|   b|    m1|   20|
|   1|   b|    m2|   21|
|   1|   b|    m3|   22|
|   2|   a|    m1|  100|
|   2|   a|    m2|  101|
|   2|   a|    m3|  102|
|   2|   b|    m1|  200|
|   2|   b|    m2|  201|
|   2|   b|    m3|  202|
+----+----+------+-----+
```

### REM | ___
Adds a comment. Returns intact source dataframe.
#### Syntax
```
| REM __commentary__ 
```
#### Example
```
| REM comment with a lot of words
```

### cc | conncomp
Calculates connected components in graph.
```
| cc src=__source_column_ dst=__destination_column__
```
#### Example
```
| cc src=node dst=relation
```
```
Source dataset:
+----+--------+
|node|relation|
+----+--------+
|   1|       0|
|   1|       2|
|   2|       5|
|   3|       4|
|   4|       6|
+----+--------+
```
```
Result dataset:
+----+--------+---+
|node|relation| cc|
+----+--------+---+
|   1|       0|  0|
|   1|       2|  0|
|   2|       5|  0|
|   3|       4|  3|
|   4|       6|  3|
+----+--------+---+
```


## Dependencies

- dispatcher-sdk_2.11  1.2.0
- sbt 1.5.8
- scala 2.11.12
- eclipse temurin 1.8.0_312 (formerly known as AdoptOpenJDK)

## Deployment

1. make pack or make build.
2. Copy the `build/OTLExtend` directory to `/opt/otp/dispatcher/plugins` (or unpack the resulting archive `otlextend-<PluginVersion>-<Branch>.tar.gz` into the same directory).
3. Rename loglevel.properties.example => loglevel.properties.
4. Rename plugin.conf.example => plugin.conf.
5. If necessary, configure plugin.conf and loglevel.properties.
6. Restart dispatcher.

## Running the tests

sbt test

## Versioning

We use [SemVer](http://semver.org/) for versioning. For the versions available, see the tags on this repository.  

## Authors
 
Sergey Ermilov (sermilov@ot.ru)
Nikolay Ryabykh (nryabykh@isgneuro.com)

## License

[OT.PLATFORM. License agreement.](LICENSE.md)