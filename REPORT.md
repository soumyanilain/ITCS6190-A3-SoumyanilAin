# Assignment #3: Report

**Name:** Soumyanil Ain
**Student ID:** 801488534
**Email:** sain@charlotte.edu

---

## Seed and commands

Seed used for `datagen.py`: **801488534**

The commands you ran, in order:

```bash
python datagen.py 801488534
docker compose up -d
docker cp main.py spark-master:/opt/spark/work-dir/
docker exec -it spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark/work-dir/main.py /opt/spark/work-dir/shared/input /opt/spark/work-dir/shared/output
docker compose down
```

---

## Part 1: load and clean

How you implemented rule 1 (dropoff after pickup) and rule 2 (fare present and numeric):

I don't filter straight away. First I add three helper columns to the raw trips so every
row carries its own verdict:

- `fare_num = try_cast(fare AS DOUBLE)`. Empty strings, `N/A`, `unknown` and `12,40` all
  come back as `null` instead of crashing the job.
- `bad_time = NOT coalesce(dropoff_ts > pickup_ts, false)`. Rule 1 asks for the dropoff to
  be strictly after the pickup, and the `coalesce` makes a missing timestamp count as bad
  too, instead of slipping through as `null`.
- `bad_fare = fare_num IS NULL` (rule 2).

The counts come from one `agg` over those flags (`count(1)`, then `sum` of each flag cast to
int, and `sum(bad_time OR bad_fare)` for `removed`). Because `removed` uses OR, a row that
broke both rules would show up in both `bad_time` and `bad_fare` but only once in `removed`.
In my data no row broke both (the generator picks one defect per row), which is why
1574 + 1459 = 3033. The clean trips are the rows where neither flag is set, with `fare_num`
renamed back to `fare` (so `fare` is now a `DOUBLE`), plus `day = to_date(pickup_ts)` and
`duration_min = (unix_timestamp(dropoff_ts) - unix_timestamp(pickup_ts)) / 60`, rounded to
2 decimals. They are written to `output/clean_trips` as Parquet.

Why `fare` is read as a string, and what would have happened with a `DOUBLE` column:

With a `DOUBLE` column the CSV reader would quietly turn every bad fare into `null` while
reading the file. You would end up with nulls but no way to tell an empty fare from `N/A`
or `12,40`, and no way to look at the original text to understand what went wrong. Reading
it as a string keeps the raw value, so I could see the bad values (950 empty, 178 `N/A`,
167 `12,40`, 164 `unknown`) and count them before deciding to drop them. And since Spark 4
runs with ANSI mode on, a plain `cast("N/A" AS DOUBLE)` would throw an error and kill the
job, which is why the conversion uses `try_cast`.

The five counts:

| rows_total | bad_time | bad_fare | removed | rows_kept |
| ---------- | -------- | -------- | ------- | --------- |
| 100000     | 1574     | 1459     | 3033    | 96967     |

Two or three bad rows, as they appear in `trips.csv`:

```
T000008,D004,17,3,2026-09-25 19:31:13,2026-09-25 19:24:33,1.47,7.55,cash      <- dropoff before pickup
T000094,D123,8,1,2026-09-03 18:18:24,2026-09-03 18:31:30,3.39,N/A,card         <- fare is N/A
T000150,D189,11,8,2026-09-01 12:47:19,2026-09-01 13:35:37,15.5,,card           <- fare is empty
T001664,D024,6,10,2026-09-26 19:55:11,2026-09-26 20:10:00,3.83,"12,40",app    <- comma as decimal separator
```

---

## Part 2a: trips and revenue per zone per day

First ten rows, and one sentence on what they show:

```
+----------+-------+---------------+-----+-------+
|day       |zone_id|zone_name      |trips|revenue|
+----------+-------+---------------+-----+-------+
|2026-09-01|1      |Uptown Center  |308  |6054.01|
|2026-09-01|2      |First Ward     |263  |5148.77|
|2026-09-01|3      |Fourth Ward    |263  |5195.26|
|2026-09-01|4      |South End      |245  |4816.33|
|2026-09-01|5      |Dilworth       |173  |3470.33|
|2026-09-01|6      |Plaza Midwood  |138  |2658.03|
|2026-09-01|7      |NoDa           |161  |2959.59|
|2026-09-01|8      |University City|174  |3618.15|
|2026-09-01|9      |Ballantyne     |128  |2477.27|
|2026-09-01|10     |SouthPark      |192  |3899.85|
+----------+-------+---------------+-----+-------+
```

The result has 750 rows, exactly 30 days x 25 zones, so every zone had pickups every day,
and Uptown Center is the busiest zone both on September 1 and over the month (about
$181k in fares, against about $12k for Mint Hill at the bottom).

## Part 2b: each driver's busiest day

First ten rows, and one sentence on what they show:

```
+---------+----------+-----+
|driver_id|day       |trips|
+---------+----------+-----+
|D001     |2026-09-17|22   |
|D002     |2026-09-10|18   |
|D003     |2026-09-26|20   |
|D004     |2026-09-12|24   |
|D005     |2026-09-27|19   |
|D006     |2026-09-17|19   |
|D007     |2026-09-01|19   |
|D008     |2026-09-02|24   |
|D009     |2026-09-20|25   |
|D010     |2026-09-27|19   |
+---------+----------+-----+
```

A driver's busiest day has between 16 and 29 trips (D209 on September 12 is the top),
compared with an average of about 13 trips per driver per day, and 68 of the 250 drivers
had two or more days tied for their best, so the "earliest day wins" rule in the window
ordering actually changes the answer for them.

## Part 3: activity per borough (Spark SQL)

Your SQL query and its result:

```sql
SELECT z.borough,
       COUNT(*)                     AS trips,
       ROUND(SUM(t.fare), 2)        AS revenue,
       ROUND(AVG(t.distance_km), 2) AS avg_distance_km
FROM trips t
JOIN zones z ON t.pickup_zone = z.zone_id
GROUP BY z.borough
ORDER BY revenue DESC
```

```
+-------+-----+---------+---------------+
|borough|trips|revenue  |avg_distance_km|
+-------+-----+---------+---------------+
|Central|41158|810495.85|5.34           |
|South  |18491|365847.26|5.36           |
|West   |12829|253478.62|5.35           |
|North  |12505|248804.06|5.39           |
|East   |11984|238651.57|5.41           |
+-------+-----+---------+---------------+
```

Central alone has about 42% of the trips and the revenue, while the average distance is
almost the same everywhere (5.34 to 5.41 km), so the boroughs differ in how many rides they
get, not in how long the rides are.

---

## The two plans

`explain()` output of part 2a (DataFrame API):

```
== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=false
+- Sort [day#39 ASC NULLS FIRST, zone_id#9 ASC NULLS FIRST], true, 0
   +- Exchange rangepartitioning(day#39 ASC NULLS FIRST, zone_id#9 ASC NULLS FIRST, 200), ENSURE_REQUIREMENTS, [plan_id=1527]
      +- HashAggregate(keys=[day#39, zone_id#9, zone_name#10], functions=[count(1), sum(fare#38)])
         +- Exchange hashpartitioning(day#39, zone_id#9, zone_name#10, 200), ENSURE_REQUIREMENTS, [plan_id=1524]
            +- HashAggregate(keys=[day#39, zone_id#9, zone_name#10], functions=[partial_count(1), partial_sum(fare#38)])
               +- Project [fare#38, day#39, zone_id#9, zone_name#10]
                  +- BroadcastHashJoin [pickup_zone#2], [zone_id#9], Inner, BuildRight, false, false
                     :- Project [pickup_zone#2, try_cast(fare#7 as double) AS fare#38, cast(pickup_ts#4 as date) AS day#39]
                     :  +- Filter (((isnotnull(fare#7) AND coalesce((dropoff_ts#5 > pickup_ts#4), false)) AND isnotnull(try_cast(fare#7 as double))) AND isnotnull(pickup_zone#2))
                     :     +- FileScan csv [pickup_zone#2,pickup_ts#4,dropoff_ts#5,fare#7] Batched: false, DataFilters: [isnotnull(fare#7), coalesce((dropoff_ts#5 > pickup_ts#4), false), isnotnull(try_cast(fare#7 as d..., Format: CSV, Location: InMemoryFileIndex(1 paths)[file:/opt/spark/work-dir/shared/input/trips.csv], PartitionFilters: [], PushedFilters: [IsNotNull(fare), IsNotNull(pickup_zone)], ReadSchema: struct<pickup_zone:int,pickup_ts:timestamp,dropoff_ts:timestamp,fare:string>
                     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, false] as bigint)),false), [plan_id=1519]
                        +- Filter isnotnull(zone_id#9)
                           +- FileScan csv [zone_id#9,zone_name#10] Batched: false, DataFilters: [isnotnull(zone_id#9)], Format: CSV, Location: InMemoryFileIndex(1 paths)[file:/opt/spark/work-dir/shared/input/zones.csv], PartitionFilters: [], PushedFilters: [IsNotNull(zone_id)], ReadSchema: struct<zone_id:int,zone_name:string>
```

`explain()` output of part 3 (Spark SQL):

```
== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=false
+- Sort [revenue#197 DESC NULLS LAST], true, 0
   +- Exchange rangepartitioning(revenue#197 DESC NULLS LAST, 200), ENSURE_REQUIREMENTS, [plan_id=1577]
      +- HashAggregate(keys=[borough#11], functions=[count(1), sum(fare#38), avg(distance_km#6)])
         +- Exchange hashpartitioning(borough#11, 200), ENSURE_REQUIREMENTS, [plan_id=1574]
            +- HashAggregate(keys=[borough#11], functions=[partial_count(1), partial_sum(fare#38), partial_avg(distance_km#6)])
               +- Project [distance_km#6, fare#38, borough#11]
                  +- BroadcastHashJoin [pickup_zone#2], [zone_id#9], Inner, BuildRight, false, false
                     :- Project [pickup_zone#2, distance_km#6, try_cast(fare#7 as double) AS fare#38]
                     :  +- Filter (((isnotnull(fare#7) AND coalesce((dropoff_ts#5 > pickup_ts#4), false)) AND isnotnull(try_cast(fare#7 as double))) AND isnotnull(pickup_zone#2))
                     :     +- FileScan csv [pickup_zone#2,pickup_ts#4,dropoff_ts#5,distance_km#6,fare#7] Batched: false, DataFilters: [isnotnull(fare#7), coalesce((dropoff_ts#5 > pickup_ts#4), false), isnotnull(try_cast(fare#7 as d..., Format: CSV, Location: InMemoryFileIndex(1 paths)[file:/opt/spark/work-dir/shared/input/trips.csv], PartitionFilters: [], PushedFilters: [IsNotNull(fare), IsNotNull(pickup_zone)], ReadSchema: struct<pickup_zone:int,pickup_ts:timestamp,dropoff_ts:timestamp,distance_km:double,fare:string>
                     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, false] as bigint)),false), [plan_id=1569]
                        +- Filter isnotnull(zone_id#9)
                           +- FileScan csv [zone_id#9,borough#11] Batched: false, DataFilters: [isnotnull(zone_id#9)], Format: CSV, Location: InMemoryFileIndex(1 paths)[file:/opt/spark/work-dir/shared/input/zones.csv], PartitionFilters: [], PushedFilters: [IsNotNull(zone_id)], ReadSchema: struct<zone_id:int,borough:string>
```

Your comparison: where the file scans are, which operator is the join and of which kind,
where the exchanges are and why, and what the two plans have in common although they were
written in different languages.

**Scans.** Each plan has two `FileScan csv` nodes at the leaves, one for `trips.csv` and
one for `zones.csv`. Neither plan reads the Parquet I wrote in part 1: `clean` is still a
lineage, so every query goes back to the raw CSV and redoes the cleaning. You can see part
1 sitting right above the trips scan as a `Filter` with `coalesce(dropoff_ts > pickup_ts,
false)` and `isnotnull(try_cast(fare as double))`. Only the simple `IsNotNull` checks get
pushed into the CSV reader (`PushedFilters`); the timestamp comparison and the `try_cast`
have to run as a separate filter. Column pruning works in both: part 2a only reads the
four trip columns it needs, part 3 also reads `distance_km`, and each plan reads only the
zone column it uses (`zone_name` or `borough`).

**Join.** Both use a `BroadcastHashJoin`, inner, `BuildRight`. The zones file is 544
bytes, far below the 10 MB `autoBroadcastJoinThreshold`, so Spark ships the whole
zones table to every executor (`BroadcastExchange`) and builds a hash table from it. The big
trips table never moves for the join.

**Exchanges.** There are three in each plan. The `BroadcastExchange` is for the join. Then
the aggregation runs in two steps: a partial `HashAggregate` on each partition
(`partial_count`, `partial_sum`, and `partial_avg` in part 3), an `Exchange hashpartitioning`
on the grouping keys so all rows of one group land in the same partition, and a final
`HashAggregate` that merges the partial results. Last, the `ORDER BY` needs a global order,
so there is an `Exchange rangepartitioning` on the sort key followed by a `Sort`. Both
exchanges use the default 200 shuffle partitions; `AdaptiveSparkPlan isFinalPlan=false`
means AQE will shrink that at runtime once it sees how little data there is.

**What they share.** Apart from the grouping keys, the extra `avg` and the sort key, the
two plans are the same tree: same scans, same cleaning filter, same broadcast join, same
two-phase aggregation, same exchanges. The DataFrame calls and the SQL string both become
the same logical plan and go through the same Catalyst optimizer, so the language you write
in does not change how Spark runs the query.

---

## At scale

With 200 million trips instead of 100,000: what would you keep as it is, and what would you
change first? One paragraph.

I would keep the cleaning logic (read `fare` as text, `try_cast`, flag then count), the
broadcast join with zones (25 rows stays tiny no matter how many trips there are), the
two-phase aggregations and the window for 2b, since they all scale with the data and only
shuffle small aggregated rows. The first thing I would change is that every query
re-reads the raw CSV and re-runs the cleaning: my 100,000-row file is about 7.4 MB, so 200
million rows would be roughly 15 GB of text, read and parsed again for every part. I would clean once, write `clean_trips` as Parquet partitioned by
`day`, and run parts 2 and 3 on that Parquet (or at least cache the clean DataFrame), so
each query reads only the columns it needs from a compressed columnar format and can skip
whole days. After that I would raise `spark.sql.shuffle.partitions` (or rely on AQE) so the
shuffles are not stuck at 200 big partitions, and put the input on HDFS or S3 instead of a
folder mounted on every container.

---

## Problems and fixes

What went wrong and how you fixed it, with the actual error messages. If nothing went wrong,
say so.

Nothing failed. All four parts ran on the first full run with no errors. To be sure the
results were right and not just error-free, I recomputed the five counts and all rows of
parts 2a, 2b and 3 with a separate plain-Python script reading the same CSV, and they
matched (750 rows in 2a, 250 in 2b). The one thing I watched out for was the tie-breaking in
2b: 68 drivers have more than one day tied for their highest count, so ordering the window
by `trips DESC` alone would have picked an arbitrary one of those days; adding `day ASC` as the second
sort key fixes it.
