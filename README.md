# Assignment #3: Trip Analytics with Spark

**ITCS 6190/8190 - Cloud Computing for Data Analysis - Fall 2026**

In Hands-on L5 you ran a Spark application somebody else had written, and in Hands-on L6 you
filled in four DataFrame queries over a small, clean dataset. In this assignment the data is
bigger and it is not clean. You decide how to load it so that the bad rows can be found,
remove them under two stated rules, answer two questions with the DataFrame API and one with
Spark SQL, and read the plans Spark builds for both.

By the end you should be able to take a raw dataset, make it trustworthy, and answer
questions about it on a Spark cluster, in either of the two languages Spark offers for
structured data.

**Worth 2 points** (the rubric is out of 100 and is scaled to 2). **Opens Tuesday, September
22. Due by 11:59 pm on Tuesday, October 27**, on Canvas. Work individually.

Five weeks sounds like a lot, but the midterm is on October 6 and there is no class on
October 13. A pace that works: **part 1 before the midterm** (it is the smallest part and it
gets your cluster and your data working), **parts 2 and 3 in the two weeks after**, and the
report in the last few days. Do not start on October 25.

---

## The data

`datagen.py` generates your data from a seed. Use your student ID: the data is then yours,
and the grader regenerates it from the seed you write in the report.

`trips.csv`, 100,000 rows, one per ride in September 2026:

| column         | example               | notes                                      |
| -------------- | --------------------- | ------------------------------------------ |
| `trip_id`      | `T000001`             |                                            |
| `driver_id`    | `D081`                | 250 drivers                                |
| `pickup_zone`  | `23`                  | zone id, see zones.csv                     |
| `dropoff_zone` | `1`                   |                                            |
| `pickup_ts`    | `2026-09-20 23:06:04` |                                            |
| `dropoff_ts`   | `2026-09-20 23:22:00` | **sometimes before the pickup**            |
| `distance_km`  | `4.85`                |                                            |
| `fare`         | `17.23`               | **sometimes empty, sometimes not a number** |
| `payment_type` | `card`                | card, app or cash                          |

`zones.csv`, 25 rows: `zone_id`, `zone_name`, `borough`.

About 3 percent of the trips are invalid, in two ways. Some have a dropoff timestamp earlier
than the pickup. Some have a fare that is empty, or a string such as `N/A` or `12,40`. Both
kinds must go before any analysis, and you must report how many there were.

---

## The work

`main.py` is laid out like the hands-on: loading is set up, and four functions are yours to
complete. Each docstring gives the exact columns, ordering and tie-breaking rules; the
result of each part is written to `shared-folder/output/<part>/` as a single CSV file, and
the grader compares those files with the expected values for your seed.

### Part 1: load and clean (the design decision)

Look at how `main.py` reads `trips.csv`. The `fare` column is read as a **string**. That is
deliberate: with a `DOUBLE` column, Spark's CSV reader would turn every bad fare into `null`
while reading, and you would never know how many there were or what they looked like.
Reading the text and converting it yourself keeps the problem visible. In Spark 4, ANSI mode
is on, so a plain `cast` of `"N/A"` to a number raises an error; `try_cast` gives you `null`
instead, which you can count and filter.

Implement `part1_clean`:

- **Rule 1.** A trip is invalid if `dropoff_ts` is not after `pickup_ts`.
- **Rule 2.** A trip is invalid if its fare is missing or is not a number.
- Produce the clean trips with `fare` as a `DOUBLE`, plus a `day` column (the pickup date)
  and a `duration_min` column, and write them to Parquet under `shared-folder/output/clean_trips`.
- Return the counts: `rows_total, bad_time, bad_fare, removed, rows_kept`. A row that breaks
  both rules counts in both `bad_time` and `bad_fare`, but once in `removed`.

Your report explains how you implemented the two rules and why the fare had to be read as a
string.

### Part 2: two questions with the DataFrame API

`part2a_zone_revenue`: trips and revenue per pickup zone per day, with the zone name joined
in. `part2b_driver_busiest_day`: each driver's busiest day, which needs a window function.
The docstrings have the details.

### Part 3: one question in Spark SQL, and two plans

`part3_borough_sql`: register the clean trips and the zones as temporary views and answer,
in **one SQL query**, how many trips, how much revenue and what average distance each
borough had. `createOrReplaceTempView` and `spark.sql` are all you need; the L5 slides
introduce Spark SQL, and Lecture 9 will go deeper.

`main.py` then prints the physical plan of part 2a (DataFrame API) and of part 3 (SQL). Read
them side by side for the report: where are the file scans, which operator is the join and of
which kind, where are the exchanges, and what is the same in both plans even though you
wrote them in different languages.

### Rules

Use the DataFrame API and Spark SQL only. No RDD operations, no `collect()` followed by
Python loops, no pandas.

---

## What is in this repository

| Path | What it is |
| ---- | ---------- |
| `main.py` | the application: loading is set up, the four functions are yours |
| `datagen.py` | generates `trips.csv` and `zones.csv` from a seed (plain Python) |
| `docker-compose.yml` | the Spark cluster from Hands-on L5 and L6: one master, two workers, `apache/spark:4.2.0` |
| `docker-compose.codespaces.yml`, `conf/spark-defaults.conf` | the same cluster, for GitHub Codespaces |
| `REPORT.md` | the report template you fill in |
| `shared-folder/input/` | where `datagen.py` writes the CSV files |
| `shared-folder/output/` | where the results land |

---

## Steps

### 1. Make your own copy of this repository

On the repository page, click the green **Use this template** button, then **Create a new
repository**. Name it `ITCS6190-A3-<your-name>` and set the visibility to **Public**. Do not
fork and do not clone this repository directly. Clone *your* repository and work in it.

### 2. Generate your data

```bash
python3 datagen.py <your student ID>
```

Without Python on your machine, start the cluster first and run it in the master container:

```bash
docker cp datagen.py spark-master:/opt/spark/work-dir/shared/
docker exec spark-master python3 /opt/spark/work-dir/shared/datagen.py <your student ID> \
  /opt/spark/work-dir/shared/input
```

Open `trips.csv` and find a few of the bad rows before you write any code.

### 3. Start the cluster

```bash
docker compose up -d
```

Check <http://localhost:8080> for two workers in state ALIVE. In a **GitHub Codespace**, use
`docker compose -f docker-compose.codespaces.yml up -d` instead, as in the hands-ons, and add
the same `-f` flag to `docker compose down` at the end.

### 4. Run, fill in, run again

```bash
docker cp main.py spark-master:/opt/spark/work-dir/

docker exec -it spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark/work-dir/main.py \
  /opt/spark/work-dir/shared/input \
  /opt/spark/work-dir/shared/output
```

As given, the program loads the data, prints the schema and reports every part as not
implemented. Complete one function at a time, `docker cp` after **every** edit, and run
again. The PySpark shell (`docker exec -it spark-master /opt/spark/bin/pyspark --master
spark://spark-master:7077`) is the place to try an expression before it goes in the file.

The run takes a minute or two on 100,000 rows. Open <http://localhost:4040> while it runs
and look at the jobs, the stages and the SQL / DataFrame tab; the two plans you compare in
the report are there as diagrams.

### 5. Stop the cluster

```bash
docker compose down
```

---

## What to commit

- Your completed `main.py`
- The four results: `shared-folder/output/part1/`, `part2a/`, `part2b/`, `part3/` (the
  `part-...csv` files; `_SUCCESS` and `.crc` files are ignored by `.gitignore`)
- `REPORT.md`, with your seed

The generated input CSVs and the Parquet folder are ignored on purpose: the grader
regenerates your data from the seed. Leave `README.md`, `datagen.py` and the compose files
as they are.

---

## Report

Fill in `REPORT.md`. Keep it short and specific.

**Seed and commands.** The seed, and the commands you ran.

**Part 1.** How you implemented the two rules, the five counts, and why the fare is read as
a string. Two or three of the bad rows you found, as they appear in the file.

**Parts 2 and 3.** The first ten rows of each result, and one sentence per result on what it
says about your data.

**The two plans.** The `explain()` output of part 2a and part 3, and your comparison: scans,
join and its kind, exchanges, and what the two plans share.

**At scale.** One paragraph: with 200 million trips instead of 100,000, what in your program
would you keep, and what would you change first?

**Problems and fixes.** With the actual error messages. If nothing went wrong, say so.

---

## Submission

Post the URL of **your** repository on Canvas by the deadline. Keep it public until grades
are posted. There is no need to add the instructor or the TAs as collaborators.

---

## Optional: go further

Not graded. Cache the clean trips before parts 2 and 3 and look at the Storage tab and the
job times; or read `clean_trips` back from Parquet and compare the plan and the bytes
scanned with reading the CSV; or write part 2b in SQL with a window function and compare the
plans once more.
