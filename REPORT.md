# Assignment #3: Report

**Name:**
**Student ID:**
**Email:**

---

## Seed and commands

Seed used for `datagen.py`:

The commands you ran, in order:

```bash

```

---

## Part 1: load and clean

How you implemented rule 1 (dropoff after pickup) and rule 2 (fare present and numeric):



Why `fare` is read as a string, and what would have happened with a `DOUBLE` column:



The five counts:

| rows_total | bad_time | bad_fare | removed | rows_kept |
| ---------- | -------- | -------- | ------- | --------- |
|            |          |          |         |           |

Two or three bad rows, as they appear in `trips.csv`:

```

```

---

## Part 2a: trips and revenue per zone per day

First ten rows, and one sentence on what they show:

```

```

## Part 2b: each driver's busiest day

First ten rows, and one sentence on what they show:

```

```

## Part 3: activity per borough (Spark SQL)

Your SQL query and its result:

```sql

```

```

```

---

## The two plans

`explain()` output of part 2a (DataFrame API):

```

```

`explain()` output of part 3 (Spark SQL):

```

```

Your comparison: where the file scans are, which operator is the join and of which kind,
where the exchanges are and why, and what the two plans have in common although they were
written in different languages.



---

## At scale

With 200 million trips instead of 100,000: what would you keep as it is, and what would you
change first? One paragraph.



---

## Problems and fixes

What went wrong and how you fixed it, with the actual error messages. If nothing went wrong,
say so.


