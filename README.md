# Hadoop Examples
Some simple, kinda introductory projects based on Apache Hadoop to be used as guides in order to make the MapReduce model look less weird or boring.

## Preparations & Prerequisites
* [Latest stable version](https://hadoop.apache.org/docs/current/) of Hadoop or at least the one used here, [3.3.0](https://hadoop.apache.org/docs/r3.3.0/).
*  A single node setup is enough. You can also use the applications in a local cluster or in a cloud service, with needed changes on the map splits and the number of reducers, of course.
* Of course, having (a somehow recent version of) Java installed. I have `openjdk 11.0.5` installed to my 32-bit Ubuntu 16.04 system, and if I can do it, so can you.

## Projects
Each project comes with its very own:
* **input data** (`.csv` files in a folder ready to be copied to the HDFS). 
* **execution guide** (found in the source code of each project _but_ also being heavily dependent of your setup of java and environment variables, so in case the guide doesn't work, you can always google/yahoo/bing/altavista your way to execution).

The projects featured in this repo are:

#### [AvgPrice](https://github.com/Coursal/Hadoop-Examples/tree/main/AvgPrice)
Calculating the average price of houses for sale by zipcode.

#### [BankTransfers](https://github.com/Coursal/Hadoop-Examples/tree/main/BankTransfers)
A typical "sum-it-up" example where for each bank we calculate the number and the sum of its transfers.

#### [MaxTemp](https://github.com/Coursal/Hadoop-Examples/tree/main/MaxTemp)
Typical case of finding the max recorded temperature for every city.

#### [Medals](https://github.com/Coursal/Hadoop-Examples/tree/main/Medals)
An interesting application of working on Olympic game stats  in order to see the total wins of gold, silver, and bronze medals of every athlete.

#### [NormGrades](https://github.com/Coursal/Hadoop-Examples/tree/main/NormGrades)
Just a plain old normalization example for a bunch of students and their grades.

#### [OldestTree](https://github.com/Coursal/Hadoop-Examples/tree/main/OldestTree)
Finding the oldest tree per city district. Child's play.

#### [ScoreComp](https://github.com/Coursal/Hadoop-Examples/tree/main/ScoreComp)
The most challenging and abstract one. Every key-character (`A`-`E`) has 3 numbers as values, two negatives and one positive. We just calculate the score for every character based on the following expression `character_score = pos / (-1 * (neg_1 + neg_2))`.

#### [SymDiff](https://github.com/Coursal/Hadoop-Examples/tree/main/SymDiff)
A simple way to calculate the symmetric difference between the records of two files, based on each record's ID.

#### [PatientFilter](https://github.com/Coursal/Hadoop-Examples/tree/main/PatientFilter)
Filtering out patients' records where their number of cycles column is equal to `1` and their counseling column is equal to `No`.

---

_Check out the equivalent **Spark Examples** [here](https://github.com/Coursal/Spark-Examples)._
