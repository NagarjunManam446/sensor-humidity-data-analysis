# sensor-humidity-data-analysis

Arguments:
need to pass directory path "/user/local/../../testing.csv"

### Dependencies

- [Git](https://git-scm.com/) 2.18+
- [Maven download](https://maven.apache.org/download.cgi)
- [Scala and Spark(Refer Pom.xml for details about versions)] 


### Installation

#### Build through Maven

Output of each function:
******************************************
Data read from csv files:
--------------------------
+--------+--------+
|sensorid|humidity|
+--------+--------+
|s2      |80      |
|s3      |NaN     |
|s2      |78      |
|s1      |98      |
|s1      |10      |
|s2      |88      |
|s1      |NaN     |
+--------+--------+

******************************************
stdOutMessage will print below message:
--------------------------------------
Num of processed files:2
Num of processed measurements:5
Num of failed measurements:2

******************************************
minMaxAvg will print below message:
----------------------------------
  +--------+---+---+---+
  |sensorid|Min|Max|Avg|
  +--------+---+---+---+
  |s2      |78 |88 |82 |
  |s1      |10 |98 |54 |
  |s3      |NaN|NaN|NaN|
  +--------+---+---+---+