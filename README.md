## Description: Monte-Carlo Simulation for Stock Prediction using Apache Spark
### Name: Amna Irfan

### Instructions

Install [IntelliJ](https://www.jetbrains.com/student/), JDK, Scala runtime, IntelliJ Scala plugin and the [Simple Build Toolkit (SBT)](https://www.scala-sbt.org/1.x/docs/index.html) and make sure that you can run Java monitoring tools.

Open the project in IntelliJ and build the project. This may take some time as the Library Dependencies mentioned in the build.sbt file will be downloaded and added to the classpath of the project.

Alternatively, you can run the project using command line. Open the terminal and `cd` into the project directory. 

This package includes only one main class which uses Apache spark to run multiple simulations for stock prediction spanning over a specific date range.
The output of this project is structured in the following way.

1. Each simulation has a separate folder with a number associated to it. The number indicates the simulation number.
2. Inside each simulation folder are date folders.
3. Inside each date folder is the Spark job output. In the case of this app, each line in the output is a tuple consisting of the name of the stock 
that the investor owned (or bought that day), the closing amount of the stock and the investor's share. For example, an output of (AAPL, 70, 0.5) means that
the investor has a 50% share of a single stock. This means the value of his investment on that day for AAPL is $35.

Please check the description.pdf file in the root directory of this project for more information on our implementation.

There are two ways to run this application

#### Locally
1. Update the configuration file to use `local` as the env.
2. Since there is only one single main class in this project, simply running `sbt clean compile run` should be enough.
3. If the user wants they can update the configuration file of this project to change the date range and number of simulations they need to run. 
4. Make sure to only choose dates between 11/16/18 - 11/15/19 as this project does not contain data beyond that.
5. Once the project has completed running, check the output folder under /resources/stocks
6. The amount made by the investor is in the result folder of each simulation.

#### Hortonworks
1. Update the configuration file to use `hadoop` as the env.
2. Update the main file (the task class) to be run in the sbt.build file.
3. To run, cd into the project and run the command `sbt clean compile assembly`
4. Export the jar file to your hadoop environment.
5. Make sure the input folder is also available on hdfs.
6. Run the command `spark-submit --verbose --deploy-mode cluster {location to jar file}`
7. Checkout output in specified directory.


This project has a total of 8 tests in Scala. In order to run them using the terminal, `cd` into the project directory and run the command `sbt clean compile test`


