#!/bin/bash

rm final_project.jar
$HADOOP_HOME/bin/hadoop com.sun.tools.javac.Main Main.java
jar cf final_project.jar *.class
rm *.class
cp final_project.jar ../
