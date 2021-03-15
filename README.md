# HbaseIndex


Course Page : https://www.cs.ucr.edu/~vagelis/classes/CS242/index.htm


###searchmr:
	mvn clean compile package assembly:single
	hadoop jar /home/karna/projects/HbaseIndex/searchmr/target/HbaseIndex-1.0.0-jar-with-dependencies.jar edu.ucr.abhi.index.Indexer <hdfs path to files>
	
	hadoop jar /home/karna/projects/HbaseIndex/searchmr/target/HbaseIndex-1.0.0-jar-with-dependencies.jar edu.ucr.abhi.search.Rank <query> <hdfs path to files>

###spring:
	mvn clean package spring-boot:repackage
	java -jar target/spring-mvc-0.0.1-SNAPSHOT.jar
