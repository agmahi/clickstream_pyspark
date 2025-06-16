# clickstream_pyspark
pyspark streaming app to process clickstream data and publish it to a dashboard

# Set Up
Install docker to run kafka locally. 
* Once docker is installed, create a compose.yaml to manage your Docker components (services, volumes, networks, etc.)
* Run `docker compose up -d` from the directory that has your yaml file

**References:** [Confluent Guide](https://developer.confluent.io/confluent-tutorials/kafka-on-docker/?utm_medium=sem&utm_source=google&utm_campaign=ch.sem_br.nonbrand_tp.prs_tgt.dsa_mt.dsa_rgn.namer_lng.eng_dv.all_con.confluent-developer&utm_term=&creative=&device=c&placement=&gad_source=1&gad_campaignid=19560855036&gbraid=0AAAAADRv2c3vRPcAhbiLx4cxC1ijGFFHl&gclid=Cj0KCQjw0qTCBhCmARIsAAj8C4aJenxSKW9Wx0eg20LtQ6d98IXbnHs9HFi5_XQ2A6JqM4rR5zjH9zsaAkjvEALw_wcB)


After Kafka is setup, create a **topic** using the command
```./kafka-topics.sh --create --topic my-topic --bootstrap-server broker:29092```

Then, create a **producer** that writes events to this topic:
``` ./kafka-console-producer.sh  --topic my-topic --bootstrap-server broker:29092```

Now, we will create a Spark app to consume these events and write to a db sink, but first, we need to set up Spark. 

install spark using homebrew
```brew install apache-spark``` at the time of this writeup, it installed spark 4.0.0. This installs the Spark components (core engine and other components)
Install pyspark, run ```pip3 install pyspark```. Make sure spark and pyspark are added to `PATH` and SPARK_HOME points to the spark bin. 

Note: I also had to install JAVA as spark requires JVM - at the time of this writeup, I installed openjdk@17 (required for Spark 4.0.0)


References: [Conduktor Guide](https://conduktor.io/blog/getting-started-with-pyspark-and-kafka)
[ChatGPT Troubleshooting Guide](https://chatgpt.com/share/684fbf08-6ca4-800b-b327-19c30a4f61f0)


Project Plan by Gemini