# Seedtag DE codetest

## Architecture & design

### General overview

I decided to use apache beam to read from kafka, because it's a really handy framework that allows you to define custom transforms where you can process data however you please. For storage, I used a containerized postgres database. To simplify the process, for each text I recieve from kafka, I will detect the entities within it, and then store the entities, lables and readers in the database, which will then allow me to filter on the timestamp and sum the number of readers for each entity.

### Issues and workarounds

I wan't able to read messages using the default ReadFromKafka trasform or the kafka consumer from kafka-python (My guess is because the streaming service was setup using confluent's kafka), and the consumer from confluent-kafka didn't sit well with apache beam either, so as a workaround, I set up a dummy Create transform to start the pipeline, and then (I agree it's a little bit unpleasant) I made a custom ReadFromKafka DoFn that will read the data from kafka and then push it to the transforms downstream.

### Code logic

#### Reading data
Like I mentioned previously, I have a dummy Create transform that returns None, and then I setup a ReafFromKafka DoFn that will replicate the same logic as in the listener service, and then yield the messages it reads.

#### Classifiying the data
To exract the entities from the text, in the classify_entities function in the utils.py file, I used a simple regex pattern, I thought about classifiying the entities beforehand, therefore I'd only use the entities that have the same labels as the text inside the pattern, but seeing as how regex works faster with more constraints I decided to leave it as is, which would also make us able to process texts from different sources at the same time.
I then generate a dictionary for each entity, with the name, label, number of readers and timestamp that I yield to the writeToDatabase transform.

#### Storing data
In the database.py, I used sqlalchemy to create a model for the table, an engine and a session to connect to the database, and then inside the writeToDatabase transform I just add the enitities yielded by the Classify tranform.

## How to run

I put the code to my pipeline in the consumer-service folder, and created a corresponding service in the docker-compose file, in order to run the pipeline please make sure to run this command first:
`docker compose up postgres zookeeper kafka classification-service data-streaming-service`
This will make sure that the streaming service and classification service are up and running before we start the consumer service, otherwise you might get some odd behaviours like I did.
Then start the consumer service like so:
`docker compose up consumer-service`
With this the pipeline will start and you will start to see the entities in your logs. This would start to ppopulate the database.
To get the desired output, you can either connect to the postgres database using any tool or vscode extention in my case to execute this query:

`SELECT entity, sum(readers) as total_readrs FROM entities WHERE timestamp > 'min_timestamp' and timestamp < 'max_timestamp' GROUP BY entity ORDER BY total_readrs DESC LIMIT N`

Or, inside the search folder, I have setup a script using click, for this, you will need to have sqlalchemy, psycopg2 and click installed in your python environment, and then you can just run the main.py and pass it the n, min_ts and max_ts as command line arguments.
I have used `"%Y-%m-%d %H:%M:%S"`as the date format, but you can just modify the format in the script and use whichever format you please.

## Next steps

### Setup the input properly
The first thing I would modify in a real world setting, is the input, I would either change the streaming service so that it uses kafka-python as well, so I can read from it using the transform provided by beam, or look more deeply on a proper solution that doesn't involve a while true, or change the choice of framework altogether, and use something else like flink.

### Testing
The 2nd and very important thing to add is the tests, I would add unit and integration tests to the code, and then of course include them in the ci pipeline to avoid any unforseen behaviour once in production.

### Setup storage
Over a long period of time the size of our table is bound to become astronomical, so I'd rather store the data in cloud provided data warehouse like BigQuery, and use a partitioned table to keep the costs in check, daily partitioning for example, also, having a partitioned table will help us setup a data retention system easily to get rid of old/unused data.

### Setup monitoring
Monitoring is a very important part of the job, using something like prometheus or even datadog, I'd add performance and error metrics all over the codebase, and then build monitors on top of these metrics to keep watch over the pipeline and be able to intervene as soon as things start to go awry. 
