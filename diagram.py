from diagrams import Diagram, Cluster, Edge
from diagrams.onprem.workflow import Airflow
from diagrams.onprem.queue import Kafka
from diagrams.onprem.database import Cassandra, Postgresql
from diagrams.onprem.analytics import Spark
from diagrams.onprem.inmemory import Redis
from diagrams.onprem.client import User, Client # Now importing Client for the UIs
from diagrams.programming.language import Python
from diagrams.generic.compute import Rack

with Diagram("ELT Pipeline Detailed Architecture", show=False, direction="TB") as diag:
    user = User("Random User API")
   
    with Cluster("Monitoring & UIs"):
        # Changed from Python to Client to get a more generic UI icon
        flower = Client("Flower UI")
        kafka_ui = Client("Kafka UI")
   
    with Cluster("ELT Pipeline"):
        with Cluster("Orchestration (Airflow)"):
            airflow_webserver = Airflow("Webserver")
            airflow_scheduler = Airflow("Scheduler")
            airflow_worker = Airflow("Worker")
           
            with Cluster("Airflow Backend"):
                postgres = Postgresql("PostgreSQL")
                redis = Redis("Redis")
           
            airflow_services = [airflow_webserver, airflow_scheduler, airflow_worker]
            airflow_services >> Edge(label="metadata") >> postgres
            airflow_worker >> Edge(label="results/queue") >> redis
           
        with Cluster("Streaming Platform (Kafka)"):
            kafka = Kafka("Kafka Broker")
            # Removed the Zookeeper node as requested
           
        with Cluster("Data Processing (Spark)"):
            spark_master = Spark("Spark Master")
            # Changed to two separate Spark worker nodes
            spark_worker1 = Spark("Spark Worker 1")
            spark_worker2 = Spark("Spark Worker 2")
            spark_workers = [spark_worker1, spark_worker2]

            spark_submit = Python("Spark Submit Job")
            spark_master - spark_workers
           
        with Cluster("Data Store (Cassandra)"):
            cassandra = Cassandra("Cassandra")
            # Removed the cassandra_init node as requested
   
    # Data Flow
    user >> airflow_worker
    airflow_worker >> kafka
    kafka >> spark_submit
    spark_submit >> spark_master
    spark_workers >> cassandra
   
    # Monitoring Connections
    airflow_worker >> flower
    kafka >> kafka_ui
