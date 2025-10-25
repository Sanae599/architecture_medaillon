from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="ingest_kg",
    catchup=False,
    description="pipeline medaillon",
    tags=["medaillon", "neo4j", "bash"],
) as dag:

    #raw : génération des données
    seed = BashOperator(
        task_id="seed_data",
        bash_command="python3 /opt/airflow/scripts/generate_sample_data.py"
    )

    #bronze : conversion en parquet
    bronze = BashOperator(
        task_id="bronze_parquet",
        bash_command="cd /opt/airflow && python3 /opt/airflow/scripts/to_parquet.py"    
    )

    #silver : traitement des nodes
    silver_nodes = BashOperator(
        task_id="silver_nodes",
        bash_command="cd /opt/airflow && python3 /opt/airflow/scripts/nodes_to_silver.py"
    )

    #silver : partitionnement des edges
    silver_partition = BashOperator(
        task_id="silver_partition",
        bash_command="cd /opt/airflow && python3 /opt/airflow/scripts/partition_edges.py"
    )

    #silver : contrôle qualité
    silver_quality = BashOperator(
        task_id="silver_quality_check",
        bash_command="cd /opt/airflow && python3 /opt/airflow/quality/gx_checkpoint.py"
    )

    #gold : conversion vers csv neo4j
    gold = BashOperator(
        task_id="gold_convert",
        bash_command="cd /opt/airflow && python3 /opt/airflow/scripts/convert_toneo4j_csv.py"
    )

    #import final vers neo4j
    import_to_neo4j = BashOperator(
    task_id="import_to_neo4j",
    bash_command="bash /opt/airflow/scripts/import_goldtoneo4j_cypher_bash.sh ",
    do_xcom_push=False
    )

    seed >> bronze >> silver_nodes >> silver_partition >> silver_quality >> gold >> import_to_neo4j
