from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig, RenderConfig
from cosmos.profiles import PostgresUserPasswordProfileMapping
from cosmos.constants import LoadMode


# Configuration des chemins
DBT_PROJECT_PATH = Path("/usr/local/airflow/dags/nyc_yellow_taxi_dwh")
DBT_EXECUTABLE_PATH = "/usr/local/bin/dbt"

# Configuration du profil Postgres
profile_config = ProfileConfig(
    profile_name="nyc_yellow_taxi_dwh",
    target_name="dev",
    profile_mapping=PostgresUserPasswordProfileMapping(
        conn_id="postgres_dbt",  
        profile_args={
            "schema": "gold"
        }
    )
)

# Configuration de l'exécution
execution_config = ExecutionConfig(
    dbt_executable_path=DBT_EXECUTABLE_PATH,
)

# Configuration du rendu - IMPORTANT pour éviter les timeouts
render_config = RenderConfig(
    load_method=LoadMode.DBT_LS,  # Utilise dbt ls au lieu du parsing complet
    select=[]  # Sera défini par task group
)

# Arguments par défaut du DAG
default_args = {
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

# Création du DAG
with DAG(
    dag_id="gold_layer",
    default_args=default_args,
    description="Construction de la couche Gold: dimensions, facts et aggregates",
    start_date=datetime(2025, 11, 1),
    schedule_interval=None,
    catchup=False,
    tags=["gold", "dbt", "dwh"],
    max_active_runs=1,
) as dag:

    start = EmptyOperator(task_id="start")

    # Étape 1: Créer toutes les dimensions (sans tests)
    build_dimensions = DbtTaskGroup(
        group_id="build_dimensions",
        project_config=ProjectConfig(
            dbt_project_path=DBT_PROJECT_PATH,
        ),
        profile_config=profile_config,
        execution_config=execution_config,
        render_config=RenderConfig(
            load_method=LoadMode.DBT_LS,
            select=["dim_vendor", "dim_datetime", "dim_location", "dim_rate_code", 
                   "dim_payment_type", "dim_store_forward", "dim_trip_category"],
            test_behavior="none",  # Désactiver les tests pour les dimensions
        ),
        operator_args={
            "install_deps": True,
        },
    )

    # Étape 2: Créer la table de faits (sans tests)
    build_facts = DbtTaskGroup(
        group_id="build_facts",
        project_config=ProjectConfig(
            dbt_project_path=DBT_PROJECT_PATH,
        ),
        profile_config=profile_config,
        execution_config=execution_config,
        render_config=RenderConfig(
            load_method=LoadMode.DBT_LS,
            select=["fact_taxi_trips"],
            test_behavior="none",  # Désactiver les tests pour le moment
        ),
        operator_args={
            "install_deps": False,
        },
    )

    # Étape 3: Créer les aggregates (sans tests)
    build_aggregates = DbtTaskGroup(
        group_id="build_aggregates",
        project_config=ProjectConfig(
            dbt_project_path=DBT_PROJECT_PATH,
        ),
        profile_config=profile_config,
        execution_config=execution_config,
        render_config=RenderConfig(
            load_method=LoadMode.DBT_LS,
            select=["tag:aggregate"],
        ),
        operator_args={
            "install_deps": False,
        },
    )

    # Étape 4: Exécuter TOUS les tests après la construction complète
    run_tests = DbtTaskGroup(
        group_id="run_all_tests",
        project_config=ProjectConfig(
            dbt_project_path=DBT_PROJECT_PATH,
        ),
        profile_config=profile_config,
        execution_config=execution_config,
        render_config=RenderConfig(
            load_method=LoadMode.DBT_LS,
            select=["dim_vendor", "dim_datetime", "dim_location", "dim_rate_code",
                   "dim_payment_type", "dim_store_forward", "dim_trip_category",
                   "fact_taxi_trips", "tag:aggregate"],
            test_behavior="after_all",  # Exécuter tous les tests à la fin
        ),
        operator_args={
            "install_deps": False,
        },
    )
    end = EmptyOperator(task_id="end")

    # Définir l'ordre d'exécution
    start >> build_dimensions >> build_facts >> build_aggregates >> run_tests >> end