from mtaairflowlib import DAG
from datetime import timedelta, datetime
from mtaairflowlib.spark.taskgroups import SparkBatchTaskGroup
from mtaairflowlib.utils import path_in_dags
from mtaairflowlib.utils.models import Zone

with DAG(
   dag_id="HERE_road_dim_dag",
   start_date=datetime(2025, 8, 27),
   schedule=None,  # manual
   catchup=False,
   default_args={
       "owner": "airflow",
       "email": ["marc.zitelli@mtahq.org", "julia.lynn@nyct.com"],
   },
) as dag:

   load_dim = SparkBatchTaskGroup(  # load spark job, write to table
       group_id="dim-here-road-links",
       script=path_in_dags("here", "spark", "dim_here_road_links.py"),
       script_kwargs={
           "source_zone": Zone("core", "blob"),
           "target_zone": Zone("core", "delta"),
           "target": "dim_here_road_links",
           "length_crs": 6539,
       },
       dag=dag,
   )
