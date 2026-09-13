from google.cloud import monitoring_v3
from google.cloud import compute_v1
import streamlit as st
from dotenv import load_dotenv
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import pandas as pd

tz = ZoneInfo("Asia/Ho_Chi_Minh")

load_dotenv(override=True)

def get_instance_names(project_id) -> dict:
    client = compute_v1.InstancesClient()
    request = compute_v1.AggregatedListInstancesRequest(project=project_id)
    mapping = {}
    for zone, response in client.aggregated_list(request=request):
        if response.instances:
            for instance in response.instances:
                mapping[str(instance.id)] = instance.name
    return mapping

# lấy metric value cho tất cả các instance trong project, trả về dataframe với các cột: timestamp, instance_id, hostname, percent_used
def get_metric_value(project_id, metric_type, start_time=datetime.now() - timedelta(hours=1), end_time=datetime.now(), **kwargs) -> pd.DataFrame:
     
     metric_type = str(metric_type).strip()
     instance_names = get_instance_names(project_id)
     project_name = f"projects/{project_id}"
     
     client = monitoring_v3.MetricServiceClient()
     interval = monitoring_v3.TimeInterval(
          {
          "start_time": start_time,
          "end_time": end_time
          }
     )

     if "cpu_time" in metric_type or "usage_time" in metric_type:
          per_series_aligner = monitoring_v3.Aggregation.Aligner.ALIGN_DELTA
     else:
          per_series_aligner = monitoring_v3.Aggregation.Aligner.ALIGN_MEAN

     aggregation = monitoring_v3.Aggregation(
          {
               "alignment_period": timedelta(minutes=1),
               "per_series_aligner": per_series_aligner,
          }    
     )
     
     metric_filter = f'metric.type = "{metric_type}" AND resource.type = "gce_instance"'
     if "memory/percent_used" in metric_type:
         metric_filter += ' AND metric.labels.state = "used"'
     elif "memory/bytes_used" in metric_type:
         metric_filter += ' AND metric.labels.state = "used"'
         
     results = client.list_time_series(
          request={
               "name": project_name,
               "filter": metric_filter,
               "interval": interval,
               "view": monitoring_v3.ListTimeSeriesRequest.TimeSeriesView.FULL,
               "aggregation": aggregation,
          }
     )
     
     instance_data = []
     for series in results:
          instance_id = series.resource.labels.get('instance_id')
          hostname = instance_names.get(instance_id, f"Unknown-{instance_id}")
          for point in series.points:
               typed_value = point.value
               value_kind = None

               raw_typed_value = getattr(typed_value, "_pb", None)
               if raw_typed_value is not None and hasattr(raw_typed_value, "WhichOneof"):
                    value_kind = raw_typed_value.WhichOneof("value")

               if value_kind is None:
                    if getattr(typed_value, "double_value", None) is not None:
                         metric_value = typed_value.double_value
                    else:
                         metric_value = typed_value.int64_value
               else:
                    metric_value = getattr(typed_value, value_kind)

               if metric_value is None:
                    continue
               metric_value = float(metric_value)
               timestamp = point.interval.end_time
               instance_data.append(
                    {
                         "timestamp": timestamp.astimezone(tz),
                         "instance_id": instance_id,
                         "hostname": hostname,
                         "metric_value": metric_value,
                    }
               )

     return pd.DataFrame(instance_data)