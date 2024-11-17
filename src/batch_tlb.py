import json
from collections import defaultdict
from datetime import datetime

class BatchTLB:
    def __init__(self, user_exp_file, trace_file, log_file, output_file):
        self.user_exp_file = user_exp_file
        self.trace_file = trace_file
        self.log_file = log_file
        self.output_file = output_file

    def load_data(self, file_path):
        with open(file_path, 'r') as f:
            return json.load(f)

    def compute_metrics(self):
        # Load all data sources
        user_exp_data = self.load_data(self.user_exp_file)
        trace_data = self.load_data(self.trace_file)
        log_data = self.load_data(self.log_file)

        # Index trace data by traceId -> list of spanIds
        trace_to_spans = {
            trace["traceId"]: [span["spanId"] for span in trace["spans"]]
            for trace in trace_data
        }

        # Index logs by spanId
        logs_by_span = defaultdict(list)
        for log in log_data:
            logs_by_span[log["spanId"]].append(log)

        # Initialize result storage
        client_metrics = defaultdict(lambda: {"page_view_time": 0, "retry_count": 0, "timeout_count": 0, "error_count": 0})

        # Compute metrics for each user event
        for event in user_exp_data:
            client_id = event["clientId"]
            trace_id = event["traceId"]
            event_type = event["eventType"]

            # Process page view time
            if event_type == "page_view_start":
                start_time = datetime.fromisoformat(event["timestamp"])
                end_event = next(
                    (e for e in user_exp_data if e["clientId"] == client_id and e["traceId"] == trace_id and e["eventType"] == "page_view_end"),
                    None
                )
                if end_event:
                    end_time = datetime.fromisoformat(end_event["timestamp"])
                    page_view_time = (end_time - start_time).total_seconds()
                    client_metrics[client_id]["page_view_time"] += page_view_time

            # Process logs associated with the trace
            if trace_id in trace_to_spans:
                span_ids = trace_to_spans[trace_id]
                for span_id in span_ids:
                    if span_id in logs_by_span:
                        for log in logs_by_span[span_id]:
                            if log["eventType"] == "RETRY":
                                client_metrics[client_id]["retry_count"] += 1
                            elif log["eventType"] == "TIMEOUT":
                                client_metrics[client_id]["timeout_count"] += 1
                            elif log["eventType"] == "ERROR":
                                client_metrics[client_id]["error_count"] += 1

        # Write the result to the output file
        with open(self.output_file, 'w') as f:
            json.dump(client_metrics, f, indent=2)

        print(f"Metrics computation completed. Output written to {self.output_file}")
