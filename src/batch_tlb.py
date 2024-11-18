import json
from collections import defaultdict
from datetime import datetime

"""
This is a mock of TLB. Currently TLB cannot accept multiple batch input files from different sources.
For demo purpose, we have coded up the metric logic.
"""
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

        # Group user experience events by clientId
        grouped_events = defaultdict(list)
        for event in user_exp_data:
            grouped_events[event["clientId"]].append(event)

        # Compute metrics for each client
        for client_id, events in grouped_events.items():
            # Sort events by timestamp
            events.sort(key=lambda e: datetime.fromisoformat(e["timestamp"]))

            # Track start and end events for page view time
            last_start_time = None

            for event in events:
                event_type = event["eventType"]
                timestamp = datetime.fromisoformat(event["timestamp"])

                if event_type == "page_view_start":
                    last_start_time = timestamp
                elif event_type == "page_view_end" and last_start_time:
                    page_view_time = (timestamp - last_start_time).total_seconds()
                    client_metrics[client_id]["page_view_time"] += page_view_time
                    last_start_time = None  # Reset after processing the pair

            # Process logs associated with the client
            for event in events:
                trace_id = event.get("traceId")
                if trace_id and trace_id in trace_to_spans:
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
