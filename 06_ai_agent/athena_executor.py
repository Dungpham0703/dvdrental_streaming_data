import os
import time
import boto3
from typing import Dict, Any, List, Tuple


class AthenaQueryExecutor:
    def __init__(
        self,
        region_name: str = None,
        database: str = "dvdrental_mart",
        output_location: str = None,
    ):
        self.region_name = region_name or os.getenv("AWS_REGION", "us-east-1")
        self.database = database or os.getenv("ATHENA_DATABASE", "dvdrental_mart")
        self.output_location = output_location or os.getenv(
            "ATHENA_S3_OUTPUT", "s3://dvdrental-athena-query-results/"
        )

        self.client = None
        try:
            self.client = boto3.client("athena", region_name=self.region_name)
        except Exception as e:
            print(f"[WARN] Boto3 Athena client init note: {e}")

    def execute_query(self, sql_query: str) -> Dict[str, Any]:
        start_time = time.time()

        if self.client and os.getenv("AWS_ACCESS_KEY_ID"):
            try:
                data, columns = self._run_live_athena_query(sql_query)
                execution_time = int((time.time() - start_time) * 1000)
                chart_type = self.detect_chart_type(data, columns)
                return {
                    "success": True,
                    "columns": columns,
                    "data": data,
                    "chartType": chart_type,
                    "execution_time_ms": execution_time,
                    "mode": "live_athena",
                }
            except Exception as e:
                print(f"[WARN] Live Athena query failed, using simulator fallback: {e}")

        data, columns = self._simulate_query_execution(sql_query)
        execution_time = int((time.time() - start_time) * 1000)
        chart_type = self.detect_chart_type(data, columns)

        return {
            "success": True,
            "columns": columns,
            "data": data,
            "chartType": chart_type,
            "execution_time_ms": execution_time,
            "mode": "simulator",
        }

    def execute_widgets(
        self, widgets_spec: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        results = []
        for w in widgets_spec:
            chart_type = w.get("chartType", "").lower()
            if chart_type in ["textarea", "text", "note", "alert"] or not w.get("sql"):
                results.append(
                    {
                        "title": w.get("title", ""),
                        "sql": "",
                        "chartType": chart_type or "textarea",
                        "columns": [],
                        "data": [{"content": w.get("content", w.get("title", ""))}],
                        "execution_time_ms": 0,
                        "mode": "static_text",
                    }
                )
                continue

            exec_res = self.execute_query(w["sql"])
            results.append(
                {
                    "title": w.get("title", "Analysis Widget"),
                    "sql": w["sql"],
                    "chartType": w.get("chartType") or exec_res["chartType"],
                    "columns": exec_res["columns"],
                    "data": exec_res["data"],
                    "execution_time_ms": exec_res["execution_time_ms"],
                    "mode": exec_res["mode"],
                }
            )
        return results

    def _run_live_athena_query(self, sql_query: str) -> Tuple[List[Dict], List[str]]:
        response = self.client.start_query_execution(
            QueryString=sql_query,
            QueryExecutionContext={"Database": self.database},
            ResultConfiguration={"OutputLocation": self.output_location},
        )
        execution_id = response["QueryExecutionId"]

        while True:
            status_resp = self.client.get_query_execution(QueryExecutionId=execution_id)
            state = status_resp["QueryExecution"]["Status"]["State"]

            if state == "SUCCEEDED":
                break
            elif state in ["FAILED", "CANCELLED"]:
                reason = status_resp["QueryExecution"]["Status"].get(
                    "StateChangeReason", "Query Failed"
                )
                raise RuntimeError(f"Athena Query {state}: {reason}")
            time.sleep(0.5)

        results_resp = self.client.get_query_results(QueryExecutionId=execution_id)
        rows = results_resp["ResultSet"]["Rows"]

        if not rows:
            return [], []

        columns = [col["VarCharValue"] for col in rows[0]["Data"]]
        data = []

        for row in rows[1:]:
            row_data = {}
            for idx, col_val in enumerate(row["Data"]):
                col_name = columns[idx]
                val = col_val.get("VarCharValue", None)
                if val is not None:
                    if val.isdigit():
                        val = int(val)
                    else:
                        try:
                            val = float(val)
                        except ValueError:
                            pass
                row_data[col_name] = val
            data.append(row_data)

        return data, columns

    def _simulate_query_execution(self, sql_query: str) -> Tuple[List[Dict], List[str]]:
        sql_lower = sql_query.lower()

        if "information_schema" in sql_lower or "table_name" in sql_lower:
            data = [
                {
                    "table_name": "fct_payment",
                    "table_type": "Fact Table (Bảng sự kiện)",
                    "description": "Thanh toán & doanh thu USD",
                },
                {
                    "table_name": "fct_rental",
                    "table_type": "Fact Table (Bảng sự kiện)",
                    "description": "Lượt thuê đĩa & thời gian thuê",
                },
                {
                    "table_name": "dim_customer",
                    "table_type": "Dimension Table (Bảng chiều - SCD2)",
                    "description": "Lịch sử thông tin khách hàng",
                },
                {
                    "table_name": "dim_film",
                    "table_type": "Dimension Table (Bảng chiều)",
                    "description": "Danh mục phim & thể loại",
                },
                {
                    "table_name": "dim_store",
                    "table_type": "Dimension Table (Bảng chiều)",
                    "description": "Chi nhánh & vị trí cửa hàng",
                },
                {
                    "table_name": "dim_staff",
                    "table_type": "Dimension Table (Bảng chiều)",
                    "description": "Nhân viên phục vụ",
                },
                {
                    "table_name": "dim_actor",
                    "table_type": "Dimension Table (Bảng chiều)",
                    "description": "Diễn viên đóng phim",
                },
            ]
            return data, ["table_name", "table_type", "description"]

        if "sum(amount)" in sql_lower and "group by" not in sql_lower:
            return [{"total_revenue": 67416.50, "total_transactions": 14596}], [
                "total_revenue",
                "total_transactions",
            ]

        if "month" in sql_lower or "payment_date" in sql_lower:
            data = [
                {"month": "2026-01", "revenue": 11450.25, "total_rentals": 2410},
                {"month": "2026-02", "revenue": 13820.50, "total_rentals": 2980},
                {"month": "2026-03", "revenue": 16940.00, "total_rentals": 3650},
                {"month": "2026-04", "revenue": 18200.75, "total_rentals": 3910},
                {"month": "2026-05", "revenue": 19500.00, "total_rentals": 4120},
                {"month": "2026-06", "revenue": 21800.50, "total_rentals": 4680},
            ]
            return data, ["month", "revenue", "total_rentals"]

        if "count(" in sql_lower and "group by" not in sql_lower:
            return [{"total_count": 16}], ["total_count"]

        if "category" in sql_lower:
            data = [
                {"category": "Sports", "total_revenue": 12845.50, "rental_count": 2840},
                {
                    "category": "Animation",
                    "total_revenue": 11620.00,
                    "rental_count": 2590,
                },
                {"category": "Action", "total_revenue": 10980.75, "rental_count": 2450},
                {"category": "Sci-Fi", "total_revenue": 9840.25, "rental_count": 2210},
                {"category": "Family", "total_revenue": 8920.00, "rental_count": 1980},
                {"category": "Drama", "total_revenue": 8210.50, "rental_count": 1840},
            ]
            return data, ["category", "total_revenue", "rental_count"]

        data = [
            {
                "customer_id": 1,
                "email": "MARY.SMITH@dvdrental.org",
                "total_spent": 118.68,
                "rentals": 30,
            },
            {
                "customer_id": 2,
                "email": "PATRICIA.JOHNSON@dvdrental.org",
                "total_spent": 128.73,
                "rentals": 31,
            },
            {
                "customer_id": 3,
                "email": "LINDA.WILLIAMS@dvdrental.org",
                "total_spent": 135.74,
                "rentals": 32,
            },
            {
                "customer_id": 4,
                "email": "BARBARA.JONES@dvdrental.org",
                "total_spent": 81.78,
                "rentals": 22,
            },
            {
                "customer_id": 5,
                "email": "ELIZABETH.BROWN@dvdrental.org",
                "total_spent": 144.62,
                "rentals": 38,
            },
        ]
        return data, ["customer_id", "email", "total_spent", "rentals"]

    @staticmethod
    def detect_chart_type(data: List[Dict[str, Any]], columns: List[str]) -> str:
        if not data or not columns:
            return "table"

        if len(data) == 1 and len(columns) <= 2:
            return "stat"

        first_row = data[0]
        string_cols = []
        numeric_cols = []
        date_cols = []

        date_keywords = ["month", "date", "year", "day", "time", "period"]

        for col in columns:
            val = first_row.get(col)
            if isinstance(val, (int, float)):
                numeric_cols.append(col)
            elif isinstance(val, str):
                string_cols.append(col)
                if any(kw in col.lower() for kw in date_keywords):
                    date_cols.append(col)

        if date_cols and numeric_cols:
            return "line"

        if string_cols and numeric_cols:
            if len(data) <= 6:
                return "pie"
            return "bar"

        return "table"
