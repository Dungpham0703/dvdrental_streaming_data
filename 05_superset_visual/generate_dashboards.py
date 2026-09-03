import os
import yaml
import json
import uuid
import shutil
from pathlib import Path

OUTPUT_DIR = "dashboards_as_code"
DB_NAME = "dvdrental_lakehouse"
SCHEMA = "dvdrental_mart"

CUSTOM_CSS = """
/* Enterprise Premium Dark Theme */
body, .dashboard-content {
    background-color: #0f172a !important;
    font-family: 'Inter', system-ui, -apple-system, sans-serif !important;
}

/* Glassmorphism Cards */
.grid-card {
    border-radius: 12px !important;
    background: rgba(30, 41, 59, 0.6) !important;
    backdrop-filter: blur(12px) !important;
    -webkit-backdrop-filter: blur(12px) !important;
    border: 1px solid rgba(255, 255, 255, 0.05) !important;
    box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.1), 0 2px 4px -1px rgba(0, 0, 0, 0.06) !important;
    transition: all 0.3s ease !important;
}

.grid-card:hover {
    border-color: rgba(99, 102, 241, 0.3) !important;
    box-shadow: 0 10px 15px -3px rgba(0, 0, 0, 0.2) !important;
    transform: translateY(-2px) !important;
}

/* Header Text & Title Styling */
.header-title, .chart-header {
    font-weight: 600 !important;
    letter-spacing: -0.01em !important;
    color: #f8fafc !important;
}

/* Big Number Cards Styling */
.big-number-total {
    font-weight: 800 !important;
    background: linear-gradient(135deg, #818cf8 0%, #c084fc 100%) !important;
    -webkit-background-clip: text !important;
    -webkit-text-fill-color: transparent !important;
}

/* Tabs Styling */
.nav-tabs {
    border-bottom: 2px solid rgba(255,255,255,0.1) !important;
    margin-bottom: 20px !important;
}
.nav-tabs > li > a {
    color: #94a3b8 !important;
    font-weight: 600 !important;
    border: none !important;
    padding: 12px 24px !important;
    border-radius: 8px 8px 0 0 !important;
}
.nav-tabs > li.active > a, .nav-tabs > li.active > a:hover {
    color: #f8fafc !important;
    background-color: rgba(99, 102, 241, 0.2) !important;
    border-bottom: 2px solid #818cf8 !important;
}
"""

def generate_uuid(name):
    return str(uuid.uuid5(uuid.NAMESPACE_DNS, name))

def create_dir_if_not_exists(path):
    Path(path).mkdir(parents=True, exist_ok=True)

def read_env():
    env_vars = {}
    env_path = ".env"
    if os.path.exists(env_path):
        with open(env_path, "r") as f:
            for line in f:
                if line.strip() and not line.startswith("#"):
                    key, value = line.strip().split("=", 1)
                    env_vars[key] = value
    return env_vars

def generate_database(env_vars):
    access_key = env_vars.get("AWS_ACCESS_KEY_ID")
    secret_key = env_vars.get("AWS_SECRET_ACCESS_KEY")
    region = env_vars.get("AWS_REGION")
    bucket = env_vars.get("S3_BUCKET_NAME")
    
    return {
        "database_name": DB_NAME,
        "sqlalchemy_uri": f"awsathena+rest://{access_key}:{secret_key}@athena.{region}.amazonaws.com/{SCHEMA}?s3_staging_dir=s3://{bucket}/athena-results/",
        "cache_timeout": None,
        "expose_in_sqllab": True,
        "allow_run_async": False,
        "allow_ctas": False,
        "allow_cvas": False,
        "allow_file_upload": False,
        "extra": {},
        "uuid": generate_uuid(f"db_{DB_NAME}"),
        "version": "1.0.0"
    }

def generate_dataset(name, sql, cols=None, mets=None):
    if cols is None: cols = []
    if mets is None: mets = []
    
    col_objs = [{"column_name": c, "type": "STRING", "is_dttm": ("date" in c or "day" in c or "month" in c)} for c in cols]
    met_objs = [{"metric_name": m, "expression": f"MAX({m})"} for m in mets]
    
    return {
        "table_name": name,
        "schema": SCHEMA,
        "sql": sql.strip(),
        "filter_select_enabled": True,
        "uuid": generate_uuid(f"dataset_{name}"),
        "metrics": met_objs,
        "columns": col_objs,
        "database_uuid": generate_uuid(f"db_{DB_NAME}"),
        "version": "1.0.0"
    }

def generate_chart(name, viz_type, dataset_name, params_dict):
    params_dict["viz_type"] = viz_type
    params_dict["datasource"] = f"{generate_uuid(f'dataset_{dataset_name}')}__table"
    return {
        "slice_name": name,
        "viz_type": viz_type,
        "params": params_dict,
        "uuid": generate_uuid(f"chart_{name}"),
        "dataset_uuid": generate_uuid(f"dataset_{dataset_name}"),
        "version": "1.0.0"
    }

class LayoutBuilder:
    def __init__(self, title, subtitle):
        self.pos = {
            "DASHBOARD_VERSION_KEY": "v2",
            "ROOT_ID": {"type": "ROOT", "id": "ROOT_ID", "children": ["GRID_ID"]},
            "GRID_ID": {"type": "GRID", "id": "GRID_ID", "children": [], "parents": ["ROOT_ID"]},
            "HEADER_ID": {"id": "HEADER_ID", "type": "HEADER", "meta": {"text": title}}
        }
        self.counter = 0
        self.add_markdown_header(title, subtitle)
        
    def add_markdown_header(self, title, subtitle):
        row_id = f"ROW_{self.counter}"; self.counter += 1
        md_id = f"MARKDOWN_{self.counter}"; self.counter += 1
        self.pos["GRID_ID"]["children"].append(row_id)
        self.pos[row_id] = {"type": "ROW", "id": row_id, "children": [md_id], "parents": ["ROOT_ID", "GRID_ID"]}
        self.pos[md_id] = {
            "type": "MARKDOWN", "id": md_id, "children": [], "parents": ["ROOT_ID", "GRID_ID", row_id],
            "meta": {
                "code": f"<div style='padding: 16px 24px; background: linear-gradient(135deg, rgba(30,27,75,0.8) 0%, rgba(49,46,129,0.8) 100%); border-radius: 12px; border: 1px solid rgba(99,102,241,0.2); margin-bottom: 8px;'><h1 style='margin:0; color:#ffffff; font-size:26px; font-weight:800; letter-spacing: -0.5px;'>{title}</h1><p style='margin:6px 0 0 0; color:#a5b4fc; font-size:14px; font-weight: 500;'>{subtitle}</p></div>",
                "width": 12, "height": 18
            }
        }

    def add_divider(self, text, parent_id="GRID_ID", parents_path=None):
        if parents_path is None: parents_path = ["ROOT_ID"]
        row_id = f"ROW_{self.counter}"; self.counter += 1
        md_id = f"MARKDOWN_{self.counter}"; self.counter += 1
        self.pos[parent_id]["children"].append(row_id)
        self.pos[row_id] = {"type": "ROW", "id": row_id, "children": [md_id], "parents": parents_path + [parent_id]}
        self.pos[md_id] = {
            "type": "MARKDOWN", "id": md_id, "children": [], "parents": parents_path + [parent_id, row_id],
            "meta": {
                "code": f"<div style='border-bottom: 2px solid rgba(99,102,241,0.3); margin-top: 16px; margin-bottom: 8px;'><h3 style='color: #e2e8f0; font-size: 18px; margin:0 0 8px 0; font-weight: 600;'>{text}</h3></div>",
                "width": 12, "height": 12
            }
        }
        
    def add_tabs(self, tab_names, parent_id="GRID_ID", parents_path=None):
        if parents_path is None: parents_path = ["ROOT_ID"]
        tabs_id = f"TABS_{self.counter}"; self.counter += 1
        self.pos[parent_id]["children"].append(tabs_id)
        self.pos[tabs_id] = {"type": "TABS", "id": tabs_id, "children": [], "parents": parents_path + [parent_id]}
        
        tab_ids = []
        for name in tab_names:
            tab_id = f"TAB_{self.counter}"; self.counter += 1
            self.pos[tabs_id]["children"].append(tab_id)
            self.pos[tab_id] = {"type": "TAB", "id": tab_id, "children": [], "parents": parents_path + [parent_id, tabs_id], "meta": {"text": name}}
            tab_ids.append(tab_id)
            
        return tab_ids, tabs_id
        
    def add_row(self, charts, parent_id="GRID_ID", parents_path=None):
        if parents_path is None: parents_path = ["ROOT_ID"]
        row_id = f"ROW_{self.counter}"; self.counter += 1
        self.pos[parent_id]["children"].append(row_id)
        self.pos[row_id] = {"type": "ROW", "id": row_id, "children": [], "parents": parents_path + [parent_id], "meta": {"background": "BACKGROUND_TRANSPARENT"}}
        
        for ch, width, height in charts:
            ch_id = f"CHART_{self.counter}"; self.counter += 1
            self.pos[row_id]["children"].append(ch_id)
            self.pos[ch_id] = {
                "type": "CHART", "id": ch_id, "children": [], "parents": parents_path + [parent_id, row_id],
                "meta": {"chartId": self.counter, "sliceName": ch["slice_name"], "uuid": ch["uuid"], "width": width, "height": height}
            }
            
    def get_position(self):
        return self.pos

def generate_dashboard_advanced(title, slug, layout_builder):
    return {
        "dashboard_title": title,
        "css": CUSTOM_CSS,
        "slug": slug,
        "uuid": generate_uuid(f"dash_{slug}"),
        "position": layout_builder.get_position(),
        "metadata": {
            "refresh_frequency": 300, 
            "color_scheme": "googleCategory10c",
            "label_colors": {
                'High Spender': '#2E86AB',
                'Medium Spender': '#5BC0EB',
                'Low Spender': '#F6F5AE',
                'New Customer': '#F06449',
                'VIP': '#2E86AB',
                'Gold': '#5BC0EB',
                'Silver': '#F6F5AE',
                'Bronze': '#F06449',
                'G': '#368F8B',
                'PG': '#5BC0EB',
                'PG-13': '#F6CD61',
                'R': '#FE8A71',
                'NC-17': '#F24236',
                'True': '#368F8B',
                'False': '#F24236',
                'Active': '#368F8B',
                'Inactive': '#F24236'
            }
        },
        "version": "1.0.0"
    }

def save_yaml(filepath, data):
    with open(filepath, "w", encoding='utf-8') as f:
        yaml.dump(data, f, default_flow_style=False, sort_keys=False, allow_unicode=True)

def main():
    if os.path.exists(OUTPUT_DIR): shutil.rmtree(OUTPUT_DIR)
    for p in ["databases", f"datasets/{DB_NAME}", "charts", "dashboards"]: create_dir_if_not_exists(f"{OUTPUT_DIR}/{p}")

    env_vars = read_env()
    save_yaml(f"{OUTPUT_DIR}/metadata.yaml", {"version": "1.0.0", "type": "Dashboard", "timestamp": "2024-01-01T00:00:00Z"})
    save_yaml(f"{OUTPUT_DIR}/databases/{DB_NAME}.yaml", generate_database(env_vars))
    
    datasets = []
    charts = []
    dashboards = []
    
    # ---------------------------------------------------------
    # 1. Revenue & Payment Analytics (Tabs & Columns)
    # ---------------------------------------------------------
    ds_1_1 = generate_dataset("ds_rev_overview", "SELECT SUM(amount) as total_revenue FROM fct_payment", cols=["total_revenue"], mets=["total_revenue"]); datasets.append(ds_1_1)
    ch_1_1 = generate_chart("Total Revenue & Payments", "big_number_total", ds_1_1["table_name"], {"metric": "total_revenue"}); charts.append(ch_1_1)
    
    ds_1_2 = generate_dataset("ds_rev_trend", "SELECT date_trunc('month', cast(payment_date as date)) as payment_month, SUM(amount) as monthly_revenue FROM fct_payment GROUP BY 1", cols=["payment_month", "monthly_revenue"], mets=["monthly_revenue"]); datasets.append(ds_1_2)
    ch_1_2 = generate_chart("Monthly Revenue Trend", "echarts_timeseries_line", ds_1_2["table_name"], {"x_axis": "payment_month", "metrics": ["monthly_revenue"]}); charts.append(ch_1_2)
    
    ds_1_3 = generate_dataset("ds_rev_store", "SELECT s.city_name || ', ' || s.country_name as store_location, SUM(p.amount) as total_revenue FROM fct_payment p JOIN dim_store s ON p.store_id = s.store_id GROUP BY 1", cols=["store_location", "total_revenue"], mets=["total_revenue"]); datasets.append(ds_1_3)
    ch_1_3 = generate_chart("Revenue by Store Location", "pie", ds_1_3["table_name"], {"groupby": ["store_location"], "metric": "total_revenue"}); charts.append(ch_1_3)
    
    ds_1_4 = generate_dataset("ds_rev_category", "SELECT cat as category, SUM(p.amount) as total_revenue FROM fct_payment p JOIN dim_film f ON p.film_id = f.film_id CROSS JOIN UNNEST(SPLIT(f.category_name, ',')) as t(cat) GROUP BY 1", cols=["category", "total_revenue"], mets=["total_revenue"]); datasets.append(ds_1_4)
    ch_1_4 = generate_chart("Revenue by Genre", "pie", ds_1_4["table_name"], {"groupby": ["category"], "metric": "total_revenue", "innerRadius": 45, "show_labels": True}); charts.append(ch_1_4)
    
    ds_1_5 = generate_dataset("ds_top10_film", "SELECT f.title, SUM(p.amount) as total_revenue FROM fct_payment p JOIN dim_film f ON p.film_id = f.film_id GROUP BY 1 ORDER BY total_revenue DESC LIMIT 10", cols=["title", "total_revenue"], mets=["total_revenue"]); datasets.append(ds_1_5)
    ch_1_5 = generate_chart("Top 10 Films", "table", ds_1_5["table_name"], {"groupby": ["title"], "metrics": ["total_revenue"]}); charts.append(ch_1_5)

    l1 = LayoutBuilder("Revenue Analytics", "Executive Financial Performance Overview")
    l1.add_divider("Key Performance Indicator")
    l1.add_row([(ch_1_1, 12, 18)])
    
    tab_ids, tabs_parent = l1.add_tabs(["Market Trends", "Product & Category"])
    # Tab 1
    l1.add_row([(ch_1_2, 8, 60), (ch_1_3, 4, 60)], parent_id=tab_ids[0], parents_path=["ROOT_ID", "GRID_ID", tabs_parent])
    # Tab 2
    l1.add_row([(ch_1_4, 4, 60), (ch_1_5, 8, 60)], parent_id=tab_ids[1], parents_path=["ROOT_ID", "GRID_ID", tabs_parent])
    dashboards.append(generate_dashboard_advanced("Revenue Analytics", "revenue-analytics", l1))
    
    # ---------------------------------------------------------
    # 2. Rental Operations
    # ---------------------------------------------------------
    ds_2_1 = generate_dataset("ds_rental_kpi", "SELECT COUNT(*) as total_rentals, AVG(rental_duration_days) as avg_duration FROM fct_rental", cols=["total_rentals", "avg_duration"], mets=["total_rentals"]); datasets.append(ds_2_1)
    ch_2_1 = generate_chart("Total Rentals Overview", "big_number_total", ds_2_1["table_name"], {"metric": "total_rentals", "subheader": "Total Volume"}); charts.append(ch_2_1)
    
    ds_2_2 = generate_dataset("ds_rental_trend", "SELECT date_trunc('day', cast(rental_date as date)) as rental_day, COUNT(*) as rental_count FROM fct_rental GROUP BY 1", cols=["rental_day", "rental_count"], mets=["rental_count"]); datasets.append(ds_2_2)
    ch_2_2 = generate_chart("Daily Rental Volume Trend", "echarts_timeseries_line", ds_2_2["table_name"], {"x_axis": "rental_day", "metrics": ["rental_count"]}); charts.append(ch_2_2)
    
    ds_2_3 = generate_dataset("ds_rental_dist", "SELECT rental_duration_days, COUNT(*) as rental_count FROM fct_rental WHERE rental_duration_days IS NOT NULL GROUP BY 1", cols=["rental_duration_days", "rental_count"], mets=["rental_count"]); datasets.append(ds_2_3)
    ch_2_3 = generate_chart("Rental Duration Distribution", "dist_bar", ds_2_3["table_name"], {"groupby": ["rental_duration_days"], "metrics": ["rental_count"]}); charts.append(ch_2_3)
    
    ds_2_4 = generate_dataset("ds_overdue", "SELECT c.first_name || ' ' || c.last_name as customer, f.title, r.rental_duration_days - f.rental_duration as overdue_days FROM fct_rental r JOIN dim_customer c ON r.customer_id = c.customer_id JOIN dim_film f ON r.film_id = f.film_id WHERE r.rental_duration_days > f.rental_duration ORDER BY overdue_days DESC LIMIT 20", cols=["customer", "title", "overdue_days"], mets=["overdue_days"]); datasets.append(ds_2_4)
    ch_2_4 = generate_chart("Top Overdue Rentals List", "table", ds_2_4["table_name"], {"groupby": ["customer", "title"], "metrics": ["overdue_days"]}); charts.append(ch_2_4)
    
    l2 = LayoutBuilder("Rental Operations", "Monitoring Film Returns, Trends & Overdue Rentals")
    l2.add_divider("Daily Operations")
    l2.add_row([(ch_2_1, 12, 18)])
    l2.add_row([(ch_2_2, 12, 50)])
    l2.add_divider("Return Metrics & Overdue")
    l2.add_row([(ch_2_3, 4, 50), (ch_2_4, 8, 50)])
    dashboards.append(generate_dashboard_advanced("Rental Operations", "rental-operations", l2))

    # ---------------------------------------------------------
    # 3. Customer Intelligence
    # ---------------------------------------------------------
    ds_3_1 = generate_dataset("ds_top_customers", "SELECT c.first_name || ' ' || c.last_name as name, c.email, SUM(p.amount) as total_spent FROM dim_customer c JOIN fct_payment p ON c.customer_id = p.customer_id WHERE c.is_current = true GROUP BY 1, 2 ORDER BY total_spent DESC LIMIT 20", cols=["name", "email", "total_spent"], mets=["total_spent"]); datasets.append(ds_3_1)
    ch_3_1 = generate_chart("Top 20 VIP Customers", "table", ds_3_1["table_name"], {"groupby": ["name", "email"], "metrics": ["total_spent"]}); charts.append(ch_3_1)
    
    ds_3_2 = generate_dataset("ds_cust_segments", "WITH cust_spend AS (SELECT customer_id, SUM(amount) as total FROM fct_payment GROUP BY 1) SELECT CASE WHEN total >= 150 THEN 'VIP' WHEN total >= 100 THEN 'Gold' WHEN total >= 50 THEN 'Silver' ELSE 'Bronze' END as segment, COUNT(*) as cust_count FROM cust_spend GROUP BY 1", cols=["segment", "cust_count"], mets=["cust_count"]); datasets.append(ds_3_2)
    ch_3_2 = generate_chart("Customer Spend Segments", "pie", ds_3_2["table_name"], {"groupby": ["segment"], "metric": "cust_count", "innerRadius": 45}); charts.append(ch_3_2)
    
    ds_3_3 = generate_dataset("ds_active_cust", "SELECT CASE WHEN is_active THEN 'Active' ELSE 'Inactive' END as status, COUNT(*) as count FROM dim_customer WHERE is_current=true GROUP BY 1", cols=["status", "count"], mets=["count"]); datasets.append(ds_3_3)
    ch_3_3 = generate_chart("Active vs Inactive", "pie", ds_3_3["table_name"], {"groupby": ["status"], "metric": "count"}); charts.append(ch_3_3)

    l3 = LayoutBuilder("Customer Intelligence", "Customer Segmentation & Lifetime Value Analysis")
    l3.add_divider("Demographics & Segments")
    l3.add_row([(ch_3_3, 4, 40), (ch_3_2, 8, 40)])
    l3.add_divider("VIP Roster")
    l3.add_row([(ch_3_1, 12, 60)])
    dashboards.append(generate_dashboard_advanced("Customer Intelligence", "customer-intelligence", l3))

    # ---------------------------------------------------------
    # 4. Film Catalog
    # ---------------------------------------------------------
    ds_4_1 = generate_dataset("ds_film_stats", "SELECT COUNT(*) as total_films, AVG(rental_rate) as avg_rate FROM dim_film", cols=["total_films", "avg_rate"], mets=["total_films"]); datasets.append(ds_4_1)
    ch_4_1 = generate_chart("Catalog Overview", "big_number_total", ds_4_1["table_name"], {"metric": "total_films"}); charts.append(ch_4_1)
    
    ds_4_2 = generate_dataset("ds_film_rating", "SELECT rating, COUNT(*) as count FROM dim_film GROUP BY 1", cols=["rating", "count"], mets=["count"]); datasets.append(ds_4_2)
    ch_4_2 = generate_chart("Films Breakdown by MPAA Rating", "pie", ds_4_2["table_name"], {"groupby": ["rating"], "metric": "count"}); charts.append(ch_4_2)
    
    ds_4_3 = generate_dataset("ds_top_actors", "SELECT first_name || ' ' || last_name as name, film_count FROM dim_actor ORDER BY film_count DESC LIMIT 15", cols=["name", "film_count"], mets=["film_count"]); datasets.append(ds_4_3)
    ch_4_3 = generate_chart("Top 15 Most Cast Actors", "dist_bar", ds_4_3["table_name"], {"groupby": ["name"], "metrics": ["film_count"]}); charts.append(ch_4_3)

    l4 = LayoutBuilder("Film Catalog & Inventory", "Film Inventory, Rating Distributions & Actor Statistics")
    l4.add_divider("Library Stats")
    l4.add_row([(ch_4_1, 4, 30), (ch_4_2, 8, 30)])
    l4.add_divider("Casting Highlights")
    l4.add_row([(ch_4_3, 12, 50)])
    dashboards.append(generate_dashboard_advanced("Film Catalog & Inventory", "film-catalog", l4))
    
    # Generate files
    for ds in datasets: save_yaml(f"{OUTPUT_DIR}/datasets/{DB_NAME}/{ds['table_name']}.yaml", ds)
    for ch in charts: save_yaml(f"{OUTPUT_DIR}/charts/{ch['slice_name'].replace(' ', '_').replace('/', '_')}.yaml", ch)
    for d in dashboards: save_yaml(f"{OUTPUT_DIR}/dashboards/{d['slug']}.yaml", d)

if __name__ == "__main__":
    main()
