import json

with open("semantic_dictionary.json", "r", encoding="utf-8") as f:
    data = json.load(f)

for table_name, table_info in data.get("tables", {}).items():
    for col_name, col_info in table_info.get("columns", {}).items():
        if col_info.get("data_type") in ["TIMESTAMP", "DATE"]:
            col_info["data_type"] = "STRING (ISO 8601)"

with open("semantic_dictionary.json", "w", encoding="utf-8") as f:
    json.dump(data, f, ensure_ascii=False, indent=2)
