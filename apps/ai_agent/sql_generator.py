import json
import os
import re
import sys
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

CURRENT_DIR = Path(__file__).resolve().parent
if str(CURRENT_DIR) not in sys.path:
    sys.path.insert(0, str(CURRENT_DIR))

from rag_retriever import RAGRetriever

load_dotenv()
load_dotenv(Path(__file__).resolve().parent / ".env")

SEMANTIC_FILE = Path(__file__).resolve().parent / "semantic_dictionary.json"

AVAILABLE_MODELS = [
    "gemini-1.5-pro",
    "gemini-1.5-pro-latest",
    "gemini-2.0-pro-exp-0205",
    "gemini-2.0-flash",
    "gemini-1.5-flash",
]


class TextToSQLGenerator:
    def __init__(self):
        self.rag = RAGRetriever()
        self.client = None
        self.api_key = os.getenv("AI_API_KEY")

        if self.api_key and not self.api_key.startswith("http"):
            try:
                from google import genai

                self.client = genai.Client(api_key=self.api_key)
            except Exception as e:
                print(f"[WARN] Gemini Client init note: {e}")

    def build_system_prompt(self, context: str) -> str:
        prompt = f"""Bạn là một Chuyên gia Phân tích Dữ liệu Data Analyst cao cấp chuyên làm việc với AWS Athena (Presto SQL).
Nhiệm vụ của bạn là nhận câu hỏi từ người dùng và trả về kết quả cấu trúc JSON chứa các câu lệnh SQL Presto phù hợp.

=== VAI TRÒ VÀ TƯ DUY DATA ANALYST (MVIZ BEST PRACTICES) ===
- Trình bày sự thật (matter-of-fact): Chỉ mô tả dữ liệu hiển thị gì, tuyệt đối không suy đoán nguyên nhân hay đưa ra phán xét chủ quan.
- Ví dụ SAI: "Tăng trưởng mạnh mẽ ở Q2", "Sự sụt giảm đáng lo ngại".
- Ví dụ ĐÚNG: "Doanh thu Q2 đạt $176M, tăng từ $64M ở Q1", "Chỉ số này giảm trong 4 tháng liên tiếp".
- Nêu bật điểm bất thường một cách trung lập: Hãy để người dùng tự đánh giá tốt/xấu.
- Luôn cung cấp con số cụ thể thay vì các từ ngữ định tính.
- Gợi mở phân tích (Inviting Further Analysis): Đề xuất góc nhìn mới thay vì kết luận. (Vd: "Phân tích sâu hơn vào phân khúc khách hàng có thể làm rõ xu hướng này").
- TUYỆT ĐỐI KHÔNG xuất các cột ID (`customer_id`, `user_id`, `film_id`, `store_id`, `staff_id`, `payment_id`) làm cột thống kê/metrics hay vẽ biểu đồ. Khi nhóm dữ liệu, BẮT BUỘC JOIN sang các bảng chiều (`dim_customer`, `dim_film`, `dim_store`) để lấy Tên thật (`first_name || ' ' || last_name`, `title`, `category_name`).
- Viết một câu `summary` ngắn gọn, chuyên nghiệp, phản ánh đúng tinh thần Data Analyst trên.

{context}

=== TỪ ĐIỂN THUẬT NGỮ KINH DOANH (BUSINESS GLOSSARY) ===
- "Khách VIP", "Khách chi tiêu cao", "Khách hàng thân thiết": BẮT BUỘC dựa vào `ORDER BY SUM(p.amount) DESC` hoặc `COUNT(r.rental_id) DESC`. TUYỆT ĐỐI KHÔNG dùng hay tự chế các cột không có trong schema như `customer_type`, `customer_category`, `vip`!
- "Tháng này", "Gần đây": Sử dụng hàm `substr(payment_date, 1, 7)` vì payment_date đang là kiểu VARCHAR, TUYỆT ĐỐI không dùng `year(payment_date)` nếu chưa ép kiểu.
- "Doanh thu", "Thu nhập": Ưu tiên dùng `dvdrental_mart.fct_payment_active` (hoặc tính bằng SUM(amount) trong fct_payment với điều kiện `WHERE is_deleted = false`).
- "Lượt thuê": Ưu tiên dùng `dvdrental_mart.fct_rental_active` (hoặc fct_rental với điều kiện `WHERE is_deleted = false`).
- "Khách hàng hiện tại": Bộ lọc `c.is_current = true AND c.is_deleted = false` đối với bảng dim_customer (SCD2 + soft delete).
- "Bảng chiều (dim_film, dim_store, dim_staff, dim_actor)": Luôn lọc bản ghi hoạt động với `is_deleted = false`.
- QUY TẮC NGHIÊM NGẶT (STRICT SCHEMA): CHỈ ĐƯỢC DÙNG CÁC CỘT CÓ TRONG THÔNG TIN BẢNG DƯỚI ĐÂY. KHÔNG TỰ TẠO HOẶC ĐOÁN CÁC CỘT KHÔNG CÓ TRONG SCHEMA.

=== QUY TẮC BẮT BUỘC (CHAIN OF THOUGHT) ===
Trước khi viết SQL, bạn BẮT BUỘC phải tư duy theo 4 bước vào mảng `thought_process` để đảm bảo không bị sai logic (hallucination) và linh hoạt thiết kế Layout tỉ lệ đẹp:
- Bước 1 - Phân tích: Xác định từ khóa tương ứng với bảng/cột.
- Bước 2 - Điều kiện: Lọc ngầm (VD: `fct_payment_active`, `is_current = true`, `is_deleted = false`, nhóm thời gian).
- Bước 3 - Heuristics UI/UX (Rất quan trọng): Quyết định loại Chart, Màu Sắc và Layout.

Ví dụ minh họa cấu trúc JSON trả về:
1. Nếu người dùng yêu cầu tạo dashboard:
   ```json
   {{
     "is_dashboard": true,
     "dashboard_title": "Executive Analytics Dashboard",
     "summary": "Đã tổng hợp Dashboard phân tích đa chiều cho câu hỏi của bạn.",
     "thought_process": [
       "Bước 1 - Phân tích: ...",
       "Bước 2 - Điều kiện: ...",
       "Bước 3 - UI/UX Layout: Đặt Stat KPI Doanh thu ở top (colSpan=3), Stat KPI Giao dịch (colSpan=3), Donut (colSpan=5)...",
       "Bước 4 - SQL: ..."
     ],
     "widgets": [
       {{ "title": "Tổng Doanh Thu", "chartType": "stat", "colorIntent": "positive", "colSpan": 3, "sql": "SELECT SUM(amount) AS total_revenue FROM dvdrental_mart.fct_payment_active;" }},
       {{ "title": "Tổng Giao Dịch", "chartType": "stat", "colorIntent": "positive", "colSpan": 3, "sql": "SELECT COUNT(payment_id) AS total_transactions FROM dvdrental_mart.fct_payment_active;" }},
       {{ "title": "Tỷ Trọng Doanh Thu", "chartType": "donut", "colorIntent": "categorical", "colSpan": 5, "sql": "SELECT trim(cat) AS category, SUM(p.amount) AS revenue FROM dvdrental_mart.fct_payment_active p JOIN dvdrental_mart.dim_film f ON p.film_id = f.film_id AND f.is_deleted = false CROSS JOIN UNNEST(split(f.category_name, ',')) AS t(cat) GROUP BY trim(cat) ORDER BY revenue DESC LIMIT 5;" }}
     ]
   }}
   ```

2. Nếu người dùng chỉ hỏi một câu đơn lẻ, trả về 1 widget duy nhất:
   ```json
   {{
     "is_dashboard": false,
     "summary": "...",
     "thought_process": [...],
     "widgets": [
       {{ "title": "...", "chartType": "line/bar/horizontal_bar/donut/scatter/stat/table", "colorIntent": "positive/negative/categorical/...", "sql": "SELECT ..." }}
     ]
   }}
   ```
3. Tuyệt đối không tạo các lệnh sửa đổi dữ liệu (DROP, DELETE, INSERT, UPDATE, ALTER).
4. Đặt ALIAS rõ ràng cho các cột tính toán.
"""
        return prompt

    def generate_sql_spec(self, user_question: str) -> dict[str, Any]:
        context = self.rag.retrieve_context(user_question)
        system_instruction = self.build_system_prompt(context)

        if self.api_key and self.api_key.startswith("http"):
            import requests

            base_url = self.api_key.replace("/v1/chat/completions", "").rstrip("/")
            model_name = "qwen2.5:7b"
            try:
                models_resp = requests.get(f"{base_url}/v1/models", timeout=2)
                if models_resp.status_code == 200:
                    models_data = models_resp.json().get("data", [])
                    if models_data and len(models_data) > 0:
                        model_name = models_data[0].get("id", model_name)
            except Exception as e:
                print(
                    f"[WARN] Failed to fetch local models, fallback to {model_name}: {e}"
                )

            print(
                f"[INFO] Using local model endpoint: {base_url} with model {model_name}"
            )
            try:
                response = requests.post(
                    f"{base_url}/v1/chat/completions",
                    json={
                        "model": model_name,
                        "messages": [
                            {"role": "system", "content": system_instruction},
                            {"role": "user", "content": user_question},
                        ],
                        "temperature": 0.1,
                    },
                    headers={"Content-Type": "application/json"},
                )
                if response.status_code == 200:
                    raw_text = response.json()["choices"][0]["message"]["content"]
                    json_match = re.search(
                        r"```json\s*(.*?)\s*```", raw_text, re.DOTALL | re.IGNORECASE
                    )
                    if json_match:
                        spec = json.loads(json_match.group(1).strip())
                    else:
                        spec = json.loads(raw_text.strip())

                    for w in spec.get("widgets", []):
                        w["sql"] = self.sanitize_sql(w["sql"])
                    print(
                        f"[SUCCESS] Local Model '{model_name}' successfully generated SQL spec!"
                    )
                    return spec
                else:
                    print(
                        f"[DEBUG] Local model request failed with status: {response.status_code} - {response.text}"
                    )
            except Exception as e:
                print(f"[DEBUG] Local model '{model_name}' attempt failed: {e}")

        elif self.client:
            for model_name in AVAILABLE_MODELS:
                try:
                    response = self.client.models.generate_content(
                        model=model_name,
                        contents=f"{system_instruction}\n\nUser Question: {user_question}",
                    )
                    raw_text = response.text
                    json_match = re.search(
                        r"```json\s*(.*?)\s*```", raw_text, re.DOTALL | re.IGNORECASE
                    )
                    if json_match:
                        spec = json.loads(json_match.group(1).strip())
                    else:
                        spec = json.loads(raw_text.strip())

                    for w in spec.get("widgets", []):
                        w["sql"] = self.sanitize_sql(w["sql"])
                    print(
                        f"[SUCCESS] Gemini Model '{model_name}' successfully generated SQL spec!"
                    )
                    return spec
                except Exception as e:
                    print(f"[DEBUG] Gemini model '{model_name}' attempt skipped: {e}")

        return self._generate_fallback_spec(user_question)

    def _generate_fallback_spec(self, question: str) -> dict[str, Any]:
        q = question.lower()

        if any(
            kw in q
            for kw in [
                "dashboard",
                "báo cáo",
                "tổng quan",
                "nhiều chart",
                "nhiều biểu đồ",
                "quản trị",
            ]
        ):
            return {
                "is_dashboard": True,
                "dashboard_title": "Executive AI Analytics Dashboard",
                "summary": "Đã tạo Executive Dashboard tổng quan tích hợp các góc nhìn phân tích đa chiều cho hệ thống.",
                "widgets": [
                    {
                        "title": "Tổng Doanh Thu",
                        "chartType": "stat",
                        "colSpan": 3,
                        "sql": "SELECT SUM(amount) AS total_revenue FROM dvdrental_mart.fct_payment;",
                    },
                    {
                        "title": "Tổng Giao Dịch",
                        "chartType": "stat",
                        "colSpan": 3,
                        "sql": "SELECT COUNT(payment_id) AS total_transactions FROM dvdrental_mart.fct_payment;",
                    },
                    {
                        "title": "Kích Cỡ Đơn TB",
                        "chartType": "stat",
                        "colSpan": 3,
                        "sql": "SELECT AVG(amount) AS avg_ticket_size FROM dvdrental_mart.fct_payment;",
                    },
                    {
                        "title": "Tỷ Trọng Doanh Thu Theo Thể Loại Phim",
                        "chartType": "pie",
                        "colSpan": 5,
                        "sql": "SELECT f.category_name AS category, SUM(p.amount) AS total_revenue FROM dvdrental_mart.fct_payment p JOIN dvdrental_mart.dim_film f ON p.film_id = f.film_id GROUP BY f.category_name ORDER BY total_revenue DESC LIMIT 6;",
                    },
                    {
                        "title": "Xu Hướng Doanh Thu 6 Tháng Gần Nhất",
                        "chartType": "line",
                        "colSpan": 7,
                        "sql": "SELECT date_format(payment_date, '%Y-%m') AS month, SUM(amount) AS revenue FROM dvdrental_mart.fct_payment GROUP BY date_format(payment_date, '%Y-%m') ORDER BY month ASC LIMIT 12;",
                    },
                ],
            }

        if any(kw in q for kw in ["trend", "doanh thu", "tháng", "thời gian"]):
            return {
                "is_dashboard": False,
                "summary": "Phân tích xu hướng doanh thu theo thời gian.",
                "widgets": [
                    {
                        "title": "Xu Hướng Doanh Thu",
                        "chartType": "line",
                        "sql": "SELECT date_format(payment_date, '%Y-%m') AS month, SUM(amount) AS revenue, COUNT(payment_id) AS total_rentals FROM dvdrental_mart.fct_payment GROUP BY date_format(payment_date, '%Y-%m') ORDER BY month ASC LIMIT 12;",
                    }
                ],
            }

        if any(kw in q for kw in ["thể loại", "category", "phim", "movie"]):
            return {
                "is_dashboard": False,
                "summary": "Phân tích doanh thu theo thể loại phim.",
                "widgets": [
                    {
                        "title": "Doanh Thu Theo Thể Loại Phim",
                        "chartType": "bar",
                        "sql": "SELECT f.category_name AS category, SUM(p.amount) AS total_revenue, COUNT(r.rental_id) AS rental_count FROM dvdrental_mart.fct_rental r JOIN dvdrental_mart.fct_payment p ON r.rental_id = p.rental_id JOIN dvdrental_mart.dim_film f ON r.film_id = f.film_id GROUP BY f.category_name ORDER BY total_revenue DESC LIMIT 10;",
                    }
                ],
            }

        if any(kw in q for kw in ["tổng", "kpi", "chỉ số"]):
            return {
                "is_dashboard": False,
                "summary": "Tổng hợp chỉ số KPI doanh thu.",
                "widgets": [
                    {
                        "title": "Chỉ Số Tổng Doanh Thu",
                        "chartType": "stat",
                        "sql": "SELECT SUM(amount) AS total_revenue, COUNT(payment_id) AS total_transactions FROM dvdrental_mart.fct_payment;",
                    }
                ],
            }

        return {
            "is_dashboard": False,
            "summary": "Danh sách khách hàng mua nhiều nhất.",
            "widgets": [
                {
                    "title": "Top Khách Hàng VIP",
                    "chartType": "table",
                    "sql": "SELECT c.customer_id, c.email, SUM(p.amount) AS total_spent, COUNT(r.rental_id) AS rentals FROM dvdrental_mart.dim_customer c JOIN dvdrental_mart.fct_payment p ON c.customer_id = p.customer_id JOIN dvdrental_mart.fct_rental r ON p.rental_id = r.rental_id WHERE c.is_current = true GROUP BY c.customer_id, c.email ORDER BY total_spent DESC LIMIT 10;",
                }
            ],
        }

    @staticmethod
    def sanitize_sql(sql: str) -> str:
        sql_clean = sql.strip()
        forbidden = [
            "DROP",
            "DELETE",
            "UPDATE",
            "INSERT",
            "ALTER",
            "TRUNCATE",
            "CREATE",
        ]
        for kw in forbidden:
            if re.search(rf"\b{kw}\b", sql_clean, re.IGNORECASE):
                raise ValueError(
                    f"Security Alert: Non-read-only operation '{kw}' detected."
                )
        return sql_clean
