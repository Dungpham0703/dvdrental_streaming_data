# ruff: noqa: E402
import sys
import json
from pathlib import Path
from typing import Dict, Any

CURRENT_DIR = Path(__file__).resolve().parent
if str(CURRENT_DIR) not in sys.path:
    sys.path.insert(0, str(CURRENT_DIR))

# pyrefly: ignore [missing-import]
import chromadb
from sentence_transformers import SentenceTransformer

SEMANTIC_FILE = Path(__file__).resolve().parent / "semantic_dictionary.json"
CHROMA_DB_DIR = Path(__file__).resolve().parent / "chroma_db"


class RAGRetriever:
    def __init__(self):
        self.client = chromadb.PersistentClient(path=str(CHROMA_DB_DIR))
        self.collection_name = "tino_data_semantic"
        self.embedding_model = SentenceTransformer("all-MiniLM-L6-v2")
        self.semantic_dict = self._load_semantic_dictionary()
        self.collection = self._get_or_create_collection()

    def _load_semantic_dictionary(self) -> Dict[str, Any]:
        if SEMANTIC_FILE.exists():
            with open(SEMANTIC_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        return {}

    def _get_or_create_collection(self):
        try:
            collection = self.client.get_collection(name=self.collection_name)
            return collection
        except Exception:
            collection = self.client.create_collection(name=self.collection_name)
            self._populate_collection(collection)
            return collection

    def _populate_collection(self, collection):
        documents = []
        ids = []
        metadatas = []

        tables_dict = self.semantic_dict.get("tables", {})
        for table_name, table_info in tables_dict.items():
            doc_text = f"Table: {table_name}\nDescription: {table_info.get('description', '')}\nColumns: {', '.join(table_info.get('columns', []))}"
            documents.append(doc_text)
            ids.append(f"table_{table_name}")
            metadatas.append({"type": "table", "name": table_name})

        heuristics_doc = """
[1. QUY TẮC CHỌN CHART (Chart Selection Heuristics)]
- Số đơn (Single Number / Aggregation): -> `stat` (Ví dụ: Tổng doanh thu, Số active user). BẮT BUỘC mỗi KPI/Metrics phải là một Widget Độc Lập để tiện kéo thả.
- 1 Cột thời gian (Date/Timestamp) + 1 hoặc nhiều Metrics: -> `line` hoặc `area` (Phân tích xu hướng).
- 1 Cột phân loại (Categorical < 6 items) + 1 Metric (%) tổng bằng 100%: -> `donut` / `pie` (Tỷ lệ đóng góp). Nếu > 6 items: Bắt buộc chuyển sang `horizontal_bar`.
- 1 Cột phân loại (Categorical > 6 items) + 1 Metric: -> `horizontal_bar` (nếu tên danh mục dài) hoặc `bar` (nếu tên ngắn).
- 2 Cột số liên tục (Numeric vs Numeric): -> `scatter` (Tìm tương quan).
- Nhiều cột thuộc tính + Cần đọc chi tiết: -> `table`.

[2. QUY TẮC MÀU SẮC (Semantic Colors)]
Tuyệt đối KHÔNG trả về mã màu Hex. Chỉ trả về CẢM XÚC/NGỮ NGHĨA (colorIntent):
- `positive`: Tăng trưởng, Doanh thu, Tích cực.
- `negative` / `warning`: Giảm, Nợ, Rủi ro.
- `categorical`: Phân loại nhiều mục.
- `indigo` / `emerald` / `sunset`: Các theme màu đẹp nếu muốn một màu chủ đạo.

[3. QUY TẮC LAYOUT (Dashboard Layout Hierarchy)]
- `textarea`: Bắt buộc dùng để giải thích hoặc báo cáo. `colSpan = 16`. Hỗ trợ Markdown.
- `note` / `alert`: Dùng để cảnh báo, lưu ý. `colSpan = 16`. Hỗ trợ Markdown.
- `stat`: BẮT BUỘC trả về từng số KPI thành từng widget riêng biệt với `colSpan = 3` (hoặc `colSpan = 4`) để dễ dàng hoán đổi vị trí. Không gộp nhiều cột SUM/COUNT vào 1 câu lệnh nếu là Dashboard.
- `line` / `area`: `colSpan = 8`.
- `pie` / `donut` / `bar`: `colSpan = 4` hoặc `colSpan = 8`.
- `table`: `colSpan = 12` hoặc `16`.

[4. QUY TẮC CẤM DÙNG ID LÀM METRICS (No Raw ID Metrics Rule)]
- TUYỆT ĐỐI KHÔNG SELECT các cột ID vô nghĩa (`user_id`, `customer_id`, `film_id`, `staff_id`, `store_id`, `payment_id`, `rental_id`) để đưa vào các biểu đồ hay KPI stat card.
- Không bao giờ thực hiện SUM, AVG, hay vẽ biểu đồ số liệu trên các cột ID. ID chỉ là khóa liên kết (Join Key).
- Khi nhóm theo đối tượng, BẮT BUỘC JOIN sang bảng chiều (dim_customer, dim_film, dim_store, dim_staff) để lấy Tên thật (`first_name`, `last_name`, `title`, `category_name`) thay vì GROUP BY cột ID.
- Ví dụ SAI: `SELECT customer_id, SUM(amount) FROM fct_payment GROUP BY customer_id`
- Ví dụ ĐÚNG: `SELECT c.first_name || ' ' || c.last_name AS customer_name, SUM(p.amount) AS total_spent FROM fct_payment p JOIN dim_customer c ON p.customer_id = c.customer_id AND c.is_current = true GROUP BY c.first_name, c.last_name`
        """
        documents.append(heuristics_doc)
        ids.append("rules_heuristics")
        metadatas.append({"type": "rule", "name": "heuristics"})

        if not documents:
            return

        embeddings = self.embedding_model.encode(documents).tolist()
        collection.add(
            ids=ids, embeddings=embeddings, documents=documents, metadatas=metadatas
        )

    def retrieve_context(self, query: str, top_k: int = 3) -> str:
        heuristics_result = self.collection.get(ids=["rules_heuristics"])
        heuristics = (
            heuristics_result["documents"][0]
            if heuristics_result and heuristics_result["documents"]
            else ""
        )

        query_embedding = self.embedding_model.encode([query]).tolist()
        results = self.collection.query(
            query_embeddings=query_embedding, n_results=top_k, where={"type": "table"}
        )

        retrieved_docs = (
            results["documents"][0] if results and results["documents"] else []
        )

        context = "=== BỐ CỤC, MÀU SẮC & UI/UX ===\n" + heuristics + "\n\n"
        context += "=== BẢNG DỮ LIỆU LIÊN QUAN TRÍCH XUẤT TỪ RAG ===\n"
        context += "\n\n".join(retrieved_docs)

        return context


if __name__ == "__main__":
    retriever = RAGRetriever()
    print(retriever.retrieve_context("doanh thu theo thể loại phim"))
