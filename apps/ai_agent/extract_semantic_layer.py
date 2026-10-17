import json
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[2]
DBT_MART_PATH = BASE_DIR / "pipelines" / "dbt" / "models" / "mart"
OUTPUT_FILE = Path(__file__).resolve().parent / "semantic_dictionary.json"

COMPLETE_TABLE_DEFINITIONS = {
    "fct_payment": {
        "description": "Bảng Fact giao dịch thanh toán. Enriched với film_id và store_id thông qua rental → inventory.",
        "columns": {
            "payment_id": {
                "description": "Primary key — Mã giao dịch thanh toán",
                "data_type": "INTEGER",
                "role": "pk",
            },
            "customer_id": {
                "description": "FK → dim_customer. Mã khách hàng",
                "data_type": "INTEGER",
                "role": "fk",
            },
            "staff_id": {
                "description": "FK → dim_staff. Nhân viên xử lý thanh toán",
                "data_type": "INTEGER",
                "role": "fk",
            },
            "rental_id": {
                "description": "FK → fct_rental. Mã đơn thuê tương ứng",
                "data_type": "INTEGER",
                "role": "fk",
            },
            "film_id": {
                "description": "FK → dim_film. Mã phim (enriched qua inventory)",
                "data_type": "INTEGER",
                "role": "fk",
            },
            "store_id": {
                "description": "FK → dim_store. Mã cửa hàng (enriched qua inventory)",
                "data_type": "INTEGER",
                "role": "fk",
            },
            "amount": {
                "description": "Số tiền thanh toán (USD). Luôn > 0",
                "data_type": "DOUBLE",
                "role": "measure",
            },
            "payment_date": {
                "description": "Thời điểm thanh toán",
                "data_type": "TIMESTAMP",
                "role": "time",
            },
            "cdc_capture_at": {
                "description": "Thời gian CDC capture (dùng cho incremental)",
                "data_type": "TIMESTAMP",
                "role": "technical",
            },
        },
        "metrics": {
            "total_revenue": "SUM(amount) — Tổng doanh thu",
            "transaction_count": "COUNT(payment_id) — Tổng số giao dịch",
            "avg_payment": "AVG(amount) — Giá trị trung bình mỗi giao dịch",
        },
        "business_rules": [
            "Tổng doanh thu: `SUM(amount)`",
            "Doanh thu theo tháng: `date_format(payment_date, '%Y-%m')` hoặc `date_trunc('month', payment_date)`",
            "Doanh thu theo ngày: `date_format(payment_date, '%Y-%m-%d')` hoặc `date_trunc('day', payment_date)`",
            "JOIN tới dim_customer qua `customer_id`",
            "JOIN tới dim_film qua `film_id` (đã enriched, không cần qua fct_rental)",
            "JOIN tới dim_store qua `store_id` (đã enriched, không cần qua fct_rental)",
        ],
        "relationships": [
            {"column": "customer_id", "references": "dim_customer.customer_id"},
            {"column": "staff_id", "references": "dim_staff.staff_id"},
            {"column": "rental_id", "references": "fct_rental.rental_id"},
            {"column": "film_id", "references": "dim_film.film_id"},
            {"column": "store_id", "references": "dim_store.store_id"},
        ],
    },
    "fct_rental": {
        "description": "Bảng Fact giao dịch thuê đĩa phim. Enriched với film_id, store_id qua inventory. Có computed rental_duration_days.",
        "columns": {
            "rental_id": {
                "description": "Primary key — Mã đơn thuê",
                "data_type": "INTEGER",
                "role": "pk",
            },
            "rental_date": {
                "description": "Ngày/giờ bắt đầu thuê",
                "data_type": "TIMESTAMP",
                "role": "time",
            },
            "return_date": {
                "description": "Ngày/giờ trả phim. NULL = chưa trả",
                "data_type": "TIMESTAMP",
                "role": "time",
            },
            "customer_id": {
                "description": "FK → dim_customer. Mã khách hàng",
                "data_type": "INTEGER",
                "role": "fk",
            },
            "film_id": {
                "description": "FK → dim_film. Mã phim (enriched qua inventory)",
                "data_type": "INTEGER",
                "role": "fk",
            },
            "store_id": {
                "description": "FK → dim_store. Mã cửa hàng (enriched qua inventory)",
                "data_type": "INTEGER",
                "role": "fk",
            },
            "staff_id": {
                "description": "FK → dim_staff. Nhân viên xử lý",
                "data_type": "INTEGER",
                "role": "fk",
            },
            "rental_duration_days": {
                "description": "Số ngày thuê (computed: return_date - rental_date). NULL nếu chưa trả",
                "data_type": "INTEGER",
                "role": "measure",
            },
            "cdc_capture_at": {
                "description": "Thời gian CDC capture (dùng cho incremental)",
                "data_type": "TIMESTAMP",
                "role": "technical",
            },
        },
        "metrics": {
            "total_rentals": "COUNT(rental_id) — Tổng lượt thuê",
            "avg_rental_duration": "AVG(rental_duration_days) — Thời gian thuê trung bình (ngày)",
            "unreturned_count": "COUNT(CASE WHEN return_date IS NULL THEN 1 END) — Số phim chưa trả",
        },
        "business_rules": [
            "Tổng lượt thuê: `COUNT(rental_id)`",
            "Thời gian thuê trung bình: `AVG(rental_duration_days)`",
            "Phim chưa trả: `return_date IS NULL`",
            "JOIN tới dim_customer qua `customer_id`",
            "JOIN tới dim_film qua `film_id` (đã enriched, không cần qua inventory)",
            "JOIN tới dim_store qua `store_id` (đã enriched)",
        ],
        "relationships": [
            {"column": "customer_id", "references": "dim_customer.customer_id"},
            {"column": "staff_id", "references": "dim_staff.staff_id"},
            {"column": "film_id", "references": "dim_film.film_id"},
            {"column": "store_id", "references": "dim_store.store_id"},
        ],
    },
    "dim_customer": {
        "description": "Bảng chiều khách hàng — SCD Type 2 (lịch sử thay đổi địa chỉ). Dùng is_current = true để lấy bản ghi mới nhất.",
        "columns": {
            "customer_key": {
                "description": "Surrogate key (dbt snapshot dbt_scd_id). Unique per version",
                "data_type": "STRING",
                "role": "pk",
            },
            "customer_id": {
                "description": "Natural key — Mã khách hàng. Có thể trùng do SCD2",
                "data_type": "INTEGER",
                "role": "nk",
            },
            "first_name": {
                "description": "Tên khách hàng",
                "data_type": "STRING",
                "role": "attribute",
            },
            "last_name": {
                "description": "Họ khách hàng",
                "data_type": "STRING",
                "role": "attribute",
            },
            "email": {
                "description": "Email khách hàng",
                "data_type": "STRING",
                "role": "attribute",
            },
            "store_id": {
                "description": "FK → dim_store. Cửa hàng đăng ký",
                "data_type": "INTEGER",
                "role": "fk",
            },
            "is_active": {
                "description": "Trạng thái hoạt động (boolean flag)",
                "data_type": "BOOLEAN",
                "role": "attribute",
            },
            "active": {
                "description": "Trạng thái active (numeric)",
                "data_type": "INTEGER",
                "role": "attribute",
            },
            "create_date": {
                "description": "Ngày tạo tài khoản khách hàng",
                "data_type": "DATE",
                "role": "time",
            },
            "address": {
                "description": "Địa chỉ dòng 1",
                "data_type": "STRING",
                "role": "attribute",
            },
            "address2": {
                "description": "Địa chỉ dòng 2 (có thể NULL)",
                "data_type": "STRING",
                "role": "attribute",
            },
            "district": {
                "description": "Quận/Huyện",
                "data_type": "STRING",
                "role": "attribute",
            },
            "city_name": {
                "description": "Thành phố",
                "data_type": "STRING",
                "role": "attribute",
            },
            "country_name": {
                "description": "Quốc gia",
                "data_type": "STRING",
                "role": "attribute",
            },
            "postal_code": {
                "description": "Mã bưu chính",
                "data_type": "STRING",
                "role": "attribute",
            },
            "phone": {
                "description": "Số điện thoại",
                "data_type": "STRING",
                "role": "attribute",
            },
            "valid_from": {
                "description": "SCD2: Thời điểm bắt đầu hiệu lực bản ghi",
                "data_type": "TIMESTAMP",
                "role": "scd2",
            },
            "valid_to": {
                "description": "SCD2: Thời điểm kết thúc hiệu lực. NULL = bản ghi hiện tại",
                "data_type": "TIMESTAMP",
                "role": "scd2",
            },
            "is_current": {
                "description": "SCD2: True nếu là bản ghi mới nhất (valid_to IS NULL)",
                "data_type": "BOOLEAN",
                "role": "scd2",
            },
        },
        "metrics": {},
        "business_rules": [
            "⚠️ BẮT BUỘC: Luôn thêm `WHERE is_current = true` khi JOIN với bảng fact để lấy thông tin khách hàng hiện tại",
            "Nếu cần lịch sử thay đổi, bỏ điều kiện is_current",
            "Tên đầy đủ: `first_name || ' ' || last_name`",
            "Phân tích theo vùng: dùng `city_name`, `country_name`",
        ],
        "relationships": [
            {"column": "store_id", "references": "dim_store.store_id"},
        ],
    },
    "dim_film": {
        "description": "Bảng chiều phim — Bao gồm thông tin thể loại, ngôn ngữ, giá thuê, và số lượng diễn viên.",
        "columns": {
            "film_id": {
                "description": "Primary key — Mã phim",
                "data_type": "INTEGER",
                "role": "pk",
            },
            "title": {
                "description": "Tên phim",
                "data_type": "STRING",
                "role": "attribute",
            },
            "description": {
                "description": "Mô tả nội dung phim",
                "data_type": "STRING",
                "role": "attribute",
            },
            "release_year": {
                "description": "Năm phát hành",
                "data_type": "INTEGER",
                "role": "attribute",
            },
            "language_name": {
                "description": "Ngôn ngữ phim (English, etc.)",
                "data_type": "STRING",
                "role": "attribute",
            },
            "category_name": {
                "description": "Thể loại phim (Action, Comedy, Drama, Horror, etc.)",
                "data_type": "STRING",
                "role": "attribute",
            },
            "rental_duration": {
                "description": "Thời hạn thuê tối đa (ngày)",
                "data_type": "INTEGER",
                "role": "attribute",
            },
            "rental_rate": {
                "description": "Giá thuê phim (USD)",
                "data_type": "DOUBLE",
                "role": "measure",
            },
            "film_length_minutes": {
                "description": "Thời lượng phim (phút)",
                "data_type": "INTEGER",
                "role": "attribute",
            },
            "replacement_cost": {
                "description": "Giá thay thế phim (USD)",
                "data_type": "DOUBLE",
                "role": "measure",
            },
            "rating": {
                "description": "Nhãn phân loại (PG, PG-13, R, NC-17, G)",
                "data_type": "STRING",
                "role": "attribute",
            },
            "special_features": {
                "description": "Tính năng đặc biệt (Trailers, Commentaries, etc.)",
                "data_type": "STRING",
                "role": "attribute",
            },
            "actor_count": {
                "description": "Số lượng diễn viên tham gia",
                "data_type": "INTEGER",
                "role": "measure",
            },
        },
        "metrics": {
            "avg_rental_rate": "AVG(rental_rate) — Giá thuê trung bình",
            "total_films": "COUNT(film_id) — Tổng số phim",
        },
        "business_rules": [
            "Phân tích theo thể loại: GROUP BY `category_name`",
            "Phân tích theo rating: GROUP BY `rating`",
            "Top phim hot: JOIN fct_rental → COUNT(rental_id) GROUP BY film_id",
            "Phim giá thuê cao: ORDER BY `rental_rate` DESC",
        ],
        "relationships": [],
    },
    "dim_store": {
        "description": "Bảng chiều cửa hàng — Bao gồm địa chỉ đầy đủ và thông tin quản lý.",
        "columns": {
            "store_id": {
                "description": "Primary key — Mã cửa hàng",
                "data_type": "INTEGER",
                "role": "pk",
            },
            "manager_first_name": {
                "description": "Tên quản lý cửa hàng",
                "data_type": "STRING",
                "role": "attribute",
            },
            "manager_last_name": {
                "description": "Họ quản lý cửa hàng",
                "data_type": "STRING",
                "role": "attribute",
            },
            "address": {
                "description": "Địa chỉ dòng 1",
                "data_type": "STRING",
                "role": "attribute",
            },
            "address2": {
                "description": "Địa chỉ dòng 2",
                "data_type": "STRING",
                "role": "attribute",
            },
            "district": {
                "description": "Quận/Huyện",
                "data_type": "STRING",
                "role": "attribute",
            },
            "city_name": {
                "description": "Thành phố",
                "data_type": "STRING",
                "role": "attribute",
            },
            "country_name": {
                "description": "Quốc gia",
                "data_type": "STRING",
                "role": "attribute",
            },
            "postal_code": {
                "description": "Mã bưu chính",
                "data_type": "STRING",
                "role": "attribute",
            },
            "phone": {
                "description": "Số điện thoại cửa hàng",
                "data_type": "STRING",
                "role": "attribute",
            },
        },
        "metrics": {},
        "business_rules": [
            "Tên quản lý đầy đủ: `manager_first_name || ' ' || manager_last_name`",
            "Phân tích theo cửa hàng: GROUP BY `store_id`",
        ],
        "relationships": [],
    },
    "dim_staff": {
        "description": "Bảng chiều nhân viên — Bao gồm thông tin liên lạc và địa chỉ đầy đủ.",
        "columns": {
            "staff_id": {
                "description": "Primary key — Mã nhân viên",
                "data_type": "INTEGER",
                "role": "pk",
            },
            "first_name": {
                "description": "Tên nhân viên",
                "data_type": "STRING",
                "role": "attribute",
            },
            "last_name": {
                "description": "Họ nhân viên",
                "data_type": "STRING",
                "role": "attribute",
            },
            "email": {
                "description": "Email nhân viên",
                "data_type": "STRING",
                "role": "attribute",
            },
            "username": {
                "description": "Tên đăng nhập",
                "data_type": "STRING",
                "role": "attribute",
            },
            "store_id": {
                "description": "FK → dim_store. Cửa hàng làm việc",
                "data_type": "INTEGER",
                "role": "fk",
            },
            "is_active": {
                "description": "Trạng thái hoạt động",
                "data_type": "BOOLEAN",
                "role": "attribute",
            },
            "address": {
                "description": "Địa chỉ dòng 1",
                "data_type": "STRING",
                "role": "attribute",
            },
            "address2": {
                "description": "Địa chỉ dòng 2",
                "data_type": "STRING",
                "role": "attribute",
            },
            "district": {
                "description": "Quận/Huyện",
                "data_type": "STRING",
                "role": "attribute",
            },
            "city_name": {
                "description": "Thành phố",
                "data_type": "STRING",
                "role": "attribute",
            },
            "country_name": {
                "description": "Quốc gia",
                "data_type": "STRING",
                "role": "attribute",
            },
            "postal_code": {
                "description": "Mã bưu chính",
                "data_type": "STRING",
                "role": "attribute",
            },
            "phone": {
                "description": "Số điện thoại",
                "data_type": "STRING",
                "role": "attribute",
            },
        },
        "metrics": {},
        "business_rules": [
            "Tên đầy đủ: `first_name || ' ' || last_name`",
            "Phân tích hiệu suất nhân viên: JOIN fct_payment/fct_rental qua `staff_id`",
        ],
        "relationships": [
            {"column": "store_id", "references": "dim_store.store_id"},
        ],
    },
    "dim_actor": {
        "description": "Bảng chiều diễn viên — Bao gồm số lượng phim đã tham gia.",
        "columns": {
            "actor_id": {
                "description": "Primary key — Mã diễn viên",
                "data_type": "INTEGER",
                "role": "pk",
            },
            "first_name": {
                "description": "Tên diễn viên",
                "data_type": "STRING",
                "role": "attribute",
            },
            "last_name": {
                "description": "Họ diễn viên",
                "data_type": "STRING",
                "role": "attribute",
            },
            "film_count": {
                "description": "Số lượng phim đã tham gia",
                "data_type": "INTEGER",
                "role": "measure",
            },
        },
        "metrics": {
            "avg_films_per_actor": "AVG(film_count) — Số phim trung bình mỗi diễn viên",
        },
        "business_rules": [
            "Tên đầy đủ: `first_name || ' ' || last_name`",
            "⚠️ Không có FK trực tiếp tới fct_rental hay fct_payment. Để phân tích doanh thu theo diễn viên, cần JOIN qua bảng trung gian stg_film_actor.",
        ],
        "relationships": [],
    },
}

JOIN_GUIDE = [
    {
        "purpose": "Doanh thu theo khách hàng",
        "join": "fct_payment p JOIN dim_customer c ON p.customer_id = c.customer_id AND c.is_current = true",
    },
    {
        "purpose": "Doanh thu theo thể loại phim",
        "join": "fct_payment p JOIN dim_film f ON p.film_id = f.film_id",
        "note": "fct_payment đã có film_id enriched — KHÔNG cần join qua fct_rental",
    },
    {
        "purpose": "Doanh thu theo cửa hàng",
        "join": "fct_payment p JOIN dim_store s ON p.store_id = s.store_id",
        "note": "fct_payment đã có store_id enriched — KHÔNG cần join qua fct_rental",
    },
    {
        "purpose": "Doanh thu theo nhân viên",
        "join": "fct_payment p JOIN dim_staff st ON p.staff_id = st.staff_id",
    },
    {
        "purpose": "Lượt thuê theo phim",
        "join": "fct_rental r JOIN dim_film f ON r.film_id = f.film_id",
    },
    {
        "purpose": "Lượt thuê theo khách hàng",
        "join": "fct_rental r JOIN dim_customer c ON r.customer_id = c.customer_id AND c.is_current = true",
    },
    {
        "purpose": "Doanh thu kèm thông tin thuê",
        "join": "fct_payment p JOIN fct_rental r ON p.rental_id = r.rental_id",
    },
]


def generate_semantic_dictionary():
    """Build the complete Semantic Layer JSON from ground-truth table definitions."""
    tables_data = {}
    for table_name, table_def in COMPLETE_TABLE_DEFINITIONS.items():
        tables_data[table_name] = {
            "description": table_def["description"],
            "columns": table_def["columns"],
            "metrics": table_def.get("metrics", {}),
            "business_rules": table_def.get("business_rules", []),
            "relationships": table_def.get("relationships", []),
        }

    semantic_layer = {
        "database_name": "dvdrental_mart",
        "engine_type": "AWS Athena (Presto SQL)",
        "total_tables": len(tables_data),
        "total_columns": sum(len(t["columns"]) for t in tables_data.values()),
        "dialect_rules": [
            "Sử dụng cú pháp Athena (Presto SQL).",
            "Khi nhóm theo thời gian, sử dụng `date_format(timestamp_col, '%Y-%m')` cho tháng, `date_format(timestamp_col, '%Y-%m-%d')` cho ngày.",
            "Hàm `date_trunc('month', timestamp_col)` cũng hợp lệ nhưng trả về TIMESTAMP.",
            "Sử dụng `LIMIT` ở cuối câu lệnh khi cần lấy Top N.",
            "Tuyệt đối KHÔNG tạo các câu lệnh UPDATE, DELETE, DROP, INSERT, ALTER, TRUNCATE, CREATE.",
            "Luôn sử dụng ALIAS rõ ràng cho mọi cột tính toán (VD: `SUM(amount) AS total_revenue`).",
            "Khi JOIN với dim_customer, LUÔN thêm `AND c.is_current = true` (SCD2).",
            "fct_payment và fct_rental đã có film_id, store_id enriched — KHÔNG cần join qua inventory hay bảng trung gian.",
        ],
        "join_guide": JOIN_GUIDE,
        "tables": tables_data,
    }

    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(semantic_layer, f, ensure_ascii=False, indent=2)

    print(f"\n{'='*60}")
    print("  SEMANTIC DICTIONARY — EXTRACTION REPORT")
    print(f"{'='*60}")
    print(f"  Output: {OUTPUT_FILE}")
    print(f"  Tables: {len(tables_data)}")
    print(f"  Total columns: {sum(len(t['columns']) for t in tables_data.values())}")
    print(f"  Join guides: {len(JOIN_GUIDE)}")
    print()
    for tname, tdef in tables_data.items():
        cols = list(tdef["columns"].keys())
        print(f"  [Table] {tname} ({len(cols)} cols)")
        print(f"     -> {', '.join(cols)}")
    print(f"{'='*60}\n")

    return semantic_layer


if __name__ == "__main__":
    generate_semantic_dictionary()
