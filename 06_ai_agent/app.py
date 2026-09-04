# ruff: noqa: E402
import sys
import json
import asyncio
from pathlib import Path
from typing import Optional, Dict, Any, List

CURRENT_DIR = Path(__file__).resolve().parent
if str(CURRENT_DIR) not in sys.path:
    sys.path.insert(0, str(CURRENT_DIR))

from dotenv import load_dotenv

# pyrefly: ignore [missing-import]
import uvicorn

# pyrefly: ignore [missing-import]
from fastapi import FastAPI, HTTPException

# pyrefly: ignore [missing-import]
from fastapi.middleware.cors import CORSMiddleware

# pyrefly: ignore [missing-import]
from fastapi.responses import StreamingResponse

# pyrefly: ignore [missing-import]
from pydantic import BaseModel

env_path = CURRENT_DIR / ".env"
load_dotenv(dotenv_path=env_path)
load_dotenv()

from sql_generator import TextToSQLGenerator
from athena_executor import AthenaQueryExecutor

app = FastAPI(
    title="Tino Data — AI Text-to-SQL Analytics Agent",
    description="Natural language data analytics & Multi-Widget Executive AI Dashboard Generator with SSE Streaming",
    version="3.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

generator = TextToSQLGenerator()
executor = AthenaQueryExecutor()


class QueryRequest(BaseModel):
    prompt: str
    custom_sql: Optional[str] = None


class WidgetResponse(BaseModel):
    title: str
    sql: str
    chartType: str
    columns: List[str]
    data: List[Dict[str, Any]]
    execution_time_ms: int
    mode: str


class QueryResponse(BaseModel):
    success: bool
    prompt: str
    summary: str
    thought_process: Optional[List[str]] = None
    is_dashboard: bool
    dashboard_title: Optional[str] = "Tino Data Analytics Dashboard"
    widgets: List[WidgetResponse]


def sse_event(data: dict) -> str:
    return f"data: {json.dumps(data, ensure_ascii=False)}\n\n"


@app.get("/health")
def health_check():
    return {"status": "ok", "service": "Tino Data AI Analytics Agent v3.0"}


@app.get("/api/schema")
def get_schema():
    return generator.semantic_dict


@app.post("/api/query", response_model=QueryResponse)
def handle_query(request: QueryRequest):
    if not request.prompt and not request.custom_sql:
        raise HTTPException(
            status_code=400, detail="Either 'prompt' or 'custom_sql' is required."
        )

    try:
        if request.custom_sql:
            sql = generator.sanitize_sql(request.custom_sql)
            exec_res = executor.execute_query(sql)
            widget = WidgetResponse(
                title="Custom SQL Query Execution",
                sql=sql,
                chartType=exec_res["chartType"],
                columns=exec_res["columns"],
                data=exec_res["data"],
                execution_time_ms=exec_res["execution_time_ms"],
                mode=exec_res["mode"],
            )
            return QueryResponse(
                success=True,
                prompt=request.prompt or "Custom SQL Execution",
                summary="Đã thực thi câu lệnh SQL người dùng tùy chỉnh.",
                thought_process=["Truy vấn trực tiếp bằng Custom SQL."],
                is_dashboard=False,
                dashboard_title=None,
                widgets=[widget],
            )

        spec = generator.generate_sql_spec(request.prompt)
        populated_widgets = executor.execute_widgets(spec.get("widgets", []))

        widget_responses = [
            WidgetResponse(
                title=w["title"],
                sql=w["sql"],
                chartType=w["chartType"],
                columns=w["columns"],
                data=w["data"],
                execution_time_ms=w["execution_time_ms"],
                mode=w["mode"],
            )
            for w in populated_widgets
        ]

        return QueryResponse(
            success=True,
            prompt=request.prompt,
            summary=spec.get("summary", "Đã phân tích xong dữ liệu."),
            thought_process=spec.get("thought_process", []),
            is_dashboard=spec.get("is_dashboard", False),
            dashboard_title=spec.get(
                "dashboard_title", "Tino Data Analytics Dashboard"
            ),
            widgets=widget_responses,
        )

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/query/stream")
async def handle_query_stream(request: QueryRequest):
    async def event_generator():
        try:
            yield sse_event(
                {
                    "type": "step",
                    "step": "analyze",
                    "status": "running",
                    "message": "Phân tích yêu cầu người dùng",
                }
            )
            await asyncio.sleep(0.3)
            yield sse_event(
                {
                    "type": "step",
                    "step": "analyze",
                    "status": "done",
                    "message": "Phân tích yêu cầu người dùng",
                    "detail": f"Prompt: {request.prompt}",
                }
            )

            yield sse_event(
                {
                    "type": "step",
                    "step": "generate_sql",
                    "status": "running",
                    "message": "Sinh SQL via Gemini AI",
                }
            )

            try:
                spec = await asyncio.to_thread(
                    generator.generate_sql_spec, request.prompt
                )
            except Exception as e:
                yield sse_event(
                    {
                        "type": "step",
                        "step": "generate_sql",
                        "status": "error",
                        "message": "Sinh SQL via Gemini AI",
                        "detail": f"Lỗi: {str(e)}",
                    }
                )
                yield sse_event(
                    {
                        "type": "error",
                        "message": f"Không thể sinh SQL: {str(e)}",
                        "detail": str(e),
                    }
                )
                return

            widgets_spec = spec.get("widgets", [])
            yield sse_event(
                {
                    "type": "step",
                    "step": "generate_sql",
                    "status": "done",
                    "message": "Sinh SQL via Gemini AI",
                    "detail": f"Đã tạo {len(widgets_spec)} truy vấn SQL",
                }
            )

            populated_widgets = []
            for i, widget in enumerate(widgets_spec):
                step_id = f"execute_widget_{i}"
                widget_title = widget.get("title", f"Widget {i+1}")
                widget_sql = widget.get("sql", "")

                yield sse_event(
                    {
                        "type": "step",
                        "step": step_id,
                        "status": "running",
                        "message": f"Thực thi: {widget_title}",
                        "detail": widget_sql[:200],
                    }
                )

                try:
                    exec_result = await asyncio.to_thread(
                        executor.execute_query, widget_sql
                    )
                    enriched_widget = {
                        **widget,
                        "columns": exec_result.get("columns", []),
                        "data": exec_result.get("data", []),
                        "execution_time_ms": exec_result.get("execution_time_ms", 0),
                        "mode": exec_result.get("mode", "Athena"),
                        "chartType": widget.get(
                            "chartType", exec_result.get("chartType", "table")
                        ),
                    }
                    populated_widgets.append(enriched_widget)

                    yield sse_event(
                        {
                            "type": "step",
                            "step": step_id,
                            "status": "done",
                            "message": f"Thực thi: {widget_title}",
                            "detail": f"{len(exec_result.get('data', []))} dòng — {exec_result.get('execution_time_ms', 0)}ms",
                        }
                    )

                except Exception as e:
                    yield sse_event(
                        {
                            "type": "step",
                            "step": step_id,
                            "status": "error",
                            "message": f"Lỗi thực thi: {widget_title}",
                            "detail": f"SQL: {widget_sql[:100]}... | Error: {str(e)}",
                        }
                    )

            result_data = {
                "success": True,
                "prompt": request.prompt,
                "summary": spec.get("summary", "Đã phân tích xong dữ liệu."),
                "thought_process": spec.get("thought_process", []),
                "is_dashboard": spec.get("is_dashboard", False),
                "dashboard_title": spec.get(
                    "dashboard_title", "Tino Data Analytics Dashboard"
                ),
                "widgets": populated_widgets,
            }

            yield sse_event({"type": "result", "data": result_data})

        except Exception as e:
            yield sse_event(
                {
                    "type": "error",
                    "message": f"Lỗi hệ thống: {str(e)}",
                    "detail": str(e),
                }
            )

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
