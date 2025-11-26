from fastapi import FastAPI, Request, Response
import uvicorn
import uuid
import time
import logging
from prometheus_client import Counter, Histogram, generate_latest, CONTENT_TYPE_LATEST
import clickhouse_connect
from datetime import datetime
import json

# 创建FastAPI应用
app = FastAPI(title="可观测性示例应用")

# 初始化ClickHouse客户端
client = clickhouse_connect.get_client(
    host='clickhouse',
    port=8123,
    username='admin',
    password='password'
)

# 创建数据表
def init_tables():
    # 指标表
    client.command('''
        CREATE TABLE IF NOT EXISTS metrics (
            timestamp DateTime64(3),
            trace_id String,
            metric_name String,
            value Float64,
            labels String
        ) ENGINE = MergeTree()
        ORDER BY (timestamp, trace_id, metric_name)
    ''')
    
    # 日志表
    client.command('''
        CREATE TABLE IF NOT EXISTS logs (
            timestamp DateTime64(3),
            trace_id String,
            level String,
            message String,
            service String,
            extra_fields String
        ) ENGINE = MergeTree()
        ORDER BY (timestamp, trace_id, level)
    ''')
    
    # Trace表
    client.command('''
        CREATE TABLE IF NOT EXISTS traces (
            timestamp DateTime64(3),
            trace_id String,
            span_id String,
            parent_span_id String,
            operation_name String,
            duration_ms Float64,
            service_name String,
            tags String
        ) ENGINE = MergeTree()
        ORDER BY (timestamp, trace_id, span_id)
    ''')

# Prometheus指标
REQUEST_COUNT = Counter('http_requests_total', 'Total HTTP Requests', ['method', 'endpoint', 'status'])
REQUEST_DURATION = Histogram('http_request_duration_seconds', 'HTTP Request Duration')

@app.on_event("startup")
async def startup_event():
    init_tables()
    print("可观测性示例应用已启动")

@app.middleware("http")
async def observability_middleware(request: Request, call_next):
    trace_id = str(uuid.uuid4())
    start_time = time.time()
    
    # 记录请求开始日志
    client.insert('logs', [[
        datetime.now(),
        trace_id,
        'INFO',
        f"Starting request: {request.method} {request.url.path}",
        'example-app',
        json.dumps({"method": request.method, "path": str(request.url.path)})
    ]])
    
    try:
        response = await call_next(request)
        duration = time.time() - start_time
        
        # 记录指标到ClickHouse
        labels = {
            "method": request.method, 
            "endpoint": str(request.url.path),
            "status": response.status_code
        }
        client.insert('metrics', [[
            datetime.now(),
            trace_id,
            "http_requests_total",
            1,
            json.dumps(labels)
        ]])
        
        client.insert('metrics', [[
            datetime.now(),
            trace_id,
            "http_request_duration_seconds",
            duration,
            json.dumps(labels)
        ]])
        
        # 记录Trace
        client.insert('traces', [[
            datetime.now(),
            trace_id,
            str(uuid.uuid4()),
            "",
            f"{request.method} {request.url.path}",
            duration * 1000,
            "example-app",
            json.dumps(labels)
        ]])
        
        # 记录请求完成日志
        client.insert('logs', [[
            datetime.now(),
            trace_id,
            'INFO',
            f"Request completed: {response.status_code}",
            'example-app',
            json.dumps({
                "method": request.method, 
                "path": str(request.url.path),
                "status_code": response.status_code,
                "duration": duration
            })
        ]])
        
        # 在响应头中添加trace_id
        response.headers["X-Trace-ID"] = trace_id
        return response
        
    except Exception as e:
        duration = time.time() - start_time
        client.insert('logs', [[
            datetime.now(),
            trace_id,
            'ERROR',
            f"Request failed: {str(e)}",
            'example-app',
            json.dumps({"error": str(e), "duration": duration})
        ]])
        raise

@app.get("/")
async def root(request: Request):
    return {"message": "Hello, Observability!", "trace_id": request.headers.get("X-Trace-ID", "unknown")}

@app.get("/users/{user_id}")
async def get_user(user_id: int, request: Request):
    # 模拟业务处理
    time.sleep(0.1)
    
    client.insert('logs', [[
        datetime.now(),
        request.headers.get("X-Trace-ID", "unknown"),
        'INFO',
        f"Fetching user {user_id}",
        'example-app',
        json.dumps({"user_id": user_id})
    ]])
    
    return {"user_id": user_id, "name": f"User {user_id}", "trace_id": request.headers.get("X-Trace-ID", "unknown")}

@app.get("/search")
async def search(q: str, request: Request):
    # 模拟搜索操作
    time.sleep(0.2)
    
    client.insert('logs', [[
        datetime.now(),
        request.headers.get("X-Trace-ID", "unknown"),
        'INFO',
        f"Searching for: {q}",
        'example-app',
        json.dumps({"query": q})
    ]])
    
    return {"results": [f"Result 1 for {q}", f"Result 2 for {q}"], "trace_id": request.headers.get("X-Trace-ID", "unknown")}

@app.get("/metrics")
async def metrics():
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)

@app.get("/trace/{trace_id}")
async def get_trace_data(trace_id: str):
    """统一查询API - 根据trace_id获取所有相关数据"""
    metrics_data = client.query("SELECT * FROM metrics WHERE trace_id = %s", [trace_id]).result_rows
    logs_data = client.query("SELECT * FROM logs WHERE trace_id = %s", [trace_id]).result_rows
    traces_data = client.query("SELECT * FROM traces WHERE trace_id = %s", [trace_id]).result_rows
    
    return {
        "trace_id": trace_id,
        "metrics": metrics_data,
        "logs": logs_data,
        "traces": traces_data
    }

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
