"""
启动 Vanna 2.0 Demo 服务器
使用 Mock LLM + Chinook SQLite 数据库，无需 API Key 即可体验。
访问 http://localhost:8000 查看 Web UI
访问 http://localhost:8000/docs 查看 API 文档
"""

import os
import sys

# 确保 vanna src 在 path 中
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "vanna", "src"))

from vanna import Agent, AgentConfig, ToolRegistry, User
from vanna.core.agent.config import UiFeatures
from vanna.core.user import UserResolver, RequestContext
from vanna.integrations.local import MemoryConversationStore
from vanna.integrations.local.agent_memory.in_memory import DemoAgentMemory
from vanna.integrations.openai import OpenAILlmService
from vanna.integrations.mssql import MSSQLRunner
from vanna.integrations.sqlite import SqliteRunner
from vanna.tools import RunSqlTool
from vanna.servers.fastapi.app import VannaFastAPIServer


# --- 使用 Windows 登录名作为用户身份 ---
class WindowsUserResolver(UserResolver):
    async def resolve_user(self, request_context: RequestContext) -> User:
        username = os.getenv("USERNAME", "unknown")
        return User(
            id=username,
            username=username,
            email=f"{username}@aurora.com",
            group_memberships=[],
            permissions=[],
        )


def create_agent() -> Agent:
    # ---- SQL Server 连接 ----
    MSSQL_SERVER = "ANVDEVSQLVPM01"
    MSSQL_DATABASE = "Aurora_APAC_DEV_Output_China"
    odbc_conn_str = (
        "DRIVER={ODBC Driver 17 for SQL Server};"
        f"SERVER={MSSQL_SERVER};"
        f"DATABASE={MSSQL_DATABASE};"
        "Trusted_Connection=Yes"
    )

    # ---- MiniMax (OpenAI 兼容) ----
    api_key = os.getenv("MINIMAX_API_KEY", "sk-api-rHBf73gU6H_6yf3g3BQrFrH_X8S7RV4BshBLc3VrJKELEhKUKgzusmEny6kh-htl6Y8KT6v2-yKCR4suNLCRNKFjEGn5JSgxAUmeu4xTRpzvIEZWySrxHSE")

    # 企业网络 SSL 拦截 workaround：禁用证书验证
    import httpx
    http_client = httpx.Client(verify=False)

    llm = OpenAILlmService(
        model="MiniMax-M2.5",
        api_key=api_key,
        base_url="https://api.minimaxi.com/v1",
        http_client=http_client,
    )

    tools = ToolRegistry()
    tools.register_local_tool(
        RunSqlTool(sql_runner=MSSQLRunner(odbc_conn_str=odbc_conn_str)),
        access_groups=[],
    )

    # 隐藏中间过程，只展示最终结果
    ui_features = UiFeatures(
        feature_group_access={
            "tool_names": ["admin"],
            "tool_arguments": ["admin"],
            "tool_error": ["admin"],
            "tool_invocation_message_in_chat": ["admin"],
            "memory_detailed_results": ["admin"],
        }
    )

    return Agent(
        llm_service=llm,
        tool_registry=tools,
        user_resolver=WindowsUserResolver(),
        agent_memory=DemoAgentMemory(),
        conversation_store=MemoryConversationStore(),
        config=AgentConfig(
            max_tool_iterations=30,
            stream_responses=False,
            include_thinking_indicators=False,
            ui_features=ui_features,
        ),
    )


def main():
    agent = create_agent()
    server = VannaFastAPIServer(agent, config={"cors": {"enabled": True}})
    app = server.create_app()

    # 删掉默认的首页路由（带登录页面的），替换为直接进入聊天
    app.routes[:] = [r for r in app.routes if not (hasattr(r, 'path') and r.path == '/')]

    from fastapi.responses import Response, FileResponse
    from fastapi.staticfiles import StaticFiles

    # Path to the new UI build output
    ui_dist_dir = os.path.join(os.path.dirname(__file__), "vanna-ui", "dist")

    # Serve static assets from vanna-ui/dist/assets/
    app.mount("/assets", StaticFiles(directory=os.path.join(ui_dist_dir, "assets")), name="static-assets")

    @app.get("/favicon.ico")
    async def favicon():
        favicon_path = os.path.join(ui_dist_dir, "vite.svg")
        if os.path.exists(favicon_path):
            return FileResponse(favicon_path, media_type="image/svg+xml")
        return Response(status_code=204)

    @app.get("/", response_class=Response)
    async def index():
        return FileResponse(os.path.join(ui_dist_dir, "index.html"), media_type="text/html")

    print("=" * 55)
    print("  Vanna 2.0 Server (MiniMax M2.5 + Aurora China MSSQL)")
    print("  Web UI:  http://localhost:8000")
    print("  API Doc: http://localhost:8000/docs")
    print("=" * 55)

    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)


if __name__ == "__main__":
    main()
