# 需求文档：Vanna UI 重写

## 简介

重写 Vanna 项目的前端 UI，替换现有的 Lit Web Component 聊天界面（`vanna/frontends/webcomponent/` 和 `vanna_components.js`）。新 UI 采用现代化、紧凑的设计风格，保持与现有 FastAPI 后端（SSE/WebSocket/Polling）的完全兼容。新增设置页面，允许用户配置数据库连接和其他参数。当前系统连接 Aurora APAC DEV MSSQL 数据库，使用 MiniMax M2.5 作为 LLM 服务。重写完成后，旧的 Web Component 前端代码将被删除。

## 术语表

- **Vanna_UI**: 重写后的 Vanna 前端用户界面应用，采用现代化紧凑设计
- **Chat_Panel**: 用户与 AI 进行自然语言对话的聊天面板组件
- **SQL_Viewer**: 展示 AI 生成的 SQL 查询语句的代码查看器组件
- **Result_Table**: 展示 SQL 查询结果的数据表格组件
- **Chart_Renderer**: 将查询结果渲染为可视化图表的组件
- **Conversation_Manager**: 管理多轮对话历史和会话切换的模块
- **API_Client**: 与 FastAPI 后端通信的客户端模块，支持 SSE、WebSocket 和 Polling
- **Theme_Engine**: 管理 UI 主题（亮色/暗色）切换的模块
- **Export_Module**: 将查询结果导出为 CSV/Excel 文件的模块
- **Settings_Page**: 用户配置页面，用于管理数据库连接、LLM 参数等系统设置
- **FastAPI_Backend**: 现有的 Vanna FastAPI 服务端，提供 `/api/vanna/v2/chat_sse`、`/api/vanna/v2/chat_websocket`、`/api/vanna/v2/chat_poll` 三个端点
- **Component_Registry**: 负责将后端返回的 Rich Component 映射到前端渲染器的注册表

## 需求

### 需求 1：现代化紧凑聊天界面

**用户故事：** 作为数据分析师，我希望通过一个现代化、紧凑的聊天界面与系统对话查询数据库，以便高效地获取所需数据。

#### 验收标准

1. THE Vanna_UI SHALL 提供一个全屏布局的 Chat_Panel，采用紧凑的间距和现代化的视觉设计
2. WHEN 用户提交问题时，THE Chat_Panel SHALL 以流式方式逐步显示 AI 的回复内容
3. THE Chat_Panel SHALL 使用紧凑的气泡样式区分用户消息和 AI 回复消息
4. WHEN AI 正在处理请求时，THE Chat_Panel SHALL 在输入区域附近显示紧凑的状态指示器
5. IF AI 返回错误信息，THEN THE Chat_Panel SHALL 以内联方式展示错误详情，并提供重试按钮
6. THE Chat_Panel SHALL 支持通过键盘快捷键（Enter 发送，Shift+Enter 换行）提交消息
7. WHEN 用户滚动查看历史消息时，THE Chat_Panel SHALL 显示"回到底部"浮动按钮
8. THE Chat_Panel SHALL 在无消息时显示简洁的欢迎引导界面，包含示例问题建议

### 需求 2：后端通信兼容

**用户故事：** 作为开发者，我希望新 UI 完全兼容现有的 FastAPI 后端 API，以便无需修改后端代码即可使用新界面。

#### 验收标准

1. THE API_Client SHALL 支持通过 SSE（Server-Sent Events）端点 `/api/vanna/v2/chat_sse` 进行流式通信
2. THE API_Client SHALL 支持通过 WebSocket 端点 `/api/vanna/v2/chat_websocket` 进行实时通信
3. THE API_Client SHALL 支持通过 Polling 端点 `/api/vanna/v2/chat_poll` 进行轮询通信
4. THE API_Client SHALL 发送符合 ChatRequest 模型的请求，包含 message、conversation_id、request_id 和 metadata 字段
5. THE API_Client SHALL 正确解析 ChatStreamChunk 响应，提取 rich 组件数据和可选的 simple 组件数据
6. WHEN SSE 连接失败时，THE API_Client SHALL 自动降级到 Polling 模式，确保通信不中断
7. THE API_Client SHALL 正确处理 SSE 流中的 `[DONE]` 终止信号
8. THE API_Client SHALL 支持处理 component_update 类型的增量更新消息

### 需求 3：SQL 查询展示

**用户故事：** 作为数据分析师，我希望清晰地查看 AI 生成的 SQL 查询语句，以便验证查询逻辑的正确性。

#### 验收标准

1. WHEN AI 返回 SQL 查询时，THE SQL_Viewer SHALL 以语法高亮的方式展示 SQL 代码
2. THE SQL_Viewer SHALL 提供一键复制 SQL 语句到剪贴板的功能
3. THE SQL_Viewer SHALL 以紧凑的可折叠面板形式嵌入聊天消息中，默认展开显示
4. WHEN SQL 查询超过 10 行时，THE SQL_Viewer SHALL 自动折叠并显示"展开查看完整 SQL"按钮

### 需求 4：查询结果数据表格

**用户故事：** 作为数据分析师，我希望以表格形式查看查询结果，以便快速浏览和分析数据。

#### 验收标准

1. WHEN 后端返回 DataFrameComponent 时，THE Result_Table SHALL 以紧凑的表格形式展示查询结果数据
2. THE Result_Table SHALL 支持按列排序功能（升序和降序）
3. THE Result_Table SHALL 支持关键字搜索过滤功能
4. WHEN 查询结果超过 50 行时，THE Result_Table SHALL 采用虚拟滚动方式展示，确保页面性能
5. THE Result_Table SHALL 根据数据类型（数字、文本、日期）自动对齐和格式化单元格内容
6. THE Result_Table SHALL 提供将数据导出为 CSV 的快捷按钮

### 需求 5：数据可视化

**用户故事：** 作为数据分析师，我希望将查询结果以图表形式展示，以便直观地理解数据趋势和分布。

#### 验收标准

1. WHEN 后端返回 ChartComponent 时，THE Chart_Renderer SHALL 渲染对应的图表配置
2. THE Chart_Renderer SHALL 支持渲染柱状图、折线图、饼图和散点图
3. WHEN 用户悬停在图表数据点上时，THE Chart_Renderer SHALL 显示详细的数据提示框（tooltip）
4. THE Chart_Renderer SHALL 提供将图表导出为 PNG 图片的功能
5. THE Chart_Renderer SHALL 以紧凑的卡片形式嵌入聊天消息流中

### 需求 6：数据导出

**用户故事：** 作为数据分析师，我希望将查询结果导出为文件，以便在其他工具中进一步分析。

#### 验收标准

1. THE Export_Module SHALL 支持将查询结果导出为 CSV 格式文件
2. THE Export_Module SHALL 支持将查询结果导出为 Excel（.xlsx）格式文件
3. WHEN 用户点击导出按钮时，THE Export_Module SHALL 自动生成包含时间戳的文件名
4. THE Export_Module SHALL 在导出 Excel 文件时保留数据类型格式（数字、日期等）

### 需求 7：对话管理

**用户故事：** 作为用户，我希望管理多个对话会话，以便组织不同主题的查询历史。

#### 验收标准

1. THE Conversation_Manager SHALL 在可折叠的侧边栏中展示对话历史列表
2. WHEN 用户点击"新建对话"按钮时，THE Conversation_Manager SHALL 创建一个新的空白对话会话
3. WHEN 用户选择历史对话时，THE Conversation_Manager SHALL 加载并展示该对话的完整消息记录
4. THE Conversation_Manager SHALL 支持删除指定的历史对话
5. THE Conversation_Manager SHALL 自动为每个对话生成摘要标题，基于对话的第一条用户消息

### 需求 8：设置页面

**用户故事：** 作为用户，我希望通过设置页面配置数据库连接和其他系统参数，以便灵活地切换数据源和调整系统行为。

#### 验收标准

1. THE Settings_Page SHALL 提供数据库连接配置界面，包含服务器地址、数据库名称、认证方式等字段
2. THE Settings_Page SHALL 提供 LLM 服务配置界面，包含模型名称、API Key、Base URL 等字段
3. WHEN 用户修改设置并点击保存时，THE Settings_Page SHALL 验证配置的有效性并持久化保存
4. THE Settings_Page SHALL 提供"测试连接"按钮，允许用户验证数据库连接是否可用
5. IF 配置验证失败，THEN THE Settings_Page SHALL 显示具体的错误信息，指明哪个字段存在问题
6. THE Settings_Page SHALL 通过顶部导航栏或侧边栏中的入口访问，与聊天界面之间可快速切换
7. THE Settings_Page SHALL 提供 UI 偏好设置，包含主题选择（亮色/暗色）和语言偏好

### 需求 9：主题与布局

**用户故事：** 作为用户，我希望界面紧凑现代且支持主题切换，以便在不同环境下舒适地使用。

#### 验收标准

1. THE Theme_Engine SHALL 支持亮色（light）和暗色（dark）两种主题模式
2. WHEN 用户切换主题时，THE Theme_Engine SHALL 即时更新所有 UI 组件的视觉样式
3. THE Theme_Engine SHALL 将用户的主题偏好保存到 localStorage，在下次访问时自动应用
4. THE Vanna_UI SHALL 采用紧凑的全屏布局，最大化利用屏幕空间
5. THE Vanna_UI SHALL 提供可折叠的侧边栏，折叠时仅显示图标，展开时显示完整内容
6. THE Vanna_UI SHALL 采用不超过 8px 的紧凑内边距和 4px 的元素间距作为基础间距单位

### 需求 10：Rich Component 渲染系统

**用户故事：** 作为用户，我希望 UI 能正确渲染后端返回的各种富组件，以便获得丰富的交互体验。

#### 验收标准

1. THE Component_Registry SHALL 正确渲染后端返回的 DataFrameComponent（数据表格组件）
2. THE Component_Registry SHALL 正确渲染后端返回的 ChartComponent（图表组件）
3. THE Component_Registry SHALL 正确渲染后端返回的 CardComponent（卡片组件）
4. THE Component_Registry SHALL 正确渲染后端返回的 ProgressBarComponent 和 ProgressDisplayComponent
5. THE Component_Registry SHALL 正确渲染后端返回的 NotificationComponent（通知组件）
6. THE Component_Registry SHALL 正确渲染后端返回的 LogViewerComponent（日志查看器组件）
7. THE Component_Registry SHALL 正确渲染后端返回的 BadgeComponent、IconTextComponent 和 StatusIndicatorComponent
8. THE Component_Registry SHALL 正确渲染后端返回的 ButtonComponent 和 ButtonGroupComponent，并处理用户点击事件回传给后端
9. THE Component_Registry SHALL 正确渲染后端返回的 ArtifactComponent（代码/内容制品组件）
10. THE Component_Registry SHALL 支持 TextComponent 的 Markdown 渲染
11. IF 后端返回未知类型的组件，THEN THE Component_Registry SHALL 以紧凑的 JSON 格式展示原始数据，避免渲染失败
12. THE Component_Registry SHALL 支持组件的增量更新（update lifecycle），无需重新创建整个组件

### 需求 11：删除旧 UI 代码

**用户故事：** 作为开发者，我希望在新 UI 完成后删除旧的 Web Component 前端代码，以便保持代码库整洁。

#### 验收标准

1. WHEN 新 UI 开发完成后，THE Vanna_UI SHALL 替换 `run_vanna.py` 中的旧 HTML 模板，使用新 UI 的入口页面
2. WHEN 新 UI 开发完成后，THE Vanna_UI SHALL 移除对 `vanna_components.js` 文件的依赖
3. THE Vanna_UI SHALL 更新 `run_vanna.py` 中的静态文件服务路由，指向新 UI 的构建产物

### 需求 12：性能与可访问性

**用户故事：** 作为用户，我希望界面响应迅速且可无障碍使用，以便高效地完成工作。

#### 验收标准

1. WHEN 页面首次加载时，THE Vanna_UI SHALL 在 3 秒内完成首屏渲染（在标准网络条件下）
2. WHEN 用户输入消息时，THE Chat_Panel SHALL 在 100 毫秒内响应用户的键盘输入
3. THE Vanna_UI SHALL 为所有交互元素提供键盘导航支持
4. THE Vanna_UI SHALL 为所有图标和非文本元素提供适当的 ARIA 标签
5. THE Vanna_UI SHALL 确保文本与背景的对比度符合 WCAG 2.1 AA 级标准（对比度至少 4.5:1）
