# 实现计划：Vanna UI 重写

## 概述

基于 React + TypeScript + Vite 技术栈，从零构建 Vanna 前端 UI 单页应用。采用增量式开发策略：先搭建项目骨架和核心类型，再逐步实现通信层、状态管理、UI 组件，最后集成联调并清理旧代码。

## 任务列表

- [x] 1. 项目初始化与基础架构
  - [x] 1.1 使用 Vite 创建 React + TypeScript 项目，安装核心依赖
    - 在项目根目录创建 `vanna-ui/` 目录
    - 使用 Vite 初始化项目：`npm create vite@latest vanna-ui -- --template react-ts`
    - 安装依赖：`antd`, `@ant-design/icons`, `echarts`, `echarts-for-react`, `zustand`, `xlsx`, `react-router-dom`, `react-window`, `prismjs`, `react-markdown`, `remark-gfm`
    - 安装开发依赖：`vitest`, `@testing-library/react`, `@testing-library/jest-dom`, `jsdom`, `fast-check`
    - 配置 `vitest.config.ts`，设置 jsdom 环境和 globals
    - _需求: 12.1_

  - [x] 1.2 定义核心 TypeScript 类型 (`src/types/index.ts`)
    - 定义 `RichComponentData`、`ComponentType`、`ChatStreamChunk`、`ChatRequest`、`ChatResponse` 接口
    - 定义 `Message`、`Conversation`、`TransportMode`、`APIClientConfig` 接口
    - 定义 `DataFrameData`、`ChartData`、`ArtifactData`、`ButtonData`、`ComponentUpdateData` 接口
    - 定义 `DatabaseConfig`、`LLMConfig`、`UIPreferences`、`AppSettings` 接口
    - 所有类型需与设计文档中的数据模型完全对应
    - _需求: 2.4, 2.5, 10.1-10.12_

  - [x] 1.3 创建 App 入口和路由配置 (`src/App.tsx`, `src/main.tsx`)
    - 配置 React Router，定义 `/` (聊天页面) 和 `/settings` (设置页面) 路由
    - 在 App 中集成 Ant Design ConfigProvider，配置紧凑主题
    - 创建基础布局骨架（侧边栏 + 主内容区）
    - _需求: 9.4, 9.5, 8.6_

- [x] 2. API 通信层实现
  - [x] 2.1 实现 API_Client SSE 通信 (`src/api/client.ts`)
    - 实现 `sendSSE` 方法，使用 fetch + ReadableStream 处理 SSE 流
    - 正确解析 `data:` 前缀的 SSE 事件数据
    - 处理 `[DONE]` 终止信号
    - 处理 `component_update` 类型的增量更新消息
    - _需求: 2.1, 2.7, 2.8_

  - [x] 2.2 实现 API_Client Polling 通信
    - 实现 `sendPoll` 方法，发送 POST 请求到 `/api/vanna/v2/chat_poll`
    - 解析 `ChatResponse` 响应，提取所有 chunks
    - _需求: 2.3_

  - [x] 2.3 实现 SSE 降级到 Polling 的自动切换逻辑
    - 在 `sendMessage` 统一接口中，SSE 失败时自动降级到 Polling
    - 确保降级后的请求内容与原始请求一致
    - _需求: 2.6_

  - [x] 2.4 实现 API_Client WebSocket 通信
    - 实现 `connectWebSocket`、`sendWebSocket`、`onWebSocketMessage`、`disconnectWebSocket` 方法
    - 处理 WebSocket 连接/断开/重连逻辑
    - _需求: 2.2_

  - [x] 2.5 实现 ChatRequest 序列化
    - 确保序列化后的 JSON 包含 message、conversation_id、request_id、metadata 四个字段
    - 自动生成 request_id（UUID）
    - _需求: 2.4_

  - [x] 2.6 编写 API_Client 属性测试
    - **Property 3: ChatRequest 序列化完整性**
    - **验证: 需求 2.4**
    - **Property 4: ChatStreamChunk 解析往返**
    - **验证: 需求 2.5, 2.1, 2.2, 2.3**
    - **Property 5: SSE 降级到 Polling**
    - **验证: 需求 2.6**

- [x] 3. 状态管理层实现
  - [x] 3.1 实现聊天状态管理 (`src/stores/chatStore.ts`)
    - 使用 Zustand 创建 chatStore，管理当前消息列表和加载状态
    - 实现 addMessage、updateMessage、appendComponent 方法
    - 处理流式消息追加逻辑：每个 chunk 到达时追加到 components 数组
    - 处理错误状态设置：错误时设置 status='error' 并保留错误详情
    - _需求: 1.2, 1.5_

  - [x] 3.2 编写聊天状态属性测试
    - **Property 1: 流式消息追加**
    - **验证: 需求 1.2**
    - **Property 2: 错误消息保留**
    - **验证: 需求 1.5**

  - [x] 3.3 实现对话管理状态 (`src/stores/conversationStore.ts`)
    - 使用 Zustand 创建 conversationStore，管理对话列表和活跃对话
    - 实现 createConversation、deleteConversation、switchConversation 方法
    - 实现 generateTitle 方法：基于第一条用户消息生成标题
    - 使用 Zustand persist middleware 持久化到 localStorage
    - _需求: 7.1, 7.2, 7.3, 7.4, 7.5_

  - [x] 3.4 编写对话管理属性测试
    - **Property 15: 对话创建与删除往返**
    - **验证: 需求 7.2, 7.4**
    - **Property 16: 对话切换加载完整性**
    - **验证: 需求 7.3**
    - **Property 17: 对话标题自动生成**
    - **验证: 需求 7.5**

  - [x] 3.5 实现主题状态管理 (`src/stores/themeStore.ts`)
    - 使用 Zustand 创建 themeStore，管理 light/dark 主题模式
    - 实现 toggle 和 setMode 方法
    - 使用 persist middleware 将主题偏好保存到 localStorage
    - _需求: 9.1, 9.2, 9.3_

  - [x] 3.6 编写主题状态属性测试
    - **Property 20: 主题偏好持久化往返**
    - **验证: 需求 9.3**

  - [x] 3.7 实现设置状态管理 (`src/stores/settingsStore.ts`)
    - 使用 Zustand 创建 settingsStore，管理数据库、LLM、UI 偏好配置
    - 实现 updateDatabase、updateLLM、updateUI 方法
    - 实现 validateSettings 方法：验证配置有效性，返回具体字段错误
    - 实现 saveSettings 方法：保存到 localStorage
    - 预留 testConnection 接口
    - _需求: 8.1, 8.2, 8.3, 8.5_

  - [x] 3.8 编写设置状态属性测试
    - **Property 18: 设置验证与持久化往返**
    - **验证: 需求 8.3**
    - **Property 19: 无效配置错误定位**
    - **验证: 需求 8.5**

- [x] 4. 检查点 - 确保所有测试通过
  - 确保所有测试通过，如有问题请询问用户。

- [x] 5. 工具函数与导出模块
  - [x] 5.1 实现数据格式化工具 (`src/utils/format.ts`)
    - 实现 formatCell 函数：根据列类型（number/text/date）格式化单元格值
    - 实现 getAlignment 函数：数字右对齐、文本左对齐、日期左对齐
    - _需求: 4.5_

  - [x] 5.2 编写格式化工具属性测试
    - **Property 10: 单元格格式化一致性**
    - **验证: 需求 4.5**

  - [x] 5.3 实现导出工具 (`src/utils/export.ts`)
    - 实现 exportToCSV 函数：生成 CSV 字符串并触发下载
    - 实现 exportToExcel 函数：使用 SheetJS 生成 xlsx 文件，保留数据类型
    - 实现 generateFilename 函数：生成 `{prefix}_{YYYYMMDD}_{HHmmss}` 格式文件名
    - _需求: 6.1, 6.2, 6.3, 6.4_

  - [x] 5.4 编写导出工具属性测试
    - **Property 12: CSV 导出数据完整性**
    - **验证: 需求 6.1**
    - **Property 13: Excel 导出数据类型保留**
    - **验证: 需求 6.2, 6.4**
    - **Property 14: 导出文件名时间戳**
    - **验证: 需求 6.3**

- [x] 6. Rich Component 渲染系统
  - [x] 6.1 实现 ComponentRegistry (`src/components/rich/ComponentRegistry.tsx`)
    - 创建组件类型到渲染器的映射表 componentMap
    - 实现 RichComponentRenderer 入口组件，根据 type 分发到对应渲染器
    - 处理 lifecycle 字段（create/update/replace/remove）
    - _需求: 10.1-10.9, 10.12_

  - [x] 6.2 实现 TextComponent 和 FallbackComponent
    - TextComponent (`src/components/rich/TextComponent.tsx`)：使用 react-markdown + remark-gfm 渲染 Markdown 文本
    - FallbackComponent (`src/components/rich/FallbackComponent.tsx`)：以紧凑 JSON 格式展示未知组件的原始数据
    - _需求: 10.10, 10.11_

  - [x] 6.3 编写组件注册表属性测试
    - **Property 21: 组件类型映射完整性**
    - **验证: 需求 10.1-10.9**
    - **Property 22: Markdown 文本渲染**
    - **验证: 需求 10.10**
    - **Property 23: 未知组件类型兜底**
    - **验证: 需求 10.11**

  - [x] 6.4 实现 SQLViewer (`src/components/rich/SQLViewer.tsx`)
    - 使用 Prism.js 实现 SQL 语法高亮
    - 实现一键复制到剪贴板功能
    - 实现可折叠面板：超过 10 行自动折叠，提供展开/折叠按钮
    - 紧凑的嵌入式设计
    - _需求: 3.1, 3.2, 3.3, 3.4_

  - [x] 6.5 编写 SQLViewer 属性测试
    - **Property 7: SQL 自动折叠阈值**
    - **验证: 需求 3.4**

  - [x] 6.6 实现 ResultTable (`src/components/rich/ResultTable.tsx`)
    - 使用 Ant Design Table 组件渲染数据表格
    - 实现按列排序功能（升序/降序）
    - 实现关键字搜索过滤功能
    - 超过 50 行使用 react-window 虚拟滚动
    - 根据列类型自动对齐和格式化（调用 format.ts）
    - 提供 CSV 导出快捷按钮
    - _需求: 4.1, 4.2, 4.3, 4.4, 4.5, 4.6_

  - [x] 6.7 编写 ResultTable 属性测试
    - **Property 8: 表格排序正确性**
    - **验证: 需求 4.2**
    - **Property 9: 表格搜索过滤正确性**
    - **验证: 需求 4.3**
    - **Property 24: DataFrameComponent 渲染数据完整性**
    - **验证: 需求 4.1**

  - [x] 6.8 实现 ChartRenderer (`src/components/rich/ChartRenderer.tsx`)
    - 使用 echarts-for-react 渲染图表
    - 支持 bar、line、pie、scatter 四种图表类型
    - 配置 tooltip 悬停数据提示框
    - 实现导出为 PNG 功能
    - 紧凑卡片嵌入样式
    - _需求: 5.1, 5.2, 5.3, 5.4, 5.5_

  - [x] 6.9 编写 ChartRenderer 属性测试
    - **Property 11: 图表类型映射**
    - **验证: 需求 5.2**

  - [x] 6.10 实现其余 Rich Components
    - CardComponent (`src/components/rich/CardComponent.tsx`) - _需求: 10.3_
    - ProgressBar (`src/components/rich/ProgressBar.tsx`) - _需求: 10.4_
    - NotificationComponent (`src/components/rich/NotificationComponent.tsx`) - _需求: 10.5_
    - LogViewer (`src/components/rich/LogViewer.tsx`) - _需求: 10.6_
    - BadgeComponent (`src/components/rich/BadgeComponent.tsx`) - _需求: 10.7_
    - ButtonComponent (`src/components/rich/ButtonComponent.tsx`)，处理点击事件回传后端 - _需求: 10.8_
    - ArtifactComponent (`src/components/rich/ArtifactComponent.tsx`) - _需求: 10.9_

  - [x] 6.11 编写增量更新属性测试
    - **Property 6: 增量更新处理**
    - **验证: 需求 2.8, 10.12**

- [x] 7. 检查点 - 确保所有测试通过
  - 确保所有测试通过，如有问题请询问用户。

- [x] 8. 聊天界面组件
  - [x] 8.1 实现 ChatInput (`src/components/chat/ChatInput.tsx`)
    - 使用 Ant Design Input.TextArea 创建输入框
    - 实现 Enter 发送、Shift+Enter 换行的键盘快捷键
    - 在 AI 处理时显示紧凑的状态指示器（loading spinner）
    - 禁用发送按钮当输入为空或正在加载时
    - _需求: 1.6, 1.4, 12.2_

  - [x] 8.2 实现 MessageBubble (`src/components/chat/MessageBubble.tsx`)
    - 使用紧凑气泡样式区分用户消息和 AI 回复
    - 用户消息显示文本内容
    - AI 回复通过 ComponentRegistry 渲染 Rich Components
    - 错误状态时内联显示错误详情和重试按钮
    - _需求: 1.3, 1.5_

  - [x] 8.3 实现 WelcomeGuide (`src/components/chat/WelcomeGuide.tsx`)
    - 无消息时显示简洁的欢迎引导界面
    - 包含示例问题建议，点击可直接发送
    - _需求: 1.8_

  - [x] 8.4 实现 ChatPanel (`src/components/chat/ChatPanel.tsx`)
    - 组合 ChatInput、MessageBubble、WelcomeGuide 组件
    - 实现消息列表的自动滚动到底部
    - 实现"回到底部"浮动按钮（当用户向上滚动时显示）
    - 连接 chatStore 和 API_Client，处理发送消息和接收流式响应
    - _需求: 1.1, 1.2, 1.7_

- [x] 9. 侧边栏与页面布局
  - [x] 9.1 实现 ConversationSidebar (`src/components/sidebar/ConversationSidebar.tsx`)
    - 可折叠侧边栏，折叠时仅显示图标
    - 展示对话历史列表，显示对话标题和时间
    - 提供"新建对话"按钮
    - 支持选择历史对话和删除对话
    - 提供设置页面入口和主题切换按钮
    - _需求: 7.1, 7.2, 7.3, 7.4, 9.5, 8.6_

  - [x] 9.2 实现 ChatPage (`src/pages/ChatPage.tsx`)
    - 组合 ConversationSidebar 和 ChatPanel
    - 全屏布局，紧凑间距（8px 内边距，4px 元素间距）
    - 连接 conversationStore，处理对话切换逻辑
    - _需求: 9.4, 9.6_

  - [x] 9.3 实现 SettingsPage (`src/pages/SettingsPage.tsx`)
    - 创建 DatabaseConfig 表单组件：服务器地址、数据库名称、认证方式等字段
    - 创建 LLMConfig 表单组件：模型名称、API Key、Base URL 等字段
    - 创建 UIPreferences 组件：主题选择、语言偏好
    - 实现保存按钮和验证逻辑，无效字段高亮显示错误信息
    - 预留"测试连接"按钮
    - _需求: 8.1, 8.2, 8.3, 8.4, 8.5, 8.7_

- [x] 10. 主题集成与全局样式
  - [x] 10.1 集成 Ant Design 主题引擎
    - 在 App.tsx 中通过 ConfigProvider 配置 compactAlgorithm
    - 根据 themeStore 动态切换 darkAlgorithm
    - 确保所有组件在亮色/暗色主题下视觉正确
    - 配置全局紧凑样式：8px 内边距、4px 元素间距
    - _需求: 9.1, 9.2, 9.3, 9.6_

  - [x] 10.2 添加可访问性支持
    - 为所有交互元素添加键盘导航支持
    - 为图标和非文本元素添加 ARIA 标签
    - 确保文本与背景对比度满足 4.5:1 要求
    - _需求: 12.3, 12.4, 12.5_

- [x] 11. 检查点 - 确保所有测试通过
  - 确保所有测试通过，如有问题请询问用户。

- [x] 12. 集成联调与旧代码清理
  - [x] 12.1 配置 Vite 构建输出与后端集成
    - 配置 `vite.config.ts` 的 build.outDir 指向后端可服务的静态文件目录
    - 配置开发代理，将 `/api` 请求代理到 FastAPI 后端
    - 确保生产构建产物可被 FastAPI 正确服务
    - _需求: 11.3_

  - [x] 12.2 更新 `run_vanna.py` 集成新 UI
    - 替换旧 HTML 模板，使用新 UI 的 `index.html` 入口
    - 更新静态文件服务路由，指向 `vanna-ui/dist/` 构建产物
    - 移除对 `vanna_components.js` 的依赖引用
    - _需求: 11.1, 11.2, 11.3_

- [x] 13. 最终检查点 - 确保所有测试通过
  - 确保所有测试通过，如有问题请询问用户。

## 备注

- 标记 `*` 的子任务为可选任务，可跳过以加速 MVP 开发
- 每个任务引用了具体的需求编号，确保可追溯性
- 检查点任务用于阶段性验证，确保增量开发的稳定性
- 属性测试验证设计文档中定义的 24 个正确性属性
- 单元测试和属性测试互补，属性测试覆盖通用规律，单元测试覆盖具体边界情况
