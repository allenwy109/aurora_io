// 组件类型枚举（与后端 ComponentType 对应）
export type ComponentType =
  | 'text'
  | 'card'
  | 'container'
  | 'status_card'
  | 'progress_display'
  | 'log_viewer'
  | 'badge'
  | 'icon_text'
  | 'task_list'
  | 'progress_bar'
  | 'button'
  | 'button_group'
  | 'table'
  | 'dataframe'
  | 'chart'
  | 'code_block'
  | 'status_indicator'
  | 'notification'
  | 'alert'
  | 'artifact'
  | 'status_bar_update'
  | 'task_tracker_update'
  | 'chat_input_update';

// 基础组件结构
export interface RichComponentData {
  id: string;
  type: ComponentType;
  lifecycle: 'create' | 'update' | 'replace' | 'remove';
  data: Record<string, any>;
  children: string[];
  timestamp: string;
  visible: boolean;
  interactive: boolean;
}

// 简单组件数据（ChatStreamChunk 中的可选字段）
export interface SimpleComponentData {
  type: string;
  content: string;
}

// 聊天请求
export interface ChatRequest {
  message: string;
  conversation_id?: string;
  request_id?: string;
  metadata?: Record<string, any>;
}

// 聊天流式响应块
export interface ChatStreamChunk {
  rich: RichComponentData;
  simple?: SimpleComponentData | null;
  conversation_id: string;
  request_id: string;
  timestamp: number;
}

// 聊天完整响应
export interface ChatResponse {
  chunks: ChatStreamChunk[];
  conversation_id: string;
  request_id: string;
  total_chunks: number;
}

// 传输模式
export type TransportMode = 'sse' | 'websocket' | 'poll';

// API 客户端配置
export interface APIClientConfig {
  sseEndpoint: string;
  wsEndpoint: string;
  pollEndpoint: string;
  preferredMode: TransportMode;
  autoFallback: boolean;
}

// 消息
export interface Message {
  id: string;
  role: 'user' | 'assistant';
  content?: string;
  components: RichComponentData[];
  timestamp: number;
  status: 'sending' | 'streaming' | 'done' | 'error';
  error?: string;
}

// 对话
export interface Conversation {
  id: string;
  title: string;
  messages: Message[];
  createdAt: number;
  updatedAt: number;
}

// DataFrameComponent 数据
export interface DataFrameData {
  data: Record<string, any>[];
  columns: string[];
  title?: string;
  description?: string;
  row_count: number;
  column_count: number;
  searchable: boolean;
  sortable: boolean;
  exportable: boolean;
  column_types: Record<string, string>;
  page_size: number;
}

// ChartComponent 数据
export interface ChartData {
  chart_type: string;
  data: Record<string, any>;
  title?: string;
  width?: string | number;
  height?: string | number;
  config: Record<string, any>;
}

// ArtifactComponent 数据
export interface ArtifactData {
  artifact_type: string;
  content: string;
  title?: string;
  language?: string;
  metadata: Record<string, any>;
}

// ButtonComponent 数据
export interface ButtonData {
  label: string;
  action: string;
  variant?: string;
  disabled?: boolean;
  icon?: string;
}

// ComponentUpdate 数据（增量更新）
export interface ComponentUpdateData {
  type: 'component_update';
  data: {
    operation: 'create' | 'update' | 'replace' | 'remove';
    component_id: string;
    component?: RichComponentData;
  };
}

// 数据库配置
export interface DatabaseConfig {
  server: string;
  database: string;
  authType: 'windows' | 'sql';
  username?: string;
  password?: string;
  driver: string;
}

// LLM 配置
export interface LLMConfig {
  model: string;
  apiKey: string;
  baseUrl: string;
}

// UI 偏好
export interface UIPreferences {
  theme: 'light' | 'dark';
  language: 'zh-CN' | 'en-US';
}

// 应用设置
export interface AppSettings {
  database: DatabaseConfig;
  llm: LLMConfig;
  ui: UIPreferences;
}
