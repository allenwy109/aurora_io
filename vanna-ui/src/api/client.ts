import type {
  APIClientConfig,
  ChatRequest,
  ChatResponse,
  ChatStreamChunk,
} from '../types';

/** Default API client configuration */
const DEFAULT_CONFIG: APIClientConfig = {
  sseEndpoint: '/api/vanna/v2/chat_sse',
  wsEndpoint: '/api/vanna/v2/chat_websocket',
  pollEndpoint: '/api/vanna/v2/chat_poll',
  preferredMode: 'sse',
  autoFallback: true,
};

/** Generate a UUID v4 string */
function generateUUID(): string {
  return crypto.randomUUID?.() ??
    'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, (c) => {
      const r = (Math.random() * 16) | 0;
      const v = c === 'x' ? r : (r & 0x3) | 0x8;
      return v.toString(16);
    });
}

/** Build a ChatRequest with auto-generated request_id */
export function createChatRequest(
  message: string,
  conversationId?: string,
  metadata?: Record<string, any>,
): ChatRequest {
  return {
    message,
    conversation_id: conversationId,
    request_id: generateUUID(),
    metadata: metadata ?? {},
  };
}

/**
 * Serialize a ChatRequest to a JSON string ensuring all four required fields
 * (message, conversation_id, request_id, metadata) are always present.
 *
 * - conversation_id defaults to null when not provided
 * - request_id is auto-generated (UUID) if missing
 * - metadata defaults to empty object when not provided
 */
export function serializeChatRequest(request: ChatRequest): string {
  return JSON.stringify({
    message: request.message,
    conversation_id: request.conversation_id ?? null,
    request_id: request.request_id ?? generateUUID(),
    metadata: request.metadata ?? {},
  });
}


/**
 * Parse a single SSE line (after the "data: " prefix) into a ChatStreamChunk.
 * Returns null for the [DONE] sentinel.
 */
export function parseSSEData(data: string): ChatStreamChunk | null {
  const trimmed = data.trim();
  if (trimmed === '[DONE]') return null;
  return JSON.parse(trimmed) as ChatStreamChunk;
}

export class APIClient {
  private config: APIClientConfig;
  private ws: WebSocket | null = null;
  private wsMessageHandler: ((chunk: ChatStreamChunk) => void) | null = null;
  private wsReconnectAttempts = 0;
  private static readonly WS_MAX_RETRIES = 3;

  constructor(config?: Partial<APIClientConfig>) {
    this.config = { ...DEFAULT_CONFIG, ...config };
  }

  /**
   * Send a chat request via SSE (Server-Sent Events).
   *
   * Uses fetch + ReadableStream to consume the SSE stream.
   * Each SSE event line prefixed with "data: " is parsed as JSON.
   * The stream terminates when "data: [DONE]" is received.
   */
  async sendSSE(
    request: ChatRequest,
    onChunk: (chunk: ChatStreamChunk) => void,
    onDone: () => void,
    onError: (error: Error) => void,
  ): Promise<void> {
    try {
      const response = await fetch(this.config.sseEndpoint, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(request),
      });

      if (!response.ok) {
        throw new Error(`SSE request failed: ${response.status} ${response.statusText}`);
      }

      const body = response.body;
      if (!body) {
        throw new Error('Response body is empty');
      }

      const reader = body.getReader();
      const decoder = new TextDecoder();
      let buffer = '';

      while (true) {
        const { done, value } = await reader.read();
        if (done) break;

        buffer += decoder.decode(value, { stream: true });

        // Process complete lines from the buffer
        const lines = buffer.split('\n');
        // Keep the last (potentially incomplete) line in the buffer
        buffer = lines.pop() ?? '';

        for (const line of lines) {
          const trimmedLine = line.trim();
          if (!trimmedLine) continue; // skip empty lines (SSE separators)

          if (trimmedLine.startsWith('data: ') || trimmedLine.startsWith('data:')) {
            const dataContent = trimmedLine.startsWith('data: ')
              ? trimmedLine.slice(6)
              : trimmedLine.slice(5);

            const chunk = parseSSEData(dataContent);
            if (chunk === null) {
              // [DONE] signal received
              onDone();
              return;
            }

            onChunk(chunk);
          }
        }
      }

      // If stream ended without [DONE], still signal completion
      // Process any remaining buffer content
      if (buffer.trim()) {
        const trimmedLine = buffer.trim();
        if (trimmedLine.startsWith('data: ') || trimmedLine.startsWith('data:')) {
          const dataContent = trimmedLine.startsWith('data: ')
            ? trimmedLine.slice(6)
            : trimmedLine.slice(5);

          const chunk = parseSSEData(dataContent);
          if (chunk === null) {
            onDone();
            return;
          }
          onChunk(chunk);
        }
      }

      onDone();
    } catch (error) {
      onError(error instanceof Error ? error : new Error(String(error)));
    }
  }

  /**
   * Connect to the WebSocket endpoint.
   *
   * Returns a promise that resolves when the connection is open.
   * Implements reconnection logic: max 3 retries with increasing delay
   * (1s, 2s, 4s).
   */
  connectWebSocket(): Promise<void> {
    return new Promise<void>((resolve, reject) => {
      const tryConnect = () => {
        // Build ws:// or wss:// URL from the configured endpoint
        const protocol = globalThis.location?.protocol === 'https:' ? 'wss:' : 'ws:';
        const host = globalThis.location?.host ?? 'localhost';
        const url = this.config.wsEndpoint.startsWith('ws')
          ? this.config.wsEndpoint
          : `${protocol}//${host}${this.config.wsEndpoint}`;

        const socket = new WebSocket(url);

        socket.onopen = () => {
          this.ws = socket;
          this.wsReconnectAttempts = 0;
          this.attachSocketListeners(socket);
          resolve();
        };

        socket.onerror = () => {
          // onerror is always followed by onclose, so reconnect logic lives in onclose
        };

        socket.onclose = () => {
          // If we haven't successfully connected yet, attempt reconnect
          if (this.ws !== socket) {
            this.wsReconnectAttempts++;
            if (this.wsReconnectAttempts <= APIClient.WS_MAX_RETRIES) {
              const delay = Math.pow(2, this.wsReconnectAttempts - 1) * 1000; // 1s, 2s, 4s
              setTimeout(tryConnect, delay);
            } else {
              this.wsReconnectAttempts = 0;
              reject(new Error(`WebSocket connection failed after ${APIClient.WS_MAX_RETRIES} retries`));
            }
          }
        };
      };

      tryConnect();
    });
  }

  /**
   * Send a ChatRequest as JSON through the WebSocket connection.
   * Throws if the WebSocket is not connected.
   */
  sendWebSocket(request: ChatRequest): void {
    if (!this.ws || this.ws.readyState !== WebSocket.OPEN) {
      throw new Error('WebSocket is not connected');
    }
    this.ws.send(JSON.stringify(request));
  }

  /**
   * Register a handler for incoming WebSocket messages.
   * Each message is parsed as a ChatStreamChunk JSON.
   */
  onWebSocketMessage(handler: (chunk: ChatStreamChunk) => void): void {
    this.wsMessageHandler = handler;
    // If already connected, attach immediately
    if (this.ws) {
      this.attachSocketListeners(this.ws);
    }
  }

  /**
   * Close the WebSocket connection and clean up.
   */
  disconnectWebSocket(): void {
    if (this.ws) {
      this.ws.onclose = null; // prevent reconnect logic on intentional close
      this.ws.onerror = null;
      this.ws.onmessage = null;
      this.ws.close();
      this.ws = null;
    }
    this.wsMessageHandler = null;
    this.wsReconnectAttempts = 0;
  }

  /**
   * Attach the message listener to a WebSocket instance.
   */
  private attachSocketListeners(socket: WebSocket): void {
    socket.onmessage = (event: MessageEvent) => {
      if (this.wsMessageHandler) {
        try {
          const chunk = JSON.parse(event.data) as ChatStreamChunk;
          this.wsMessageHandler(chunk);
        } catch {
          // Silently ignore unparseable messages
        }
      }
    };
  }

  /**
   * Send a chat request via Polling.
   *
   * POSTs to the poll endpoint and returns the full ChatResponse
   * containing all chunks at once.
   */
  async sendPoll(request: ChatRequest): Promise<ChatResponse> {
    const response = await fetch(this.config.pollEndpoint, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(request),
    });

    if (!response.ok) {
      throw new Error(`Poll request failed: ${response.status} ${response.statusText}`);
    }

    const data: ChatResponse = await response.json();
    return data;
  }

  /**
   * Unified message sending interface with automatic fallback.
   *
   * - If preferredMode is 'sse' (default), tries SSE first.
   *   When SSE fails and autoFallback is true, falls back to Polling.
   * - If preferredMode is 'poll', goes directly to Polling.
   * - If preferredMode is 'websocket', throws (not yet implemented).
   *
   * The fallback poll request uses the identical ChatRequest object
   * to ensure request content consistency.
   */
  async sendMessage(
    request: ChatRequest,
    onChunk: (chunk: ChatStreamChunk) => void,
    onDone: () => void,
    onError: (error: Error) => void,
  ): Promise<void> {
    if (this.config.preferredMode === 'poll') {
      return this.sendViaPoll(request, onChunk, onDone, onError);
    }

    if (this.config.preferredMode === 'websocket') {
      return this.sendViaWebSocket(request, onChunk, onDone, onError);
    }

    // Default: SSE with optional fallback to poll
    let sseFailed = false;
    let sseError: Error | undefined;

    await this.sendSSE(
      request,
      onChunk,
      onDone,
      (error) => {
        sseFailed = true;
        sseError = error;
      },
    );

    if (sseFailed && this.config.autoFallback) {
      // Fallback to polling with the same request object
      return this.sendViaPoll(request, onChunk, onDone, onError);
    }

    if (sseFailed && !this.config.autoFallback) {
      onError(sseError!);
    }
  }

  /**
   * Internal helper: send via WebSocket. Connects if not already connected,
   * registers the chunk handler, sends the request, and waits for a [DONE]-like
   * signal or relies on the caller to manage the lifecycle.
   */
  private async sendViaWebSocket(
    request: ChatRequest,
    onChunk: (chunk: ChatStreamChunk) => void,
    onDone: () => void,
    onError: (error: Error) => void,
  ): Promise<void> {
    try {
      if (!this.ws || this.ws.readyState !== WebSocket.OPEN) {
        await this.connectWebSocket();
      }
      this.onWebSocketMessage(onChunk);
      this.sendWebSocket(request);
      // WebSocket is persistent — signal done immediately after sending.
      // The actual streaming chunks arrive asynchronously via the handler.
      onDone();
    } catch (error) {
      onError(error instanceof Error ? error : new Error(String(error)));
    }
  }

  /**
   * Internal helper: send via poll and replay chunks through the
   * streaming callbacks so callers get a consistent interface.
   */
  private async sendViaPoll(
    request: ChatRequest,
    onChunk: (chunk: ChatStreamChunk) => void,
    onDone: () => void,
    onError: (error: Error) => void,
  ): Promise<void> {
    try {
      const response = await this.sendPoll(request);
      for (const chunk of response.chunks) {
        onChunk(chunk);
      }
      onDone();
    } catch (error) {
      onError(error instanceof Error ? error : new Error(String(error)));
    }
  }
}

export default APIClient;
