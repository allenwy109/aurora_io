import React, { useCallback, useEffect, useRef, useState } from 'react';
import { Button } from 'antd';
import { VerticalAlignBottomOutlined } from '@ant-design/icons';
import ChatInput from './ChatInput';
import MessageBubble from './MessageBubble';
import WelcomeGuide from './WelcomeGuide';
import { useChatStore } from '../../stores/chatStore';
import APIClient, { createChatRequest } from '../../api/client';

export interface ChatPanelProps {
  conversationId?: string;
}

const apiClient = new APIClient();

function generateId(): string {
  return crypto.randomUUID?.() ??
    `msg-${Date.now()}-${Math.random().toString(36).slice(2, 9)}`;
}

const ChatPanel: React.FC<ChatPanelProps> = ({ conversationId }) => {
  const {
    messages,
    isLoading,
    addMessage,
    appendComponent,
    updateMessage,
    setLoading,
    setError,
  } = useChatStore();

  const messagesEndRef = useRef<HTMLDivElement>(null);
  const scrollContainerRef = useRef<HTMLDivElement>(null);
  const [showScrollBtn, setShowScrollBtn] = useState(false);
  const isNearBottomRef = useRef(true);

  const scrollToBottom = useCallback(() => {
    if (typeof messagesEndRef.current?.scrollIntoView === 'function') {
      messagesEndRef.current.scrollIntoView({ behavior: 'smooth' });
    }
  }, []);

  // Auto-scroll only when user is near the bottom
  useEffect(() => {
    if (isNearBottomRef.current) {
      scrollToBottom();
    }
  }, [messages, scrollToBottom]);

  // Track scroll position to show/hide "back to bottom" button
  const handleScroll = useCallback(() => {
    const el = scrollContainerRef.current;
    if (!el) return;
    const distanceFromBottom = el.scrollHeight - el.scrollTop - el.clientHeight;
    const nearBottom = distanceFromBottom <= 100;
    isNearBottomRef.current = nearBottom;
    setShowScrollBtn(!nearBottom);
  }, []);

  const handleSend = useCallback(
    (text: string) => {
      // Add user message
      const userMsg = {
        id: generateId(),
        role: 'user' as const,
        content: text,
        components: [],
        timestamp: Date.now(),
        status: 'done' as const,
      };
      addMessage(userMsg);

      // Add placeholder assistant message
      const assistantId = generateId();
      addMessage({
        id: assistantId,
        role: 'assistant',
        components: [],
        timestamp: Date.now(),
        status: 'streaming',
      });

      setLoading(true);

      const request = createChatRequest(text, conversationId);

      apiClient.sendMessage(
        request,
        (chunk) => {
          appendComponent(assistantId, chunk.rich);
        },
        () => {
          updateMessage(assistantId, { status: 'done' });
          setLoading(false);
        },
        (error) => {
          setError(assistantId, error.message || 'Unknown error');
          setLoading(false);
        },
      );
    },
    [conversationId, addMessage, appendComponent, updateMessage, setLoading, setError],
  );

  const handleRetry = useCallback(
    (messageId: string) => {
      // Find the user message that preceded the failed assistant message
      const idx = messages.findIndex((m) => m.id === messageId);
      if (idx <= 0) return;
      const prevMsg = messages[idx - 1];
      if (prevMsg.role !== 'user' || !prevMsg.content) return;
      handleSend(prevMsg.content);
    },
    [messages, handleSend],
  );

  return (
    <div
      style={{
        display: 'flex',
        flexDirection: 'column',
        height: '100%',
        padding: 8,
        position: 'relative',
      }}
    >
      <div
        ref={scrollContainerRef}
        onScroll={handleScroll}
        role="log"
        aria-label="Chat messages"
        aria-live="polite"
        style={{
          flex: 1,
          overflowY: 'auto',
          padding: '0 8px',
        }}
      >
        {messages.length === 0 ? (
          <WelcomeGuide onSendExample={handleSend} />
        ) : (
          messages.map((msg) => (
            <MessageBubble key={msg.id} message={msg} onRetry={handleRetry} />
          ))
        )}
        <div ref={messagesEndRef} />
      </div>

      {showScrollBtn && (
        <Button
          shape="circle"
          icon={<VerticalAlignBottomOutlined />}
          onClick={scrollToBottom}
          aria-label="Scroll to bottom"
          style={{
            position: 'absolute',
            bottom: 72,
            right: 24,
            zIndex: 10,
            boxShadow: '0 2px 8px rgba(0,0,0,0.15)',
          }}
        />
      )}

      <ChatInput onSend={handleSend} isLoading={isLoading} />
    </div>
  );
};

ChatPanel.displayName = 'ChatPanel';

export default ChatPanel;
