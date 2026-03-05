import React from 'react';
import { Button, Typography } from 'antd';
import { ReloadOutlined } from '@ant-design/icons';
import type { Message, RichComponentData } from '../../types';
import RichComponentRenderer from '../rich/ComponentRegistry';

export interface MessageBubbleProps {
  message: Message;
  onRetry?: (messageId: string) => void;
}

const userBubbleStyle: React.CSSProperties = {
  maxWidth: '70%',
  padding: 8,
  borderRadius: 8,
  background: '#1677ff',
  color: '#fff',
  alignSelf: 'flex-end',
  wordBreak: 'break-word',
};

const assistantBubbleStyle: React.CSSProperties = {
  maxWidth: '85%',
  padding: 8,
  alignSelf: 'flex-start',
};

const MessageBubble: React.FC<MessageBubbleProps> = ({ message, onRetry }) => {
  const isUser = message.role === 'user';

  return (
    <div
      style={{
        display: 'flex',
        flexDirection: 'column',
        alignItems: isUser ? 'flex-end' : 'flex-start',
        marginBottom: 4,
      }}
    >
      {isUser ? (
        <div style={userBubbleStyle}>
          <Typography.Text style={{ color: '#fff', margin: 0 }}>
            {message.content}
          </Typography.Text>
        </div>
      ) : (
        <div style={assistantBubbleStyle}>
          {message.components.map((comp: RichComponentData) => (
            <RichComponentRenderer key={comp.id} component={comp} />
          ))}
        </div>
      )}

      {message.status === 'error' && message.error && (
        <div
          role="alert"
          style={{
            display: 'flex',
            alignItems: 'center',
            gap: 4,
            marginTop: 4,
            padding: '4px 8px',
            borderRadius: 4,
            background: '#fff2f0',
            border: '1px solid #ffccc7',
            fontSize: 12,
            color: '#ff4d4f',
          }}
        >
          <Typography.Text type="danger" style={{ fontSize: 12 }}>
            {message.error}
          </Typography.Text>
          {onRetry && (
            <Button
              type="link"
              size="small"
              danger
              icon={<ReloadOutlined />}
              onClick={() => onRetry(message.id)}
              aria-label="Retry message"
              style={{ padding: '0 4px', height: 'auto', fontSize: 12 }}
            >
              重试
            </Button>
          )}
        </div>
      )}
    </div>
  );
};

MessageBubble.displayName = 'MessageBubble';

export default MessageBubble;
