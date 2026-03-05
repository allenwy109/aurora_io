import React, { useState, useCallback } from 'react';
import { Input, Button, Spin } from 'antd';
import { SendOutlined, LoadingOutlined } from '@ant-design/icons';

export interface ChatInputProps {
  onSend: (message: string) => void;
  isLoading: boolean;
}

const ChatInput: React.FC<ChatInputProps> = ({ onSend, isLoading }) => {
  const [value, setValue] = useState('');

  const handleSend = useCallback(() => {
    const trimmed = value.trim();
    if (!trimmed || isLoading) return;
    onSend(trimmed);
    setValue('');
  }, [value, isLoading, onSend]);

  const handleKeyDown = useCallback(
    (e: React.KeyboardEvent<HTMLTextAreaElement>) => {
      if (e.key === 'Enter' && !e.shiftKey) {
        e.preventDefault();
        handleSend();
      }
    },
    [handleSend],
  );

  const sendDisabled = !value.trim() || isLoading;

  return (
    <div
      style={{
        display: 'flex',
        alignItems: 'flex-end',
        gap: 4,
        padding: 8,
      }}
    >
      <Input.TextArea
        value={value}
        onChange={(e) => setValue(e.target.value)}
        onKeyDown={handleKeyDown}
        placeholder="输入消息..."
        autoSize={{ minRows: 1, maxRows: 6 }}
        disabled={isLoading}
        aria-label="Chat message input"
        style={{ flex: 1 }}
      />
      {isLoading ? (
        <Spin indicator={<LoadingOutlined style={{ fontSize: 16 }} />} size="small" aria-label="AI is processing" />
      ) : (
        <Button
          type="primary"
          icon={<SendOutlined />}
          onClick={handleSend}
          disabled={sendDisabled}
          aria-label="Send message"
        />
      )}
    </div>
  );
};

ChatInput.displayName = 'ChatInput';

export default ChatInput;
