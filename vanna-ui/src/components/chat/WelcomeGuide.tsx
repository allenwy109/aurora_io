import React, { useCallback } from 'react';
import { Typography, Card } from 'antd';
import { MessageOutlined } from '@ant-design/icons';

export interface WelcomeGuideProps {
  onSendExample: (message: string) => void;
}

const exampleQuestions = [
  '显示所有数据库表',
  '查询最近一个月的销售数据',
  '生成销售趋势图表',
  '帮我写一个SQL查询',
];

const WelcomeGuide: React.FC<WelcomeGuideProps> = ({ onSendExample }) => {
  const handleKeyDown = useCallback(
    (q: string) => (e: React.KeyboardEvent) => {
      if (e.key === 'Enter' || e.key === ' ') {
        e.preventDefault();
        onSendExample(q);
      }
    },
    [onSendExample],
  );

  return (
    <div
      style={{
        display: 'flex',
        flexDirection: 'column',
        alignItems: 'center',
        justifyContent: 'center',
        height: '100%',
        padding: 8,
        gap: 8,
      }}
    >
      <MessageOutlined aria-hidden="true" style={{ fontSize: 32, color: '#1677ff', marginBottom: 4 }} />
      <Typography.Title level={4} style={{ margin: 0 }}>
        你好，有什么可以帮你的？
      </Typography.Title>
      <Typography.Text type="secondary" style={{ marginBottom: 4 }}>
        试试以下问题，或直接输入你的问题
      </Typography.Text>
      <div
        role="group"
        aria-label="Example questions"
        style={{
          display: 'flex',
          flexWrap: 'wrap',
          gap: 4,
          justifyContent: 'center',
          maxWidth: 480,
        }}
      >
        {exampleQuestions.map((q) => (
          <Card
            key={q}
            hoverable
            size="small"
            role="button"
            tabIndex={0}
            aria-label={`Send example question: ${q}`}
            onClick={() => onSendExample(q)}
            onKeyDown={handleKeyDown(q)}
            style={{
              cursor: 'pointer',
              padding: 0,
              borderRadius: 6,
            }}
            styles={{ body: { padding: '6px 12px' } }}
          >
            <Typography.Text>{q}</Typography.Text>
          </Card>
        ))}
      </div>
    </div>
  );
};

WelcomeGuide.displayName = 'WelcomeGuide';

export default WelcomeGuide;
