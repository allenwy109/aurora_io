import React from 'react';
import { Card } from 'antd';
import type { RichComponentData } from '../../types';

const CardComponent: React.FC<{ data: RichComponentData }> = ({ data }) => {
  const { title, content, metadata } = data.data ?? {};

  return (
    <Card
      size="small"
      title={title}
      style={{ margin: '4px 0' }}
      styles={{ body: { padding: 8 } }}
    >
      {content && <div style={{ fontSize: 13, lineHeight: 1.5 }}>{content}</div>}
      {metadata && (
        <div style={{ fontSize: 12, color: '#595959', marginTop: 4 }}>
          {typeof metadata === 'object'
            ? Object.entries(metadata).map(([key, value]) => (
                <span key={key} style={{ marginRight: 8 }}>
                  {key}: {String(value)}
                </span>
              ))
            : String(metadata)}
        </div>
      )}
    </Card>
  );
};

CardComponent.displayName = 'CardComponent';

export default CardComponent;
