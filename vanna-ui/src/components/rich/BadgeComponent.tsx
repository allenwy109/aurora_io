import React from 'react';
import { Tag } from 'antd';
import type { RichComponentData } from '../../types';

const BadgeComponent: React.FC<{ data: RichComponentData }> = ({ data }) => {
  const { label, color, status, icon } = data.data ?? {};

  // Map status to Ant Design preset colors
  const statusColorMap: Record<string, string> = {
    success: 'green',
    error: 'red',
    warning: 'orange',
    info: 'blue',
    default: 'default',
    processing: 'processing',
  };

  const tagColor = color ?? (status ? statusColorMap[status] : undefined) ?? 'default';

  return (
    <span style={{ margin: '4px 0', display: 'inline-block' }}>
      <Tag color={tagColor}>
        {icon && <span style={{ marginRight: 4 }}>{icon}</span>}
        {label ?? 'Badge'}
      </Tag>
    </span>
  );
};

BadgeComponent.displayName = 'BadgeComponent';

export default BadgeComponent;
