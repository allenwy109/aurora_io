import React from 'react';
import { Progress } from 'antd';
import type { RichComponentData } from '../../types';

const ProgressBar: React.FC<{ data: RichComponentData }> = ({ data }) => {
  const { percent = 0, status, label } = data.data ?? {};

  return (
    <div style={{ padding: '4px 0', margin: '4px 0' }}>
      {label && <div style={{ fontSize: 12, color: '#595959', marginBottom: 4 }}>{label}</div>}
      <Progress
        percent={typeof percent === 'number' ? percent : Number(percent) || 0}
        status={status}
        size="small"
        format={(p) => `${p}%`}
        aria-label={label ? `${label}: ${typeof percent === 'number' ? percent : Number(percent) || 0}%` : `Progress: ${typeof percent === 'number' ? percent : Number(percent) || 0}%`}
      />
    </div>
  );
};

ProgressBar.displayName = 'ProgressBar';

export default ProgressBar;
