import React from 'react';
import type { RichComponentData } from '../../types';

const FallbackComponent: React.FC<{ data: RichComponentData }> = ({ data }) => (
  <div
    role="alert"
    style={{
      padding: 8,
      border: '1px dashed #faad14',
      borderRadius: 4,
      margin: '4px 0',
      background: '#fffbe6',
    }}
  >
    <div style={{ fontSize: 12, color: '#ad6800', marginBottom: 4 }}>
      Unknown component: {data.type}
    </div>
    <pre style={{ fontSize: 11, margin: 0, whiteSpace: 'pre-wrap', wordBreak: 'break-all' }}>
      {JSON.stringify(data, null, 2)}
    </pre>
  </div>
);

FallbackComponent.displayName = 'FallbackComponent';

export default FallbackComponent;
