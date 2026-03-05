import React from 'react';
import { Alert } from 'antd';
import type { RichComponentData } from '../../types';

const VALID_TYPES = new Set(['success', 'info', 'warning', 'error']);

const NotificationComponent: React.FC<{ data: RichComponentData }> = ({ data }) => {
  const { message, description, type = 'info', closable } = data.data ?? {};
  const alertType = VALID_TYPES.has(type) ? type : 'info';

  return (
    <div style={{ margin: '4px 0' }}>
      <Alert
        message={message ?? 'Notification'}
        description={description}
        type={alertType}
        showIcon
        closable={closable ?? false}
        style={{ padding: 8 }}
      />
    </div>
  );
};

NotificationComponent.displayName = 'NotificationComponent';

export default NotificationComponent;
