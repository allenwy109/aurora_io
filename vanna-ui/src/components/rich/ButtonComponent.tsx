import React, { useCallback, useState } from 'react';
import { Button } from 'antd';
import type { RichComponentData, ButtonData } from '../../types';
import APIClient from '../../api/client';

const variantMap: Record<string, 'primary' | 'default' | 'dashed' | 'text' | 'link'> = {
  primary: 'primary',
  default: 'default',
  dashed: 'dashed',
  text: 'text',
  link: 'link',
};

const ButtonComponent: React.FC<{ data: RichComponentData }> = ({ data }) => {
  const { label, action, variant, disabled, icon } = (data.data ?? {}) as ButtonData;
  const [loading, setLoading] = useState(false);

  const handleClick = useCallback(async () => {
    if (!action) return;
    setLoading(true);
    try {
      const client = new APIClient();
      await client.sendPoll({
        message: action,
        metadata: { source: 'button', component_id: data.id },
      });
    } catch {
      // Silently handle - error will surface through chat flow
    } finally {
      setLoading(false);
    }
  }, [action, data.id]);

  const btnType = variant ? variantMap[variant] ?? 'default' : 'default';

  return (
    <span style={{ margin: '4px 0', display: 'inline-block' }}>
      <Button
        type={btnType}
        disabled={disabled}
        loading={loading}
        onClick={handleClick}
        size="small"
        aria-label={label ?? 'Button'}
      >
        {icon && <span style={{ marginRight: 4 }}>{icon}</span>}
        {label ?? 'Button'}
      </Button>
    </span>
  );
};

ButtonComponent.displayName = 'ButtonComponent';

export default ButtonComponent;
