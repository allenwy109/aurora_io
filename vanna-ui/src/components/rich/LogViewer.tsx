import React, { useEffect, useRef, useState, useCallback } from 'react';
import { DownOutlined, RightOutlined } from '@ant-design/icons';
import type { RichComponentData } from '../../types';

const LogViewer: React.FC<{ data: RichComponentData }> = ({ data }) => {
  const { entries = [], title, collapsible = false } = data.data ?? {};
  const [collapsed, setCollapsed] = useState(false);
  const containerRef = useRef<HTMLDivElement>(null);

  const logEntries: string[] = Array.isArray(entries) ? entries.map(String) : [String(entries)];

  // Auto-scroll to bottom when entries change
  useEffect(() => {
    if (!collapsed && containerRef.current) {
      containerRef.current.scrollTop = containerRef.current.scrollHeight;
    }
  }, [logEntries.length, collapsed]);

  const toggleCollapse = useCallback(() => setCollapsed((prev) => !prev), []);

  return (
    <div
      style={{
        border: '1px solid #e8e8e8',
        borderRadius: 4,
        margin: '4px 0',
        overflow: 'hidden',
        fontSize: 12,
      }}
    >
      {/* Header */}
      <div
        style={{
          display: 'flex',
          alignItems: 'center',
          padding: '4px 8px',
          background: '#fafafa',
          borderBottom: collapsed ? 'none' : '1px solid #e8e8e8',
          gap: 4,
        }}
      >
        {collapsible && (
          <button
            onClick={toggleCollapse}
            aria-label={collapsed ? 'Expand logs' : 'Collapse logs'}
            aria-expanded={!collapsed}
            style={{
              background: 'none',
              border: 'none',
              cursor: 'pointer',
              padding: 0,
              display: 'flex',
              alignItems: 'center',
              color: '#595959',
            }}
          >
            {collapsed ? <RightOutlined style={{ fontSize: 10 }} /> : <DownOutlined style={{ fontSize: 10 }} />}
          </button>
        )}
        <span style={{ color: '#595959' }}>{title ?? 'Logs'}</span>
        <span style={{ color: '#595959', marginLeft: 'auto' }}>{logEntries.length} entries</span>
      </div>

      {/* Log entries */}
      {!collapsed && (
        <div
          ref={containerRef}
          role="log"
          aria-label={title ? `${title} log entries` : 'Log entries'}
          style={{
            maxHeight: 200,
            overflow: 'auto',
            padding: 8,
            background: '#1e1e1e',
            fontFamily: "'SFMono-Regular', Consolas, 'Liberation Mono', Menlo, monospace",
            lineHeight: 1.4,
          }}
        >
          {logEntries.map((entry, i) => (
            <div key={i} style={{ color: '#d4d4d4', whiteSpace: 'pre-wrap', wordBreak: 'break-all' }}>
              {entry}
            </div>
          ))}
        </div>
      )}
    </div>
  );
};

LogViewer.displayName = 'LogViewer';

export default LogViewer;
