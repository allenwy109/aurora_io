import React, { useState, useMemo, useCallback } from 'react';
import Prism from 'prismjs';
import 'prismjs/components/prism-sql';
import { CopyOutlined, CheckOutlined, DownOutlined, RightOutlined } from '@ant-design/icons';
import type { RichComponentData } from '../../types';

const LINE_THRESHOLD = 10;

const SQLViewer: React.FC<{ data: RichComponentData }> = ({ data }) => {
  const sql = (data.data?.content as string) ?? '';
  const lineCount = sql.split('\n').length;
  const [collapsed, setCollapsed] = useState(lineCount > LINE_THRESHOLD);
  const [copied, setCopied] = useState(false);

  const highlightedHTML = useMemo(() => {
    if (!sql) return '';
    return Prism.highlight(sql, Prism.languages.sql, 'sql');
  }, [sql]);

  const handleCopy = useCallback(async () => {
    try {
      await navigator.clipboard.writeText(sql);
      setCopied(true);
      setTimeout(() => setCopied(false), 2000);
    } catch {
      // Fallback: ignore if clipboard API unavailable
    }
  }, [sql]);

  const toggleCollapse = useCallback(() => {
    setCollapsed((prev) => !prev);
  }, []);

  return (
    <div
      style={{
        border: '1px solid #e8e8e8',
        borderRadius: 4,
        margin: '4px 0',
        overflow: 'hidden',
        fontSize: 13,
      }}
    >
      {/* Header bar */}
      <div
        style={{
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'space-between',
          padding: '4px 8px',
          background: '#fafafa',
          borderBottom: '1px solid #e8e8e8',
        }}
      >
        <div style={{ display: 'flex', alignItems: 'center', gap: 4 }}>
          {lineCount > LINE_THRESHOLD && (
            <button
              onClick={toggleCollapse}
              aria-label={collapsed ? 'Expand SQL' : 'Collapse SQL'}
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
              {collapsed ? <RightOutlined style={{ fontSize: 11 }} /> : <DownOutlined style={{ fontSize: 11 }} />}
            </button>
          )}
          <span style={{ color: '#595959', fontSize: 12 }}>SQL</span>
          {collapsed && (
            <button
              onClick={toggleCollapse}
              style={{
                background: 'none',
                border: 'none',
                cursor: 'pointer',
                padding: 0,
                color: '#1677ff',
                fontSize: 12,
                marginLeft: 4,
              }}
              aria-label="Expand SQL"
            >
              展开查看完整 SQL ({lineCount} 行)
            </button>
          )}
        </div>
        <button
          onClick={handleCopy}
          aria-label="Copy SQL"
          style={{
            background: 'none',
            border: 'none',
            cursor: 'pointer',
            padding: '2px 4px',
            display: 'flex',
            alignItems: 'center',
            gap: 4,
            color: copied ? '#52c41a' : '#595959',
            fontSize: 12,
          }}
        >
          {copied ? <CheckOutlined style={{ fontSize: 12 }} /> : <CopyOutlined style={{ fontSize: 12 }} />}
          {copied ? '已复制' : '复制'}
        </button>
      </div>

      {/* Code block */}
      {!collapsed && (
        <pre
          style={{
            margin: 0,
            padding: 8,
            overflow: 'auto',
            background: '#f6f8fa',
            fontFamily: "'SFMono-Regular', Consolas, 'Liberation Mono', Menlo, monospace",
            fontSize: 13,
            lineHeight: 1.5,
          }}
        >
          <code
            className="language-sql"
            dangerouslySetInnerHTML={{ __html: highlightedHTML }}
          />
        </pre>
      )}
    </div>
  );
};

SQLViewer.displayName = 'SQLViewer';

export default SQLViewer;
