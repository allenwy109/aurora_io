import React, { useState, useMemo, useCallback } from 'react';
import Prism from 'prismjs';
import 'prismjs/components/prism-javascript';
import 'prismjs/components/prism-typescript';
import 'prismjs/components/prism-python';
import 'prismjs/components/prism-sql';
import 'prismjs/components/prism-json';
import { CopyOutlined, CheckOutlined, DownOutlined, RightOutlined } from '@ant-design/icons';
import type { RichComponentData, ArtifactData } from '../../types';

const ArtifactComponent: React.FC<{ data: RichComponentData }> = ({ data }) => {
  const { artifact_type, content = '', title, language, metadata } = (data.data ?? {}) as ArtifactData;
  const [collapsed, setCollapsed] = useState(false);
  const [copied, setCopied] = useState(false);

  const isCode = artifact_type === 'code' || !!language;
  const lang = language ?? 'text';

  const highlightedHTML = useMemo(() => {
    if (!isCode || !content) return '';
    const grammar = Prism.languages[lang];
    if (!grammar) return content.replace(/</g, '&lt;').replace(/>/g, '&gt;');
    return Prism.highlight(content, grammar, lang);
  }, [content, lang, isCode]);

  const handleCopy = useCallback(async () => {
    try {
      await navigator.clipboard.writeText(content);
      setCopied(true);
      setTimeout(() => setCopied(false), 2000);
    } catch {
      // Ignore clipboard errors
    }
  }, [content]);

  const toggleCollapse = useCallback(() => setCollapsed((prev) => !prev), []);

  const displayTitle = title ?? (artifact_type ? `Artifact (${artifact_type})` : 'Artifact');

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
      {/* Header */}
      <div
        style={{
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'space-between',
          padding: '4px 8px',
          background: '#fafafa',
          borderBottom: collapsed ? 'none' : '1px solid #e8e8e8',
        }}
      >
        <div style={{ display: 'flex', alignItems: 'center', gap: 4 }}>
          <button
            onClick={toggleCollapse}
            aria-label={collapsed ? 'Expand artifact' : 'Collapse artifact'}
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
          <span style={{ color: '#595959', fontSize: 12, fontWeight: 500 }}>{displayTitle}</span>
          {language && <span style={{ color: '#595959', fontSize: 11, marginLeft: 4 }}>{language}</span>}
        </div>
        <button
          onClick={handleCopy}
          aria-label="Copy content"
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

      {/* Content */}
      {!collapsed && (
        <div style={{ padding: 8, background: isCode ? '#f6f8fa' : '#fff' }}>
          {isCode ? (
            <pre
              style={{
                margin: 0,
                overflow: 'auto',
                fontFamily: "'SFMono-Regular', Consolas, 'Liberation Mono', Menlo, monospace",
                fontSize: 13,
                lineHeight: 1.5,
              }}
            >
              <code dangerouslySetInnerHTML={{ __html: highlightedHTML }} />
            </pre>
          ) : (
            <div style={{ fontSize: 13, lineHeight: 1.5, whiteSpace: 'pre-wrap' }}>{content}</div>
          )}
          {metadata && Object.keys(metadata).length > 0 && (
            <div style={{ fontSize: 11, color: '#595959', marginTop: 4, borderTop: '1px solid #f0f0f0', paddingTop: 4 }}>
              {Object.entries(metadata).map(([key, value]) => (
                <span key={key} style={{ marginRight: 8 }}>
                  {key}: {String(value)}
                </span>
              ))}
            </div>
          )}
        </div>
      )}
    </div>
  );
};

ArtifactComponent.displayName = 'ArtifactComponent';

export default ArtifactComponent;
