import React from 'react';
import Markdown from 'react-markdown';
import remarkGfm from 'remark-gfm';
import type { RichComponentData } from '../../types';

const TextComponent: React.FC<{ data: RichComponentData }> = ({ data }) => {
  const content = (data.data?.content as string) ?? '';

  return (
    <div style={{ padding: '4px 0', fontSize: 14, lineHeight: 1.6 }}>
      <Markdown remarkPlugins={[remarkGfm]}>{content}</Markdown>
    </div>
  );
};

TextComponent.displayName = 'TextComponent';

export default TextComponent;
