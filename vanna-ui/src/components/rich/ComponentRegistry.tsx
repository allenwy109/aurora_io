import React from 'react';
import type { RichComponentData } from '../../types';
import TextComponent from './TextComponent';
import FallbackComponent from './FallbackComponent';
import SQLViewer from './SQLViewer';
import ResultTable from './ResultTable';
import ChartRenderer from './ChartRenderer';
import CardComponent from './CardComponent';
import ProgressBar from './ProgressBar';
import NotificationComponent from './NotificationComponent';
import LogViewer from './LogViewer';
import BadgeComponent from './BadgeComponent';
import ButtonComponent from './ButtonComponent';
import ArtifactComponent from './ArtifactComponent';

// Component renderer type
type ComponentRenderer = React.FC<{ data: RichComponentData }>;

// Placeholder renderer factory for components not yet implemented
const createPlaceholder = (typeName: string): ComponentRenderer => {
  const Placeholder: ComponentRenderer = ({ data }) => (
    <div style={{ padding: 8, border: '1px solid #d9d9d9', borderRadius: 4, margin: '4px 0' }} role="region" aria-label={`${typeName} component`}>
      <div style={{ fontSize: 12, color: '#595959', marginBottom: 4 }}>
        [{typeName}] {data.id}
      </div>
      <pre style={{ fontSize: 11, margin: 0, whiteSpace: 'pre-wrap', wordBreak: 'break-all' }}>
        {JSON.stringify(data.data, null, 2)}
      </pre>
    </div>
  );
  Placeholder.displayName = `${typeName}Placeholder`;
  return Placeholder;
};

// Component type to renderer mapping
export const componentMap: Record<string, ComponentRenderer> = {
  text: TextComponent,
  dataframe: ResultTable,
  chart: ChartRenderer,
  card: CardComponent,
  progress_bar: ProgressBar,
  progress_display: createPlaceholder('ProgressDisplay'),
  notification: NotificationComponent,
  log_viewer: LogViewer,
  badge: BadgeComponent,
  icon_text: createPlaceholder('IconTextComponent'),
  status_indicator: createPlaceholder('StatusIndicator'),
  button: ButtonComponent,
  button_group: createPlaceholder('ButtonGroupComponent'),
  artifact: ArtifactComponent,
  code_block: SQLViewer,
};

/** Returns the renderer for a given component type, or undefined if not found. */
export function getRenderer(type: string): ComponentRenderer | undefined {
  return componentMap[type];
}

/** Main entry component - dispatches to the correct renderer based on component type and lifecycle. */
const RichComponentRenderer: React.FC<{ component: RichComponentData }> = ({ component }) => {
  // Handle remove lifecycle - render nothing
  if (component.lifecycle === 'remove') {
    return null;
  }

  // Handle visibility
  if (!component.visible) {
    return null;
  }

  const Renderer = componentMap[component.type];

  if (Renderer) {
    return <Renderer data={component} />;
  }

  // Unknown type - use fallback
  return <FallbackComponent data={component} />;
};

export { FallbackComponent };
export default RichComponentRenderer;
