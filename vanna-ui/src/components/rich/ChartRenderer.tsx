import React, { useRef, useCallback, useMemo } from 'react';
import ReactECharts from 'echarts-for-react';
import { Card, Button, Tooltip } from 'antd';
import { DownloadOutlined } from '@ant-design/icons';
import type { RichComponentData, ChartData } from '../../types';
import type { ECharts } from 'echarts';

interface ChartRendererProps {
  data: RichComponentData;
}

/** Build ECharts option object from ChartData. */
function buildOption(chartData: ChartData): Record<string, any> {
  const { chart_type, data, title, config } = chartData;

  // Tooltip trigger differs for pie charts
  const tooltipTrigger = chart_type === 'pie' ? 'item' : 'axis';

  const baseOption: Record<string, any> = {
    tooltip: { trigger: tooltipTrigger },
    ...(title ? { title: { text: title, left: 'center', textStyle: { fontSize: 14 } } } : {}),
  };

  // If data already contains a fully-formed series array, use it directly.
  // Otherwise wrap the data payload as a single series with the correct type.
  if (data && Array.isArray(data.series)) {
    // Ensure each series has the chart_type set
    const series = data.series.map((s: Record<string, any>) => ({
      type: chart_type,
      ...s,
    }));
    const { series: _s, ...rest } = data;
    Object.assign(baseOption, rest, { series });
  } else {
    // Treat the whole data object as a single series config
    baseOption.series = [{ type: chart_type, ...data }];
  }

  // Merge any additional config overrides
  if (config && typeof config === 'object') {
    Object.assign(baseOption, config);
  }

  return baseOption;
}

const ChartRenderer: React.FC<ChartRendererProps> = ({ data }) => {
  const chartRef = useRef<ReactECharts | null>(null);
  const chartData = data.data as unknown as ChartData;

  const option = useMemo(() => buildOption(chartData), [chartData]);

  const handleExportPNG = useCallback(() => {
    const instance: ECharts | undefined = chartRef.current?.getEchartsInstance();
    if (!instance) return;
    const url = instance.getDataURL({ type: 'png', backgroundColor: '#fff' });
    const link = document.createElement('a');
    link.href = url;
    link.download = `${chartData.title || 'chart'}.png`;
    link.click();
  }, [chartData.title]);

  const height = chartData.height ?? 300;
  const width = chartData.width ?? '100%';

  return (
    <Card
      size="small"
      style={{ margin: '4px 0', width: typeof width === 'number' ? width : undefined }}
      styles={{ body: { padding: 8 } }}
      title={chartData.title ? <span style={{ fontSize: 13 }}>{chartData.title}</span> : undefined}
      extra={
        <Tooltip title="导出 PNG">
          <Button
            type="text"
            size="small"
            icon={<DownloadOutlined />}
            onClick={handleExportPNG}
            aria-label="Export chart as PNG"
          />
        </Tooltip>
      }
    >
      <ReactECharts
        ref={chartRef}
        option={option}
        style={{ height: typeof height === 'number' ? height : height, width: '100%' }}
        opts={{ renderer: 'canvas' }}
        notMerge
        aria-label={chartData.title ? `Chart: ${chartData.title}` : 'Data chart'}
      />
    </Card>
  );
};

export default ChartRenderer;
