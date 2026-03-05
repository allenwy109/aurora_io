import React, { useMemo, useState } from 'react';
import { Table, Input, Button, Space, Typography } from 'antd';
import { DownloadOutlined, SearchOutlined } from '@ant-design/icons';
import type { ColumnsType } from 'antd/es/table';
import type { RichComponentData, DataFrameData } from '../../types';
import { formatCell, getAlignment } from '../../utils/format';
import { exportToCSV } from '../../utils/export';

const { Text } = Typography;

interface ResultTableProps {
  data: RichComponentData;
}

const VIRTUAL_SCROLL_THRESHOLD = 50;

const ResultTable: React.FC<ResultTableProps> = ({ data }) => {
  const dfData = data.data as DataFrameData;
  const [searchText, setSearchText] = useState('');

  const columns: ColumnsType<Record<string, any>> = useMemo(() => {
    return dfData.columns.map((col) => {
      const colType = dfData.column_types?.[col] ?? 'text';
      const align = getAlignment(colType);

      const column: ColumnsType<Record<string, any>>[number] = {
        title: col,
        dataIndex: col,
        key: col,
        align,
        ellipsis: true,
        render: (value: any) => formatCell(value, colType),
      };

      if (dfData.sortable) {
        column.sorter = (a: Record<string, any>, b: Record<string, any>) => {
          const va = a[col];
          const vb = b[col];
          if (va == null && vb == null) return 0;
          if (va == null) return -1;
          if (vb == null) return 1;
          if (colType === 'number') {
            return Number(va) - Number(vb);
          }
          return String(va).localeCompare(String(vb));
        };
      }

      return column;
    });
  }, [dfData.columns, dfData.column_types, dfData.sortable]);

  const filteredData = useMemo(() => {
    const rows = dfData.data ?? [];
    if (!searchText.trim()) return rows;
    const keyword = searchText.toLowerCase();
    return rows.filter((row) =>
      dfData.columns.some((col) => {
        const val = row[col];
        return val != null && String(val).toLowerCase().includes(keyword);
      })
    );
  }, [dfData.data, dfData.columns, searchText]);

  const handleExport = () => {
    exportToCSV(dfData.columns, filteredData);
  };

  const useVirtualScroll = filteredData.length > VIRTUAL_SCROLL_THRESHOLD;

  return (
    <div style={{ margin: '4px 0' }}>
      {dfData.title && (
        <Text strong style={{ display: 'block', marginBottom: 4, fontSize: 13 }}>
          {dfData.title}
        </Text>
      )}
      {dfData.description && (
        <Text type="secondary" style={{ display: 'block', marginBottom: 4, fontSize: 12 }}>
          {dfData.description}
        </Text>
      )}

      <Space style={{ marginBottom: 8 }} size={8}>
        {dfData.searchable && (
          <Input
            placeholder="搜索..."
            prefix={<SearchOutlined />}
            allowClear
            size="small"
            style={{ width: 200 }}
            value={searchText}
            onChange={(e) => setSearchText(e.target.value)}
            aria-label="Search table data"
          />
        )}
        {dfData.exportable && (
          <Button
            size="small"
            icon={<DownloadOutlined />}
            onClick={handleExport}
            aria-label="Export table data as CSV"
          >
            CSV
          </Button>
        )}
      </Space>

      <Table
        columns={columns}
        dataSource={filteredData}
        rowKey={(_, index) => String(index)}
        size="small"
        bordered
        pagination={
          useVirtualScroll
            ? { pageSize: dfData.page_size || 50, size: 'small', showSizeChanger: false }
            : false
        }
        scroll={useVirtualScroll ? { y: 400 } : undefined}
      />
    </div>
  );
};

export default ResultTable;
