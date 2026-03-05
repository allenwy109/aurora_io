export type ColumnType = 'number' | 'text' | 'date' | string;
export type Alignment = 'left' | 'right' | 'center';

/**
 * Format a cell value based on its column type.
 * - number: toLocaleString() for thousand separators; null/undefined → ''
 * - text: convert to string; null/undefined → ''
 * - date: format as YYYY-MM-DD if valid date, otherwise return as-is string
 * - unknown type: convert to string
 */
export function formatCell(value: any, columnType: ColumnType): string {
  if (value === null || value === undefined) {
    return '';
  }

  switch (columnType) {
    case 'number': {
      const num = typeof value === 'number' ? value : Number(value);
      if (Number.isNaN(num)) {
        return String(value);
      }
      return num.toLocaleString();
    }
    case 'date': {
      const date = value instanceof Date ? value : new Date(value);
      if (isNaN(date.getTime())) {
        return String(value);
      }
      const y = date.getFullYear();
      const m = String(date.getMonth() + 1).padStart(2, '0');
      const d = String(date.getDate()).padStart(2, '0');
      return `${y}-${m}-${d}`;
    }
    case 'text':
      return String(value);
    default:
      return String(value);
  }
}

/**
 * Get the text alignment for a column based on its type.
 * - number → 'right'
 * - text → 'left'
 * - date → 'left'
 * - unknown → 'left'
 */
export function getAlignment(columnType: ColumnType): Alignment {
  switch (columnType) {
    case 'number':
      return 'right';
    case 'text':
    case 'date':
    default:
      return 'left';
  }
}
