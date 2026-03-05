import * as XLSX from 'xlsx';

export interface ExportOptions {
  filename?: string;
  format: 'csv' | 'xlsx';
  preserveTypes?: boolean;
}

/**
 * Generate a filename with timestamp in the format: {prefix}_{YYYYMMDD}_{HHmmss}
 * Default prefix is 'export'.
 */
export function generateFilename(prefix: string = 'export'): string {
  const now = new Date();
  const yyyy = now.getFullYear();
  const mm = String(now.getMonth() + 1).padStart(2, '0');
  const dd = String(now.getDate()).padStart(2, '0');
  const hh = String(now.getHours()).padStart(2, '0');
  const min = String(now.getMinutes()).padStart(2, '0');
  const ss = String(now.getSeconds()).padStart(2, '0');
  return `${prefix}_${yyyy}${mm}${dd}_${hh}${min}${ss}`;
}

/**
 * Escape a CSV cell value. Wraps in double quotes if the value contains
 * commas, double quotes, or newlines. Internal double quotes are doubled.
 */
function escapeCSVValue(value: any): string {
  const str = value === null || value === undefined ? '' : String(value);
  if (str.includes(',') || str.includes('"') || str.includes('\n') || str.includes('\r')) {
    return `"${str.replace(/"/g, '""')}"`;
  }
  return str;
}

/**
 * Build a CSV string from columns and rows (pure function, no DOM needed).
 */
export function buildCSVString(columns: string[], rows: Record<string, any>[]): string {
  const header = columns.map(escapeCSVValue).join(',');
  const dataRows = rows.map((row) =>
    columns.map((col) => escapeCSVValue(row[col])).join(',')
  );
  return [header, ...dataRows].join('\n');
}

/**
 * Export data to CSV: builds the CSV string and triggers a browser download.
 */
export function exportToCSV(
  columns: string[],
  rows: Record<string, any>[],
  options?: ExportOptions
): void {
  const csv = buildCSVString(columns, rows);
  const filename = options?.filename ?? `${generateFilename()}.csv`;

  const blob = new Blob([csv], { type: 'text/csv;charset=utf-8;' });
  const url = URL.createObjectURL(blob);
  const link = document.createElement('a');
  link.href = url;
  link.download = filename;
  document.body.appendChild(link);
  link.click();
  document.body.removeChild(link);
  URL.revokeObjectURL(url);
}


/**
 * Build a workbook from columns, rows, and optional column types.
 * Exported for testing without triggering file download.
 */
export function buildWorkbook(
  columns: string[],
  rows: Record<string, any>[],
  columnTypes?: Record<string, string>
): XLSX.WorkBook {
  const wb = XLSX.utils.book_new();

  // Build array-of-arrays with header
  const aoa: any[][] = [columns];
  for (const row of rows) {
    const rowArr = columns.map((col) => {
      const value = row[col];
      if (value === null || value === undefined) return '';

      const colType = columnTypes?.[col];

      // Preserve number types
      if (colType === 'number') {
        const num = typeof value === 'number' ? value : Number(value);
        return Number.isNaN(num) ? String(value) : num;
      }

      // Preserve date types
      if (colType === 'date') {
        const date = value instanceof Date ? value : new Date(value);
        return isNaN(date.getTime()) ? String(value) : date;
      }

      return value;
    });
    aoa.push(rowArr);
  }

  const ws = XLSX.utils.aoa_to_sheet(aoa, { cellDates: true });

  // Set date format for date columns
  if (columnTypes) {
    const dateColIndices = columns
      .map((col, i) => (columnTypes[col] === 'date' ? i : -1))
      .filter((i) => i >= 0);

    if (dateColIndices.length > 0) {
      for (let r = 1; r <= rows.length; r++) {
        for (const c of dateColIndices) {
          const cellRef = XLSX.utils.encode_cell({ r, c });
          const cell = ws[cellRef];
          if (cell && cell.t === 'd') {
            cell.z = 'yyyy-mm-dd';
          }
        }
      }
    }
  }

  XLSX.utils.book_append_sheet(wb, ws, 'Sheet1');
  return wb;
}

/**
 * Export data to Excel (.xlsx) using SheetJS.
 * Preserves data types: numbers stay as numbers, dates as dates.
 */
export function exportToExcel(
  columns: string[],
  rows: Record<string, any>[],
  columnTypes?: Record<string, string>,
  options?: ExportOptions
): void {
  const wb = buildWorkbook(columns, rows, columnTypes);
  const filename = options?.filename ?? `${generateFilename()}.xlsx`;
  XLSX.writeFile(wb, filename);
}
