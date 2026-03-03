import { jsPDF } from 'jspdf';

export function downloadPropertiesPdf(properties, report, filename = 'gesix-data-quality-report.pdf') {
  const doc = new jsPDF({ orientation: 'portrait', unit: 'mm', format: 'a4' });
  const pageW = doc.internal.pageSize.getWidth();
  const margin = 15;
  let y = 20;
  const lineHeight = 7;

  // Title
  doc.setFontSize(18);
  doc.text('Gesix Data Quality Report', margin, y);
  y += lineHeight * 2;

  // Overall score
  if (report) {
    doc.setFontSize(12);
    doc.setFont(undefined, 'bold');
    doc.text(`Overall Trustability: ${report.overall_trustability}%`, margin, y);
    y += lineHeight;
    doc.text(`Total Records: ${report.total_records}`, margin, y);
    y += lineHeight * 1.5;

    // 7 Dimensions
    doc.setFontSize(13);
    doc.text('Quality Dimensions', margin, y);
    y += lineHeight;

    doc.setDrawColor(200, 200, 200);
    doc.line(margin, y - 2, pageW - margin, y - 2);
    y += 4;

    doc.setFontSize(10);
    const dimensions = report.dimensions || {};
    Object.entries(dimensions).forEach(([dim, value]) => {
      const score = typeof value === 'object' ? value.score : value;
      const status = score >= 90 ? 'PASS' : score >= 70 ? 'WARN' : 'FAIL';
      doc.setFont(undefined, 'bold');
      doc.text(dim, margin, y);
      doc.setFont(undefined, 'normal');
      doc.text(`${score}%  [${status}]`, margin + 50, y);
      y += lineHeight;
    });

    y += lineHeight;
    doc.line(margin, y - 2, pageW - margin, y - 2);
    y += 4;
  }

  // Data table
  doc.setFontSize(13);
  doc.setFont(undefined, 'bold');
  doc.text('Dataset Preview', margin, y);
  y += lineHeight;
  doc.setFont(undefined, 'normal');
  doc.setFontSize(10);
  doc.text(`Total properties: ${properties.length}`, margin, y);
  y += lineHeight * 1.5;

  if (properties.length === 0) {
    doc.text('No dataset records to display.', margin, y);
    doc.save(filename);
    return;
  }

  const allKeys = Object.keys(properties[0]);
  const headers = allKeys.slice(0, 5);
  const availableWidth = pageW - (margin * 2);
  const colWidth = availableWidth / headers.length;

  const renderHeader = (currentY) => {
    doc.setFont(undefined, 'bold');
    headers.forEach((header, i) => {
      const cleanHeader = header.replace(/_/g, ' ').toUpperCase();
      doc.text(cleanHeader.substring(0, 15), margin + (i * colWidth), currentY);
    });
    doc.setFont(undefined, 'normal');
    return currentY + lineHeight;
  };

  y = renderHeader(y);
  doc.setDrawColor(200, 200, 200);
  doc.line(margin, y - 2, pageW - margin, y - 2);
  y += 4;

  for (const row of properties) {
    if (y > 270) {
      doc.addPage();
      y = 20;
      y = renderHeader(y);
      y += 4;
    }
    headers.forEach((header, i) => {
      const val = row[header] != null ? String(row[header]) : '—';
      doc.text(val.substring(0, 15), margin + (i * colWidth), y);
    });
    y += lineHeight;
  }

  doc.save(filename);
}