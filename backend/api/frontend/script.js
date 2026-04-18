// ─────────────────────────────────────────────────────────────
//  SQL File Converter — script.js
//  Flow:
//    SQL  → upload → POST /convert (auto) → backend mapping → render
//    CSV/Excel → parse local → render (ไม่ผ่าน backend)
//    Override → POST /override/:id → sync ทันที
// ─────────────────────────────────────────────────────────────

const API_BASE = window.API_BASE || '';

// ── State ──────────────────────────────────────────────────
let currentData   = {};  // { [tableName]: { headers, rows, fileName, fileType, backendCols? } }
let uploadedFiles = [];  // { name, type, fileObj }
let sessionId     = null;
let converted     = false;

// ─── File Input / Drag & Drop ──────────────────────────────
document.getElementById('fileInput').addEventListener('change', e => handleFiles(e.target.files));

function onDragOver(e)  { e.preventDefault(); document.getElementById('dropzone').classList.add('drag-over'); }
function onDragLeave()  { document.getElementById('dropzone').classList.remove('drag-over'); }
function onDrop(e) {
  e.preventDefault();
  document.getElementById('dropzone').classList.remove('drag-over');
  handleFiles(e.dataTransfer.files);
}

// ═══════════════════════════════════════════════════════════
//  HANDLE FILES — entry point
// ═══════════════════════════════════════════════════════════
async function handleFiles(files) {
  if (!files || files.length === 0) return;

  const supported = Array.from(files).filter(f => /\.(csv|xlsx|sql)$/i.test(f.name));
  if (!supported.length) {
    showStatus('uploadStatus', 'error', 'ไม่รองรับไฟล์ประเภทนี้ (CSV, Excel, SQL เท่านั้น)');
    return;
  }

  // Reset
  currentData   = {};
  uploadedFiles = [];
  sessionId     = null;
  converted     = false;
  document.getElementById('fileList').innerHTML = '';
  document.getElementById('convertBtn').disabled = true;
  clearUI();
  setLoading(true);

  const sqlFiles   = supported.filter(f => /\.sql$/i.test(f.name));
  const localFiles = supported.filter(f => /\.(csv|xlsx)$/i.test(f.name));

  // Register all files
  supported.forEach(f => {
    const ext  = f.name.split('.').pop().toLowerCase();
    const type = ext === 'sql' ? 'sql' : ext === 'csv' ? 'csv' : 'excel';
    uploadedFiles.push({ name: f.name, type, fileObj: f });
    renderFileChip(f.name, type);
  });

  // 1. Parse CSV / Excel locally
  await Promise.all(localFiles.map(f => parseLocalFile(f)));

  // 2. SQL → ส่ง backend ทันที (auto mapping)
  if (sqlFiles.length > 0) {
    showStatus('uploadStatus', 'info', `⏳ กำลัง mapping ${sqlFiles.length} SQL file กับ backend...`);
    await sendSQLToBackend(sqlFiles);
  } else {
    setLoading(false);
    onAllDone();
  }
}

// ─── Parse CSV / Excel locally ─────────────────────────────
function parseLocalFile(file) {
  return new Promise(resolve => {
    const ext    = file.name.split('.').pop().toLowerCase();
    const reader = new FileReader();

    if (ext === 'csv') {
      reader.onload = e => {
        try { parseCSV(file.name, e.target.result); } catch {}
        resolve();
      };
      reader.readAsText(file, 'utf-8');
    } else {
      reader.onload = e => {
        try {
          const wb = XLSX.read(new Uint8Array(e.target.result), { type: 'array' });
          wb.SheetNames.forEach(sheet => {
            const rows = XLSX.utils.sheet_to_json(wb.Sheets[sheet]);
            if (rows.length > 0) {
              const key = file.name.replace(/\.[^/.]+$/, '') +
                          (wb.SheetNames.length > 1 ? '_' + sheet : '');
              currentData[key] = { headers: Object.keys(rows[0]), rows, fileName: file.name, fileType: 'excel' };
            }
          });
        } catch {}
        resolve();
      };
      reader.readAsArrayBuffer(file);
    }
  });
}

// ─── CSV parser ─────────────────────────────────────────────
function parseCSV(fileName, text) {
  const lines    = text.replace(/\r\n/g, '\n').replace(/\r/g, '\n').split('\n');
  const nonEmpty = lines.filter(l => l.trim());
  if (nonEmpty.length < 2) return;
  const headers = parseCSVLine(nonEmpty[0]);
  const rows    = nonEmpty.slice(1).map(line => {
    const vals = parseCSVLine(line);
    return headers.reduce((obj, h, i) => { obj[h] = vals[i] ?? ''; return obj; }, {});
  });
  currentData[fileName.replace(/\.[^/.]+$/, '')] = { headers, rows, fileName, fileType: 'csv' };
}

function parseCSVLine(line) {
  const result = []; let cur = '', inQ = false;
  for (let i = 0; i < line.length; i++) {
    const ch = line[i];
    if (ch === '"') {
      if (inQ && line[i+1] === '"') { cur += '"'; i++; } else inQ = !inQ;
    } else if (ch === ',' && !inQ) { result.push(cur.trim()); cur = ''; }
    else cur += ch;
  }
  result.push(cur.trim());
  return result;
}

// ═══════════════════════════════════════════════════════════
//  BACKEND — POST /convert  (auto-trigger เมื่อ upload SQL)
// ═══════════════════════════════════════════════════════════
async function sendSQLToBackend(sqlFiles) {
  const form = new FormData();
  sqlFiles.forEach(f => form.append('files', f, f.name));

  try {
    const res = await fetch(`${API_BASE}/convert`, { method: 'POST', body: form });

    if (!res.ok) {
      const err = await res.json().catch(() => ({ detail: res.statusText }));
      throw new Error(err.detail || `HTTP ${res.status}`);
    }

    const data = await res.json();
    sessionId = data.session_id;

    // ใส่ backend mapping result เข้า currentData
    applyBackendTables(data.tables, data.unknown || {});

    const unknownCount = Object.values(data.unknown || {}).flat().length;
    if (unknownCount > 0) renderUnknownWarnings(data.unknown);

    showStatus('uploadStatus', 'success',
      `✓ Backend mapping สำเร็จ — ${Object.keys(data.tables).length} table` +
      (unknownCount ? ` (⚠️ ${unknownCount} unknown type)` : '')
    );

  } catch (err) {
    showStatus('uploadStatus', 'error', '❌ Backend: ' + err.message);
  } finally {
    setLoading(false);
    onAllDone();
  }
}

// ── นำ backend result มาใส่ currentData ──────────────────
function applyBackendTables(tables, unknown) {
  Object.entries(tables).forEach(([tableName, cols]) => {
    const fileName    = cols[0]?.file || 'unknown.sql';
    const unknownCols = (unknown[tableName] || []).map(u => u.column_name);

    currentData[tableName] = {
      headers    : cols.map(c => c.column_name),
      rows       : [],          // SQL = schema only ไม่มี data rows
      fileName,
      fileType   : 'sql',
      backendCols: cols.map(c => ({
        ...c,
        isUnknown: unknownCols.includes(c.column_name)
      }))
    };
  });
}

// ── หลังทุก file พร้อม ─────────────────────────────────────
function onAllDone() {
  converted = true;
  const tableCount = Object.keys(currentData).length;
  const rowCount   = Object.values(currentData).reduce((s, t) => s + t.rows.length, 0);

  updateStats(uploadedFiles.length, tableCount, rowCount);
  updateBadges(tableCount, rowCount, sessionId ? 'mapped' : 'loaded');
  renderTypePanel();
  renderTables();
  document.getElementById('convertBtn').disabled = false;

  // แสดง session card
  if (sessionId) {
    const card = document.getElementById('sessionCard');
    const disp = document.getElementById('sessionIdDisplay');
    if (card) card.style.display = 'block';
    if (disp) disp.textContent   = sessionId;
  }
}

// ═══════════════════════════════════════════════════════════
//  CONVERT BUTTON — re-send SQL ไป backend (refresh mapping)
// ═══════════════════════════════════════════════════════════
async function convertData() {
  const sqlFiles = uploadedFiles.filter(f => f.type === 'sql').map(f => f.fileObj);

  if (!sqlFiles.length) {
    showStatus('convertStatus', 'success', '✓ ไม่มีไฟล์ SQL — ข้อมูล local พร้อมแล้ว');
    return;
  }

  if (sessionId) await deleteSession(true);

  setLoading(true);
  showStatus('convertStatus', 'info', '⏳ Re-mapping กับ backend...');
  await sendSQLToBackend(sqlFiles);
  setLoading(false);
}

// ═══════════════════════════════════════════════════════════
//  OVERRIDE — POST /override/:id
// ═══════════════════════════════════════════════════════════
async function applyOverride(tableName, columnName, newType, selectEl) {
  // อัปเดต local ก่อน
  const t = currentData[tableName];
  if (t?.backendCols) {
    const col = t.backendCols.find(c => c.column_name === columnName);
    if (col) col.final_type = newType;
  }

  if (!sessionId) {
    flashSelect(selectEl, 'local');
    reRenderCardPills(tableName);
    return;
  }

  try {
    const res = await fetch(`${API_BASE}/override/${sessionId}`, {
      method : 'POST',
      headers: { 'Content-Type': 'application/json' },
      body   : JSON.stringify({ table: tableName, column: columnName, new_type: newType })
    });

    if (!res.ok) throw new Error((await res.json().catch(()=>({}))).detail || res.statusText);

    const updated = await res.json();
    if (t?.backendCols && updated.updated_column) {
      const col = t.backendCols.find(c => c.column_name === columnName);
      if (col) Object.assign(col, updated.updated_column);
    }
    flashSelect(selectEl, 'ok');
    reRenderCardPills(tableName);

  } catch (err) {
    flashSelect(selectEl, 'err');
    showStatus('convertStatus', 'error', '❌ Override: ' + err.message);
  }
}

function flashSelect(el, state) {
  if (!el) return;
  el.classList.remove('saved', 'err-flash');
  void el.offsetWidth;
  if (state === 'ok' || state === 'local') {
    el.classList.add('saved');
    setTimeout(() => el.classList.remove('saved'), 1200);
  } else {
    el.classList.add('err-flash');
    setTimeout(() => el.classList.remove('err-flash'), 1200);
  }
}

function reRenderCardPills(tableName) {
  const el = document.getElementById('pills-' + tableName);
  if (!el) return;
  const t = currentData[tableName];
  if (t?.backendCols) el.innerHTML = buildPillsHTML(t.backendCols);
}

// ═══════════════════════════════════════════════════════════
//  RESULT / DELETE SESSION
// ═══════════════════════════════════════════════════════════
async function fetchResult() {
  if (!sessionId) { showStatus('convertStatus', 'error', 'ยังไม่มี session'); return; }
  setLoading(true);
  try {
    const res = await fetch(`${API_BASE}/result/${sessionId}`);
    if (!res.ok) throw new Error((await res.json().catch(()=>({}))).detail || res.statusText);
    const data = await res.json();
    applyBackendTables(data.tables, data.unknown || {});
    renderTypePanel();
    renderTables();
    showStatus('convertStatus', 'success', '✓ Refresh result สำเร็จ');
  } catch (err) {
    showStatus('convertStatus', 'error', '❌ ' + err.message);
  } finally { setLoading(false); }
}

async function deleteSession(silent = false) {
  if (!sessionId) return;
  try {
    await fetch(`${API_BASE}/session/${sessionId}`, { method: 'DELETE' });
    if (!silent) showStatus('convertStatus', 'success', '✓ ลบ session แล้ว');
  } catch {}
  sessionId = null;
}

async function handleDeleteSession() {
  await deleteSession();
  const card = document.getElementById('sessionCard');
  const disp = document.getElementById('sessionIdDisplay');
  if (card) card.style.display = 'none';
  if (disp) disp.textContent   = '—';
}

// ═══════════════════════════════════════════════════════════
//  TYPE PANEL (sidebar) — แสดง mapping จาก backend
// ═══════════════════════════════════════════════════════════
function renderTypePanel() {
  const body = document.getElementById('typeTableBody');
  const keys  = Object.keys(currentData);

  if (!keys.length) {
    body.innerHTML = '<tr><td colspan="3"><div class="empty-hint">No file loaded</div></td></tr>';
    return;
  }

  // หา SQL table แรก
  const sqlKey = keys.find(k => currentData[k].backendCols);

  if (sqlKey) {
    const cols = currentData[sqlKey].backendCols;
    body.innerHTML = cols.map(col => `
      <tr class="${col.isUnknown ? 'row-unknown' : ''}">
        <td>
          <span class="col-name">${col.column_name}</span>
          ${col.isUnknown ? '<span class="unk-badge">?</span>' : ''}
        </td>
        <td>
          <span class="inferred-badge">${col.logical_type || col.raw_type || '—'}</span>
          <div class="src-type">${col.source_sql_type || ''}</div>
        </td>
        <td>
          <select class="type-select"
            onchange="applyOverride('${sqlKey}','${col.column_name}',this.value,this)">
            ${buildTypeOptions(col.final_type || col.source_sql_type || '')}
          </select>
        </td>
      </tr>`).join('');
  } else {
    // CSV/Excel — infer local
    const first = currentData[keys[0]];
    body.innerHTML = first.headers.map(h => {
      const inf = inferLocalType(first.rows.map(r => r[h]));
      return `<tr>
        <td><span class="col-name">${h}</span></td>
        <td><span class="inferred-badge">${inf}</span></td>
        <td>
          <select class="type-select">${buildTypeOptions(inf)}</select>
        </td>
      </tr>`;
    }).join('');
  }
}

function inferLocalType(values) {
  const s = values.filter(v => v !== '' && v != null).slice(0, 50);
  if (!s.length)                                    return 'VARCHAR';
  if (s.every(v => /^-?\d+$/.test(v)))             return 'INT';
  if (s.every(v => /^-?\d+(\.\d+)?$/.test(v)))     return 'DECIMAL';
  if (s.every(v => /^\d{4}-\d{2}-\d{2}/.test(v))) return 'DATE';
  if (s.every(v => /^(true|false|0|1)$/i.test(v))) return 'BOOLEAN';
  return 'VARCHAR';
}

function buildTypeOptions(selected = '') {
  const types = ['VARCHAR','NVARCHAR','NVARCHAR(MAX)','CHAR',
                 'INT','BIGINT','SMALLINT','TINYINT',
                 'DECIMAL','FLOAT','DOUBLE','NUMBER',
                 'DATE','DATETIME','TIMESTAMP',
                 'BOOLEAN','BIT','TEXT','NTEXT'];
  const list = types.includes(selected) ? types : (selected ? [selected, ...types] : types);
  return list.map(t => `<option${t === selected ? ' selected' : ''}>${t}</option>`).join('');
}

// ═══════════════════════════════════════════════════════════
//  RENDER TABLES
// ═══════════════════════════════════════════════════════════
const FILE_TYPE_META = {
  csv  : { label:'CSV',   icon:'📄', color:'var(--accent)',  dim:'rgba(0,214,143,0.12)' },
  excel: { label:'Excel', icon:'📊', color:'var(--accent2)', dim:'rgba(0,148,255,0.12)' },
  sql  : { label:'SQL',   icon:'🗃️',  color:'var(--warn)',    dim:'rgba(245,166,35,0.12)' },
};

function renderTables() {
  const grid = document.getElementById('tablesGrid');
  const bulk = document.getElementById('bulkSection');
  const keys  = Object.keys(currentData);

  if (!keys.length) {
    grid.innerHTML = `<div class="empty-state">
      <div class="empty-state-icon">📭</div>
      <div class="empty-state-text">ไม่พบตารางในไฟล์นี้</div>
    </div>`;
    bulk.classList.remove('visible');
    return;
  }

  const groups = {};
  keys.forEach(k => {
    const ft = currentData[k].fileType || 'csv';
    if (!groups[ft]) groups[ft] = [];
    groups[ft].push(k);
  });

  bulk.classList.add('visible');

  grid.innerHTML = ['csv','excel','sql'].filter(ft => groups[ft]).map(ft => {
    const meta      = FILE_TYPE_META[ft];
    const tkeys     = groups[ft];
    const totalRows = tkeys.reduce((s, k) => s + currentData[k].rows.length, 0);
    return `
      <div class="type-group">
        <div class="type-group-header" style="--g-color:${meta.color};--g-dim:${meta.dim}">
          <span class="type-group-icon">${meta.icon}</span>
          <span class="type-group-label">${meta.label}</span>
          <span class="type-group-count">${tkeys.length} table${tkeys.length>1?'s':''} · ${totalRows.toLocaleString()} rows</span>
          <div class="type-group-line"></div>
        </div>
        <div class="tables-subgrid">
          ${tkeys.map(k => buildTableCard(k)).join('')}
        </div>
      </div>`;
  }).join('');
}

function buildTableCard(k) {
  const t     = currentData[k];
  const isSql = !!t.backendCols;

  // Backend column pills (SQL only)
  const pillsBlock = isSql
    ? `<div class="backend-cols" id="pills-${k}">${buildPillsHTML(t.backendCols)}</div>`
    : '';

  // Data preview
  const shownCols   = t.headers.slice(0, 5);
  const previewRows = t.rows.slice(0, 3);
  const moreRows    = t.rows.length - 3;

  const theadHtml  = shownCols.map(h =>
    `<th title="${h}">${h.length > 14 ? h.slice(0,14)+'…' : h}</th>`).join('');
  const tbodyHtml  = previewRows.map(r =>
    `<tr>${shownCols.map(h => `<td>${String(r[h]??'').slice(0,20)}</td>`).join('')}</tr>`
  ).join('');
  const noDataHtml = isSql
    ? `<tr><td colspan="${shownCols.length||1}" class="no-data-cell">Schema only — ไม่มี INSERT data</td></tr>`
    : `<tr><td colspan="${shownCols.length}" class="no-data-cell">No data</td></tr>`;

  const sessionTag = sessionId
    ? `<span class="session-tag" title="session: ${sessionId}">🔗 mapped</span>` : '';

  return `
  <div class="table-card">
    <div class="table-card-header">
      <div class="table-card-icon">${isSql ? '🗃️' : '📊'}</div>
      <div style="min-width:0;flex:1">
        <div class="table-card-name" title="${k}">${k} ${sessionTag}</div>
        <div class="table-card-meta">
          <span>${t.headers.length}</span> cols ·
          ${isSql
            ? `<span class="mapped-label">backend mapped</span> · ${t.fileName}`
            : `<span>${t.rows.length.toLocaleString()}</span> rows · ${t.fileName}`}
        </div>
      </div>
    </div>
    ${pillsBlock}
    <div class="preview-wrap">
      <table class="preview-table">
        <thead><tr>${theadHtml || '<th>—</th>'}</tr></thead>
        <tbody>${tbodyHtml || noDataHtml}</tbody>
      </table>
    </div>
    ${moreRows > 0 ? `<div class="preview-more">+${moreRows.toLocaleString()} more rows</div>` : ''}
    <div class="table-card-actions">
      <button class="btn-card-dl csv"   onclick="downloadTable('${k}','csv')">
        ⬇ ${isSql ? 'Mapping CSV' : 'CSV'}
      </button>
      <button class="btn-card-dl excel" onclick="downloadTable('${k}','excel')">
        ⬇ ${isSql ? 'Mapping Excel' : 'Excel'}
      </button>
    </div>
  </div>`;
}

function buildPillsHTML(backendCols) {
  const show = backendCols.slice(0, 6);
  const more = backendCols.length - 6;
  return show.map(c => `
    <span class="bcol-pill${c.isUnknown ? ' unknown' : ''}" title="source: ${c.source_sql_type||''}">
      ${c.column_name}<em>${c.final_type || c.logical_type || '?'}</em>
    </span>`).join('') +
    (more > 0 ? `<span class="bcol-more">+${more} more</span>` : '');
}

// ── Unknown type warnings ─────────────────────────────────
function renderUnknownWarnings(unknown) {
  document.getElementById('unknownWarnings')?.remove();
  const items = Object.entries(unknown).flatMap(([tbl, cols]) =>
    cols.map(c => `<li><b>${tbl}</b>.<span>${c.column_name}</span> — ${c.reason||'ไม่รู้จัก type'}</li>`)
  );
  if (!items.length) return;
  const div = document.createElement('div');
  div.id        = 'unknownWarnings';
  div.className = 'warn-panel';
  div.innerHTML = `
    <div class="warn-panel-header">
      ⚠️ Unknown Types (${items.length})
      <button onclick="this.parentElement.parentElement.remove()">✕</button>
    </div>
    <ul>${items.join('')}</ul>`;
  document.getElementById('tablesGrid').insertAdjacentElement('beforebegin', div);
}

// ═══════════════════════════════════════════════════════════
//  DOWNLOAD
// ═══════════════════════════════════════════════════════════
const MAP_HEADERS = ['#','column_name','file','raw_type','logical_type','final_type','source_sql_type'];

function toMappingRows(backendCols) {
  return backendCols.map((c, i) => ({
    '#'             : i,
    column_name     : c.column_name,
    file            : c.file            || '',
    raw_type        : c.raw_type        || '',
    logical_type    : c.logical_type    || '',
    final_type      : c.final_type      || '',
    source_sql_type : c.source_sql_type || '',
  }));
}

function downloadTable(key, fmt) {
  const t = currentData[key];
  if (!t) return;

  if (t.backendCols) {
    // SQL → download column mapping (ตรงกับ Postman)
    const rows = toMappingRows(t.backendCols);
    if (fmt === 'csv') {
      const body = [MAP_HEADERS.join(','),
        ...rows.map(r => MAP_HEADERS.map(h => escCSV(r[h]??'')).join(','))
      ].join('\n');
      triggerDownload(new Blob(['\uFEFF'+body],{type:'text/csv;charset=utf-8;'}), key+'_mapping.csv');
    } else {
      const wb = XLSX.utils.book_new();
      const ws = XLSX.utils.json_to_sheet(rows, { header: MAP_HEADERS });
      ws['!cols'] = [{wch:4},{wch:24},{wch:16},{wch:14},{wch:14},{wch:20},{wch:32}];
      XLSX.utils.book_append_sheet(wb, ws, 'column_mapping');
      XLSX.writeFile(wb, key+'_mapping.xlsx');
    }
    return;
  }

  // CSV / Excel → data rows
  if (fmt === 'csv') dlCSV(key, t);
  else               dlExcel(key, t);
}

function downloadAllCSV() {
  const keys = Object.keys(currentData);
  if (!keys.length) return;
  const sqlKeys   = keys.filter(k =>  currentData[k].backendCols);
  const localKeys = keys.filter(k => !currentData[k].backendCols);

  if (localKeys.length) {
    let out = localKeys.map(k => {
      const t = currentData[k];
      return `### ${k}\n` + t.headers.map(escCSV).join(',') + '\n' +
        t.rows.map(r => t.headers.map(h => escCSV(r[h]??'')).join(',')).join('\n');
    }).join('\n\n');
    triggerDownload(new Blob(['\uFEFF'+out],{type:'text/csv;charset=utf-8;'}), 'tables_'+Date.now()+'.csv');
  }

  sqlKeys.forEach(k => downloadTable(k, 'csv'));
  showStatus('convertStatus', 'success', '✓ ดาวน์โหลด CSV สำเร็จ');
}

function downloadAllExcel() {
  const keys = Object.keys(currentData);
  if (!keys.length) return;
  const wb = XLSX.utils.book_new();

  keys.forEach(k => {
    const t = currentData[k];
    if (t.backendCols) {
      const ws = XLSX.utils.json_to_sheet(toMappingRows(t.backendCols), { header: MAP_HEADERS });
      ws['!cols'] = [{wch:4},{wch:24},{wch:16},{wch:14},{wch:14},{wch:20},{wch:32}];
      XLSX.utils.book_append_sheet(wb, ws, k.substring(0,31));
    } else {
      XLSX.utils.book_append_sheet(wb, XLSX.utils.json_to_sheet(t.rows), k.substring(0,31));
    }
  });

  XLSX.writeFile(wb, 'all_tables_'+Date.now()+'.xlsx');
  showStatus('convertStatus', 'success', '✓ ดาวน์โหลด Excel สำเร็จ');
}

function dlCSV(name, table) {
  const body = [table.headers.map(escCSV).join(','),
    ...table.rows.map(r => table.headers.map(h => escCSV(r[h]??'')).join(','))
  ].join('\n');
  triggerDownload(new Blob(['\uFEFF'+body],{type:'text/csv;charset=utf-8;'}), name+'.csv');
}

function dlExcel(name, table) {
  const wb = XLSX.utils.book_new();
  XLSX.utils.book_append_sheet(wb, XLSX.utils.json_to_sheet(table.rows), name.substring(0,31));
  XLSX.writeFile(wb, name+'.xlsx');
}

function escCSV(v) {
  const s = String(v);
  return (s.includes(',')||s.includes('"')||s.includes('\n')) ? '"'+s.replace(/"/g,'""')+'"' : s;
}

function triggerDownload(blob, filename) {
  const a = Object.assign(document.createElement('a'),
    { href: URL.createObjectURL(blob), download: filename, style: 'display:none' });
  document.body.appendChild(a); a.click(); document.body.removeChild(a);
}

// ═══════════════════════════════════════════════════════════
//  UI HELPERS
// ═══════════════════════════════════════════════════════════
function renderFileChip(name, type) {
  const div = document.createElement('div');
  div.className = 'file-item';
  div.innerHTML = `
    <span class="file-type-badge ${type}">${type.toUpperCase()}</span>
    <span class="file-name" title="${name}">${name}</span>
    <button class="file-remove" onclick="this.parentElement.remove()">✕</button>`;
  document.getElementById('fileList').appendChild(div);
}

function clearUI() {
  document.getElementById('tablesGrid').innerHTML = `
    <div class="empty-state">
      <div class="empty-state-icon">🗄️</div>
      <div class="empty-state-text">อัปโหลดไฟล์ CSV, Excel หรือ SQL เพื่อเริ่มต้น</div>
    </div>`;
  document.getElementById('bulkSection').classList.remove('visible');
  document.getElementById('typeTableBody').innerHTML =
    '<tr><td colspan="3"><div class="empty-hint">No file loaded</div></td></tr>';
  const card = document.getElementById('sessionCard');
  if (card) card.style.display = 'none';
  document.getElementById('unknownWarnings')?.remove();
  updateStats(0,0,0);
  updateBadges(0,0,'ready');
}

function updateStats(files, tables, rows) {
  document.getElementById('statFiles').textContent  = files;
  document.getElementById('statTables').textContent = tables;
  document.getElementById('statRows').textContent   = rows.toLocaleString();
}

function updateBadges(tables, rows, status) {
  document.getElementById('badgeTables').textContent = tables+' tables';
  document.getElementById('badgeRows').textContent   = rows.toLocaleString()+' rows';
  const b = document.getElementById('badgeStatus');
  b.textContent = status;
  b.className   = 'badge' + ({mapped:' converted', loaded:' active'}[status] || '');
}

function showStatus(id, type, msg) {
  const el = document.getElementById(id);
  if (!el) return;
  el.textContent = msg;
  el.className   = 'status-bar '+type+' show';
  if (type === 'success') setTimeout(() => el.classList.remove('show'), 4000);
}

function setLoading(on) {
  document.getElementById('loadingBar').classList.toggle('active', on);
}

// ── Health check ──────────────────────────────────────────
async function checkHealth() {
  try {
    const res = await fetch(`${API_BASE}/health`);
    setBackendStatus(res.ok && (await res.json()).status === 'ok');
  } catch { setBackendStatus(false); }
}

function setBackendStatus(ok) {
  const dot = document.getElementById('backendDot');
  const lbl = document.getElementById('backendLabel');
  if (!dot||!lbl) return;
  dot.className   = 'status-dot '+(ok?'online':'offline');
  lbl.textContent = ok ? 'API Online' : 'API Offline';
}

// ── Init ──────────────────────────────────────────────────
window.addEventListener('DOMContentLoaded', () => {
  checkHealth();
  setInterval(checkHealth, 30_000);
});

window.addEventListener('beforeunload', () => {
  if (sessionId) navigator.sendBeacon(`${API_BASE}/session/${sessionId}`, '{}');
});