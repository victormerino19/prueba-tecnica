// UI JS sin dependencias externas
function getApiKey() {
  // Tomar de localStorage o del campo de entrada si existe
  try {
    var stored = localStorage.getItem('apiKey') || '';
    var input = document.getElementById('api-key-input');
    var val = input ? (input.value || stored) : stored;
    if (val && val !== stored) {
      try { localStorage.setItem('apiKey', val); } catch (e) {}
    }
    return val || '';
  } catch (e) { return ''; }
}

function postTransacciones() {
  var ta = document.getElementById('tx-body');
  var apiKey = getApiKey();
  var payload;
  try { payload = JSON.parse(ta.value); }
  catch (e) { document.getElementById('tx-result').textContent = 'JSON invalido: ' + e.message; return; }
  fetch('/transacciones', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', 'X-API-Key': apiKey },
    body: JSON.stringify(payload)
  })
  .then(function(resp) {
    return resp.text().then(function(text) {
      document.getElementById('tx-result').textContent = 'HTTP ' + resp.status + '\n\n' + text;
    });
  })
  .catch(function(e) { document.getElementById('tx-result').textContent = 'Error: ' + e.message; });
}

function postRespaldos() {
  var apiKey = getApiKey();
  var formato = document.getElementById('bk-formato').value;
  var dir = document.getElementById('bk-dir').value || 'respaldos';
  var tablas = [];
  var ids = ['bk-dep','bk-job','bk-emp'];
  for (var i = 0; i < ids.length; i++) {
    var el = document.getElementById(ids[i]);
    if (el && el.checked) { tablas.push(el.value); }
  }
  var payload = { formato: formato, directorio: dir, tablas: tablas };
  fetch('/respaldos', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', 'X-API-Key': apiKey },
    body: JSON.stringify(payload)
  })
  .then(function(resp) {
    return resp.text().then(function(text) {
      var out = document.getElementById('bk-result');
      try {
        var json = JSON.parse(text);
        var dur = json.duracion_ms_total || json.duracion_ms || 0;
        var totalRegs = 0;
        if (Array.isArray(json.respaldos)) {
          for (var i = 0; i < json.respaldos.length; i++) {
            totalRegs += json.respaldos[i].registros || 0;
          }
        }
        out.textContent = 'Listo. Duró ' + dur + ' ms. Registros: ' + totalRegs;
      } catch (e) {
        out.textContent = 'HTTP ' + resp.status + ' - ' + text;
      }
    });
  })
  .catch(function(e) { document.getElementById('bk-result').textContent = 'Error: ' + e.message; });
}

function borrarTablaSiHayRespaldo() {
  var apiKey = getApiKey();
  var tabla = document.getElementById('del-tabla').value;
  var dir = document.getElementById('bk-dir').value || 'respaldos';
  var out = document.getElementById('del-result');
  if (!tabla) { out.textContent = 'Error: seleccione una tabla a borrar'; return; }
  var url = '/respaldos/existe?tabla=' + encodeURIComponent(tabla) + '&directorio=' + encodeURIComponent(dir) + '&solo_hoy=true';
  fetch(url, { headers: { 'X-API-Key': apiKey } })
    .then(function(resp){ return resp.json().then(function(json){ return { status: resp.status, json: json }; }); })
    .then(function(res){
      if (res.status !== 200) { out.textContent = 'HTTP ' + res.status + ' - ' + JSON.stringify(res.json); return; }
      if (!res.json.existen) { out.textContent = 'No hay respaldos de hoy para ' + tabla + ' en ' + dir; return; }
      var confirmMsg = 'Se encontraron ' + res.json.total_archivos + ' respaldos de HOY para ' + tabla + '.\n\nDeseas borrar todos los registros de la tabla?';
      if (!window.confirm(confirmMsg)) { out.textContent = 'Accion cancelada por el usuario.'; return; }
      return fetch('/limpiar_tabla', {
        method: 'DELETE',
        headers: { 'Content-Type': 'application/json', 'X-API-Key': apiKey },
        body: JSON.stringify({ tabla: tabla, directorio: dir, solo_hoy: true })
      })
      .then(function(resp){ return resp.text().then(function(text){ out.textContent = 'HTTP ' + resp.status + ' - ' + text; }); });
    })
    .catch(function(e){ out.textContent = 'Error: ' + e.message; });
}

// Departamentos sobre promedio anual
function consultarDepartamentosSobrePromedio() {
  var apiKey = getApiKey();
  var anioEl = document.getElementById('dp-anio');
  var out = document.getElementById('dp-result');
  var anio = (anioEl && anioEl.value) ? parseInt(anioEl.value, 10) : NaN;
  if (!anio || isNaN(anio)) { out.textContent = 'Error: ingrese un año válido'; return; }
  var url = '/metricas/departamentos_sobre_promedio?anio=' + encodeURIComponent(anio);
  fetch(url, { headers: { 'X-API-Key': apiKey } })
    .then(function(resp){ return resp.text().then(function(text){ return { status: resp.status, text: text }; }); })
    .then(function(res){
      try {
        var data = JSON.parse(res.text);
        if (!Array.isArray(data)) { out.textContent = 'HTTP ' + res.status + ' - ' + res.text; return; }
        var lines = ['ID, Department, Hired'];
        var labels = [], values = [];
        for (var i = 0; i < data.length; i++) {
          var row = data[i];
          lines.push([
            String(row.id || ''),
            String(row.department || ''),
            String(row.hired || 0)
          ].join(', '));
          labels.push(String(row.department || ''));
          values.push(parseInt(row.hired || 0, 10) || 0);
        }
        out.textContent = lines.join('\n');

        ensureStyles();
        ensureChartJs(function(){
          // KPIs
          var total = values.reduce(function(a,b){ return a + b; }, 0);
          var maxIdx = values.reduce(function(p, c, i, arr){ return c > arr[p] ? i : p; }, 0);
          renderKpis('dp-kpis', out, [
            { label: 'Total contrataciones', value: String(total) },
            { label: 'Top departamento', value: labels[maxIdx] + ' (' + values[maxIdx] + ')' },
            { label: 'Departamentos sobre promedio', value: String(labels.length) }
          ]);

          // Gráfico principal: barras horizontales
          var c1 = getOrCreateCanvas('dp-canvas', out);
          renderBarChart(c1, labels, values, 'Contrataciones por departamento', true);

          // Gráfico adicional 1: barras verticales del Top 10
          var sorted = labels.map(function(lbl, idx){ return { label: lbl, value: values[idx] }; })
            .sort(function(a,b){ return b.value - a.value; }).slice(0, 10);
          var topLabels = sorted.map(function(x){ return x.label; });
          var topValues = sorted.map(function(x){ return x.value; });
          var c2 = getOrCreateCanvas('dp-canvas2', out);
          renderBarChart(c2, topLabels, topValues, 'Top 10 departamentos', false);

          // Gráfico adicional 2: Pie de distribución
          var c3 = getOrCreateCanvas('dp-canvas3', out);
          renderPieChart(c3, labels, values, 'Distribución de contrataciones');

          // Tabla Top 10
          renderTable('dp-table', out, ['#','Departamento','Contrataciones'],
            topLabels.map(function(lbl, i){ return [String(i+1), lbl, String(topValues[i])]; })
          );
        });
      } catch (e) {
        out.textContent = 'HTTP ' + res.status + ' - ' + res.text;
      }
    })
    .catch(function(e){ out.textContent = 'Error: ' + e.message; });
}

function listarRespaldos() {
  var apiKey = getApiKey();
  var tabla = document.getElementById('rs-tabla').value;
  var dir = document.getElementById('rs-dir').value || 'respaldos';
  var select = document.getElementById('rs-archivos');
  var out = document.getElementById('rs-result');
  if (!tabla) { out.textContent = 'Error: seleccione una tabla'; return; }
  var url = '/respaldos/existe?tabla=' + encodeURIComponent(tabla) + '&directorio=' + encodeURIComponent(dir) + '&solo_hoy=false';
  fetch(url, { headers: { 'X-API-Key': apiKey } })
    .then(function(resp){ return resp.json().then(function(json){ return { status: resp.status, json: json }; }); })
    .then(function(res){
      if (res.status !== 200) { out.textContent = 'HTTP ' + res.status + ' - ' + JSON.stringify(res.json); return; }
      var archivos = res.json.archivos || [];
      while (select.firstChild) { select.removeChild(select.firstChild); }
      if (!archivos.length) {
        var opt = document.createElement('option'); opt.value = ''; opt.textContent = '-- No hay respaldos --'; select.appendChild(opt);
        out.textContent = 'No se encontraron respaldos en ' + dir + ' para ' + tabla; return;
      }
      archivos.sort(function(a,b){ return a.archivo < b.archivo ? 1 : -1; });
      archivos.forEach(function(item){
        var opt = document.createElement('option');
        opt.value = item.ruta;
        opt.textContent = item.archivo + ' (' + item.formato + ', ' + item.fecha + ')';
        select.appendChild(opt);
      });
      out.textContent = 'Respaldos listados: ' + archivos.length;
    })
    .catch(function(e){ out.textContent = 'Error: ' + e.message; });
}

function restaurarDesdeSeleccion() {
  var apiKey = getApiKey();
  var tabla = document.getElementById('rs-tabla').value;
  var select = document.getElementById('rs-archivos');
  var ruta = select.value;
  var out = document.getElementById('rs-result');
  if (!ruta) { out.textContent = 'Error: seleccione un respaldo'; return; }
  var formato = 'parquet';
  if (ruta.toLowerCase().endsWith('.avro')) { formato = 'avro'; }
  var payload = { formato: formato, tabla: tabla, archivo: ruta };
  fetch('/restaurar', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', 'X-API-Key': apiKey },
    body: JSON.stringify(payload)
  })
  .then(function(resp){
    return resp.text().then(function(text){
      try {
        var json = JSON.parse(text);
        var dur = json.duracion_ms || 0;
        var cant = json.restaurados || json.upsert || 0;
        out.textContent = 'Listo. Restaurados: ' + cant + '. Duró ' + dur + ' ms.';
      } catch (e) {
        out.textContent = 'HTTP ' + resp.status + ' - ' + text;
      }
    });
  })
  .catch(function(e){ out.textContent = 'Error: ' + e.message; });
}

function loadSamples() {
  var sampleMulti = {
    departamentos: [{ id: 16, departamento: 'Calidad' }],
    trabajos: [{ id: 999, trabajo: 'Developer X' }],
    empleados_contratados: [{ id: 501, nombre: 'Ana', fecha_hora: '2020-01-01T00:00:00', id_departamento: 16, id_trabajo: 999 }]
  };
  document.getElementById('tx-body').value = JSON.stringify(sampleMulti, null, 2);
  var dep = document.getElementById('bk-dep');
  var job = document.getElementById('bk-job');
  var emp = document.getElementById('bk-emp');
  if (dep) { dep.checked = true; }
  if (job) { job.checked = true; }
  if (emp) { emp.checked = true; }
}

// Cargar solo ejemplo de Transacciones
function loadSampleTransacciones() {
  var sampleMulti = {
    departamentos: [{ id: 16, departamento: 'Calidad' }],
    trabajos: [{ id: 999, trabajo: 'Developer X' }],
    empleados_contratados: [{ id: 501, nombre: 'Ana', fecha_hora: '2020-01-01T00:00:00', id_departamento: 16, id_trabajo: 999 }]
  };
  document.getElementById('tx-body').value = JSON.stringify(sampleMulti, null, 2);
}

// Cargar solo ejemplo de Respaldos
function loadSampleRespaldos() {
  var formatoEl = document.getElementById('bk-formato');
  var dirEl = document.getElementById('bk-dir');
  if (formatoEl) formatoEl.value = 'parquet';
  if (dirEl) dirEl.value = 'respaldos';
  var dep = document.getElementById('bk-dep');
  var job = document.getElementById('bk-job');
  var emp = document.getElementById('bk-emp');
  if (dep) dep.checked = true;
  if (job) job.checked = true;
  if (emp) emp.checked = true;
}

document.addEventListener('DOMContentLoaded', function() {
  loadSamples();
  // Inicializar campo API key desde localStorage
  try {
    var key = localStorage.getItem('apiKey') || '';
    var input = document.getElementById('api-key-input');
    if (input && key) { input.value = key; }
  } catch (e) {}
});

// Flujo de registro por email eliminado

// Consultar métricas trimestrales
function consultarMetricasTrimestrales() {
  var apiKey = getApiKey();
  var anioEl = document.getElementById('mt-anio');
  var nulosEl = document.getElementById('mt-nulos');
  var out = document.getElementById('mt-result');
  var anio = (anioEl && anioEl.value) ? parseInt(anioEl.value, 10) : NaN;
  var incluirNulos = !!(nulosEl && nulosEl.checked);
  if (!anio || isNaN(anio)) { out.textContent = 'Error: ingrese un año válido'; return; }
  var url = '/metricas/contrataciones_por_trimestre?anio=' + encodeURIComponent(anio) + '&incluir_nulos=' + (incluirNulos ? 'true' : 'false');
  fetch(url, { headers: { 'X-API-Key': apiKey } })
    .then(function(resp){ return resp.text().then(function(text){ return { status: resp.status, text: text }; }); })
    .then(function(res){
      try {
        var data = JSON.parse(res.text);
        if (!Array.isArray(data)) { out.textContent = 'HTTP ' + res.status + ' - ' + res.text; return; }
        var lines = ['department, job, q1, q2, q3, q4'];
        var t1 = 0, t2 = 0, t3 = 0, t4 = 0;
        var combos = [];
        for (var i = 0; i < data.length; i++) {
          var row = data[i];
          var q1 = parseInt(row.q1 || 0, 10) || 0;
          var q2 = parseInt(row.q2 || 0, 10) || 0;
          var q3 = parseInt(row.q3 || 0, 10) || 0;
          var q4 = parseInt(row.q4 || 0, 10) || 0;
          t1 += q1; t2 += q2; t3 += q3; t4 += q4;
          lines.push([
            String(row.department || ''),
            String(row.job || ''),
            String(q1),
            String(q2),
            String(q3),
            String(q4)
          ].join(', '));
          combos.push({ department: String(row.department || ''), job: String(row.job || ''), total: q1+q2+q3+q4 });
        }
        out.textContent = lines.join('\n');

        ensureStyles();
        ensureChartJs(function(){
          var quarters = ['Q1','Q2','Q3','Q4'];
          var totals = [t1,t2,t3,t4];
          // KPIs
          var sumTot = totals.reduce(function(a,b){ return a+b; }, 0);
          var maxQIdx = totals.reduce(function(p, c, i, arr){ return c > arr[p] ? i : p; }, 0);
          var variation = (totals[3] - totals[0]);
          renderKpis('mt-kpis', out, [
            { label: 'Total contrataciones', value: String(sumTot) },
            { label: 'Pico trimestral', value: quarters[maxQIdx] + ' (' + totals[maxQIdx] + ')' },
            { label: 'Variación Q1→Q4', value: (variation >= 0 ? '+' : '-') + String(Math.abs(variation)) }
          ]);

          // Gráfico principal: barras trimestrales
          var c1 = getOrCreateCanvas('mt-canvas', out);
          renderBarChart(c1, quarters, totals, 'Contrataciones por trimestre', false);

          // Gráfico adicional 1: línea de tendencia
          var c2 = getOrCreateCanvas('mt-canvas2', out);
          renderLineChart(c2, quarters, totals, 'Tendencia por trimestre');

          // Gráfico adicional 2: pie de distribución
          var c3 = getOrCreateCanvas('mt-canvas3', out);
          renderPieChart(c3, quarters, totals, 'Distribución por trimestre');

          // Tabla Top 10 de department-job por total
          var topCombos = combos.sort(function(a,b){ return b.total - a.total; }).slice(0,10);
          renderTable('mt-table', out, ['#','Departamento','Trabajo','Total'],
            topCombos.map(function(x, i){ return [String(i+1), x.department, x.job, String(x.total)]; })
          );
        });
      } catch (e) {
        out.textContent = 'HTTP ' + res.status + ' - ' + res.text;
      }
    })
    .catch(function(e){ out.textContent = 'Error: ' + e.message; });
}

function ensureChartJs(callback) {
  if (typeof window !== 'undefined' && window.Chart) { callback(); return; }
  var existing = document.getElementById('chartjs-cdn');
  if (!existing) {
    var script = document.createElement('script');
    script.id = 'chartjs-cdn';
    script.src = 'https://cdn.jsdelivr.net/npm/chart.js';
    script.onload = function(){ callback(); };
    script.onerror = function(){ callback(); };
    document.head.appendChild(script);
  } else {
    existing.onload = function(){ callback(); };
    if (window.Chart) { callback(); }
  }
}

function getOrCreateCanvas(id, outEl) {
  var el = document.getElementById(id);
  if (!el) {
    el = document.createElement('canvas');
    el.id = id;
    el.width = 400;
    el.height = 200;
    if (outEl && outEl.parentNode) {
      outEl.parentNode.insertBefore(el, outEl.nextSibling);
    } else {
      document.body.appendChild(el);
    }
  }
  return el;
}

function renderBarChart(canvasEl, labels, data, title, horizontal) {
  if (!window.Chart || !canvasEl) { return; }
  if (!window._charts) { window._charts = {}; }
  var id = canvasEl.id;
  var prev = window._charts[id];
  if (prev && typeof prev.destroy === 'function') { prev.destroy(); }
  var cfg = {
    type: 'bar',
    data: {
      labels: labels,
      datasets: [{ label: title, data: data, backgroundColor: 'rgba(54, 162, 235, 0.5)', borderColor: 'rgba(54, 162, 235, 1)', borderWidth: 1 }]
    },
    options: {
      indexAxis: horizontal ? 'y' : 'x',
      responsive: false,
      scales: { x: { beginAtZero: true }, y: { beginAtZero: true } }
    }
  };
  window._charts[id] = new Chart(canvasEl.getContext('2d'), cfg);
}

function renderLineChart(canvasEl, labels, data, title) {
  if (!window.Chart || !canvasEl) { return; }
  if (!window._charts) { window._charts = {}; }
  var id = canvasEl.id;
  var prev = window._charts[id];
  if (prev && typeof prev.destroy === 'function') { prev.destroy(); }
  var cfg = {
    type: 'line',
    data: {
      labels: labels,
      datasets: [{ label: title, data: data, borderColor: 'rgba(75, 192, 192, 1)', backgroundColor: 'rgba(75, 192, 192, 0.2)', tension: 0.3 }]
    },
    options: { responsive: false, scales: { x: { beginAtZero: true }, y: { beginAtZero: true } } }
  };
  window._charts[id] = new Chart(canvasEl.getContext('2d'), cfg);
}

function renderPieChart(canvasEl, labels, data, title) {
  if (!window.Chart || !canvasEl) { return; }
  if (!window._charts) { window._charts = {}; }
  var id = canvasEl.id;
  var prev = window._charts[id];
  if (prev && typeof prev.destroy === 'function') { prev.destroy(); }
  var colors = [
    'rgba(255, 99, 132, 0.6)', 'rgba(54, 162, 235, 0.6)', 'rgba(255, 206, 86, 0.6)',
    'rgba(75, 192, 192, 0.6)', 'rgba(153, 102, 255, 0.6)', 'rgba(255, 159, 64, 0.6)'
  ];
  var cfg = {
    type: 'pie',
    data: { labels: labels, datasets: [{ label: title, data: data, backgroundColor: labels.map(function(_, i){ return colors[i % colors.length]; }) }] },
    options: { responsive: false }
  };
  window._charts[id] = new Chart(canvasEl.getContext('2d'), cfg);
}

function renderTable(id, outEl, headers, rows) {
  var table = document.getElementById(id);
  if (!table) {
    table = document.createElement('table');
    table.id = id;
    table.className = 'data-table';
    if (outEl && outEl.parentNode) { outEl.parentNode.insertBefore(table, outEl.nextSibling); }
    else { document.body.appendChild(table); }
  }
  var thead = table.querySelector('thead');
  var tbody = table.querySelector('tbody');
  if (!thead) { thead = document.createElement('thead'); table.appendChild(thead); }
  if (!tbody) { tbody = document.createElement('tbody'); table.appendChild(tbody); }
  thead.innerHTML = '';
  tbody.innerHTML = '';
  var trh = document.createElement('tr');
  headers.forEach(function(h){ var th = document.createElement('th'); th.textContent = h; trh.appendChild(th); });
  thead.appendChild(trh);
  rows.forEach(function(r){ var tr = document.createElement('tr'); r.forEach(function(cell){ var td = document.createElement('td'); td.textContent = cell; tr.appendChild(td); }); tbody.appendChild(tr); });
}

function ensureStyles() {
  if (document.getElementById('custom-ui-styles')) { return; }
  var style = document.createElement('style');
  style.id = 'custom-ui-styles';
  style.textContent = [
    '.kpi-wrap { display:flex; gap:8px; margin:8px 0; flex-wrap:wrap; }',
    '.kpi { background:#f7f9fc; border:1px solid #e3e7ef; border-radius:8px; padding:8px 12px; box-shadow:0 1px 2px rgba(0,0,0,0.04); }',
    '.kpi .label { font-size:12px; color:#6b7280; }',
    '.kpi .value { font-size:16px; font-weight:600; color:#111827; }',
    'canvas { display:block; margin:8px 0; background:#fff; border:1px solid #e5e7eb; border-radius:6px; }',
    '.data-table { width:100%; border-collapse:collapse; margin:8px 0; font-size:14px; }',
    '.data-table th, .data-table td { border:1px solid #e5e7eb; padding:6px 8px; text-align:left; }',
    '.data-table thead th { background:#f3f4f6; font-weight:600; }',
    '.data-table tbody tr:nth-child(odd) { background:#fafafa; }'
  ].join('\n');
  document.head.appendChild(style);
}

function renderKpis(id, outEl, items) {
  var wrap = document.getElementById(id);
  if (!wrap) {
    wrap = document.createElement('div');
    wrap.id = id;
    wrap.className = 'kpi-wrap';
    if (outEl && outEl.parentNode) { outEl.parentNode.insertBefore(wrap, outEl.nextSibling); }
    else { document.body.appendChild(wrap); }
  }
  wrap.innerHTML = '';
  items.forEach(function(item){
    var el = document.createElement('div');
    el.className = 'kpi';
    var lab = document.createElement('div'); lab.className = 'label'; lab.textContent = item.label;
    var val = document.createElement('div'); val.className = 'value'; val.textContent = item.value;
    el.appendChild(lab); el.appendChild(val);
    wrap.appendChild(el);
  });
}