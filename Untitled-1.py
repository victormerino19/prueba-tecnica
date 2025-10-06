# =============================
# UI simple para pruebas
# =============================
def _html_ui() -> str:
    return (
        """
<!DOCTYPE html>
<html lang=\"es\">
<head>
  <meta charset=\"utf-8\">
  <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">
  <title>UI de Pruebas</title>
  <style>
    body { font-family: Arial, sans-serif; margin: 20px; }
    h1 { margin-bottom: 0; }
    h2 { margin-top: 24px; }
    .card { border: 1px solid #ddd; padding: 16px; border-radius: 8px; margin-bottom: 20px; }
    textarea { width: 100%; height: 180px; font-family: monospace; }
    label { display: block; margin: 8px 0 4px; }
    .row { display: flex; gap: 16px; }
    .row > div { flex: 1; }
    .btn { padding: 8px 12px; border: 1px solid #333; border-radius: 6px; background: #f5f5f5; cursor: pointer; }
    .btn:hover { background: #e9e9e9; }
    .result { white-space: pre-wrap; background: #fafafa; border: 1px solid #eee; padding: 12px; border-radius: 6px; }
    .small { color: #666; font-size: 12px; }
  </style>
  <script src="/ui.js"></script>
  </head>
  <body>
    <h1>UI de Pruebas</h1>
    <p class=\"small\">Probar endpoints de ingesta (/transacciones) y respaldos (/respaldos).</p>

    <div class=\"card\">
      <h2>Transacciones</h2>
      <p>Enviar JSON para uno o varios grupos de tablas.</p>
      <label>Payload JSON</label>
      <textarea id=\"tx-body\"></textarea>
      <div style=\"margin-top:8px;\">
        <button class=\"btn\" onclick=\"loadSampleTransacciones()\">Cargar ejemplo</button>
        <button class=\"btn\" onclick=\"postTransacciones()\">Enviar a /transacciones</button>
      </div>
      <h3>Resultado</h3>
      <div id=\"tx-result\" class=\"result\"></div>
    </div>

    <div class=\"card\">
      <h2>Respaldos</h2>
      <div class=\"row\">
        <div>
          <label>Formato</label>
          <select id=\"bk-formato\">
            <option value=\"parquet\">parquet</option>
            <option value=\"avro\">avro</option>
          </select>
        </div>
        <div>
          <label>Directorio destino</label>
          <input id=\"bk-dir\" type=\"text\" placeholder=\"respaldos\" />
        </div>
      </div>
      <label>Tablas</label>
      <div>
        <label><input id=\"bk-dep\" type=\"checkbox\" value=\"departamentos\" /> Departamentos</label>
        <label><input id=\"bk-job\" type=\"checkbox\" value=\"trabajos\" /> Trabajos</label>
        <label><input id=\"bk-emp\" type=\"checkbox\" value=\"empleados_contratados\" /> Empleados contratados</label>
      </div>
      <div style=\"margin-top:8px;\">
        <button class=\"btn\" onclick=\"loadSampleRespaldos()\">Cargar ejemplo</button>
        <button class=\"btn\" onclick=\"postRespaldos()\">Generar /respaldos</button>
      </div>
      <h3>Resultado</h3>
      <div id=\"bk-result\" class=\"result\"></div>
    </div>

  </body>
</html>
        """
    )


@app.get("/ui", response_class=HTMLResponse)
def ui_pruebas():
    return _html_ui()


# JavaScript externo para la UI
def _ui_js() -> str:
    return (
        """
// UI JS sin dependencias externas
function postTransacciones() {
  var ta = document.getElementById('tx-body');
  var payload;
  try { payload = JSON.parse(ta.value); }
  catch (e) { document.getElementById('tx-result').textContent = 'JSON invalido: ' + e.message; return; }
  fetch('/transacciones', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload)
  })
  .then(function(resp) {
    return resp.text().then(function(text) {
      document.getElementById('tx-result').textContent = 'HTTP ' + resp.status + '\\n\\n' + text;
    });
  })
  .catch(function(e) { document.getElementById('tx-result').textContent = 'Error: ' + e.message; });
}

function postRespaldos() {
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
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload)
  })
  .then(function(resp) {
    return resp.text().then(function(text) {
      document.getElementById('bk-result').textContent = 'HTTP ' + resp.status + '\\n\\n' + text;
    });
  })
  .catch(function(e) { document.getElementById('bk-result').textContent = 'Error: ' + e.message; });
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
});
        """
    )


@app.get("/ui.js")
async def ui_js():
    return PlainTextResponse(content=_ui_js(), media_type="application/javascript")