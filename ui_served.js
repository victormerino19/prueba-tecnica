
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
      document.getElementById('tx-result').textContent = 'HTTP ' + resp.status + '\n\n' + text;
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
      document.getElementById('bk-result').textContent = 'HTTP ' + resp.status + '\n\n' + text;
    });
  })
  .catch(function(e) { document.getElementById('bk-result').textContent = 'Error: ' + e.message; });
}

function borrarTablaSiHayRespaldo() {
  var tabla = document.getElementById('del-tabla').value;
  var dir = document.getElementById('bk-dir').value || 'respaldos';
  var out = document.getElementById('del-result');
  if (!tabla) { out.textContent = 'Error: seleccione una tabla a borrar'; return; }
  var url = '/respaldos/existe?tabla=' + encodeURIComponent(tabla) + '&directorio=' + encodeURIComponent(dir) + '&solo_hoy=true';
  fetch(url)
    .then(function(resp){ return resp.json().then(function(json){ return { status: resp.status, json: json }; }); })
    .then(function(res){
      if (res.status !== 200) { out.textContent = 'HTTP ' + res.status + ' - ' + JSON.stringify(res.json); return; }
      if (!res.json.existen) { out.textContent = 'No hay respaldos de hoy para ' + tabla + ' en ' + dir; return; }
      var confirmMsg = 'Se encontraron ' + res.json.total_archivos + ' respaldos de HOY para ' + tabla + '.

Deseas borrar todos los registros de la tabla?';
      if (!window.confirm(confirmMsg)) { out.textContent = 'Accion cancelada por el usuario.'; return; }
      return fetch('/limpiar_tabla', {
        method: 'DELETE',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ tabla: tabla, directorio: dir, solo_hoy: true })
      })
      .then(function(resp){ return resp.text().then(function(text){ out.textContent = 'HTTP ' + resp.status + ' - ' + text; }); });
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
});
        