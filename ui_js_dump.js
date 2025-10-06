
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
  .then(resp => resp.text().then(text => ({ status: resp.status, text })))
  .then(({ status, text }) => { document.getElementById('tx-result').textContent = 'HTTP ' + status + '

' + text; })
  .catch(e => { document.getElementById('tx-result').textContent = 'Error: ' + e.message; });
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
  .then(resp => resp.text().then(text => ({ status: resp.status, text })))
  .then(({ status, text }) => { document.getElementById('bk-result').textContent = 'HTTP ' + status + '

' + text; })
  .catch(e => { document.getElementById('bk-result').textContent = 'Error: ' + e.message; });
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

document.addEventListener('DOMContentLoaded', function() {
  loadSamples();
  var btnTx = document.getElementById('btn-tx');
  var btnBk = document.getElementById('btn-bk');
  if (btnTx) btnTx.addEventListener('click', postTransacciones);
  if (btnBk) btnBk.addEventListener('click', postRespaldos);
});
        
