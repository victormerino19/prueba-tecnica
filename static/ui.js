// UI JS sin dependencias externas
function getApiKey() {
  try { return localStorage.getItem('apiKey') || ''; } catch (e) { return ''; }
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
  // Intentar cargar apiKey desde un input oculto si existe
  var hidden = document.getElementById('api-key-hidden');
  if (hidden && hidden.value) {
    try { localStorage.setItem('apiKey', hidden.value); } catch (e) {}
  }
});

function registerUser() {
  var emailEl = document.getElementById('reg-email');
  var out = document.getElementById('reg-result');
  var email = emailEl ? (emailEl.value || '').trim() : '';
  if (!email || email.indexOf('@') === -1) {
    if (out) out.textContent = 'Por favor, ingresa un email válido.';
    return;
  }
  fetch('/register', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ email: email })
  })
  .then(function(resp){ return resp.json().then(function(json){ return { status: resp.status, json: json }; }); })
  .then(function(res){
    if (!out) return;
    if (res.status !== 200) {
      out.textContent = 'HTTP ' + res.status + ' - ' + JSON.stringify(res.json);
      return;
    }
    var key = res.json.api_key || '';
    if (key) {
      try { localStorage.setItem('apiKey', key); } catch (e) {}
      out.textContent = 'API key generada y guardada. Usuario: ' + (res.json.user_email || email);
    } else {
      out.textContent = 'No se pudo obtener API key.';
    }
  })
  .catch(function(e){ if (out) out.textContent = 'Error: ' + e.message; });
}