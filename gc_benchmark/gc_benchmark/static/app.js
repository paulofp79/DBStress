/**
 * GC Benchmark Tool — Frontend Logic
 *
 * Handles sidebar navigation, form interactions, REST API calls,
 * WebSocket live-streaming, and Chart.js chart management.
 */
(function () {
    'use strict';

    // ---- State ----
    var ws = null;
    var gcChart = null;
    var compareChart = null;
    var workloadRunning = false;
    var workloadStartPending = false;
    var workloadStates = {};
    var workloadChartSeries = {};
    var selectedWorkloadId = '';
    var loginWorkloadRunning = false;
    var dbActivityTimer = null;
    var cpoolStatsTimer = null;

    // One colour per GC event — order matches GC_SYSTEM_EVENTS in metrics.py
    var GC_COLORS = [
        '#f87171',  // gc current block congested  — red     (primary)
        '#6c9fff',  // gc current block 2-way      — blue
        '#a78bfa',  // gc current block 3-way      — violet
        '#f472b6',  // gc current block busy       — pink
        '#fb923c',  // gc cr block congested       — orange
        '#34d399',  // gc cr block 2-way           — green
        '#22d3ee',  // gc cr block 3-way           — cyan
        '#4ade80',  // gc cr block busy            — lime
        '#fbbf24',  // gc cr grant congested       — amber
        '#e879f9',  // gc cr grant 2-way           — fuchsia
    ];

    var CHART_COLORS = [
        '#6c9fff', '#f87171', '#fbbf24', '#34d399',
        '#a78bfa', '#22d3ee', '#f472b6', '#a3a3a3',
    ];
    var MAX_WORKLOAD_SEED_ROWS = 100000;

    var PAGE_TITLES = {
        connection: 'Connection',
        schema: 'Schema Setup',
        workload: 'Run Workload',
        'login-sim': 'Login Workload Simulation',
        results: 'Results & Comparison',
    };

    // ================================================================
    // Utility
    // ================================================================

    function $(id) { return document.getElementById(id); }

    function escapeHtml(text) {
        var div = document.createElement('div');
        div.textContent = text;
        return div.innerHTML;
    }

    function formatRunDateTime(value) {
        if (!value) return '-';
        var date = new Date(value);
        if (Number.isNaN(date.getTime())) {
            return String(value).replace('T', ' ').substring(0, 16);
        }
        return date.toLocaleString([], {
            year: 'numeric',
            month: 'short',
            day: '2-digit',
            hour: '2-digit',
            minute: '2-digit',
        });
    }

    function showStatus(containerId, message, type) {
        $(containerId).innerHTML =
            '<div class="status-box status-' + type + '">' + escapeHtml(message) + '</div>';
    }

    function getRunNotes(run) {
        return (run && run.notes_parsed) || {};
    }

    function getRunType(run) {
        var notes = getRunNotes(run);
        return notes.run_type === 'LOGIN_SIM' ? 'Login Simulation' : 'GC Workload';
    }

    function getRunScenario(run) {
        var notes = getRunNotes(run);
        if (notes.run_type === 'LOGIN_SIM') {
            var sessionCase = notes.session_case || 'SIMPLE_QUERY';
            var stopMode = notes.stop_mode || 'N/A';
            return sessionCase + ' · ' + stopMode;
        }
        var schema = run.schema_name || run.table_prefix || '-';
        var partition = run.partition_type || 'NONE';
        var compression = run.compression || 'NONE';
        return schema + ' · ' + partition + ' · ' + compression;
    }

    function getRunActivity(run) {
        var notes = getRunNotes(run);
        if (notes.run_type === 'LOGIN_SIM') {
            return 'Logons ' + Number(notes.logons || 0).toLocaleString() +
                ' · Queries ' + Number(notes.queries || 0).toLocaleString() +
                ' · Cycles ' + Number(notes.cycles || 0).toLocaleString();
        }
        return 'I ' + Number(run.inserts || 0).toLocaleString() +
            ' · U ' + Number(run.updates || 0).toLocaleString() +
            ' · D ' + Number(run.deletes || 0).toLocaleString();
    }

    function getCurrentWorkloadSeedRows() {
        var field = $('wl-seed-rows');
        var value = parseInt(((field || {}).value || '500'), 10);
        if (isNaN(value)) value = 500;
        value = Math.max(1, Math.min(MAX_WORKLOAD_SEED_ROWS, value));
        if (field) field.value = String(value);
        return value;
    }

    function confirmDialog(message, onConfirm) {
        var overlay = document.createElement('div');
        overlay.className = 'confirm-overlay';
        overlay.innerHTML =
            '<div class="confirm-box">' +
            '<p>' + escapeHtml(message) + '</p>' +
            '<div class="btn-group">' +
            '<button class="btn btn-danger" id="confirm-yes">Yes, proceed</button>' +
            '<button class="btn btn-secondary" id="confirm-no">Cancel</button>' +
            '</div></div>';
        document.body.appendChild(overlay);
        overlay.querySelector('#confirm-yes').addEventListener('click', function () {
            document.body.removeChild(overlay);
            onConfirm();
        });
        overlay.querySelector('#confirm-no').addEventListener('click', function () {
            document.body.removeChild(overlay);
        });
    }

    async function api(method, url, body) {
        var opts = { method: method, headers: { 'Content-Type': 'application/json' } };
        if (body) opts.body = JSON.stringify(body);
        var resp = await fetch(url, opts);
        return await resp.json();
    }

    // ================================================================
    // Navigation
    // ================================================================

    function initNav() {
        document.querySelectorAll('.nav-item').forEach(function (item) {
            item.addEventListener('click', function () {
                switchTab(item.dataset.tab);
            });
        });
    }

    function switchTab(name) {
        // Update nav
        document.querySelectorAll('.nav-item').forEach(function (n) {
            n.classList.toggle('active', n.dataset.tab === name);
        });
        // Update panels
        document.querySelectorAll('.tab-panel').forEach(function (p) {
            p.classList.toggle('active', p.id === 'tab-' + name);
        });
        // Update header title
        $('page-title').textContent = PAGE_TITLES[name] || name;
        // Load data for specific tabs
        if (name === 'results')  loadResults();
        if (name === 'workload') {
            loadSchemas();
            loadWorkloadStatus();
        }
        if (name === 'login-sim') {
            loadLoginWorkloadStatus();
        }
    }

    // -----------------------------------------------------------------------
    // Recent connections
    // -----------------------------------------------------------------------

    /**
     * Load up to 5 recent connections from the backend and render them as
     * a clickable list above the connection form.  Password is never stored
     * or shown — clicking a row fills all fields except password.
     */
    async function loadRecentConnections() {
        var section = $('recent-conns-section');
        var list    = $('recent-conns-list');
        if (!section || !list) return;

        try {
            var data = await api('GET', '/api/connections/recent');
            if (!data.ok || !data.connections || data.connections.length === 0) {
                section.style.display = 'none';
                return;
            }

            list.innerHTML = '';
            data.connections.forEach(function (c) {
                var li = document.createElement('li');
                li.className = 'recent-conn-item';
                li.setAttribute('title', 'Click to fill connection fields');

                var saved = c.saved_at
                    ? new Date(c.saved_at).toLocaleString([], {
                          month: 'short', day: 'numeric',
                          hour: '2-digit', minute: '2-digit'
                      })
                    : '';

                li.innerHTML =
                    '<span class="rc-icon">&#128279;</span>' +
                    '<span class="rc-label">' + escapeHtml(c.label || c.host) + '</span>' +
                    '<span class="rc-mode rc-mode-' + escapeHtml(c.mode || 'thin') + '">' +
                        escapeHtml((c.mode || 'thin').toUpperCase()) +
                    '</span>' +
                    (saved ? '<span class="rc-date">' + escapeHtml(saved) + '</span>' : '') +
                    '<span class="rc-use-btn">Use &#8594;</span>';

                li.addEventListener('click', function () {
                    applyRecentConnection(c);
                    document.querySelectorAll('.recent-conn-item').forEach(function (el) {
                        el.classList.remove('rc-active');
                    });
                    li.classList.add('rc-active');
                });

                list.appendChild(li);
            });

            section.style.display = 'block';
        } catch (e) {
            section.style.display = 'none';
        }
    }

    async function loadRecentCpoolConnections() {
        var section = $('recent-cpool-conns-section');
        var list = $('recent-cpool-conns-list');
        if (!section || !list) return;

        try {
            var data = await api('GET', '/api/cpool-connections/recent');
            if (!data.ok || !data.connections || data.connections.length === 0) {
                section.style.display = 'none';
                return;
            }

            list.innerHTML = '';
            data.connections.forEach(function (c) {
                var li = document.createElement('li');
                li.className = 'recent-conn-item';
                li.setAttribute('title', 'Click to fill CDB connection fields');

                var saved = c.saved_at
                    ? new Date(c.saved_at).toLocaleString([], {
                        month: 'short', day: 'numeric',
                        hour: '2-digit', minute: '2-digit'
                    })
                    : '';

                li.innerHTML =
                    '<span class="rc-icon">&#128279;</span>' +
                    '<span class="rc-label">' + escapeHtml(c.label || c.host) + '</span>' +
                    '<span class="rc-mode rc-mode-' + escapeHtml(c.mode || 'thin') + '">' +
                        escapeHtml((c.mode || 'thin').toUpperCase()) +
                    '</span>' +
                    (saved ? '<span class="rc-date">' + escapeHtml(saved) + '</span>' : '') +
                    '<span class="rc-use-btn">Use &#8594;</span>';

                li.addEventListener('click', function () {
                    applyRecentCpoolConnection(c);
                    document.querySelectorAll('#recent-cpool-conns-list .recent-conn-item').forEach(function (el) {
                        el.classList.remove('rc-active');
                    });
                    li.classList.add('rc-active');
                });

                list.appendChild(li);
            });

            section.style.display = 'block';
        } catch (e) {
            section.style.display = 'none';
        }
    }

    /**
     * Fill the connection form fields from a saved connection object.
     * Password is intentionally left untouched — cursor moves to it
     * so the user can type immediately.
     */
    function applyRecentConnection(c) {
        var hostEl = $('conn-host');
        var portEl = $('conn-port');
        var svcEl  = $('conn-service');
        var userEl = $('conn-user');
        var modeEl = $('conn-mode');

        if (hostEl) hostEl.value = c.host         || '';
        if (portEl) portEl.value = c.port         || 1521;
        if (svcEl)  svcEl.value  = c.service_name || '';
        if (userEl) userEl.value = c.user         || '';
        if (modeEl) modeEl.value = c.mode         || 'thin';

        var pwEl = $('conn-password');
        if (pwEl) { pwEl.value = ''; pwEl.focus(); }

        showStatus('conn-status', 'info',
            'Fields filled — enter your password and click Test Connection');
    }

    /** Fetch and display the active schema configuration on the workload tab. */
    async function loadSchemaState() {
        try {
            var s = await api('GET', '/api/schema/state');
            renderSchemaBar(s);
        } catch (e) { /* ignore */ }
    }

    /** Render the schema info bar from a schema object. */
    function renderSchemaBar(s) {
        var el = $('active-schema-info');
        if (!el) return;
        var part = s.partition_type || 'NONE';
        if (s.partition_detail) part += ' (' + s.partition_detail + ')';
        var comp = s.compression || 'NONE';
        var compActive = comp !== 'NONE';
        var hasSeedRows = !(s.seed_rows === undefined || s.seed_rows === null || s.seed_rows === '');
        var rowLabel = hasSeedRows ? Number(s.seed_rows || 0).toLocaleString() : 'unknown';
        el.innerHTML =
            '<span class="schema-badge"><b>' + escapeHtml(s.prefix || s.table_prefix || 'GCB') + '</b></span>' +
            '<span class="schema-badge">Tables&nbsp;<b>' + escapeHtml(String(s.table_count || 10)) + '</b></span>' +
            '<span class="schema-badge">Rows/Table&nbsp;<b>' + escapeHtml(rowLabel) + '</b></span>' +
            '<span class="schema-badge">Partition&nbsp;<b>' + escapeHtml(part) + '</b></span>' +
            '<span class="schema-badge' + (compActive ? ' schema-badge-active' : '') + '">Compression&nbsp;<b>' + escapeHtml(comp) + '</b></span>';
    }

    /**
     * Load existing benchmark schemas from Oracle and populate the select dropdown.
     * Clears and rebuilds the <select> options on each call.
     */
    async function loadSchemas() {
        var sel = $('wl-schema-select');
        if (!sel) return;

        sel.disabled = true;
        sel.innerHTML = '<option value="">Loading from Oracle…</option>';

        var data = await api('GET', '/api/schema/list');

        if (!data.ok || !data.schemas || data.schemas.length === 0) {
            sel.innerHTML = data.ok
                ? '<option value="">No benchmark schemas found — create one first</option>'
                : '<option value="">Error: ' + escapeHtml(data.message || 'Could not query Oracle') + '</option>';
            sel.disabled = false;
            return;
        }

        sel.innerHTML = '<option value="">— select a schema —</option>';
        data.schemas.forEach(function (s) {
            var opt = document.createElement('option');
            opt.value = JSON.stringify(s);
            opt.textContent = s.label;
            sel.appendChild(opt);
        });

        sel.disabled = false;

        // Auto-select the first schema so the user sees something useful immediately
        if (data.schemas.length === 1) {
            sel.selectedIndex = 1;
            applySelectedSchema(data.schemas[0]);
        }
    }

    /**
     * Apply a selected schema object to the hidden form fields and the info bar.
     * Called when the user changes the schema <select>.
     */
    function applySelectedSchema(s) {
        $('wl-schema-name').value      = s.label           || s.prefix || 'GCB';
        $('wl-prefix').value           = s.prefix          || 'GCB';
        $('wl-table-count').value      = s.table_count     || 10;
        $('wl-partition-type').value   = s.partition_type  || 'NONE';
        $('wl-partition-detail').value = s.partition_detail|| '';
        $('wl-compression').value      = s.compression     || 'NONE';
        renderSchemaBar(s);
    }

    // ================================================================
    // Connection Tab
    // ================================================================

    function getConnFields() {
        return {
            host: $('conn-host').value || 'localhost',
            port: parseInt($('conn-port').value) || 1521,
            service_name: $('conn-service').value || 'orclpdb1',
            user: $('conn-user').value,
            password: $('conn-password').value,
            mode: $('conn-mode').value,
        };
    }

    function normalizeConnText(value) {
        return String(value || '').trim().toLowerCase();
    }

    function normalizeConnPort(value) {
        var port = parseInt(value, 10);
        return isNaN(port) ? 0 : port;
    }

    function getCurrentWorkloadConnectionKey() {
        return {
            host: (($('conn-host') || {}).value || '').trim(),
            port: normalizeConnPort((($('conn-port') || {}).value || '')),
            service_name: (($('conn-service') || {}).value || '').trim(),
            user: (($('conn-user') || {}).value || '').trim(),
        };
    }

    function hasConnectionKey(key) {
        if (!key) return false;
        return !!(
            normalizeConnText(key.host) ||
            normalizeConnText(key.service_name) ||
            normalizeConnText(key.user)
        );
    }

    function sameConnectionKey(left, right) {
        if (!hasConnectionKey(left) || !hasConnectionKey(right)) return false;
        return (
            normalizeConnText(left.host) === normalizeConnText(right.host) &&
            normalizeConnPort(left.port) === normalizeConnPort(right.port) &&
            normalizeConnText(left.service_name) === normalizeConnText(right.service_name) &&
            normalizeConnText(left.user) === normalizeConnText(right.user)
        );
    }

    function workloadMessageMatchesCurrentConnection(msg) {
        if (!msg || !msg.connection_key) return true;
        var current = getCurrentWorkloadConnectionKey();
        if (!hasConnectionKey(current)) return false;
        return sameConnectionKey(current, msg.connection_key);
    }

    function buildScopedStatusUrl(path) {
        var key = getCurrentWorkloadConnectionKey();
        if (!hasConnectionKey(key)) return null;

        var params = new URLSearchParams();
        if (key.host) params.set('host', key.host);
        if (key.port) params.set('port', String(key.port));
        if (key.service_name) params.set('service_name', key.service_name);
        if (key.user) params.set('user', key.user);
        return path + '?' + params.toString();
    }

    function buildWorkloadStatusUrl() {
        return buildScopedStatusUrl('/api/workload/status');
    }

    function buildLoginWorkloadStatusUrl() {
        return buildScopedStatusUrl('/api/login-workload/status');
    }

    function isWorkloadActive(data) {
        if (!data) return false;
        if (data.running) return true;
        var phase = String(data.phase || '').toUpperCase();
        return phase === 'PREPARING' || phase === 'WARMING' || phase === 'RUNNING' || phase === 'STOPPING' || phase === 'RECOVERED';
    }

    function getActiveWorkloads() {
        return Object.keys(workloadStates).map(function (id) {
            return workloadStates[id];
        }).filter(function (item) {
            return isWorkloadActive(item);
        });
    }

    function getSelectedWorkload() {
        if (!selectedWorkloadId) return null;
        return workloadStates[selectedWorkloadId] || null;
    }

    function sortWorkloads(workloads) {
        return workloads.slice().sort(function (left, right) {
            var leftCreated = String(left.created_at || '');
            var rightCreated = String(right.created_at || '');
            if (leftCreated !== rightCreated) return rightCreated.localeCompare(leftCreated);
            return String(right.workload_id || '').localeCompare(String(left.workload_id || ''));
        });
    }

    function ensureSelectedWorkload() {
        if (selectedWorkloadId && isWorkloadActive(workloadStates[selectedWorkloadId])) return;
        var workloads = sortWorkloads(getActiveWorkloads());
        selectedWorkloadId = workloads.length ? String(workloads[0].workload_id || '') : '';
    }

    function syncWorkloadButtons() {
        workloadRunning = getActiveWorkloads().length > 0;
        $('btn-start-workload').disabled = workloadStartPending;
        $('btn-stop-workload').disabled = !getSelectedWorkload();
    }

    function buildSelectedWorkloadCaption(data) {
        if (!data) return 'No active workload selected.';
        var schema = data.schema_name || data.table_prefix || 'GCB';
        var requested = Number(data.requested_threads || data.thread_count || 0).toLocaleString();
        var physical = Number(data.physical_workers || 0).toLocaleString();
        return (
            'Selected workload ' + escapeHtml(String(data.workload_id || '')) +
            ' on schema ' + escapeHtml(schema) +
            ' · requested threads ' + escapeHtml(requested) +
            ' · physical workers ' + escapeHtml(physical)
        );
    }

    function updateSelectedWorkloadCaption(data) {
        var el = $('selected-workload-caption');
        if (!el) return;
        el.textContent = data ? (
            'Selected workload ' + String(data.workload_id || '') +
            ' on schema ' + String(data.schema_name || data.table_prefix || 'GCB') +
            ' · requested threads ' + Number(data.requested_threads || data.thread_count || 0).toLocaleString() +
            ' · physical workers ' + Number(data.physical_workers || 0).toLocaleString()
        ) : 'No active workload selected.';
    }

    function resetWorkloadDashboard() {
        $('counter-inserts').textContent = '0';
        $('counter-updates').textContent = '0';
        $('counter-deletes').textContent = '0';
        $('counter-selects').textContent = '0';
        $('counter-errors').textContent = '0';
        $('ops-per-sec').textContent = 'INS/s 0.0 · UPD/s 0.0 · DEL/s 0.0 · SEL/s 0.0';
        $('progress-fill').style.width = '0%';
        $('progress-text').textContent = 'Preparing workload...';
        $('progress-pct').textContent = '0%';
        $('elapsed-badge').textContent = 'Preparing';
        updateSelectedWorkloadCaption(null);
    }

    function removeWorkloadState(workloadId) {
        if (!workloadId) return;
        delete workloadStates[workloadId];
        if (selectedWorkloadId === workloadId) {
            selectedWorkloadId = '';
        }
        ensureSelectedWorkload();
    }

    function rememberWorkloadState(data) {
        if (!data || !data.workload_id) return;
        var workloadId = String(data.workload_id);
        if (!isWorkloadActive(data)) {
            removeWorkloadState(workloadId);
            return;
        }
        workloadStates[workloadId] = Object.assign({}, workloadStates[workloadId] || {}, data, {
            workload_id: workloadId,
        });
        ensureSelectedWorkload();
    }

    function replaceCurrentConnectionWorkloads(workloads) {
        workloadStates = {};
        (workloads || []).forEach(function (item) {
            if (item && item.workload_id) {
                workloadStates[String(item.workload_id)] = Object.assign({}, item, {
                    workload_id: String(item.workload_id),
                });
            }
        });
        ensureSelectedWorkload();
    }

    function getWorkloadChartSeries(workloadId) {
        if (!workloadId) return null;
        if (!workloadChartSeries[workloadId]) {
            var events = {};
            ALL_GC_EVENTS.forEach(function (ev) { events[ev] = []; });
            workloadChartSeries[workloadId] = {
                labels: [],
                events: events,
            };
        }
        return workloadChartSeries[workloadId];
    }

    function restoreSelectedWorkloadChart() {
        if (!gcChart) initGCChart();
        var selected = getSelectedWorkload();
        var series = selected ? getWorkloadChartSeries(String(selected.workload_id || '')) : null;

        gcChart.data.labels = series ? series.labels.slice() : [];
        gcChart.data.datasets.forEach(function (dataset) {
            dataset.data = series ? (series.events[dataset.label] || []).slice() : [];
        });
        gcChart.update('none');
    }

    function selectWorkload(workloadId) {
        var targetId = String(workloadId || '');
        if (!targetId || !workloadStates[targetId]) return;
        selectedWorkloadId = targetId;
        renderRunningWorkloadBanner();
        renderActiveWorkloads();
        updateProgress(workloadStates[targetId]);
        updateSelectedWorkloadCaption(workloadStates[targetId]);
        restoreSelectedWorkloadChart();
        syncWorkloadButtons();
    }

    function formatWorkloadNoticeMessage(msg) {
        if (!msg) return '';
        var prefix = '';
        if (msg.workload_id) {
            var current = workloadStates[String(msg.workload_id)] || {};
            var schema = current.schema_name || current.table_prefix || '';
            prefix = '[' + String(msg.workload_id) + (schema ? ' · ' + schema : '') + '] ';
        }
        return prefix + String(msg.message || '');
    }

    async function loadConnectionStatus() {
        try {
            var data = await api('GET', '/api/connection/status');
            if (data.host) $('conn-host').value = data.host;
            if (data.port) $('conn-port').value = data.port;
            if (data.service_name) $('conn-service').value = data.service_name;
            if (data.user) $('conn-user').value = data.user;
            if (data.mode) $('conn-mode').value = data.mode;
            loadWorkloadStatus();
            loadLoginWorkloadStatus();
            loadLoginProcedureStatus();
        } catch (e) { /* ignore */ }
    }

    async function testConnection() {
        showStatus('conn-status', 'Testing connection...', 'info');
        var result = await api('POST', '/api/connection/test', getConnFields());
        showStatus('conn-status', result.message, result.ok ? 'success' : 'error');
        // Refresh recent-connections list after a successful test (backend auto-saves)
        if (result.ok) {
            loadRecentConnections();
            loadWorkloadStatus();
            loadLoginWorkloadStatus();
            loadLoginProcedureStatus();
        }
    }

    async function checkPrivileges() {
        showStatus('priv-status', 'Checking privileges...', 'info');
        var result = await api('POST', '/api/connection/privileges');
        if (!result.ok) {
            showStatus('priv-status', result.message || 'Failed to check privileges', 'error');
            return;
        }
        var privs = result.privileges;
        var html = '<ul class="priv-list">';
        for (var view in privs) {
            var ok = privs[view];
            html += '<li class="' + (ok ? 'priv-ok' : 'priv-fail') + '">' +
                (ok ? '&#10003; ' : '&#10007; ') + escapeHtml(view) +
                (ok ? ' \u2014 accessible' : ' \u2014 NO ACCESS') + '</li>';
        }
        html += '</ul>';
        $('priv-status').innerHTML = html;
    }

    async function saveConnection() {
        var result = await api('POST', '/api/connection/save', getConnFields());
        showStatus('conn-status', result.message, result.ok ? 'success' : 'error');
    }

    // ================================================================
    // Schema Tab
    // ================================================================

    function getSchemaConfig() {
        var partType = document.querySelector('input[name="partition"]:checked').value;
        var hashCount = document.querySelector('input[name="hashcount"]:checked');
        var rangeInt = document.querySelector('input[name="rangeinterval"]:checked');
        return {
            table_prefix: $('schema-prefix').value || 'GCB',
            table_count: parseInt($('schema-table-count').value),
            seed_rows: Math.max(0, parseInt($('schema-seed-rows').value, 10) || 0),
            partition_type: partType,
            partition_count: hashCount ? parseInt(hashCount.value) : 8,
            range_interval: rangeInt ? rangeInt.value : 'MONTHLY',
            compression: $('schema-compression').value,
        };
    }

    function updatePartitionOptions() {
        var val = document.querySelector('input[name="partition"]:checked').value;
        $('hash-options').classList.toggle('visible', val === 'HASH');
        $('range-options').classList.toggle('visible', val === 'RANGE');
    }

    function updateCompressionWarning() {
        var val = $('schema-compression').value;
        $('hcc-warning').style.display = val.startsWith('HCC') ? 'flex' : 'none';
    }

    async function previewDDL() {
        var cfg = getSchemaConfig();
        var params = new URLSearchParams(cfg);
        var result = await api('GET', '/api/schema/preview?' + params.toString());
        $('ddl-preview-content').textContent = result.ddl || 'No DDL generated.';
        $('ddl-modal').classList.remove('hidden');
    }

    function appendSchemaLog(message) {
        var log = $('schema-log');
        log.style.display = 'block';
        var line = document.createElement('div');
        line.className = 'log-line';
        if (message.includes('ERROR')) {
            line.className += ' log-error';
        } else if (message.includes('Created') || message.includes('Dropped') || message.includes('complete')) {
            line.className += ' log-success';
        }
        line.textContent = message;
        log.appendChild(line);
        log.scrollTop = log.scrollHeight;
    }

    /**
     * Show a workload-specific warning or error notice below the config card.
     * Does not contaminate the schema log panel.
     */
    function appendWorkloadNotice(message, type) {
        var container = $('workload-notices');
        if (!container) return;
        container.style.display = 'block';
        var div = document.createElement('div');
        var level = (type === 'warning' || type === 'success' || type === 'info') ? type : 'error';
        var prefix = level === 'warning' ? '⚠ ' : (level === 'info' ? 'ℹ ' : (level === 'success' ? '✓ ' : '✖ '));
        div.className = 'status-box status-' + level;
        div.textContent = prefix + message;
        container.appendChild(div);
        container.scrollTop = container.scrollHeight;
    }

    function appendLoginWorkloadNotice(message, type) {
        var container = $('login-workload-notices');
        if (!container) return;
        container.style.display = 'block';
        var div = document.createElement('div');
        var level = (type === 'warning' || type === 'success' || type === 'info') ? type : 'error';
        var prefix = level === 'warning' ? '⚠ ' : (level === 'info' ? 'ℹ ' : (level === 'success' ? '✓ ' : '✖ '));
        div.className = 'status-box status-' + level;
        div.textContent = prefix + message;
        container.appendChild(div);
        container.scrollTop = container.scrollHeight;
    }

    async function createSchema() {
        $('schema-log').innerHTML = '';
        $('schema-log').style.display = 'block';
        appendSchemaLog('Starting schema creation...');
        ensureWebSocket();
        var result = await api('POST', '/api/schema/create', getSchemaConfig());
        if (!result.ok) {
            appendSchemaLog('ERROR: ' + (result.message || 'Unknown error'));
        }
    }

    function dropSchema() {
        confirmDialog('Drop all benchmark tables? This cannot be undone.', async function () {
            $('schema-log').innerHTML = '';
            $('schema-log').style.display = 'block';
            appendSchemaLog('Dropping schema...');
            ensureWebSocket();
            var result = await api('DELETE', '/api/schema/drop', getSchemaConfig());
            if (!result.ok) {
                appendSchemaLog('ERROR: ' + (result.message || 'Unknown error'));
            }
        });
    }

    function dropSchemaByPrefix() {
        var prefix = (($('schema-drop-prefix') || {}).value || '').trim().toUpperCase();
        if (!prefix) {
            appendSchemaLog('ERROR: Enter a table prefix to drop.');
            return;
        }

        confirmDialog('Drop all existing tables starting with ' + prefix + '_ORDER_ ? This cannot be undone.', async function () {
            $('schema-log').innerHTML = '';
            $('schema-log').style.display = 'block';
            appendSchemaLog('Dropping tables for prefix ' + prefix + '...');
            ensureWebSocket();
            var result = await api('DELETE', '/api/schema/drop-prefix', { table_prefix: prefix });
            if (!result.ok) {
                appendSchemaLog('ERROR: ' + (result.message || 'Unknown error'));
            }
        });
    }

    function killSessionsByUser() {
        var username = (($('schema-kill-user') || {}).value || 'PP').trim().toUpperCase();
        if (!username) {
            appendSchemaLog('ERROR: Enter a database user to kill sessions.');
            return;
        }

        confirmDialog('Kill all active sessions for user ' + username + '? This will disconnect those sessions immediately.', async function () {
            $('schema-log').innerHTML = '';
            $('schema-log').style.display = 'block';
            appendSchemaLog('Killing sessions for user ' + username + '...');
            ensureWebSocket();
            var result = await api('POST', '/api/schema/kill-sessions', { username: username });
            if (!result.ok) {
                appendSchemaLog('ERROR: ' + (result.message || 'Unknown error'));
            }
        });
    }

    // ================================================================
    // Workload Tab
    // ================================================================

    function getWorkloadConfig() {
        // Read contention mode from radio group
        var modeRadio = document.querySelector('input[name="contention_mode"]:checked');
        var contentionMode = modeRadio ? modeRadio.value : 'NORMAL';
        var lockHold = $('wl-lock-hold') ? parseInt($('wl-lock-hold').value) || 0 : 0;
        return {
            table_prefix:     $('wl-prefix').value               || 'GCB',
            schema_name:      $('wl-schema-name').value          || $('wl-prefix').value || 'GCB',
            table_count:      parseInt($('wl-table-count').value) || 10,
            thread_count:     parseInt($('wl-threads').value),
            duration_seconds: parseInt($('wl-duration').value)    || 60,
            seed_rows:        getCurrentWorkloadSeedRows(),
            hot_row_pct:      parseInt($('wl-hotrow').value),
            insert_pct:       parseInt($('wl-insert-pct').value, 10) || 0,
            update_pct:       parseInt($('wl-update-pct').value, 10) || 0,
            delete_pct:       parseInt($('wl-delete-pct').value, 10) || 0,
            select_pct:       parseInt($('wl-select-pct').value, 10) || 0,
            // Schema metadata — set by schema select dropdown
            partition_type:   $('wl-partition-type').value        || 'NONE',
            partition_detail: $('wl-partition-detail').value      || '',
            compression:      $('wl-compression').value           || 'NONE',
            // Contention mode
            contention_mode:  contentionMode,
            lock_hold_ms:     lockHold,
        };
    }

    async function restartPdb() {
        var pdbName = (($('wl-pdb-name') || {}).value || '').trim();
        if (!pdbName) {
            appendWorkloadNotice('Enter a PDB name first.', 'error');
            return;
        }

        var notices = $('workload-notices');
        if (notices) {
            notices.innerHTML = '';
            notices.style.display = 'block';
        }
        appendWorkloadNotice('Restarting PDB ' + pdbName.toUpperCase() + ' using the configured CDB Connection...', 'info');

        var result = await api('POST', '/api/db/pdb-restart', { pdb_name: pdbName });
        if (!result.ok) {
            appendWorkloadNotice(result.message || 'PDB restart failed.', 'error');
            return;
        }

        (result.steps || []).forEach(function (step) {
            appendWorkloadNotice(step, 'info');
        });
        appendWorkloadNotice(result.message || ('PDB ' + pdbName.toUpperCase() + ' restarted.'), 'success');
    }

    async function startWorkload() {
        if (workloadStartPending) return;

        workloadStartPending = true;
        syncWorkloadButtons();

        $('live-dashboard').style.display = 'block';
        var notices = $('workload-notices');
        if (notices) { notices.innerHTML = ''; notices.style.display = 'none'; }
        $('workload-summary').innerHTML = '';
        resetWorkloadDashboard();
        initGCChart();

        ensureWebSocket();

        try {
            var result = await api('POST', '/api/workload/start', getWorkloadConfig());
            if (!result.ok) {
                $('workload-summary').innerHTML =
                    '<div class="status-box status-error">' + escapeHtml(result.message || 'Failed to start') + '</div>';
                return;
            }

            if (result.status) {
                rememberWorkloadState(result.status);
            }
            if (result.workload_id) {
                selectedWorkloadId = String(result.workload_id);
            }

            renderRunningWorkloadBanner();
            renderActiveWorkloads();
            if (selectedWorkloadId && workloadStates[selectedWorkloadId]) {
                updateProgress(workloadStates[selectedWorkloadId]);
                updateSelectedWorkloadCaption(workloadStates[selectedWorkloadId]);
            }
            appendWorkloadNotice(result.message || 'Workload started.', 'success');
            restoreSelectedWorkloadChart();
            await loadWorkloadStatus();
        } finally {
            workloadStartPending = false;
            syncWorkloadButtons();
        }
    }

    async function stopWorkload(workloadId) {
        var targetId = String(workloadId || selectedWorkloadId || '');
        if (!targetId) return;

        var result = await api('POST', '/api/workload/stop', { workload_id: targetId });
        if (!result.ok) {
            appendWorkloadNotice(result.message || ('Failed to stop workload ' + targetId), 'error');
            return;
        }

        if (result.status) {
            rememberWorkloadState(result.status);
            if (result.status.phase === 'STOPPED' || result.status.phase === 'ERROR' || result.status.phase === 'COMPLETE') {
                removeWorkloadState(targetId);
            }
        }

        appendWorkloadNotice('Stop requested for workload ' + targetId + '.', 'info');
        renderRunningWorkloadBanner();
        renderActiveWorkloads();
        if (getSelectedWorkload()) {
            updateProgress(getSelectedWorkload());
            updateSelectedWorkloadCaption(getSelectedWorkload());
        } else {
            $('live-dashboard').style.display = 'none';
            updateSelectedWorkloadCaption(null);
        }
        syncWorkloadButtons();
    }

    function renderActiveWorkloads() {
        var panel = $('active-workloads-panel');
        var body = $('active-workloads-body');
        var subtitle = $('active-workloads-subtitle');
        if (!panel || !body) return;

        var workloads = sortWorkloads(getActiveWorkloads());
        if (workloads.length === 0) {
            panel.style.display = 'none';
            body.innerHTML = '<tr><td colspan="7" class="text-col">No active workloads on this connection.</td></tr>';
            return;
        }

        panel.style.display = 'block';
        subtitle.textContent = selectedWorkloadId
            ? ('Selected workload ' + selectedWorkloadId + ' drives the dashboard below.')
            : 'Select one workload to drive the live dashboard and chart.';

        body.innerHTML = workloads.map(function (item) {
            var workloadId = String(item.workload_id || '');
            var selected = workloadId === selectedWorkloadId;
            var schema = item.schema_name || item.table_prefix || 'GCB';
            var elapsed = Number(item.elapsed || 0).toFixed(1) + 's / ' + Number(item.duration || 0).toLocaleString() + 's';
            var requested = Number(item.requested_threads || item.thread_count || 0).toLocaleString();
            var physical = Number(item.physical_workers || 0).toLocaleString();
            var seedRows = Number(item.seed_rows || 0).toLocaleString();
            var phase = String(item.phase || 'RUNNING').toUpperCase();
            var phaseClass = 'phase-' + phase.toLowerCase();
            var restartMeta = item.orphaned ? ' · Recovered after restart' : '';

            return (
                '<tr class="' + (selected ? 'active-workload-selected' : '') + '">' +
                    '<td>' +
                        '<div class="active-workload-id">' +
                            '<strong>' + escapeHtml(workloadId) + '</strong>' +
                            '<span class="active-workload-meta">Tables ' + escapeHtml(Number(item.table_count || 0).toLocaleString()) + ' · Seed ' + escapeHtml(seedRows) + ' · Workers ' + escapeHtml(physical) + escapeHtml(restartMeta) + '</span>' +
                        '</div>' +
                    '</td>' +
                    '<td class="text-col">' + escapeHtml(schema) + '</td>' +
                    '<td>' + requested + '</td>' +
                    '<td>' + escapeHtml(item.contention_mode || 'NORMAL') + '</td>' +
                    '<td><span class="active-workload-phase ' + phaseClass + '">' + escapeHtml(phase) + '</span></td>' +
                    '<td>' + escapeHtml(elapsed) + '</td>' +
                    '<td>' +
                        '<div class="active-workload-actions">' +
                            '<button class="btn btn-secondary btn-sm" data-workload-select="' + escapeHtml(workloadId) + '">View</button>' +
                            '<button class="btn btn-danger btn-sm" data-workload-stop="' + escapeHtml(workloadId) + '">Stop</button>' +
                        '</div>' +
                    '</td>' +
                '</tr>'
            );
        }).join('');
    }

    function renderRunningWorkloadBanner(statusData) {
        var el = $('running-workload-banner');
        if (!el) return;

        var data = statusData || {};
        var workloads = sortWorkloads(getActiveWorkloads());
        var selected = getSelectedWorkload();
        var otherCount = Number(data.other_connection_workload_count || 0);
        var otherLabels = data.other_connection_labels || [];

        if (workloads.length === 0 && !otherCount) {
            el.style.display = 'none';
            el.innerHTML = '';
            return;
        }

        if (workloads.length === 0 && otherCount) {
            el.className = 'status-box status-info';
            el.innerHTML =
                'No active workloads match this connection. ' +
                'Other connection workload(s) still active in this app: <b>' +
                escapeHtml(otherLabels.join(', ') || String(otherCount)) +
                '</b>.';
            el.style.display = 'flex';
            return;
        }

        el.className = 'status-box status-warning';
        var selectedLabel = '';
        if (selected) {
            var recoveredNote = selected.orphaned
                ? ' Recovered after app restart; live updates are unavailable.'
                : '';
            selectedLabel =
                ' Selected workload <b>' + escapeHtml(String(selected.workload_id || '')) + '</b>' +
                ' on schema <b>' + escapeHtml(selected.schema_name || selected.table_prefix || 'GCB') + '</b>' +
                ' is <b>' + escapeHtml(String(selected.phase || 'RUNNING')) + '</b>' +
                ' at <b>' + escapeHtml(Number(selected.elapsed || 0).toFixed(1)) + 's / ' +
                escapeHtml(Number(selected.duration || 0).toLocaleString()) + 's</b>.' +
                recoveredNote;
        }

        el.innerHTML =
            '<b>' + escapeHtml(String(workloads.length)) + '</b> active workload(s) on this connection.' +
            selectedLabel +
            (otherCount
                ? (' <span>Other connection workload(s) in this app: <b>' + escapeHtml(String(otherCount)) + '</b>.</span>')
                : '');
        el.style.display = 'flex';
    }

    async function loadWorkloadStatus() {
        try {
            var statusUrl = buildWorkloadStatusUrl();
            if (!statusUrl) {
                workloadStates = {};
                selectedWorkloadId = '';
                renderRunningWorkloadBanner(null);
                renderActiveWorkloads();
                $('live-dashboard').style.display = 'none';
                updateSelectedWorkloadCaption(null);
                syncWorkloadButtons();
                return;
            }

            var data = await api('GET', statusUrl);
            replaceCurrentConnectionWorkloads(data.workloads || []);
            renderRunningWorkloadBanner(data);
            renderActiveWorkloads();
            syncWorkloadButtons();

            var selected = getSelectedWorkload();
            if (selected) {
                $('live-dashboard').style.display = 'block';
                updateProgress(selected);
                updateSelectedWorkloadCaption(selected);
                restoreSelectedWorkloadChart();
                ensureWebSocket();
            } else {
                $('live-dashboard').style.display = 'none';
                updateSelectedWorkloadCaption(null);
            }
        } catch (e) {
            workloadStates = {};
            selectedWorkloadId = '';
            renderRunningWorkloadBanner(null);
            renderActiveWorkloads();
            $('live-dashboard').style.display = 'none';
            updateSelectedWorkloadCaption(null);
            syncWorkloadButtons();
        }
    }

    function getLoginWorkloadConfig() {
        var stopModeRadio = document.querySelector('input[name="login-stop-mode"]:checked');
        var sessionCaseRadio = document.querySelector('input[name="login-session-case"]:checked');
        var stopMode = stopModeRadio ? stopModeRadio.value : 'CYCLES';
        var sessionCase = sessionCaseRadio ? sessionCaseRadio.value : 'SIMPLE_QUERY';
        return {
            sql_text: (($('login-sql-text') || {}).value || 'select 1 from dual').trim(),
            thread_count: parseInt((($('login-thread-count-input') || {}).value || ($('login-thread-count') || {}).value || '20'), 10) || 20,
            stop_mode: stopMode,
            iterations_per_thread: stopMode === 'CYCLES'
                ? (parseInt((($('login-iterations') || {}).value || '1000'), 10) || 0)
                : 0,
            duration_seconds: stopMode === 'DURATION'
                ? (parseInt((($('login-duration-seconds') || {}).value || '300'), 10) || 300)
                : 0,
            think_time_ms: parseInt((($('login-think-time-ms') || {}).value || '0'), 10) || 0,
            session_case: sessionCase,
            module_name: (($('login-module-name') || {}).value || 'DBSTRESS_LOGIN_SESSION_00000000').trim(),
        };
    }

    function updateLoginStopModeUI() {
        var modeRadio = document.querySelector('input[name="login-stop-mode"]:checked');
        var mode = modeRadio ? modeRadio.value : 'CYCLES';
        var durationRow = $('login-duration-row');
        var iterationsRow = $('login-iterations-row');
        if (durationRow) durationRow.style.display = mode === 'DURATION' ? '' : 'none';
        if (iterationsRow) iterationsRow.style.display = mode === 'CYCLES' ? '' : 'none';
    }

    function updateLoginSessionCaseUI() {
        var sessionCaseRadio = document.querySelector('input[name="login-session-case"]:checked');
        var sessionCase = sessionCaseRadio ? sessionCaseRadio.value : 'SIMPLE_QUERY';
        var moduleRow = $('login-module-name-row');
        if (moduleRow) moduleRow.style.display = sessionCase === 'MFES_ONLINE' ? '' : 'none';
    }

    function renderLoginProcedureStatus(data) {
        var el = $('login-procedure-status');
        if (!el) return;

        var ok = !!(data && data.ok);
        var exists = !!(data && data.exists);
        var valid = !!(data && data.valid);
        var errors = (data && data.errors) || [];
        var message = (data && data.message) || 'Unable to determine procedure status.';

        el.className = 'status-box ' + (
            valid ? 'status-success' :
            (exists ? 'status-warning' : (ok ? 'status-info' : 'status-error'))
        );

        var html = '<b>GRAV_SESSION_MFES_ONLINE</b> · ' + escapeHtml(message);
        if (errors.length) {
            html += '<br>' + escapeHtml(errors.slice(0, 3).join(' | '));
        }
        el.innerHTML = html;
    }

    async function loadLoginProcedureStatus() {
        try {
            var data = await api('GET', '/api/login-workload/procedure/status');
            renderLoginProcedureStatus(data);
            return data;
        } catch (e) {
            renderLoginProcedureStatus({
                ok: false,
                exists: false,
                valid: false,
                message: 'Failed to check procedure status.',
            });
            return null;
        }
    }

    async function createLoginProcedure() {
        renderLoginProcedureStatus({
            ok: true,
            exists: false,
            valid: false,
            message: 'Creating procedure...',
        });
        var result = await api('POST', '/api/login-workload/procedure/create');
        renderLoginProcedureStatus(result);
        appendLoginWorkloadNotice(result.message || 'Procedure action completed.', result.ok ? 'success' : 'error');
        loadLoginWorkloadStatus();
    }

    function dropLoginProcedure() {
        confirmDialog('Drop GRAV_SESSION_MFES_ONLINE from the current schema?', async function () {
            renderLoginProcedureStatus({
                ok: true,
                exists: true,
                valid: false,
                message: 'Dropping procedure...',
            });
            var result = await api('POST', '/api/login-workload/procedure/drop');
            renderLoginProcedureStatus(result);
            appendLoginWorkloadNotice(result.message || 'Procedure action completed.', result.ok ? 'success' : 'error');
            loadLoginWorkloadStatus();
        });
    }

    function resetLoginWorkloadDashboard() {
        $('login-counter-active').textContent = '0';
        $('login-counter-logons').textContent = '0';
        $('login-counter-queries').textContent = '0';
        $('login-counter-logouts').textContent = '0';
        $('login-counter-cycles').textContent = '0';
        $('login-counter-errors').textContent = '0';
        $('login-progress-fill').style.width = '0%';
        $('login-progress-text').textContent = 'Preparing login workload...';
        $('login-progress-pct').textContent = '0%';
        $('login-elapsed-badge').textContent = 'Preparing';
        $('login-ops-per-sec').textContent = 'LOGON/s 0.0 · QRY/s 0.0 · LOGOFF/s 0.0 · AVG cycle 0.0 ms';
    }

    async function startLoginWorkload() {
        if (loginWorkloadRunning) return;

        var cfg = getLoginWorkloadConfig();
        if (cfg.session_case === 'MFES_ONLINE') {
            var procStatus = await loadLoginProcedureStatus();
            if (!procStatus || !procStatus.valid) {
                $('login-workload-summary').innerHTML =
                    '<div class="status-box status-error">MFES Online requires the precreated procedure <b>GRAV_SESSION_MFES_ONLINE</b>. Create it first, then start the test again.</div>';
                return;
            }
        }

        $('login-live-dashboard').style.display = 'block';
        $('login-workload-summary').innerHTML = '';

        var notices = $('login-workload-notices');
        if (notices) {
            notices.innerHTML = '';
            notices.style.display = 'none';
        }

        resetLoginWorkloadDashboard();
        $('btn-start-login-workload').disabled = true;
        $('btn-stop-login-workload').disabled = false;
        loginWorkloadRunning = true;

        ensureWebSocket();

        var result = await api('POST', '/api/login-workload/start', cfg);
        if (!result.ok) {
            $('login-live-dashboard').style.display = 'none';
            $('login-workload-summary').innerHTML =
                '<div class="status-box status-error">' + escapeHtml(result.message || 'Failed to start login workload') + '</div>';
            loginWorkloadRunning = false;
            $('btn-start-login-workload').disabled = false;
            $('btn-stop-login-workload').disabled = true;
            return;
        }

        appendLoginWorkloadNotice(result.message || 'Login workload started.', 'success');
        loadLoginWorkloadStatus();
    }

    async function stopLoginWorkload() {
        await api('POST', '/api/login-workload/stop');
        loginWorkloadRunning = false;
        $('btn-start-login-workload').disabled = false;
        $('btn-stop-login-workload').disabled = true;
        $('login-live-dashboard').style.display = 'none';
        $('running-login-workload-banner').style.display = 'none';
        $('running-login-workload-banner').innerHTML = '';
    }

    function renderRunningLoginWorkloadBanner(data) {
        var el = $('running-login-workload-banner');
        if (!el) return;

        if (!data || (!data.running && !data.other_login_workload_running)) {
            el.style.display = 'none';
            el.innerHTML = '';
            return;
        }

        if (data.other_login_workload_running && !data.running) {
            el.className = 'status-box status-info';
            el.innerHTML =
                'Another database login workload is active in this app process: <b>' +
                escapeHtml(data.other_connection_label || 'unknown connection') +
                '</b>. This page is scoped to a different connection.';
            el.style.display = 'flex';
            return;
        }

        el.className = 'status-box status-warning';

        var requestedThreads = Number(data.requested_threads || data.thread_count || 0).toLocaleString();
        var physicalWorkers = Number(data.physical_workers || 0).toLocaleString();
        var stopMode = String(data.stop_mode || 'CYCLES').toUpperCase();
        var sessionCase = String(data.session_case || 'SIMPLE_QUERY').toUpperCase();
        var iterations = Number(data.iterations_per_thread || 0).toLocaleString();
        var targetSeconds = Number(data.duration_seconds || data.target_seconds || 0).toLocaleString();
        var cycles = Number(data.cycles || 0).toLocaleString();
        var elapsed = Number(data.elapsed || 0).toFixed(1);
        var statusMessage = data.status_message || '';
        var queryPreview = String(data.sql_text || '').replace(/\s+/g, ' ').trim();
        if (queryPreview.length > 80) {
            queryPreview = queryPreview.slice(0, 77) + '...';
        }
        var stopLabel = 'Until Stop';
        if (stopMode === 'DURATION') stopLabel = targetSeconds + 's';
        else if (stopMode === 'CYCLES') stopLabel = (iterations === '0' ? 'open-ended' : (iterations + ' / thread'));

        el.innerHTML =
            '<b>RUNNING login workload</b> ' +
            'Case <b>' + escapeHtml(sessionCase) + '</b> · ' +
            'Threads <b>' + escapeHtml(requestedThreads) + '</b> · ' +
            'Physical Workers <b>' + escapeHtml(physicalWorkers) + '</b> · ' +
            'Stop <b>' + escapeHtml(stopLabel) + '</b> · ' +
            'Cycles <b>' + escapeHtml(cycles) + '</b> · ' +
            'Elapsed <b>' + escapeHtml(elapsed) + 's</b> · ' +
            'Query <b>' + escapeHtml(queryPreview || 'select 1 from dual') + '</b> · ' +
            'Status <b>' + escapeHtml(statusMessage || 'Running') + '</b>';
        el.style.display = 'flex';
    }

    function updateLoginProgress(data) {
        $('login-counter-active').textContent = Number(data.active_connections || 0).toLocaleString();
        $('login-counter-logons').textContent = Number(data.logons || 0).toLocaleString();
        $('login-counter-queries').textContent = Number(data.queries || 0).toLocaleString();
        $('login-counter-logouts').textContent = Number(data.logouts || 0).toLocaleString();
        $('login-counter-cycles').textContent = Number(data.cycles || 0).toLocaleString();
        $('login-counter-errors').textContent = Number(data.errors || 0).toLocaleString();

        var elapsed = Number(data.elapsed || 0);
        var cycles = Number(data.cycles || 0);
        var targetCycles = Number(data.target_cycles || 0);
        var targetSeconds = Number(data.target_seconds || data.duration_seconds || 0);
        var phase = data.phase || 'RUNNING';
        var denom = elapsed > 0 ? elapsed : 1;

        $('login-ops-per-sec').textContent =
            'LOGON/s ' + ((Number(data.logons || 0)) / denom).toFixed(1) +
            ' · QRY/s ' + ((Number(data.queries || 0)) / denom).toFixed(1) +
            ' · LOGOFF/s ' + ((Number(data.logouts || 0)) / denom).toFixed(1) +
            ' · AVG cycle ' + Number(data.avg_cycle_ms || 0).toFixed(1) + ' ms';

        if (phase !== 'RUNNING') {
            var finalPct = targetCycles > 0
                ? Math.min(100, (cycles / Math.max(targetCycles, 1)) * 100)
                : (targetSeconds > 0 ? Math.min(100, (elapsed / Math.max(targetSeconds, 1)) * 100) : 100);
            $('login-progress-fill').style.width = finalPct.toFixed(1) + '%';
            $('login-progress-pct').textContent = phase;
            $('login-progress-text').textContent = data.status_message || ('Login workload ' + phase.toLowerCase());
            $('login-elapsed-badge').textContent = targetCycles > 0
                ? (Number(cycles).toLocaleString() + ' / ' + Number(targetCycles).toLocaleString() + ' cycles')
                : (targetSeconds > 0
                    ? (Math.floor(elapsed) + ' / ' + Number(targetSeconds).toLocaleString() + ' sec')
                    : (Number(cycles).toLocaleString() + ' cycles'));
            return;
        }

        if (targetCycles > 0) {
            var pct = Math.min(100, (cycles / Math.max(targetCycles, 1)) * 100);
            $('login-progress-fill').style.width = pct.toFixed(1) + '%';
            $('login-progress-pct').textContent = pct.toFixed(0) + '%';
            $('login-elapsed-badge').textContent = Number(cycles).toLocaleString() + ' / ' + Number(targetCycles).toLocaleString() + ' cycles';
        } else if (targetSeconds > 0) {
            var timePct = Math.min(100, (elapsed / Math.max(targetSeconds, 1)) * 100);
            $('login-progress-fill').style.width = timePct.toFixed(1) + '%';
            $('login-progress-pct').textContent = timePct.toFixed(0) + '%';
            $('login-elapsed-badge').textContent = Math.floor(elapsed) + ' / ' + Number(targetSeconds).toLocaleString() + ' sec';
        } else {
            $('login-progress-fill').style.width = '100%';
            $('login-progress-pct').textContent = 'OPEN';
            $('login-elapsed-badge').textContent = Number(cycles).toLocaleString() + ' cycles';
        }

        $('login-progress-text').textContent = 'Running — ' + Math.floor(elapsed) + 's elapsed';
    }

    function showLoginCompletionSummary(msg) {
        loginWorkloadRunning = false;
        $('btn-start-login-workload').disabled = false;
        $('btn-stop-login-workload').disabled = true;
        $('login-live-dashboard').style.display = 'none';
        renderRunningLoginWorkloadBanner(null);

        var currentRunId = String(msg.run_id || '');
        var s = msg.summary || {};
        var targetCycles = Number(s.target_cycles || 0);
        var targetSeconds = Number(s.target_seconds || 0);
        var cycles = Number(s.cycles || 0);
        var phase = String(s.phase || '').toUpperCase();
        var isStopped = phase === 'STOPPED' || phase === 'STOPPING';
        var title = isStopped ? 'Login Workload Stopped' : 'Login Workload Complete';
        var sessionCase = String(s.session_case || 'SIMPLE_QUERY').toUpperCase();

        api('GET', '/api/results').then(function (data) {
            var runs = (data.runs || []).filter(function (run) {
                return getRunNotes(run).run_type === 'LOGIN_SIM';
            }).sort(function (a, b) {
                return a.run_id - b.run_id;
            });

            var html = '<div class="summary-card">';
            html += '<div class="summary-card-header">';
            html += '<h3>&#10003; ' + escapeHtml(title) + (currentRunId ? (' #' + escapeHtml(currentRunId)) : '') + '</h3>';
            html += '<span class="summary-stats">';
            html += 'Case&nbsp;<b>' + escapeHtml(sessionCase) + '</b>&emsp;';
            html += 'Logons&nbsp;<b>' + Number(s.logons || 0).toLocaleString() + '</b>&emsp;';
            html += 'Queries&nbsp;<b>' + Number(s.queries || 0).toLocaleString() + '</b>&emsp;';
            html += 'Logouts&nbsp;<b>' + Number(s.logouts || 0).toLocaleString() + '</b>&emsp;';
            html += 'Cycles&nbsp;<b>' + cycles.toLocaleString() + '</b>&emsp;';
            html += 'Errors&nbsp;<b>' + Number(s.errors || 0).toLocaleString() + '</b>&emsp;';
            html += 'Avg Cycle&nbsp;<b>' + Number(s.avg_cycle_ms || 0).toFixed(1) + ' ms</b>&emsp;';
            html += 'Elapsed&nbsp;<b>' + Number(s.elapsed || 0).toFixed(1) + ' s</b>';
            html += '</span>';
            html += '</div>';
            html += '<div class="status-box status-info" style="margin-top:0">';
            html += 'Target: <b>' + escapeHtml(
                targetCycles > 0
                    ? (targetCycles.toLocaleString() + ' cycles')
                    : (targetSeconds > 0 ? (targetSeconds.toLocaleString() + ' sec') : 'until stop')
            ) + '</b> · Completed cycles: <b>' + escapeHtml(cycles.toLocaleString()) + '</b>';
            if (s.status_message) {
                html += ' · Status: <b>' + escapeHtml(s.status_message) + '</b>';
            }
            html += '</div>';

            if (runs.length > 0) {
                html += '<div class="table-wrap summary-table-wrap"><table class="data-table summary-compare-table">';
                html += '<thead><tr>';
                html += '<th>Run</th><th class="text-col">Case</th><th class="text-col">Stop</th><th>Threads</th><th>Dur</th><th class="text-col">Activity</th>';
                html += '<th class="num col-primary">gc curr congested ★</th><th class="num">gc curr 3-way</th><th class="num">gc cr congested</th>';
                html += '</tr></thead><tbody>';

                runs.forEach(function (run) {
                    var notes = getRunNotes(run);
                    var parsed = run.gc_metrics_parsed || {};
                    var agg = parsed.delta_aggregated || {};
                    var isCurrent = String(run.run_id) === currentRunId;
                    html += '<tr class="' + (isCurrent ? 'row-current' : '') + '">';
                    html += '<td><b>#' + run.run_id + '</b>' + (isCurrent ? ' <span class="badge-new">NEW</span>' : '') + '</td>';
                    html += '<td class="text-col">' + escapeHtml(notes.session_case || 'SIMPLE_QUERY') + '</td>';
                    html += '<td class="text-col">' + escapeHtml(notes.stop_mode || '-') + '</td>';
                    html += '<td>' + Number(run.thread_count || 0).toLocaleString() + '</td>';
                    html += '<td>' + Number(run.duration_secs || 0).toFixed(1) + 's</td>';
                    html += '<td class="text-col">' + escapeHtml(getRunActivity(run)) + '</td>';
                    html += '<td class="num col-primary">' + Number(agg['gc current block congested'] || 0).toLocaleString() + '</td>';
                    html += '<td class="num">' + Number(agg['gc current block 3-way'] || 0).toLocaleString() + '</td>';
                    html += '<td class="num">' + Number(agg['gc cr grant congested'] || 0).toLocaleString() + '</td>';
                    html += '</tr>';
                });

                html += '</tbody></table></div>';
            }

            html += '</div>';
            $('login-workload-summary').innerHTML = html;
            loadResults();
        }).catch(function () {
            var html = '<div class="summary-card">';
            html += '<div class="summary-card-header">';
            html += '<h3>&#10003; ' + escapeHtml(title) + '</h3>';
            html += '<span class="summary-stats">Case&nbsp;<b>' + escapeHtml(sessionCase) + '</b></span>';
            html += '</div></div>';
            $('login-workload-summary').innerHTML = html;
        });
    }

    async function loadLoginWorkloadStatus() {
        try {
            var statusUrl = buildLoginWorkloadStatusUrl();
            if (!statusUrl) {
                loginWorkloadRunning = false;
                $('btn-start-login-workload').disabled = false;
                $('btn-stop-login-workload').disabled = true;
                $('login-live-dashboard').style.display = 'none';
                renderRunningLoginWorkloadBanner(null);
                loadLoginProcedureStatus();
                return;
            }

            var data = await api('GET', statusUrl);
            var running = !!data.running;
            var otherConnectionRunning = !!data.other_login_workload_running;

            loginWorkloadRunning = running;
            $('btn-start-login-workload').disabled = running || otherConnectionRunning;
            $('btn-stop-login-workload').disabled = !running;
            renderRunningLoginWorkloadBanner(data);
            if (data.procedure_status) {
                renderLoginProcedureStatus(data.procedure_status);
            } else {
                loadLoginProcedureStatus();
            }

            if (running) {
                $('login-live-dashboard').style.display = 'block';
                updateLoginProgress(data);
                ensureWebSocket();
            } else {
                $('login-live-dashboard').style.display = 'none';
            }
        } catch (e) {
            loginWorkloadRunning = false;
            $('btn-start-login-workload').disabled = false;
            $('btn-stop-login-workload').disabled = true;
            $('login-live-dashboard').style.display = 'none';
            renderRunningLoginWorkloadBanner(null);
            loadLoginProcedureStatus();
        }
    }

    function formatInstanceCounts(payload) {
        var instances = (payload && payload.instances) || [];
        if (!instances.length) return 'No rows';
        return instances.map(function (item) {
            return 'Inst ' + item.inst_id + ': ' + Number(item.count || 0).toLocaleString();
        }).join(' | ');
    }

    async function loadDbActivity() {
        try {
            var username = (($('db-session-username') || {}).value || '').trim().toUpperCase();
            var url = '/api/db/activity';
            if (username) {
                url += '?username=' + encodeURIComponent(username);
            }
            var data = await api('GET', url);
            if (!data.ok) {
                showStatus('db-activity-status', data.message || 'Failed to load database activity', 'error');
                return;
            }

            $('db-process-total').textContent = Number((data.processes || {}).total || 0).toLocaleString();
            $('db-session-total').textContent = Number((data.sessions || {}).total || 0).toLocaleString();
            $('db-transaction-total').textContent = Number((data.transactions || {}).total || 0).toLocaleString();

            $('db-process-detail').textContent = formatInstanceCounts(data.processes);
            $('db-session-detail').textContent = formatInstanceCounts(data.sessions);
            $('db-transaction-detail').textContent = formatInstanceCounts(data.transactions);
            $('db-session-label').textContent = (data.sessions || {}).label || 'Sessions';

            $('db-activity-sampled-at').textContent = new Date(data.sampled_at).toLocaleTimeString();
            $('db-activity-status').innerHTML = '';
        } catch (e) {
            showStatus('db-activity-status', 'Failed to load database activity', 'error');
        }
    }

    function startDbActivityPolling() {
        if (dbActivityTimer) return;
        loadDbActivity();
        dbActivityTimer = setInterval(loadDbActivity, 5000);
    }

    async function loadCpoolStats() {
        try {
            var data = await api('GET', '/api/db/cpool-stats');
            if (!data.ok) {
                showStatus('cpool-stats-status', data.message || 'Failed to load connection pool stats', 'error');
                return;
            }

            var body = $('cpool-stats-body');
            var rows = data.rows || [];
            if (!rows.length) {
                body.innerHTML = '<tr><td colspan="6" class="text-col">No rows</td></tr>';
            } else {
                body.innerHTML = rows.map(function (row) {
                    return '<tr>' +
                        '<td class="text-col">' + escapeHtml(row.pool_name || '-') + '</td>' +
                        '<td>' + Number(row.num_open_servers || 0).toLocaleString() + '</td>' +
                        '<td>' + Number(row.num_busy_servers || 0).toLocaleString() + '</td>' +
                        '<td>' + Number(row.num_hits || 0).toLocaleString() + '</td>' +
                        '<td>' + Number(row.num_misses || 0).toLocaleString() + '</td>' +
                        '<td>' + Number(row.num_purged || 0).toLocaleString() + '</td>' +
                        '</tr>';
                }).join('');
            }

            $('cpool-stats-sampled-at').textContent = new Date(data.sampled_at).toLocaleTimeString();
            $('cpool-stats-status').innerHTML = '';
        } catch (e) {
            showStatus('cpool-stats-status', 'Failed to load connection pool stats', 'error');
        }
    }

    function getCpoolConnFields() {
        return {
            host: (($('cpool-host') || {}).value || '').trim(),
            port: parseInt((($('cpool-port') || {}).value || '1521'), 10) || 1521,
            service_name: (($('cpool-service') || {}).value || '').trim(),
            user: (($('cpool-user') || {}).value || '').trim(),
            password: (($('cpool-password') || {}).value || ''),
            mode: (($('cpool-mode') || {}).value || 'thin'),
        };
    }

    function applyRecentCpoolConnection(c) {
        var hostEl = $('cpool-host');
        var portEl = $('cpool-port');
        var svcEl = $('cpool-service');
        var userEl = $('cpool-user');
        var modeEl = $('cpool-mode');

        if (hostEl) hostEl.value = c.host || '';
        if (portEl) portEl.value = c.port || 1521;
        if (svcEl) svcEl.value = c.service_name || '';
        if (userEl) userEl.value = c.user || '';
        if (modeEl) modeEl.value = c.mode || 'thin';

        var pwEl = $('cpool-password');
        if (pwEl) { pwEl.value = ''; pwEl.focus(); }

        showStatus('cpool-conn-status', 'Fields filled — enter the password and click Test CDB Connection', 'info');
    }

    async function testCpoolConnection() {
        showStatus('cpool-conn-status', 'Testing CDB connection...', 'info');
        var result = await api('POST', '/api/db/cpool-connection/test', getCpoolConnFields());
        showStatus('cpool-conn-status', result.message, result.ok ? 'success' : 'error');
        if (result.ok) {
            loadRecentCpoolConnections();
            loadCpoolStats();
        }
    }

    function startCpoolStatsPolling() {
        if (cpoolStatsTimer) return;
        loadCpoolStats();
        cpoolStatsTimer = setInterval(loadCpoolStats, 5000);
    }

    function updateProgress(data) {
        if (!data) {
            resetWorkloadDashboard();
            return;
        }
        $('counter-inserts').textContent = (data.inserts || 0).toLocaleString();
        $('counter-updates').textContent = (data.updates || 0).toLocaleString();
        $('counter-deletes').textContent = (data.deletes || 0).toLocaleString();
        $('counter-selects').textContent = (data.selects || 0).toLocaleString();
        $('counter-errors').textContent = (data.errors || 0).toLocaleString();

        var elapsed = data.elapsed || 0;
        var duration = data.duration || 60;
        var phase = data.phase || 'RUNNING';
        if (phase === 'RECOVERED') {
            $('ops-per-sec').textContent = 'Recovered workload after app restart';
            $('progress-fill').style.width = '100%';
            $('progress-pct').textContent = 'RECOVERED';
            $('progress-text').textContent = data.status_message || 'Recovered workload is still running, but live counters are unavailable.';
            $('elapsed-badge').textContent = Math.floor(elapsed) + 's last seen';
            return;
        }
        if (phase !== 'RUNNING') {
            $('ops-per-sec').textContent = 'Waiting for timed run to start...';
            $('progress-fill').style.width = '100%';
            $('progress-pct').textContent = phase;
            $('progress-text').textContent = data.status_message || ('Preparing workload (' + phase + ')');
            $('elapsed-badge').textContent = phase;
            return;
        }
        var pct = Math.min(100, (elapsed / duration) * 100);
        var denom = elapsed > 0 ? elapsed : 1;
        $('ops-per-sec').textContent =
            'INS/s ' + ((data.inserts || 0) / denom).toFixed(1) +
            ' · UPD/s ' + ((data.updates || 0) / denom).toFixed(1) +
            ' · DEL/s ' + ((data.deletes || 0) / denom).toFixed(1) +
            ' · SEL/s ' + ((data.selects || 0) / denom).toFixed(1);

        $('progress-fill').style.width = pct.toFixed(1) + '%';
        $('progress-pct').textContent = pct.toFixed(0) + '%';
        $('progress-text').textContent = 'Running \u2014 ' + Math.floor(elapsed) + 's elapsed';
        $('elapsed-badge').textContent = Math.floor(elapsed) + 's / ' + duration + 's';
    }

    // All 10 GC events tracked — must match GC_SYSTEM_EVENTS in metrics.py
    var ALL_GC_EVENTS = [
        'gc current block congested',
        'gc current block 2-way',
        'gc current block 3-way',
        'gc current block busy',
        'gc cr block congested',
        'gc cr block 2-way',
        'gc cr block 3-way',
        'gc cr block busy',
        'gc cr grant congested',
        'gc cr grant 2-way',
    ];

    // Short display names for table headers
    var GC_SHORT = {
        'gc current block congested': 'curr blk cong ★',
        'gc current block 2-way':     'curr blk 2way',
        'gc current block 3-way':     'curr blk 3way',
        'gc current block busy':      'curr blk busy',
        'gc cr block congested':      'cr blk cong',
        'gc cr block 2-way':          'cr blk 2way',
        'gc cr block 3-way':          'cr blk 3way',
        'gc cr block busy':           'cr blk busy',
        'gc cr grant congested':      'cr grant cong',
        'gc cr grant 2-way':          'cr grant 2way',
    };

    /**
     * After a run completes, load ALL stored runs and render a full
     * GC-event comparison table so every execution is visible side-by-side.
     */
    async function showCompletionSummary(msg) {
        if (msg && msg.workload_id) {
            removeWorkloadState(String(msg.workload_id));
            renderRunningWorkloadBanner();
            renderActiveWorkloads();
            syncWorkloadButtons();
            if (getSelectedWorkload()) {
                updateProgress(getSelectedWorkload());
                updateSelectedWorkloadCaption(getSelectedWorkload());
            } else {
                $('live-dashboard').style.display = 'none';
                updateSelectedWorkloadCaption(null);
            }
        }

        var currentRunId = String(msg.run_id || '');
        var s = msg.summary || {};

        // Load all runs to build the comparison table
        var data = await api('GET', '/api/results');
        var runs = (data.runs || []).slice().sort(function (a, b) {
            return a.run_id - b.run_id;   // oldest → newest
        });

        var html = '<div class="summary-card">';
        html += '<div class="summary-card-header">';
        html += '<h3>&#10003; Run #' + escapeHtml(currentRunId) + ' Complete &mdash; All Runs Comparison</h3>';
        html += '<span class="summary-stats">';
        html += 'Inserts&nbsp;<b>' + (s.inserts || 0).toLocaleString() + '</b>&emsp;';
        html += 'Updates&nbsp;<b>' + (s.updates || 0).toLocaleString() + '</b>&emsp;';
        html += 'Deletes&nbsp;<b>' + (s.deletes || 0).toLocaleString() + '</b>&emsp;';
        html += 'Errors&nbsp;<b>' + (s.errors || 0).toLocaleString() + '</b>';
        html += '</span>';
        html += '</div>';

        // ---- Comparison table ----
        html += '<div class="table-wrap summary-table-wrap"><table class="data-table summary-compare-table">';
        html += '<thead><tr>';
        html += '<th>Run</th>';
        html += '<th class="text-col">Schema</th>';
        html += '<th class="text-col">Partition</th>';
        html += '<th class="text-col">Compress</th>';
        html += '<th>Threads</th>';
        html += '<th>Dur</th>';
        ALL_GC_EVENTS.forEach(function (ev) {
            var short = GC_SHORT[ev] || ev;
            var isStar = ev === 'gc current block congested';
            html += '<th class="num gc-col' + (isStar ? ' col-primary' : '') + '" title="' + escapeHtml(ev) + '">'
                + escapeHtml(short) + '</th>';
        });
        html += '</tr></thead><tbody>';

        runs.forEach(function (r) {
            var parsed  = r.gc_metrics_parsed || {};
            var agg     = parsed.delta_aggregated || {};
            var isCurr  = String(r.run_id) === currentRunId;
            var rowClass = isCurr ? 'row-current' : '';

            html += '<tr class="' + rowClass + '">';
            html += '<td><b>#' + r.run_id + '</b>' + (isCurr ? ' <span class="badge-new">NEW</span>' : '') + '</td>';
            html += '<td class="text-col">' + escapeHtml(r.table_prefix || 'GCB') + '</td>';
            html += '<td class="text-col">' + escapeHtml(r.partition_type || 'NONE') + '</td>';
            html += '<td class="text-col">' + escapeHtml(r.compression    || 'NONE') + '</td>';
            html += '<td>' + (r.thread_count  || '-') + '</td>';
            html += '<td>' + (r.duration_secs || '-') + 's</td>';

            ALL_GC_EVENTS.forEach(function (ev) {
                var val    = agg[ev] || 0;
                var isStar = ev === 'gc current block congested';
                html += '<td class="num' + (isStar ? ' col-primary' : '') + '">'
                    + val.toLocaleString() + '</td>';
            });
            html += '</tr>';
        });

        html += '</tbody></table></div></div>';
        $('workload-summary').innerHTML = html;
        loadResultsCount();
        loadWorkloadStatus();
    }

    // ================================================================
    // GC Real-time Chart
    // ================================================================

    function initGCChart() {
        var canvas = $('gc-chart');
        if (gcChart) gcChart.destroy();

        var gridColor = 'rgba(255,255,255,0.04)';
        var textColor = '#7b8fa3';

        gcChart = new Chart(canvas, {
            type: 'line',
            data: {
                labels: [],
                datasets: ALL_GC_EVENTS.map(function (ev, idx) {
                    var color = GC_COLORS[idx % GC_COLORS.length];
                    return {
                        label: ev,
                        data: [],
                        borderColor: color,
                        backgroundColor: color + '22',
                        fill: false,
                        tension: 0.3,
                        pointRadius: 2,
                        pointHoverRadius: 5,
                        borderWidth: ev === 'gc current block congested' ? 3 : 1.5,
                    };
                }),
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                animation: { duration: 150 },
                interaction: { mode: 'index', intersect: false },
                scales: {
                    x: {
                        title: { display: true, text: 'Elapsed (s)', color: textColor, font: { size: 11 } },
                        ticks: { color: textColor, font: { size: 10 } },
                        grid: { color: gridColor },
                        border: { color: gridColor },
                    },
                    y: {
                        title: { display: true, text: 'Avg Wait (ms)', color: textColor, font: { size: 11 } },
                        beginAtZero: true,
                        ticks: { color: textColor, font: { size: 10 } },
                        grid: { color: gridColor },
                        border: { color: gridColor },
                    },
                },
                plugins: {
                    legend: {
                        position: 'bottom',
                        labels: { color: textColor, font: { size: 11 }, padding: 16, usePointStyle: true, pointStyleWidth: 8 },
                    },
                },
            },
        });
    }

    function updateGCChart(gcData, workloadId) {
        var targetId = String(workloadId || selectedWorkloadId || '');
        if (!targetId || !gcData) return;
        var series = getWorkloadChartSeries(targetId);
        var elapsed = Math.floor(gcData.elapsed || 0);
        series.labels.push(elapsed + 's');

        var events = gcData.events || {};

        ALL_GC_EVENTS.forEach(function (ev) {
            var value = events[ev] || 0;
            while (series.events[ev].length < series.labels.length - 1) series.events[ev].push(0);
            series.events[ev].push(value);
        });

        if (targetId === selectedWorkloadId) {
            restoreSelectedWorkloadChart();
        }
    }

    // ================================================================
    // WebSocket
    // ================================================================

    function ensureWebSocket() {
        if (ws && ws.readyState === WebSocket.OPEN) return;

        var protocol = location.protocol === 'https:' ? 'wss:' : 'ws:';
        ws = new WebSocket(protocol + '//' + location.host + '/ws');

        ws.onopen = function () {
            $('ws-dot').classList.add('connected');
            $('ws-label').textContent = 'Connected';
        };

        ws.onmessage = function (event) {
            var msg;
            try { msg = JSON.parse(event.data); } catch (e) { return; }

            switch (msg.type) {
                case 'progress':
                    if (!workloadMessageMatchesCurrentConnection(msg)) break;
                    rememberWorkloadState(msg.data);
                    renderRunningWorkloadBanner();
                    renderActiveWorkloads();
                    syncWorkloadButtons();
                    if (getSelectedWorkload()) {
                        $('live-dashboard').style.display = 'block';
                        updateProgress(getSelectedWorkload());
                        updateSelectedWorkloadCaption(getSelectedWorkload());
                        restoreSelectedWorkloadChart();
                    } else {
                        $('live-dashboard').style.display = 'none';
                        updateSelectedWorkloadCaption(null);
                    }
                    break;
                case 'login_progress':
                    if (!workloadMessageMatchesCurrentConnection(msg)) break;
                    updateLoginProgress(msg.data);
                    break;
                case 'gc_snapshot':
                    if (!workloadMessageMatchesCurrentConnection(msg)) break;
                    updateGCChart(msg.data, msg.workload_id);
                    break;
                case 'schema_progress': appendSchemaLog(msg.message); break;
                case 'schema_complete': appendSchemaLog(msg.message); loadSchemaState(); loadSchemas(); break;
                case 'complete':
                    if (!workloadMessageMatchesCurrentConnection(msg)) break;
                    showCompletionSummary(msg);
                    break;
                case 'login_complete':
                    if (!workloadMessageMatchesCurrentConnection(msg)) break;
                    showLoginCompletionSummary(msg);
                    break;
                case 'info':
                    if (!workloadMessageMatchesCurrentConnection(msg)) break;
                    if (msg.source === 'login_workload') {
                        appendLoginWorkloadNotice(msg.message, 'info');
                    } else {
                        appendWorkloadNotice(formatWorkloadNoticeMessage(msg), 'info');
                    }
                    break;
                case 'warning':
                    if (msg.source === 'schema') {
                        appendSchemaLog('WARNING: ' + msg.message);
                    } else if (msg.source === 'login_workload') {
                        if (!workloadMessageMatchesCurrentConnection(msg)) break;
                        appendLoginWorkloadNotice(msg.message, 'warning');
                    } else {
                        if (!workloadMessageMatchesCurrentConnection(msg)) break;
                        appendWorkloadNotice(formatWorkloadNoticeMessage(msg), 'warning');
                    }
                    break;
                case 'error':
                    if (msg.source === 'schema') {
                        appendSchemaLog('ERROR: ' + msg.message);
                    } else if (msg.source === 'login_workload') {
                        if (!workloadMessageMatchesCurrentConnection(msg)) break;
                        appendLoginWorkloadNotice(msg.message, 'error');
                        loginWorkloadRunning = false;
                        $('btn-start-login-workload').disabled = false;
                        $('btn-stop-login-workload').disabled = true;
                    } else {
                        if (!workloadMessageMatchesCurrentConnection(msg)) break;
                        appendWorkloadNotice(formatWorkloadNoticeMessage(msg), 'error');
                        syncWorkloadButtons();
                    }
                    break;
            }
        };

        ws.onclose = function () {
            $('ws-dot').classList.remove('connected');
            $('ws-label').textContent = 'Disconnected';
            if (getActiveWorkloads().length || loginWorkloadRunning || workloadStartPending) {
                setTimeout(ensureWebSocket, 3000);
            }
        };

        ws.onerror = function () { /* triggers onclose */ };
    }

    // ================================================================
    // Results Tab
    // ================================================================

    async function loadResultsCount() {
        try {
            var data = await api('GET', '/api/results');
            var runs = data.runs || [];
            var badge = $('run-count');
            if (runs.length > 0) {
                badge.textContent = runs.length;
                badge.style.display = '';
            } else {
                badge.style.display = 'none';
            }
        } catch (e) { /* ignore */ }
    }

    async function loadResults() {
        var data = await api('GET', '/api/results');
        var runs = data.runs || [];
        var tbody = $('results-body');

        // Update badge
        var badge = $('run-count');
        if (runs.length > 0) {
            badge.textContent = runs.length;
            badge.style.display = '';
        } else {
            badge.style.display = 'none';
        }

        if (runs.length === 0) {
            tbody.innerHTML = '<tr><td colspan="13"><div class="empty-state">' +
                '<div class="empty-icon">&#128202;</div>' +
                'No benchmark runs yet.<br>Complete a workload or login-simulation run to see results here.' +
                '</div></td></tr>';
            return;
        }

        var gcTotals = runs.map(function (r) {
            var parsed = r.gc_metrics_parsed || {};
            var agg = parsed.delta_aggregated || {};
            return (agg['gc current block congested'] || 0) +
                (agg['gc current block 3-way'] || 0) +
                (agg['gc cr grant congested'] || 0);
        });
        var minGC = Math.min.apply(null, gcTotals);
        var maxGC = Math.max.apply(null, gcTotals);

        var html = '';
        runs.forEach(function (r, i) {
            var parsed = r.gc_metrics_parsed || {};
            var agg = parsed.delta_aggregated || {};
            var total = gcTotals[i];
            var rowClass = '';
            if (runs.length > 1) {
                if (total === minGC) rowClass = 'row-best';
                else if (total === maxGC) rowClass = 'row-worst';
            }

            var date = formatRunDateTime(r.started_at);
            var startedFinished = 'Start: ' + escapeHtml(formatRunDateTime(r.started_at)) +
                '<br>End: ' + escapeHtml(formatRunDateTime(r.finished_at));

            html += '<tr class="' + rowClass + '">' +
                '<td><input type="checkbox" class="run-check" value="' + r.run_id + '"></td>' +
                '<td>#' + r.run_id + '</td>' +
                '<td class="text-col">' + escapeHtml(date) + '</td>' +
                '<td class="text-col">' + startedFinished + '</td>' +
                '<td class="text-col">' + escapeHtml(getRunType(r)) + '</td>' +
                '<td class="text-col">' + escapeHtml(getRunScenario(r)) + '</td>' +
                '<td class="text-col">' + escapeHtml(getRunActivity(r)) + '</td>' +
                '<td>' + Number(r.thread_count || 0).toLocaleString() + '</td>' +
                '<td>' + Number(r.duration_secs || 0).toFixed(1) + 's</td>' +
                '<td class="num col-primary">' + (agg['gc current block congested'] || 0).toLocaleString() + '</td>' +
                '<td class="num">' + (agg['gc current block 3-way'] || 0).toLocaleString() + '</td>' +
                '<td class="num">' + (agg['gc cr grant congested'] || 0).toLocaleString() + '</td>' +
                '<td><button class="btn btn-danger btn-sm" ' +
                'onclick="window._deleteRun(' + r.run_id + ')">Delete</button></td>' +
                '</tr>';
        });

        tbody.innerHTML = html;
    }

    window._deleteRun = function (runId) {
        confirmDialog('Delete Run #' + runId + '? This cannot be undone.', async function () {
            await api('DELETE', '/api/results/' + runId);
            loadResults();
        });
    };

    /**
     * Build and render the comparison chart.
     * Always focused on "gc current block congested" — one horizontal bar per run,
     * labeled with scenario / layout / thread-count / duration so differences
     * between runs are immediately visible.
     */
    function renderCompareChart(data) {
        if (!data || !data.labels || data.labels.length === 0) return;

        $('compare-chart-container').style.display = 'block';
        if (compareChart) compareChart.destroy();

        var values   = data.values  || [];
        var maxVal   = Math.max.apply(null, values.concat([1]));
        var gridColor = 'rgba(255,255,255,0.04)';
        var textColor = '#7b8fa3';

        // Colour each bar on a green→red gradient based on relative wait count
        var barColors = values.map(function (v) {
            var ratio = maxVal > 0 ? v / maxVal : 0;
            if (ratio < 0.33)  return 'rgba(52,211,153,0.75)';   // green  — low waits
            if (ratio < 0.67)  return 'rgba(251,191,36,0.75)';   // amber  — mid waits
            return              'rgba(248,113,113,0.75)';          // red    — high waits
        });
        var borderColors = values.map(function (v) {
            var ratio = maxVal > 0 ? v / maxVal : 0;
            if (ratio < 0.33)  return '#34d399';
            if (ratio < 0.67)  return '#fbbf24';
            return              '#f87171';
        });

        // Dynamic height: at least 280px, 52px per run so labels have room
        var chartHeight = Math.max(280, data.labels.length * 52);
        $('compare-chart').parentElement.style.height = chartHeight + 'px';

        compareChart = new Chart($('compare-chart'), {
            type: 'bar',
            data: {
                labels: data.labels,
                datasets: [{
                    label: 'gc current block congested  (waits delta)',
                    data: values,
                    backgroundColor: barColors,
                    borderColor: borderColors,
                    borderWidth: 1,
                    borderRadius: 4,
                }],
            },
            options: {
                indexAxis: 'y',          // horizontal bars — labels on the Y axis
                responsive: true,
                maintainAspectRatio: false,
                scales: {
                    x: {
                        beginAtZero: true,
                        title: {
                            display: true,
                            text: 'Total Waits (delta)',
                            color: textColor,
                            font: { size: 11 },
                        },
                        ticks: { color: textColor, font: { size: 10 } },
                        grid:  { color: gridColor },
                        border:{ color: gridColor },
                    },
                    y: {
                        ticks: {
                            color: textColor,
                            font: { size: 11 },
                            // Truncate very long labels on small screens
                            callback: function (val, idx) {
                                var lbl = data.labels[idx] || '';
                                return lbl.length > 48 ? lbl.substring(0, 46) + '…' : lbl;
                            },
                        },
                        grid:  { color: gridColor },
                        border:{ color: gridColor },
                    },
                },
                plugins: {
                    legend: {
                        position: 'top',
                        labels: {
                            color: textColor,
                            font: { size: 12, weight: '600' },
                            usePointStyle: true,
                            pointStyleWidth: 10,
                        },
                    },
                    tooltip: {
                        callbacks: {
                            // Show full detail breakdown in tooltip
                            afterBody: function (items) {
                                var idx = items[0] && items[0].dataIndex;
                                var d   = data.details && data.details[idx];
                                if (!d) return [];
                                return [
                                    '  type               : ' + (d.run_type || '-'),
                                    '  scenario           : ' + (d.scenario || '-'),
                                    '  layout             : ' + (d.layout || '-'),
                                    '',
                                    '  gc curr congested : ' + (d.gc_congested || 0).toLocaleString(),
                                    '  gc curr 3-way     : ' + (d.gc_3way      || 0).toLocaleString(),
                                    '  gc cr congested   : ' + (d.gc_cr        || 0).toLocaleString(),
                                ];
                            },
                        },
                    },
                },
            },
        });

        $('compare-chart-container').scrollIntoView({ behavior: 'smooth' });
    }

    async function compareSelected() {
        var checks = document.querySelectorAll('.run-check:checked');
        var ids = [];
        checks.forEach(function (cb) { ids.push(cb.value); });
        if (ids.length < 2) {
            alert('Select at least 2 runs to compare.');
            return;
        }
        var data = await api('GET', '/api/results/compare?ids=' + ids.join(','));
        renderCompareChart(data);
    }

    async function compareAll() {
        var checks = document.querySelectorAll('.run-check');
        var ids = [];
        checks.forEach(function (cb) { ids.push(cb.value); });
        if (ids.length < 2) {
            alert('Need at least 2 stored runs to compare.');
            return;
        }
        var data = await api('GET', '/api/results/compare?ids=' + ids.join(','));
        renderCompareChart(data);
    }

    async function exportCSV() {
        var resp = await fetch('/api/results/export/csv');
        var blob = await resp.blob();
        var url = URL.createObjectURL(blob);
        var a = document.createElement('a');
        a.href = url;
        a.download = 'gc_benchmark_results.csv';
        a.click();
        URL.revokeObjectURL(url);
    }

    // ================================================================
    // Slider bindings
    // ================================================================

    function bindSlider(sliderId, displayId, inputId) {
        var slider = $(sliderId);
        var display = $(displayId);
        var input = inputId ? $(inputId) : null;
        if (!slider || !display) return;

        function clampValue(raw) {
            var n = parseInt(raw, 10);
            if (Number.isNaN(n)) n = parseInt(slider.value, 10) || parseInt(slider.min, 10) || 0;
            var min = parseInt(slider.min, 10);
            var max = parseInt(slider.max, 10);
            if (!Number.isNaN(min)) n = Math.max(min, n);
            if (!Number.isNaN(max)) n = Math.min(max, n);
            return n;
        }

        function syncFrom(raw) {
            var value = String(clampValue(raw));
            slider.value = value;
            display.textContent = value;
            if (input) input.value = value;
        }

        syncFrom(slider.value);
        slider.addEventListener('input', function () {
            syncFrom(slider.value);
        });
        if (input) {
            input.addEventListener('input', function () {
                display.textContent = input.value || slider.value;
            });
            input.addEventListener('change', function () {
                syncFrom(input.value);
            });
            input.addEventListener('blur', function () {
                syncFrom(input.value);
            });
        }
    }

    // ================================================================
    // Oracle GC Stress Parameters panel
    // ================================================================

    /** Fetch current Oracle GC hidden parameter values and render the table. */
    async function loadGCParams() {
        var body = $('gc-params-body');
        if (!body) return;
        body.innerHTML = '<div class="gc-params-loading">Querying Oracle…</div>';
        gcParamsMsg('', '');

        var data = await api('GET', '/api/oracle/gc_params');
        if (!data.ok || !data.params || data.params.length === 0) {
            body.innerHTML = '<div class="gc-params-loading" style="color:var(--error)">' +
                escapeHtml(data.message || 'Could not read parameters — check privileges') + '</div>';
            return;
        }

        var rows = data.params.map(function (p) {
            var isStress  = p.current === p.stress;
            var isHidden  = p.current === 'hidden' || p.current === 'unknown';
            var cls = isHidden ? 'is-unknown' : (isStress ? 'is-stress' : 'is-default');
            return '<tr>' +
                '<td><span class="gcp-name">' + escapeHtml(p.param) + '</span>' +
                    '<div style="font-size:10px;color:var(--text-muted);margin-top:2px">' + escapeHtml(p.label) + '</div></td>' +
                '<td><span class="gcp-current ' + cls + '">' + escapeHtml(p.current) + '</span></td>' +
                '<td><span class="gcp-stress">' + escapeHtml(p.stress) + '</span></td>' +
                '<td class="gcp-why">' + escapeHtml(p.why) + '</td>' +
                '</tr>';
        }).join('');

        body.innerHTML =
            '<table><thead><tr>' +
            '<th>Parameter</th><th>Current Value</th><th>Stress Value</th><th>Why it helps</th>' +
            '</tr></thead><tbody>' + rows + '</tbody></table>';
    }

    /** Apply GC stress parameters via ALTER SYSTEM SCOPE=MEMORY. */
    async function applyGCStress() {
        gcParamsMsg('Applying parameters…', '');
        var data = await api('POST', '/api/oracle/apply_gc_stress');
        if (data.ok) {
            gcParamsMsg('✓ ' + data.message, 'ok');
        } else {
            gcParamsMsg('✗ ' + (data.message || 'Failed to apply parameters'), 'error');
        }
        await loadGCParams();
    }

    /** Reset GC stress parameters to Oracle defaults. */
    async function resetGCParams() {
        gcParamsMsg('Resetting parameters…', '');
        var data = await api('POST', '/api/oracle/reset_gc_params');
        if (data.ok) {
            gcParamsMsg('✓ ' + data.message, 'ok');
        } else {
            gcParamsMsg('✗ ' + (data.message || 'Failed to reset parameters'), 'error');
        }
        await loadGCParams();
    }

    /** Show a status message below the GC params table. */
    function gcParamsMsg(text, type) {
        var el = $('gc-params-msg');
        if (!el) return;
        if (!text) { el.style.display = 'none'; el.textContent = ''; return; }
        el.textContent = text;
        el.className = 'gc-params-msg ' + (type || '');
        el.style.display = '';
    }

    // ================================================================
    // Init
    // ================================================================

    function init() {
        initNav();
        loadConnectionStatus();
        loadRecentConnections();
        loadRecentCpoolConnections();
        loadResultsCount();
        loadSchemaState();
        loadSchemas();

        // Sliders
        bindSlider('wl-threads', 'thread-count-val', 'wl-threads-input');
        bindSlider('wl-hotrow', 'hot-row-val');
        bindSlider('login-thread-count', 'login-thread-count-val', 'login-thread-count-input');

        // Partition radio change
        document.querySelectorAll('input[name="partition"]').forEach(function (r) {
            r.addEventListener('change', updatePartitionOptions);
        });

        // Compression warning
        $('schema-compression').addEventListener('change', updateCompressionWarning);

        // Connection buttons
        $('btn-test-conn').addEventListener('click', testConnection);
        $('btn-check-privs').addEventListener('click', checkPrivileges);
        $('btn-save-conn').addEventListener('click', saveConnection);

        // Schema buttons
        $('btn-preview-ddl').addEventListener('click', previewDDL);
        $('btn-create-schema').addEventListener('click', createSchema);
        $('btn-drop-schema').addEventListener('click', function () { dropSchema(); });
        $('btn-drop-prefix').addEventListener('click', function () { dropSchemaByPrefix(); });
        $('btn-kill-user-sessions').addEventListener('click', function () { killSessionsByUser(); });

        // DDL modal
        $('ddl-modal-close').addEventListener('click', function () {
            $('ddl-modal').classList.add('hidden');
        });
        $('ddl-modal').addEventListener('click', function (e) {
            if (e.target === $('ddl-modal')) $('ddl-modal').classList.add('hidden');
        });

        // Contention mode radios — show/hide lock-hold slider and tuning tips
        document.querySelectorAll('input[name="contention_mode"]').forEach(function (radio) {
            radio.addEventListener('change', function () {
                var mode = this.value;
                var lockRow = $('lock-hold-row');
                var normalMixRow = $('normal-mix-row');
                var tips    = $('tuning-tips');
                if (lockRow) lockRow.style.display = (mode === 'HAMMER')   ? '' : 'none';
                if (normalMixRow) normalMixRow.style.display = (mode === 'NORMAL') ? '' : 'none';
                if (tips)    tips.style.display    = (mode !== 'NORMAL')   ? '' : 'none';
            });
        });

        // Lock-hold slider label
        var lockSlider = $('wl-lock-hold');
        if (lockSlider) {
            lockSlider.addEventListener('input', function () {
                var v = $('lock-hold-val');
                if (v) v.textContent = this.value + ' ms';
            });
        }

        var workloadSeedRows = $('wl-seed-rows');
        if (workloadSeedRows) {
            workloadSeedRows.addEventListener('change', getCurrentWorkloadSeedRows);
            workloadSeedRows.addEventListener('blur', getCurrentWorkloadSeedRows);
        }

        var modeInit = document.querySelector('input[name="contention_mode"]:checked');
        if (modeInit) {
            var normalMixRow = $('normal-mix-row');
            if (normalMixRow) normalMixRow.style.display = (modeInit.value === 'NORMAL') ? '' : 'none';
        }

        document.querySelectorAll('input[name="login-stop-mode"]').forEach(function (radio) {
            radio.addEventListener('change', updateLoginStopModeUI);
        });
        document.querySelectorAll('input[name="login-session-case"]').forEach(function (radio) {
            radio.addEventListener('change', updateLoginSessionCaseUI);
        });
        updateLoginSessionCaseUI();
        updateLoginStopModeUI();

        // Schema select dropdown
        $('wl-schema-select').addEventListener('change', function () {
            var val = this.value;
            if (!val) return;
            try { applySelectedSchema(JSON.parse(val)); } catch (e) { /* ignore */ }
        });
        $('btn-refresh-schemas').addEventListener('click', loadSchemas);

        // GC Stress Parameters panel
        $('btn-refresh-gc-params').addEventListener('click', loadGCParams);
        $('btn-apply-gc-stress').addEventListener('click', applyGCStress);
        $('btn-reset-gc-params').addEventListener('click', resetGCParams);
        $('btn-apply-db-session-filter').addEventListener('click', loadDbActivity);
        $('btn-test-cpool-conn').addEventListener('click', testCpoolConnection);
        $('btn-restart-pdb').addEventListener('click', restartPdb);

        // Workload buttons
        $('btn-start-workload').addEventListener('click', startWorkload);
        $('btn-stop-workload').addEventListener('click', stopWorkload);
        $('btn-create-login-procedure').addEventListener('click', createLoginProcedure);
        $('btn-refresh-login-procedure').addEventListener('click', loadLoginProcedureStatus);
        $('btn-drop-login-procedure').addEventListener('click', dropLoginProcedure);
        $('btn-start-login-workload').addEventListener('click', startLoginWorkload);
        $('btn-stop-login-workload').addEventListener('click', stopLoginWorkload);

        var activeWorkloadsBody = $('active-workloads-body');
        if (activeWorkloadsBody) {
            activeWorkloadsBody.addEventListener('click', function (event) {
                var button = event.target.closest('button');
                if (!button) return;

                var selectId = button.getAttribute('data-workload-select');
                if (selectId) {
                    selectWorkload(selectId);
                    return;
                }

                var stopId = button.getAttribute('data-workload-stop');
                if (stopId) {
                    stopWorkload(stopId);
                }
            });
        }

        // Results buttons
        $('btn-compare').addEventListener('click', compareSelected);
        $('btn-compare-all').addEventListener('click', compareAll);
        $('btn-export-csv').addEventListener('click', exportCSV);
        $('btn-refresh-results').addEventListener('click', loadResults);

        // Select all checkbox
        $('select-all').addEventListener('change', function () {
            var checked = this.checked;
            document.querySelectorAll('.run-check').forEach(function (cb) {
                cb.checked = checked;
            });
        });

        // Connect WebSocket proactively
        ensureWebSocket();
        syncWorkloadButtons();
        loadWorkloadStatus();
        loadLoginWorkloadStatus();
        setInterval(loadWorkloadStatus, 5000);
        setInterval(loadLoginWorkloadStatus, 5000);
        startDbActivityPolling();
        startCpoolStatsPolling();
    }

    document.addEventListener('DOMContentLoaded', init);
})();
