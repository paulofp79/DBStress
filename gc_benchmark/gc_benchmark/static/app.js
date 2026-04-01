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

    var PAGE_TITLES = {
        connection: 'Connection',
        schema: 'Schema Setup',
        workload: 'Run Workload',
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

    function showStatus(containerId, message, type) {
        $(containerId).innerHTML =
            '<div class="status-box status-' + type + '">' + escapeHtml(message) + '</div>';
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
        if (name === 'workload') loadSchemas();
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
        el.innerHTML =
            '<span class="schema-badge"><b>' + escapeHtml(s.prefix || s.table_prefix || 'GCB') + '</b></span>' +
            '<span class="schema-badge">Tables&nbsp;<b>' + escapeHtml(String(s.table_count || 10)) + '</b></span>' +
            '<span class="schema-badge">Rows/Table&nbsp;<b>' + escapeHtml((Number(s.seed_rows || 0)).toLocaleString()) + '</b></span>' +
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

    async function loadConnectionStatus() {
        try {
            var data = await api('GET', '/api/connection/status');
            if (data.host) $('conn-host').value = data.host;
            if (data.port) $('conn-port').value = data.port;
            if (data.service_name) $('conn-service').value = data.service_name;
            if (data.user) $('conn-user').value = data.user;
            if (data.mode) $('conn-mode').value = data.mode;
        } catch (e) { /* ignore */ }
    }

    async function testConnection() {
        showStatus('conn-status', 'Testing connection...', 'info');
        var result = await api('POST', '/api/connection/test', getConnFields());
        showStatus('conn-status', result.message, result.ok ? 'success' : 'error');
        // Refresh recent-connections list after a successful test (backend auto-saves)
        if (result.ok) loadRecentConnections();
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
        div.className = 'status-box status-' + (type === 'warning' ? 'warning' : 'error');
        div.textContent = (type === 'warning' ? '⚠ ' : '✖ ') + message;
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
            table_count:      parseInt($('wl-table-count').value) || 10,
            thread_count:     parseInt($('wl-threads').value),
            duration_seconds: parseInt($('wl-duration').value)    || 60,
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

    async function startWorkload() {
        if (workloadRunning) return;

        // Reset UI
        $('live-dashboard').style.display = 'block';
        // Clear any previous workload notices
        var notices = $('workload-notices');
        if (notices) { notices.innerHTML = ''; notices.style.display = 'none'; }
        $('workload-summary').innerHTML = '';
        $('counter-inserts').textContent = '0';
        $('counter-updates').textContent = '0';
        $('counter-deletes').textContent = '0';
        $('counter-selects').textContent = '0';
        $('counter-errors').textContent = '0';
        $('ops-per-sec').textContent = 'INS/s 0.0 · UPD/s 0.0 · DEL/s 0.0 · SEL/s 0.0';
        $('progress-fill').style.width = '0%';
        $('progress-text').textContent = 'Running...';
        $('progress-pct').textContent = '0%';
        $('elapsed-badge').textContent = '0s / ' + ($('wl-duration').value || 60) + 's';

        initGCChart();

        $('btn-start-workload').disabled = true;
        $('btn-stop-workload').disabled = false;
        workloadRunning = true;

        ensureWebSocket();

        var result = await api('POST', '/api/workload/start', getWorkloadConfig());
        if (!result.ok) {
            $('workload-summary').innerHTML =
                '<div class="status-box status-error">' + escapeHtml(result.message || 'Failed to start') + '</div>';
            workloadRunning = false;
            $('btn-start-workload').disabled = false;
            $('btn-stop-workload').disabled = true;
        }
    }

    async function stopWorkload() {
        await api('POST', '/api/workload/stop');
        workloadRunning = false;
        $('btn-start-workload').disabled = false;
        $('btn-stop-workload').disabled = true;
        $('running-workload-banner').style.display = 'none';
        $('running-workload-banner').innerHTML = '';
    }

    function renderRunningWorkloadBanner(data) {
        var el = $('running-workload-banner');
        if (!el) return;

        if (!data || !data.running) {
            el.style.display = 'none';
            el.innerHTML = '';
            return;
        }

        var schema = data.table_prefix || 'GCB';
        var tables = Number(data.table_count || 0).toLocaleString();
        var requestedThreads = Number(data.requested_threads || data.thread_count || 0).toLocaleString();
        var physicalWorkers = Number(data.physical_workers || 0).toLocaleString();
        var elapsed = Number(data.elapsed || 0).toFixed(1);
        var duration = Number(data.duration || 0).toLocaleString();
        var mode = data.contention_mode || 'NORMAL';

        el.innerHTML =
            '<b>RUNNING workload</b> ' +
            'Schema <b>' + escapeHtml(schema) + '</b> · ' +
            'Tables <b>' + escapeHtml(tables) + '</b> · ' +
            'Requested Threads <b>' + escapeHtml(requestedThreads) + '</b> · ' +
            'Physical Workers/Sessions <b>' + escapeHtml(physicalWorkers) + '</b> · ' +
            'Mode <b>' + escapeHtml(mode) + '</b> · ' +
            'Elapsed <b>' + escapeHtml(elapsed) + 's / ' + escapeHtml(duration) + 's</b>';
        el.style.display = 'block';
    }

    async function loadWorkloadStatus() {
        try {
            var data = await api('GET', '/api/workload/status');
            var running = !!data.running;

            workloadRunning = running;
            $('btn-start-workload').disabled = running;
            $('btn-stop-workload').disabled = !running;
            renderRunningWorkloadBanner(data);

            if (running) {
                $('live-dashboard').style.display = 'block';
                updateProgress(data);
                ensureWebSocket();
            }
        } catch (e) {
            workloadRunning = false;
            $('btn-start-workload').disabled = false;
            $('btn-stop-workload').disabled = true;
            renderRunningWorkloadBanner(null);
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
        $('counter-inserts').textContent = (data.inserts || 0).toLocaleString();
        $('counter-updates').textContent = (data.updates || 0).toLocaleString();
        $('counter-deletes').textContent = (data.deletes || 0).toLocaleString();
        $('counter-selects').textContent = (data.selects || 0).toLocaleString();
        $('counter-errors').textContent = (data.errors || 0).toLocaleString();

        var elapsed = data.elapsed || 0;
        var duration = data.duration || 60;
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
        workloadRunning = false;
        $('btn-start-workload').disabled = false;
        $('btn-stop-workload').disabled = true;

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

    function updateGCChart(gcData) {
        if (!gcChart) return;
        var elapsed = Math.floor(gcData.elapsed || 0);
        gcChart.data.labels.push(elapsed + 's');

        var events = gcData.events || {};

        ALL_GC_EVENTS.forEach(function (ev) {
            var value = events[ev] || 0;
            var ds = gcChart.data.datasets.find(function (d) { return d.label === ev; });
            if (!ds) return;

            // Pad any missing points, then append
            while (ds.data.length < gcChart.data.labels.length - 1) ds.data.push(0);
            ds.data.push(value);
        });

        gcChart.update('none');
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
                case 'progress':        updateProgress(msg.data); break;
                case 'gc_snapshot':     updateGCChart(msg.data); break;
                case 'schema_progress': appendSchemaLog(msg.message); break;
                case 'schema_complete': appendSchemaLog(msg.message); loadSchemaState(); loadSchemas(); break;
                case 'complete':        showCompletionSummary(msg); break;
                case 'warning':
                    if (msg.source === 'schema') {
                        appendSchemaLog('WARNING: ' + msg.message);
                    } else {
                        appendWorkloadNotice(msg.message, 'warning');
                    }
                    break;
                case 'error':
                    if (msg.source === 'schema') {
                        appendSchemaLog('ERROR: ' + msg.message);
                    } else {
                        appendWorkloadNotice(msg.message, 'error');
                        // Reset workload button state on fatal error
                        workloadRunning = false;
                        $('btn-start-workload').disabled = false;
                        $('btn-stop-workload').disabled = true;
                    }
                    break;
            }
        };

        ws.onclose = function () {
            $('ws-dot').classList.remove('connected');
            $('ws-label').textContent = 'Disconnected';
            if (workloadRunning) {
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
            tbody.innerHTML = '<tr><td colspan="12"><div class="empty-state">' +
                '<div class="empty-icon">&#128202;</div>' +
                'No benchmark runs yet.<br>Complete a workload run to see results here.' +
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

            var date = r.started_at ? r.started_at.replace('T', ' ').substring(0, 16) : '-';

            html += '<tr class="' + rowClass + '">' +
                '<td><input type="checkbox" class="run-check" value="' + r.run_id + '"></td>' +
                '<td>#' + r.run_id + '</td>' +
                '<td class="text-col">' + escapeHtml(date) + '</td>' +
                '<td>' + (r.table_count || '-') + '</td>' +
                '<td class="text-col">' + escapeHtml(r.partition_type || '-') + '</td>' +
                '<td class="text-col">' + escapeHtml(r.compression || '-') + '</td>' +
                '<td>' + (r.thread_count || '-') + '</td>' +
                '<td>' + (r.duration_secs || '-') + 's</td>' +
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
     * labeled with partition / compression / thread-count / duration so differences
     * between schema configurations are immediately visible.
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

    function bindSlider(sliderId, displayId) {
        var slider = $(sliderId);
        var display = $(displayId);
        if (!slider || !display) return;
        display.textContent = slider.value;
        slider.addEventListener('input', function () {
            display.textContent = slider.value;
        });
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
        bindSlider('wl-threads', 'thread-count-val');
        bindSlider('wl-hotrow', 'hot-row-val');

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

        var modeInit = document.querySelector('input[name="contention_mode"]:checked');
        if (modeInit) {
            var normalMixRow = $('normal-mix-row');
            if (normalMixRow) normalMixRow.style.display = (modeInit.value === 'NORMAL') ? '' : 'none';
        }

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

        // Workload buttons
        $('btn-start-workload').addEventListener('click', startWorkload);
        $('btn-stop-workload').addEventListener('click', stopWorkload);

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
        loadWorkloadStatus();
        setInterval(loadWorkloadStatus, 5000);
        startDbActivityPolling();
        startCpoolStatsPolling();
    }

    document.addEventListener('DOMContentLoaded', init);
})();
