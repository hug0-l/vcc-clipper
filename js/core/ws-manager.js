/**
 * Clipper WSManager — WebSocket connection manager with isolated handler dispatch
 * and exponential-backoff auto-reconnect.
 *
 * Depends on: MessageBus (js/core/message-bus.js)
 *
 * Lifecycle events emitted on bus:
 *   'connecting'        — { }                          — WS connecting
 *   'connected'         — { }                          — WS open
 *   'disconnected'      — { wasIntentional }           — WS closed
 *   'reconnecting'      — { attempt, delay }           — auto-reconnect attempt
 *   'reconnected'       — { }                          — reconnect success
 *   'module-error'      — { module, error, type }      — module handler threw
 */
class WSManager {
    constructor(url, bus) {
        this._url = url;
        this._bus = bus;
        this._ws = null;
        this._handlers = new Map(); // type → [{handler, moduleName}]
        this._intentionalDisconnect = false;
        this._reconnectEnabled = false;
        this._reconnectAttempt = 0;
        this._reconnectTimer = null;
        this._reconnectOpts = { initialDelay: 2000, maxDelay: 30000 };
        this._peerId = null;
        this._room = null;
    }

    get connected() {
        return this._ws !== null && this._ws.readyState === WebSocket.OPEN;
    }

    get peerId() { return this._peerId; }
    get room() { return this._room; }
    get reconnectCount() { return this._reconnectAttempt; }

    /**
     * Open WebSocket connection.
     * @returns {Promise<WebSocket>}
     */
    connect() {
        this._stopReconnect();
        if (this._ws && (this._ws.readyState === WebSocket.OPEN || this._ws.readyState === WebSocket.CONNECTING)) {
            return Promise.resolve(this._ws);
        }
        return new Promise((resolve, reject) => {
            this._bus.emit('connecting', {});
            const ws = new WebSocket(this._url);
            ws.onopen = () => {
                // If another connection already won, ignore this one
                if (this._ws && this._ws !== ws) {
                    try { ws.close(); } catch (_) {}
                    return;
                }
                console.log('[WS] Connected');
                this._ws = ws;
                this._reconnectAttempt = 0;
                this._bus.emit('connected', {});
                resolve(ws);
            };
            ws.onerror = (err) => {
                console.error('[WS] Error', err);
                reject(err);
            };
            ws.onclose = () => {
                console.log('[WS] Closed');
                if (this._ws === ws) {
                    this._ws = null;
                    const intentional = this._intentionalDisconnect;
                    this._bus.emit('disconnected', { wasIntentional: intentional });
                    if (!intentional && this._reconnectEnabled) {
                        this._startReconnect();
                    }
                }
            };
            ws.onmessage = (event) => this._onMessage(event);
        });
    }

    /**
     * Intentionally close the connection. No auto-reconnect.
     */
    disconnect() {
        this._intentionalDisconnect = true;
        this._stopReconnect();
        if (this._ws) {
            try { this._ws.close(); } catch (_) {}
            this._ws = null;
        }
    }

    /**
     * Send a JSON message.
     */
    send(obj) {
        if (this._ws && this._ws.readyState === WebSocket.OPEN) {
            this._ws.send(JSON.stringify(obj));
        }
    }

    /**
     * Register a typed message handler with a module name.
     * @param {string|string[]} types - Message type(s) to handle. '*' catches all.
     * @param {function} handler - Callback receives (data, wsManager)
     * @param {string} moduleName - Module identifier for error isolation
     */
    onMessage(types, handler, moduleName) {
        if (!Array.isArray(types)) types = [types];
        const entry = { handler, moduleName: moduleName || 'anonymous' };
        for (const type of types) {
            if (!this._handlers.has(type)) {
                this._handlers.set(type, []);
            }
            this._handlers.get(type).push(entry);
        }
    }

    /**
     * Remove all handlers registered under a given module name.
     */
    unregisterModule(name) {
        for (const [type, handlers] of this._handlers) {
            this._handlers.set(type, handlers.filter(h => h.moduleName !== name));
        }
    }

    /**
     * Enable exponential-backoff auto-reconnect on unexpected disconnects.
     * @param {{initialDelay?: number, maxDelay?: number}} opts
     */
    enableAutoReconnect(opts = {}) {
        this._reconnectEnabled = true;
        if (opts.initialDelay !== undefined) this._reconnectOpts.initialDelay = opts.initialDelay;
        if (opts.maxDelay !== undefined) this._reconnectOpts.maxDelay = opts.maxDelay;
    }

    disableAutoReconnect() {
        this._reconnectEnabled = false;
        this._stopReconnect();
    }

    // ---- internal ----

    _onMessage(event) {
        let data;
        try {
            data = JSON.parse(event.data);
        } catch (_) {
            console.warn('[WS] Non-JSON message', event.data);
            return;
        }
        this._bus.emit('server-message', { msg: data, type: data.type });

        // Wildcard handlers run first
        const wildcard = this._handlers.get('*') || [];
        for (const { handler, moduleName } of wildcard) {
            try {
                handler(data, this);
            } catch (err) {
                console.error(`[WS] Handler error in module "${moduleName}" for type "${data.type}":`, err);
                this._bus.emit('module-error', { module: moduleName, error: err, type: data.type });
            }
        }

        // Type-specific handlers
        const handlers = this._handlers.get(data.type) || [];
        for (const { handler, moduleName } of handlers) {
            try {
                handler(data, this);
            } catch (err) {
                console.error(`[WS] Handler error in module "${moduleName}" for type "${data.type}":`, err);
                this._bus.emit('module-error', { module: moduleName, error: err, type: data.type });
            }
        }
    }

    _startReconnect() {
        this._stopReconnect();
        const delay = Math.min(
            this._reconnectOpts.initialDelay * Math.pow(2, this._reconnectAttempt),
            this._reconnectOpts.maxDelay
        );
        this._bus.emit('reconnecting', { attempt: this._reconnectAttempt, delay });
        this._reconnectTimer = setTimeout(() => {
            this._reconnectAttempt++;
            this._bus.emit('connecting', {});
            const ws = new WebSocket(this._url);
            ws.onopen = () => {
                if (this._ws && this._ws !== ws) {
                    try { ws.close(); } catch (_) {}
                    return;
                }
                console.log('[WS] Reconnected');
                this._ws = ws;
                const attempt = this._reconnectAttempt;
                this._reconnectAttempt = 0;
                this._reconnectTimer = null;
                this._bus.emit('reconnected', { attempt });
                this._bus.emit('connected', {});
            };
            ws.onerror = () => {
                console.log('[WS] Reconnect failed, retrying...');
                this._startReconnect();
            };
            ws.onclose = () => {
                if (this._ws === ws) {
                    this._ws = null;
                    if (this._reconnectEnabled) {
                        this._startReconnect();
                    }
                }
            };
            ws.onmessage = (event) => this._onMessage(event);
        }, delay);
    }

    _stopReconnect() {
        if (this._reconnectTimer) {
            clearTimeout(this._reconnectTimer);
            this._reconnectTimer = null;
        }
    }
}
