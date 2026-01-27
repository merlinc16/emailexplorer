#!/usr/bin/env python3
"""Simple MongoDB Document Browser"""

from flask import Flask, render_template_string, request, jsonify
from pymongo import MongoClient
from bson import ObjectId
import json

app = Flask(__name__)
client = MongoClient('localhost', 27017)
db = client['toxic_docs']

HTML = '''
<!DOCTYPE html>
<html>
<head>
    <title>toxic_docs Browser</title>
    <style>
        * { box-sizing: border-box; }
        body { font-family: system-ui, -apple-system, sans-serif; margin: 0; background: #111; color: #eee; }
        .container { display: flex; height: 100vh; }

        /* Sidebar */
        .sidebar { width: 280px; background: #1a1a1a; border-right: 1px solid #333; overflow-y: auto; flex-shrink: 0; }
        .sidebar h2 { padding: 15px; margin: 0; font-size: 14px; color: #888; border-bottom: 1px solid #333; }
        .collection { padding: 12px 15px; cursor: pointer; border-bottom: 1px solid #222; }
        .collection:hover { background: #252525; }
        .collection.active { background: #0066cc; }
        .collection .name { font-weight: 600; }
        .collection .count { font-size: 12px; color: #888; }
        .collection.active .count { color: #aac; }

        /* Schema panel */
        .schema { padding: 15px; border-bottom: 1px solid #333; }
        .schema h3 { margin: 0 0 10px 0; font-size: 12px; color: #888; }
        .schema-field { font-size: 12px; padding: 3px 0; font-family: monospace; color: #aaa; }

        /* Main content */
        .main { flex: 1; display: flex; flex-direction: column; overflow: hidden; }

        /* Navigation */
        .nav { padding: 10px 15px; background: #1a1a1a; border-bottom: 1px solid #333; display: flex; gap: 10px; align-items: center; }
        .nav button { padding: 8px 16px; background: #333; border: none; color: #eee; border-radius: 4px; cursor: pointer; }
        .nav button:hover { background: #444; }
        .nav button:disabled { opacity: 0.5; cursor: default; }
        .nav span { color: #888; font-size: 14px; }
        input[type="number"] { width: 80px; padding: 8px; background: #222; border: 1px solid #444; color: #eee; border-radius: 4px; }

        /* Document list */
        .doc-list { flex: 1; overflow-y: auto; padding: 15px; }
        .doc { background: #1a1a1a; margin-bottom: 15px; border-radius: 8px; border: 1px solid #333; }
        .doc-header { padding: 12px 15px; border-bottom: 1px solid #333; cursor: pointer; display: flex; justify-content: space-between; }
        .doc-header:hover { background: #222; }
        .doc-id { font-family: monospace; font-size: 12px; color: #888; }
        .doc-meta { font-size: 13px; color: #aaa; margin-top: 5px; }

        /* Document fields */
        .doc-fields { padding: 15px; border-bottom: 1px solid #333; display: none; background: #151515; }
        .doc.expanded .doc-fields { display: block; }
        .field-row { display: flex; padding: 5px 0; border-bottom: 1px solid #222; font-size: 13px; }
        .field-row:last-child { border: none; }
        .field-name { width: 180px; color: #888; font-family: monospace; flex-shrink: 0; }
        .field-value { color: #eee; word-break: break-word; }

        /* Document text */
        .doc-text { padding: 15px; display: none; }
        .doc.expanded .doc-text { display: block; }
        .doc-text pre {
            background: #0a0a0a; padding: 15px; border-radius: 4px;
            white-space: pre-wrap; word-wrap: break-word;
            font-size: 13px; line-height: 1.6; color: #ccc;
            max-height: 600px; overflow-y: auto; margin: 0;
        }

        .expand-icon { color: #666; }
        .doc.expanded .expand-icon { transform: rotate(90deg); }
    </style>
</head>
<body>
    <div class="container">
        <div class="sidebar">
            <h2>COLLECTIONS</h2>
            <div id="collections"></div>
            <div class="schema">
                <h3>SCHEMA (sample fields)</h3>
                <div id="schema"></div>
            </div>
        </div>
        <div class="main">
            <div class="nav">
                <button onclick="prevPage()">&larr; Prev</button>
                <button onclick="nextPage()">Next &rarr;</button>
                <span id="page-info">-</span>
                <span style="margin-left: auto;">Jump to:</span>
                <input type="number" id="jump-page" min="1" value="1" onkeyup="if(event.key==='Enter')jumpToPage()">
                <button onclick="jumpToPage()">Go</button>
            </div>
            <div class="doc-list" id="docs">
                <p style="color: #888;">Select a collection from the sidebar</p>
            </div>
        </div>
    </div>

    <script>
        let currentCollection = 'documents';
        let currentPage = 0;
        let pageSize = 20;
        let totalDocs = 0;

        async function loadCollections() {
            const resp = await fetch('/api/collections');
            const data = await resp.json();

            document.getElementById('collections').innerHTML = data.map(c => `
                <div class="collection ${c.name === currentCollection ? 'active' : ''}"
                     onclick="selectCollection('${c.name}')">
                    <div class="name">${c.name}</div>
                    <div class="count">${c.count.toLocaleString()} docs</div>
                </div>
            `).join('');
        }

        async function selectCollection(name) {
            currentCollection = name;
            currentPage = 0;
            document.querySelectorAll('.collection').forEach(el => el.classList.remove('active'));
            event.currentTarget.classList.add('active');
            await loadSchema();
            await loadDocs();
        }

        async function loadSchema() {
            const resp = await fetch('/api/schema?collection=' + currentCollection);
            const fields = await resp.json();

            document.getElementById('schema').innerHTML = fields.map(f =>
                `<div class="schema-field">${f.name}: <span style="color:#666">${f.type}</span></div>`
            ).join('');
        }

        async function loadDocs() {
            const resp = await fetch(`/api/docs?collection=${currentCollection}&skip=${currentPage * pageSize}&limit=${pageSize}`);
            const data = await resp.json();

            totalDocs = data.total;
            const totalPages = Math.ceil(totalDocs / pageSize);
            document.getElementById('page-info').textContent =
                `Page ${currentPage + 1} of ${totalPages} (${totalDocs.toLocaleString()} docs)`;
            document.getElementById('jump-page').value = currentPage + 1;

            if (data.docs.length === 0) {
                document.getElementById('docs').innerHTML = '<p style="color:#888">No documents</p>';
                return;
            }

            document.getElementById('docs').innerHTML = data.docs.map((doc, i) => {
                const text = doc.text || '[no text field]';
                const fields = Object.entries(doc)
                    .filter(([k]) => k !== 'text' && k !== '_id')
                    .map(([k, v]) => `<div class="field-row"><div class="field-name">${k}</div><div class="field-value">${formatValue(v)}</div></div>`)
                    .join('');

                const meta = [
                    doc.original_filename,
                    doc.year,
                    doc.num_pages ? doc.num_pages + ' pages' : null
                ].filter(Boolean).join(' • ') || 'No metadata';

                return `
                    <div class="doc" id="doc-${i}">
                        <div class="doc-header" onclick="toggleDoc(${i})">
                            <div>
                                <div class="doc-id">${doc._id}</div>
                                <div class="doc-meta">${meta}</div>
                            </div>
                            <span class="expand-icon">▶</span>
                        </div>
                        <div class="doc-fields">${fields || '<em style="color:#666">No additional fields</em>'}</div>
                        <div class="doc-text"><pre>${escapeHtml(text)}</pre></div>
                    </div>
                `;
            }).join('');
        }

        function formatValue(v) {
            if (v === null || v === undefined) return '<em style="color:#666">null</em>';
            if (Array.isArray(v)) return '[' + v.length + ' items]';
            if (typeof v === 'object') return JSON.stringify(v);
            return String(v);
        }

        function escapeHtml(text) {
            const div = document.createElement('div');
            div.textContent = text;
            return div.innerHTML;
        }

        function toggleDoc(i) {
            document.getElementById('doc-' + i).classList.toggle('expanded');
        }

        function prevPage() {
            if (currentPage > 0) { currentPage--; loadDocs(); }
        }

        function nextPage() {
            const totalPages = Math.ceil(totalDocs / pageSize);
            if (currentPage < totalPages - 1) { currentPage++; loadDocs(); }
        }

        function jumpToPage() {
            const page = parseInt(document.getElementById('jump-page').value) - 1;
            const totalPages = Math.ceil(totalDocs / pageSize);
            if (page >= 0 && page < totalPages) {
                currentPage = page;
                loadDocs();
            }
        }

        // Initialize
        loadCollections();
        loadSchema();
        loadDocs();
    </script>
</body>
</html>
'''

@app.route('/')
def index():
    return render_template_string(HTML)

@app.route('/api/collections')
def get_collections():
    collections = []
    for name in db.list_collection_names():
        count = db[name].estimated_document_count()
        collections.append({'name': name, 'count': count})
    return jsonify(sorted(collections, key=lambda x: -x['count']))

@app.route('/api/schema')
def get_schema():
    collection = request.args.get('collection', 'documents')
    # Get one doc to infer schema
    doc = db[collection].find_one()
    if not doc:
        return jsonify([])

    fields = []
    for key, value in doc.items():
        t = type(value).__name__
        if t == 'ObjectId': t = 'ObjectId'
        elif t == 'list': t = f'array[{len(value)}]'
        elif t == 'NoneType': t = 'null'
        elif t == 'str' and len(value) > 100: t = f'string ({len(value)} chars)'
        fields.append({'name': key, 'type': t})

    return jsonify(fields)

@app.route('/api/docs')
def get_docs():
    collection = request.args.get('collection', 'documents')
    skip = int(request.args.get('skip', 0))
    limit = int(request.args.get('limit', 20))

    coll = db[collection]
    total = coll.estimated_document_count()

    docs = []
    for doc in coll.find().skip(skip).limit(limit):
        doc['_id'] = str(doc['_id'])
        # Convert ObjectIds in arrays
        for k, v in doc.items():
            if isinstance(v, list):
                doc[k] = [str(x) if isinstance(x, ObjectId) else x for x in v]
            elif isinstance(v, ObjectId):
                doc[k] = str(v)
        docs.append(doc)

    return jsonify({'docs': docs, 'total': total})

if __name__ == '__main__':
    print("\n" + "="*50)
    print("  MongoDB Browser: http://localhost:5050")
    print("="*50 + "\n")
    app.run(host='0.0.0.0', port=5050, debug=False, threaded=True)
