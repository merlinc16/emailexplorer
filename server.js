const express = require('express');
const path = require('path');
const { spawn } = require('child_process');

const app = express();
app.use(express.json());
app.use(express.static('public'));

app.post('/api/fetch-correspondence', (req, res) => {
  const { email1, email2, name1, name2 } = req.body;
  if (!email1 || !email2) {
    return res.status(400).json({ success: false, error: 'email1 and email2 are required' });
  }

  // Write params to temp file for the Python script
  const fs = require('fs');
  const os = require('os');
  const tmpFile = path.join(os.tmpdir(), `zip-${Date.now()}.json`);
  fs.writeFileSync(tmpFile, JSON.stringify({ email1, email2, name1: name1 || 'person1', name2: name2 || 'person2' }));

  const pythonPath = path.join(__dirname, '.venv', 'bin', 'python');
  const scriptPath = path.join(__dirname, 'fetch_correspondence.py');
  const child = spawn(pythonPath, [scriptPath, tmpFile]);

  let stdout = '';
  let stderr = '';

  child.stdout.on('data', (data) => { stdout += data.toString(); });
  child.stderr.on('data', (data) => { stderr += data.toString(); });

  child.on('close', (code) => {
    fs.unlink(tmpFile, () => {});
    if (stderr) console.error('[fetch_correspondence]', stderr);
    try {
      const result = JSON.parse(stdout);
      res.json(result);
    } catch (e) {
      res.status(500).json({ success: false, error: 'Failed to parse script output', details: stdout });
    }
  });

  child.on('error', (err) => {
    fs.unlink(tmpFile, () => {});
    res.status(500).json({ success: false, error: 'Failed to spawn Python script: ' + err.message });
  });
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`Email Network Explorer running at http://localhost:${PORT}`);
});
