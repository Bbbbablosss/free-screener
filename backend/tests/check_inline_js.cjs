const fs = require('node:fs');
const vm = require('node:vm');
const path = require('node:path');

const html = fs.readFileSync(path.join(__dirname, '..', 'static', 'index.html'), 'utf8');
const scripts = [...html.matchAll(/<script\b([^>]*)>([\s\S]*?)<\/script>/gi)];
let checked = 0;
for (const [, attributes, source] of scripts) {
  if (/\bsrc\s*=|type\s*=\s*["'](?:application\/ld\+json|application\/json)/i.test(attributes)) continue;
  if (!source.trim()) continue;
  new vm.Script(source, {filename: `index.html script ${++checked}`});
}
console.log(`Syntax OK: ${checked} inline scripts`);
