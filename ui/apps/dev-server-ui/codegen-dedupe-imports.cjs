/**
 * Dedupes `import type { ... } from '...';` statements in a codegen output.
 *
 * The typescript and typescript-operations plugins both emit scalar-mapped
 * imports (e.g. `import type { SpanMetadataKind } from '@inngest/components/...';`)
 * when they share a single output file. With `useTypeImports: true` this leads
 * to duplicate identifier errors in TS strict mode. This hook merges all
 * `import type` lines for the same source path into one and drops duplicate
 * imported symbols.
 */
const fs = require('fs');
const path = require('path');

const filePath = process.argv[2];
if (!filePath) {
  process.exit(0);
}

const absPath = path.resolve(filePath);
let src;
try {
  src = fs.readFileSync(absPath, 'utf8');
} catch {
  process.exit(0);
}

const importRe =
  /^import type \{\s*([^}]+?)\s*\} from ['"]([^'"]+)['"];\s*$/gm;
const bySource = new Map();
const order = [];
let match;
while ((match = importRe.exec(src)) !== null) {
  const source = match[2];
  const symbols = match[1]
    .split(',')
    .map((s) => s.trim())
    .filter(Boolean);
  if (!bySource.has(source)) {
    bySource.set(source, new Set());
    order.push(source);
  }
  const set = bySource.get(source);
  for (const sym of symbols) {
    set.add(sym);
  }
}

if (order.length === 0) {
  process.exit(0);
}

const withoutImports = src.replace(importRe, '');
const merged = order
  .map((source) => {
    const symbols = Array.from(bySource.get(source)).sort();
    return `import type { ${symbols.join(', ')} } from '${source}';`;
  })
  .join('\n');

const cleaned = withoutImports.replace(/^\s*\n+/, '');
fs.writeFileSync(absPath, `${merged}\n\n${cleaned}`);
