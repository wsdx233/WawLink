import { createHash } from 'node:crypto';

const ATTACHMENT_MARKER = '\n\n附件（容器内真实文件）：\n';
function attachmentMimeType(name) {
  const dot = name.lastIndexOf('.'), extension = dot < 0 ? '' : name.slice(dot + 1).toLowerCase();
  if (['png', 'webp', 'gif'].includes(extension)) return `image/${extension}`;
  if (['jpg', 'jpeg'].includes(extension)) return 'image/jpeg';
  if (extension === 'svg') return 'image/svg+xml';
  if (['json', 'map'].includes(extension)) return 'application/json';
  if (['txt', 'log', 'conf', 'ini', 'toml', 'kt', 'kts', 'java', 'py', 'sh', 'rs', 'go', 'c', 'h', 'cpp', 'hpp'].includes(extension)) return 'text/plain';
  if (['md', 'markdown'].includes(extension)) return 'text/markdown';
  if (['yaml', 'yml'].includes(extension)) return 'application/yaml';
  if (['html', 'htm'].includes(extension)) return 'text/html';
  if (extension === 'css') return 'text/css';
  if (['js', 'mjs', 'cjs', 'ts', 'tsx', 'jsx'].includes(extension)) return 'text/javascript';
  if (extension === 'xml') return 'application/xml';
  if (extension === 'csv') return 'text/csv';
  return 'application/octet-stream';
}

// Keep this grammar identical to AttachmentPrompts.kt. Native messages remain
// untouched for persistence identity; only presentation fields lose transport text.
export function parseAttachmentPrompt(message, attachments = []) {
  const unchanged = () => ({ text: message, attachments });
  const marker = message.lastIndexOf(ATTACHMENT_MARKER);
  if (marker < 0) return unchanged();
  const text = message.slice(0, marker);
  const references = [];
  for (const line of message.slice(marker + ATTACHMENT_MARKER.length).split('\n')) {
    if (!line.startsWith('- ') || /[\u0000-\u001f\u007f]/u.test(line)) return unchanged();
    const separator = line.indexOf(': /', 2);
    if (separator < 0) return unchanged();
    const name = line.slice(2, separator), path = line.slice(separator + 2);
    if (!name.length || name.includes('/') || path === '/' ||
      path.endsWith('/') || path.split('/').some(segment => segment === '.' || segment === '..')) return unchanged();
    references.push({ name, path });
  }
  const remaining = [...attachments];
  const restored = references.map(({ name, path }) => {
    const index = remaining.findIndex(file => file.name === name && file.path === path);
    return index >= 0 ? remaining.splice(index, 1)[0] : {
      id: `legacy:${createHash('sha256').update(`${name}\n${path}`, 'utf8').digest('hex')}`,
      name, path, mimeType: attachmentMimeType(name), sizeBytes: 0, source: 'CONTAINER',
    };
  });
  return { text, attachments: [...restored, ...remaining] };
}
