// Shared literal full-text matching; only compact excerpts leave the guest.
function* searchContent(entry, presentedUserPrompt) {
  const prompt = presentedUserPrompt(entry);
  if (prompt) {
    yield prompt.text;
    for (const file of prompt.attachments) yield file.name;
    return;
  }
  const content = entry.type === 'message' ? entry.message?.content : entry.content;
  if (typeof content === 'string') yield content;
  else if (Array.isArray(content)) for (const block of content) {
    if (block.type === 'text' && typeof block.text === 'string') yield block.text;
    else if (block.type === 'thinking' && typeof block.thinking === 'string') yield block.thinking;
    else if (block.type === 'toolCall') {
      const stack = [block.arguments], visited = new Set();
      while (stack.length) {
        const value = stack.pop();
        if (typeof value === 'string') yield value;
        else if (value && typeof value === 'object' && !visited.has(value)) {
          visited.add(value);
          stack.push(...Object.values(value).reverse());
        }
      }
    }
  }
  for (const key of ['text', 'summary', 'title', 'label', 'name']) if (typeof entry[key] === 'string') yield entry[key];
}
export function compileHistoryQuery(value) {
  const query = typeof value === 'string' ? value.trim() : '';
  if (!query || query.length > 256) throw Error('请输入不超过 256 字的查找内容');
  return new RegExp(query.replace(/[.*+?^${}()|[\]\\]/g, '\\$&'), 'iu');
}
export function findHistoryMatch(entry, pattern, presentedUserPrompt) {
  for (const text of searchContent(entry, presentedUserPrompt)) {
    const hit = pattern.exec(text);
    if (!hit) continue;
    let start = Math.max(0, hit.index - 64);
    const lineBreak = text.lastIndexOf('\n', hit.index - 1);
    if (lineBreak >= start) start = Math.max(start, text.lastIndexOf('\n', lineBreak - 1) + 1);
    let end = Math.min(text.length, Math.max(hit.index + hit[0].length + 96, start + 200));
    if (start > 0 && /[\uDC00-\uDFFF]/u.test(text[start])) start--;
    if (end < text.length && /[\uDC00-\uDFFF]/u.test(text[end])) end++;
    return { text: text.slice(start, end), matchStart: hit.index - start, matchEnd: hit.index + hit[0].length - start };
  }
  return null;
}
