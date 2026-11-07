import * as fs from 'node:fs/promises';
import { dirname, join, posix } from 'node:path';
import { createHash } from 'node:crypto';
import { setImmediate } from 'node:timers/promises';
import { compileHistoryQuery, findHistoryMatch } from './history-search.mjs';
import { parseAttachmentPrompt } from './attachment-prompts.mjs';
import { safeReason } from './diagnostics.mjs';

async function checked(path, directory = false) {
  if (typeof path !== 'string' || !path.startsWith('/') || path.includes('\0') || posix.normalize(path) !== path)
    throw Error('历史路径无效');
  if (path !== '/') await checked(dirname(path), true);
  const stat = await fs.lstat(path);
  if (stat.isSymbolicLink() || (directory ? !stat.isDirectory() : !stat.isFile())) throw Error('历史路径包含链接或非预期文件类型');
  return stat;
}
const stamp = value => `${value.dev}:${value.ino}:${value.size}:${value.mtimeMs}:${value.ctimeMs}`;

/** Pure native file loading; no AgentSession, registry, model SDK or controller is constructed. */
export async function searchContainerSessions(request, native, emit) {
  const pattern = compileHistoryQuery(request.query);
  const agentDir = request.agentDirectory;
  if (typeof agentDir !== 'string' || !agentDir.endsWith('/.omp/agent')) throw Error('OMP 历史目录无效');
  await checked(agentDir, true);
  if (!Array.isArray(request.targets) || request.targets.some(target => typeof target?.id !== 'string' || !target.id.length ||
    target.sessionFile != null && typeof target.sessionFile !== 'string')) throw Error('会话范围无效');
  const targets = [...new Map(request.targets.map(target => [target.id, target])).values()];
  await emit({ type: 'progress', completed: 0, total: targets.length });
  let completed = 0;
  for (const { id: appSessionId, sessionFile } of targets) {
    try {
      const key = createHash('sha256').update(appSessionId, 'utf8').digest('hex');
      const directory = join(agentDir, 'wawlink', 'sessions', key);
      let target;
      if (sessionFile != null) {
        const prefix = `${directory}/`, name = sessionFile.slice(prefix.length);
        if (!sessionFile.startsWith(prefix) || name.includes('/') || !name.endsWith('.jsonl') || name === '.jsonl') throw Error('会话绑定的历史文件越界');
        target = { file: sessionFile, stat: await checked(sessionFile) };
      } else {
        let candidates;
        try {
          await checked(directory, true);
          candidates = await fs.readdir(directory);
        } catch (error) { if (error?.code === 'ENOENT') candidates = []; else throw error; }
        const files = [];
        for (const name of candidates) if (name.endsWith('.jsonl')) {
          const file = join(directory, name), stat = await checked(file);
          files.push({ file, stat });
        }
        files.sort((left, right) => right.stat.mtimeMs - left.stat.mtimeMs || right.file.localeCompare(left.file));
        target = files[0];
      }
      if (target) {
        const loaded = await native.loadSessionFile(target.file, undefined, { throwIfMissing: true });
        if (loaded.invalidHeader || loaded.malformedRecords) throw Error(`原生历史记录无效，无法读取 ${loaded.malformedRecords ?? 0} 条记录`);
        const header = loaded.entries.find(entry => entry.type === 'session');
        if (!header || typeof header.id !== 'string' || !header.id) throw Error('原生历史缺少会话身份');
        if (stamp(await checked(target.file)) !== stamp(target.stat)) throw Error('会话历史正在更新，请重新查找');
        let visited = 0;
        for (const entry of loaded.entries) {
          if (entry.type === 'session' || typeof entry.id !== 'string' || !entry.id) continue;
          const match = findHistoryMatch(entry, pattern, item => {
            if (!native.isUserRequestEntry(item)) return null;
            const text = native.userTurnDraft(item);
            return typeof text === 'string' ? parseAttachmentPrompt(text) : null;
          });
          if (match) await emit({ type: 'match', appSessionId, nativeSessionId: header.id, entryId: entry.id,
            sessionFile: target.file, role: entry.message?.role ?? '', entryType: entry.type, ...match });
          if (++visited % 64 === 0) await setImmediate();
        }
      }
    } catch (error) {
      await emit({ type: 'failure', appSessionId, message: safeReason(error?.message ?? error, { secret: request.query }) });
    }
    await emit({ type: 'progress', completed: ++completed, total: targets.length });
  }
  await emit({ type: 'complete' });
}

if (import.meta.main) {
  console.log = console.info = console.warn = console.error = () => {};
  const emit = value => new Promise((resolve, reject) => process.stdout.write(JSON.stringify(value) + '\n', error => error ? reject(error) : resolve()));
  try {
    const request = JSON.parse(await Bun.stdin.text());
    if (request.agentDirectory !== process.env.WAWLINK_HISTORY_AGENT_DIR) throw Error('历史目录身份不匹配');
    const loader = await import('/opt/omp/src/session/session-loader.ts');
    const draft = await import('/opt/omp/node_modules/@oh-my-pi/pi-tui/src/chat/transcript-entry.ts');
    await searchContainerSessions(request, { ...loader, ...draft }, emit);
    process.exit(0);
  } catch (error) {
    await emit({ type: 'error', message: safeReason(error?.message ?? error) });
    process.exit(1);
  }
}
