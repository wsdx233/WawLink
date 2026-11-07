import test from 'node:test';
import assert from 'node:assert/strict';
import * as fs from 'node:fs/promises';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { createHash } from 'node:crypto';
import { searchContainerSessions } from '../../jvmSharedMain/resources/omp-management/container-history-search.mjs';

async function fixture(run) {
  const root = await fs.mkdtemp(join(tmpdir(), 'wawlink-history-search-'));
  const agentDirectory = join(root, '.omp', 'agent');
  await fs.mkdir(agentDirectory, { recursive: true });
  const loaded = [];
  const native = {
    loadSessionFile: async file => { loaded.push(file); return { entries: (await fs.readFile(file, 'utf8')).trim().split('\n').map(line => JSON.parse(line)) }; },
    isUserRequestEntry: entry => entry.type === 'message' && entry.message.role === 'user',
    userTurnDraft: entry => entry.message.content,
  };
  const write = async (id, nativeId, content, options = {}) => {
    const key = createHash('sha256').update(id).digest('hex');
    const directory = join(agentDirectory, 'wawlink', 'sessions', key);
    await fs.mkdir(directory, { recursive: true });
    const file = join(directory, 'native.jsonl');
    await fs.writeFile(file, [
      { type: 'session', id: nativeId },
      { id: `${id}-entry`, parentId: null, type: 'message', message: { role: 'user', content } },
    ].map(value => JSON.stringify(value)).join('\n') + '\n');
    return { directory, file };
  };
  try { await run({ root, agentDirectory, native, loaded, write }); }
  finally { await fs.rm(root, { recursive: true, force: true }); }
}

test('explicit container search reads only selected native files and keeps full-text tails and native identities', () => fixture(async f => {
  await f.write('one', 'native-one', `${'长正文'.repeat(200)} NeedleCode\n\n附件（容器内真实文件）：\n- 设计图.png: /private/transport.png`);
  await f.write('unselected', 'native-unselected', 'NeedleCode');
  const updates = [];
  await searchContainerSessions({ agentDirectory: f.agentDirectory, targets: [{ id: 'one' }], query: 'needlecode' }, f.native, value => updates.push(value));
  assert.equal(f.loaded.length, 1);
  const match = updates.find(value => value.type === 'match');
  assert.equal(match.appSessionId, 'one'); assert.equal(match.nativeSessionId, 'native-one'); assert.equal(match.entryId, 'one-entry');
  assert.equal(match.text.slice(match.matchStart, match.matchEnd), 'NeedleCode');
  assert.ok(!match.text.includes('transport.png'));
  assert.deepEqual(updates.filter(value => value.type === 'progress'), [
    { type: 'progress', completed: 0, total: 1 }, { type: 'progress', completed: 1, total: 1 },
  ]);
  assert.equal(updates.at(-1).type, 'complete');
  const attachments = [];
  await searchContainerSessions({ agentDirectory: f.agentDirectory, targets: [{ id: 'one' }], query: '设计图' }, f.native, value => attachments.push(value));
  assert.equal(attachments.find(value => value.type === 'match').text, '设计图.png');
}));

test('container search refuses symlinked transcript and directory before native reading', () => fixture(async f => {
  const one = await f.write('one', 'native-one', 'needle');
  const outside = join(f.root, 'outside.jsonl');
  await fs.rename(one.file, outside); await fs.symlink(outside, one.file);
  const updates = [];
  await searchContainerSessions({ agentDirectory: f.agentDirectory, targets: [{ id: 'one' }], query: 'needle' }, f.native, value => updates.push(value));
  assert.equal(f.loaded.length, 0); assert.equal(updates.filter(value => value.type === 'match').length, 0);
  assert.match(updates.find(value => value.type === 'failure').message, /链接/);
  await fs.unlink(one.file); await fs.rmdir(one.directory);
  await fs.mkdir(join(f.root, 'outside-directory')); await fs.symlink(join(f.root, 'outside-directory'), one.directory);
  updates.length = 0;
  await searchContainerSessions({ agentDirectory: f.agentDirectory, targets: [{ id: 'one' }], query: 'needle' }, f.native, value => updates.push(value));
  assert.equal(f.loaded.length, 0); assert.match(updates.find(value => value.type === 'failure').message, /链接/);
}));

test('container search does not publish results from a transcript rewritten while loading', () => fixture(async f => {
  const one = await f.write('one', 'native-one', 'needle');
  const load = f.native.loadSessionFile;
  f.native.loadSessionFile = async file => { const result = await load(file); await fs.appendFile(one.file, '\n'); return result; };
  const updates = [];
  await searchContainerSessions({ agentDirectory: f.agentDirectory, targets: [{ id: 'one' }], query: 'needle' }, f.native, value => updates.push(value));
  assert.equal(updates.filter(value => value.type === 'match').length, 0);
  assert.match(updates.find(value => value.type === 'failure').message, /历史正在更新/);
  assert.equal(updates.at(-1).type, 'complete');
}));

test('container search exposes partial failures and still searches other selected sessions', () => fixture(async f => {
  await f.write('bad', 'native-bad', 'needle'); await f.write('good', 'native-good', 'needle');
  const load = f.native.loadSessionFile;
  f.native.loadSessionFile = async file => file.includes(createHash('sha256').update('bad').digest('hex'))
    ? { entries: [], malformedRecords: 2 } : load(file);
  const updates = [];
  await searchContainerSessions({ agentDirectory: f.agentDirectory, targets: [{ id: 'bad' }, { id: 'good' }], query: 'needle' }, f.native, value => updates.push(value));
  assert.equal(updates.filter(value => value.type === 'failure').length, 1);
  assert.equal(updates.find(value => value.type === 'match').nativeSessionId, 'native-good');
  assert.equal(updates.at(-1).type, 'complete');
}));

test('metadata transcript binding wins over a newer sibling and rejects an out-of-scope file', () => fixture(async f => {
  const one = await f.write('one', 'bound-native', 'bound needle');
  const sibling = join(one.directory, 'newer.jsonl');
  await fs.writeFile(sibling, [
    { type: 'session', id: 'wrong-newer-native' },
    { type: 'message', id: 'newer-entry', message: { role: 'user', content: 'wrong needle' } },
  ].map(value => JSON.stringify(value)).join('\n') + '\n');
  await fs.utimes(sibling, new Date(), new Date(Date.now() + 60000));
  const updates = [];
  await searchContainerSessions({ agentDirectory: f.agentDirectory, targets: [{ id: 'one', sessionFile: one.file }], query: 'needle' }, f.native, value => updates.push(value));
  assert.equal(updates.find(value => value.type === 'match').nativeSessionId, 'bound-native');
  const newer = [];
  await searchContainerSessions({ agentDirectory: f.agentDirectory, targets: [{ id: 'one' }], query: 'needle' }, f.native, value => newer.push(value));
  assert.equal(newer.find(value => value.type === 'match').nativeSessionId, 'wrong-newer-native');
  const outside = [];
  await searchContainerSessions({ agentDirectory: f.agentDirectory, targets: [{ id: 'one', sessionFile: join(f.root, 'other.jsonl') }], query: 'needle' }, f.native, value => outside.push(value));
  assert.match(outside.find(value => value.type === 'failure').message, /越界/);
}));
