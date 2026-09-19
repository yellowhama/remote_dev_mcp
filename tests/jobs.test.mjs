import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { openJobs, determineTerminalState } from '../jobs.mjs';

function deferred() {
  let resolve;
  const promise = new Promise(done => { resolve = done; });
  return { promise, resolve };
}
async function setup(t, options) {
  const directory = await fs.mkdtemp(path.join(os.tmpdir(), 'musu-jobs-'));
  t.after(async () => {
    assert.equal(path.dirname(directory), os.tmpdir());
    assert.ok(path.basename(directory).startsWith('musu-jobs-'));
    await fs.rm(directory, { recursive: true, force: true });
  });
  return { directory, jobs: await openJobs(directory, options) };
}

test('concurrent retries with reordered nested payload keys execute exactly once', async t => {
  const { jobs } = await setup(t);
  let calls = 0;
  const operation = async () => { calls++; return { output: '한글 preserved' }; };
  const responses = await Promise.all(Array.from({ length: 16 }, (_, i) => jobs.submit('alice', 'retry-key',
    i % 2 ? { args: { b: 2, a: [1, { y: 2, x: 1 }] }, tool: 'exec' }
      : { tool: 'exec', args: { a: [1, { x: 1, y: 2 }], b: 2 } }, operation)));
  await jobs.idle();
  assert.equal(new Set(responses.map(job => job.id)).size, 1);
  assert.equal(calls, 1);
  const result = jobs.get(responses[0].id, 'alice');
  assert.equal(result.state, 'succeeded');
  assert.deepEqual(result.result, { output: '한글 preserved' });
  assert.equal('owner' in result, false);
  assert.equal('keyHash' in result, false);
  result.result.output = 'modified by caller';
  assert.equal(jobs.get(result.id, 'alice').result.output, '한글 preserved');
});

test('conflicting key is rejected without poisoning subsequent admission', async t => {
  const { jobs } = await setup(t);
  await jobs.submit('alice', 'same', { tool: 'exec', args: ['a', 'b'] }, async () => 1);
  await assert.rejects(jobs.submit('alice', 'same', { tool: 'exec', args: ['b', 'a'] }, async () => 2), /conflicts/);
  const next = await jobs.submit('alice', 'different', { tool: 'exec' }, async () => 3);
  await jobs.idle();
  assert.equal(jobs.get(next.id, 'alice').result, 3);
});

test('owner boundaries hide jobs and allow independent identical request keys', async t => {
  const { jobs } = await setup(t);
  const first = await jobs.submit('alice', 'same', { tool: 'exec' }, async () => 'alice');
  const second = await jobs.submit('bob', 'same', { tool: 'exec' }, async () => 'bob');
  await jobs.idle();
  assert.notEqual(first.id, second.id);
  assert.throws(() => jobs.get(first.id, 'bob'), /not found/);
  assert.throws(() => jobs.get(first.id, ''), /not found/);
  await assert.rejects(jobs.cancel(first.id, 'bob'), /not found/);
  assert.equal(jobs.get(second.id, 'bob').result, 'bob');
});

test('execution failure persists and does not stop later jobs or retry failed work', async t => {
  const { jobs, directory } = await setup(t);
  let failures = 0;
  const failed = await jobs.submit('alice', 'failure', { tool: 'exec' }, async () => { failures++; throw new Error('boom'); });
  const next = await jobs.submit('alice', 'next', { tool: 'exec' }, async () => 'okay');
  await jobs.idle();
  assert.equal(jobs.get(failed.id, 'alice').state, 'failed');
  assert.equal(jobs.get(next.id, 'alice').state, 'succeeded');
  const reopened = await openJobs(directory);
  const retry = await reopened.submit('alice', 'failure', { tool: 'exec' }, async () => { failures++; });
  await reopened.idle();
  assert.equal(retry.id, failed.id);
  assert.equal(retry.error, 'boom');
  assert.equal(failures, 1);
});

test('queued cancellation never invokes the operation', async t => {
  const { jobs } = await setup(t);
  const release = deferred();
  const started = deferred();
  let queuedCalls = 0;
  await jobs.submit('alice', 'first', { tool: 'exec' }, async () => { started.resolve(); await release.promise; });
  await started.promise;
  const queued = await jobs.submit('alice', 'queued', { tool: 'exec' }, async () => { queuedCalls++; });
  assert.equal(queued.state, 'queued');
  await jobs.cancel(queued.id, 'alice');
  release.resolve();
  await jobs.idle();
  assert.equal(queuedCalls, 0);
  assert.equal(jobs.get(queued.id, 'alice').state, 'cancelled');
});

test('active cancellation signals execution and preserves cancelled terminal state', async t => {
  const { jobs } = await setup(t);
  const started = deferred();
  const job = await jobs.submit('alice', 'active', { tool: 'exec' }, async ({ signal, update }) => {
    await update({ state: 'running', progress: 'started' });
    started.resolve();
    await new Promise((resolve, reject) => {
      signal.addEventListener('abort', () => reject(signal.reason), { once: true });
      if (signal.aborted) reject(signal.reason);
    });
  });
  await started.promise;
  assert.equal(jobs.get(job.id, 'alice').state, 'running');
  await jobs.cancel(job.id, 'alice');
  await jobs.idle();
  assert.equal(jobs.get(job.id, 'alice').state, 'cancelled');
  assert.match(jobs.get(job.id, 'alice').error, /cancelled/);
  assert.equal((await jobs.cancel(job.id, 'alice')).state, 'cancelled');
});

test('restart converts unfinished records to interrupted_unknown without executing retries', async t => {
  const { jobs, directory } = await setup(t);
  const original = await jobs.submit('alice', 'restart', { tool: 'exec' }, async () => 'done');
  await jobs.idle();
  const filename = path.join(directory, `${original.id}.json`);
  const record = JSON.parse(await fs.readFile(filename, 'utf8'));
  await fs.copyFile(filename, `${filename}.backup`);
  record.state = 'running';
  delete record.result;
  await fs.writeFile(filename, JSON.stringify(record));
  const reopened = await openJobs(directory);
  let calls = 0;
  const retry = await reopened.submit('alice', 'restart', { tool: 'exec' }, async () => { calls++; });
  await reopened.idle();
  assert.equal(retry.state, 'interrupted_unknown');
  assert.equal(retry.id, original.id);
  assert.equal(calls, 0);
  assert.equal(JSON.parse(await fs.readFile(filename, 'utf8')).state, 'interrupted_unknown');
});

test('cancellation during a temporarily noncooperative operation is not reported as success', async t => {
  const { jobs } = await setup(t);
  const started = deferred();
  const release = deferred();
  const job = await jobs.submit('alice', 'noncooperative', { tool: 'exec' }, async () => {
    started.resolve();
    await release.promise;
    return 'operation returned after cancellation';
  });
  await started.promise;
  await jobs.cancel(job.id, 'alice');
  release.resolve();
  await jobs.idle();
  assert.equal(jobs.get(job.id, 'alice').state, 'cancelled');
});

test('queue cap rejects new jobs but accepts retries and frees slots on completion', async t => {
  const { jobs } = await setup(t, { maxPending: 1 });
  const release = deferred();
  const first = await jobs.submit('alice', 'first', { tool: 'exec' }, () => release.promise);
  await assert.rejects(jobs.submit('alice', 'second', { tool: 'exec' }, async () => 2), /queue full/);
  assert.equal((await jobs.submit('alice', 'first', { tool: 'exec' }, async () => assert.fail('retry executed'))).id, first.id);
  release.resolve();
  await jobs.idle();
  const second = await jobs.submit('alice', 'second', { tool: 'exec' }, async () => 2);
  await jobs.idle();
  assert.equal(jobs.get(second.id, 'alice').result, 2);
});

test('history bound fails closed without erasing durable idempotency records', async t => {
  const { jobs } = await setup(t, { maxRecords: 1 });
  const first = await jobs.submit('alice', 'first', { tool: 'exec' }, async () => 1);
  await jobs.idle();
  await assert.rejects(jobs.submit('alice', 'second', { tool: 'exec' }, async () => 2), /history full/);
  assert.equal((await jobs.submit('alice', 'first', { tool: 'exec' }, async () => 3)).id, first.id);
});

test('invalid request keys and owner are rejected before operation admission', async t => {
  const { jobs, directory } = await setup(t);
  for (const [owner, key] of [['', 'key'], ['alice', ''], ['alice', 'x'.repeat(129)], ['alice', 123]]) {
    await assert.rejects(jobs.submit(owner, key, { tool: 'exec' }, async () => assert.fail('invalid request executed')), /required/);
  }
  assert.deepEqual(await fs.readdir(directory), []);
});

test('determineTerminalState accurately classifies process and tool outcomes', () => {
  assert.equal(determineTerminalState(null), 'succeeded');
  assert.equal(determineTerminalState(undefined), 'succeeded');
  assert.equal(determineTerminalState({ count: 10 }), 'succeeded');
  assert.equal(determineTerminalState({ isError: true, error: 'fail' }), 'failed');
  assert.equal(determineTerminalState({ error: 'fail' }), 'failed');

  // Process results
  assert.equal(determineTerminalState({ exitCode: 0, signal: null, timedOut: false }), 'succeeded');
  assert.equal(determineTerminalState({ exitCode: 1, signal: null }), 'failed');
  assert.equal(determineTerminalState({ exitCode: null, signal: 'SIGKILL' }), 'failed');
  assert.equal(determineTerminalState({ exitCode: null, signal: null }), 'failed');
  assert.equal(determineTerminalState({ exitCode: 0, timedOut: true }), 'failed');
  assert.equal(determineTerminalState({ sessionId: 'abc', completed: false }), 'failed');
  assert.equal(determineTerminalState({ sessionId: 'abc', exitCode: 0, completed: true }), 'succeeded');
});

test('jobs properly transition to failed when process is killed by signal or null exitCode', async t => {
  const { jobs } = await setup(t);
  const killed = await jobs.submit('alice', 'killed', { tool: 'exec' }, async () => ({
    sessionId: 'session-1',
    exitCode: null,
    signal: 'SIGKILL',
    error: 'Process killed by SIGKILL'
  }));
  await jobs.idle();
  const res = jobs.get(killed.id, 'alice');
  assert.equal(res.state, 'failed');
  assert.equal(res.error, 'Process killed by SIGKILL');
  assert.equal(res.result.signal, 'SIGKILL');

  const zero = await jobs.submit('alice', 'zero', { tool: 'exec' }, async () => ({
    sessionId: 'session-2',
    exitCode: 0,
    signal: null
  }));
  await jobs.idle();
  assert.equal(jobs.get(zero.id, 'alice').state, 'succeeded');
});

