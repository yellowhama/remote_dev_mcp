import test from 'node:test';
import assert from 'node:assert/strict';
import { determineTerminalState } from '../jobs.mjs';

test('determineTerminalState: processes terminating with non-zero or null exitCode fail', () => {
  // Exit code 0 is success
  assert.equal(determineTerminalState({ exitCode: 0, signal: null, timedOut: false }), 'succeeded');
  
  // Non-zero exit code is failure
  assert.equal(determineTerminalState({ exitCode: 1, signal: null }), 'failed');
  assert.equal(determineTerminalState({ exitCode: 127, signal: null }), 'failed');
  assert.equal(determineTerminalState({ exitCode: 137, signal: null }), 'failed');

  // Null exitCode (signal, killed, abort) is failure
  assert.equal(determineTerminalState({ exitCode: null, signal: 'SIGKILL' }), 'failed');
  assert.equal(determineTerminalState({ exitCode: null, signal: 'SIGTERM' }), 'failed');
  assert.equal(determineTerminalState({ exitCode: null, signal: null }), 'failed');

  // Timeout or uncompleted is failure
  assert.equal(determineTerminalState({ exitCode: 0, timedOut: true }), 'failed');
  assert.equal(determineTerminalState({ sessionId: 'uuid-1', completed: false }), 'failed');

  // Explicit error or isError is failure
  assert.equal(determineTerminalState({ isError: true, error: 'Command failed' }), 'failed');
  assert.equal(determineTerminalState({ error: 'Failed' }), 'failed');

  // Non-process success
  assert.equal(determineTerminalState({ checkpoint: { id: 'snap-1' } }), 'succeeded');
  assert.equal(determineTerminalState({ ok: true }), 'succeeded');
  assert.equal(determineTerminalState(null), 'succeeded');
  assert.equal(determineTerminalState(undefined), 'succeeded');
});
