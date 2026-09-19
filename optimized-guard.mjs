import { McpServer } from '@modelcontextprotocol/sdk/server/mcp.js';
import * as z from 'zod/v4';
import { execFile } from 'node:child_process';
import { promisify } from 'node:util';
import { createMutationGate } from './checkpoint.mjs';
import { snapshotter, safePath, inside } from './snapshot-targets.mjs';
import { openJobs } from './jobs.mjs';

const execFilePromise = promisify(execFile);
async function runGit(args, cwd, signal) {
  const start = Date.now();
  try {
    const { stdout, stderr } = await execFilePromise('git', ['--no-optional-locks', ...args], {
      cwd,
      maxBuffer: 16 * 1024 * 1024,
      timeout: 60000,
      signal,
      env: { ...process.env, GIT_TERMINAL_PROMPT: '0' }
    });
    return {
      stdout: stdout.toString('utf8'),
      stderr: stderr.toString('utf8'),
      exitCode: 0,
      wallTimeMs: Date.now() - start
    };
  } catch (err) {
    if (err.stdout !== undefined || err.stderr !== undefined) {
      return {
        stdout: (err.stdout || '').toString('utf8'),
        stderr: (err.stderr || '').toString('utf8'),
        exitCode: err.code ?? (err.killed ? 137 : 1),
        wallTimeMs: Date.now() - start,
        error: err.message
      };
    }
    throw err;
  }
}

const roots=['/workspace/musu-bee','/workspace/llm-wiki'];
const snapshot=snapshotter({roots,backupRoot:'/backups',maxFiles:200000,maxFileBytes:256*1024*1024,maxTotalBytes:32*1024**3,workers:64});
const fastSnapshot=snapshotter({roots,backupRoot:'/backups',maxFiles:2000,maxFileBytes:128*1024*1024,maxTotalBytes:512*1024**2,workers:4});
const gate=createMutationGate(async()=>({kind:'target-or-job'}),8);
const jobs=await openJobs('/state/jobs');
const callbacks=new Map(), registrations=new WeakSet(), processOwners=new Map();
const result=data=>({content:[{type:'text',text:JSON.stringify(data)}],structuredContent:data});
const wrapped=operation=>async(...args)=>{try{return await operation(...args);}catch(e){return {...result({error:e.message}),isError:true};}};
const owner=extra=>{const id=extra?.authInfo?.clientId;if(!id)throw new Error('Authenticated OAuth client required');return id;};
const instructions=' Active host F:/workspace/musu-active is /workspace. Code/spec/TODO /workspace/musu-bee; wiki /workspace/llm-wiki. Read /workspace/AGENTS.md and repository instructions. Direct mutations get target byte backups; shell/script/patch and large trees use submit_job then get_job. Preserve recovery evidence. Same-UID shell is not a security sandbox.';
const deferred=new Set(['exec_command','run_script','apply_patch']);
const single=new Set(['write_file','replace_in_file','upload_file','make_directory','remove_path','chmod_path']);

function tokenize(cmd) {
  const tokens = [];
  const regex = /"([^"\\]*(?:\\.[^"\\]*)*)"|'([^'\\]*(?:\\.[^'\\]*)*)'|(\S+)/g;
  let match;
  while ((match = regex.exec(cmd)) !== null) {
    tokens.push(match[1] !== undefined ? match[1] : match[2] !== undefined ? match[2] : match[3]);
  }
  return tokens;
}

function isSingleCommandReadOnly(cmd) {
  const tokens = tokenize(cmd);
  if (tokens.length === 0) return false;
  const bin = tokens[0];

  if (['pwd', 'which', 'where', 'echo', 'cat', 'head', 'tail', 'ls', 'dir', 'wc', 'grep', 'sort', 'uniq', 'jq', 'date', 'printf', 'true', 'false', 'uname', 'whoami'].includes(bin)) {
    return true;
  }

  if (bin === 'node') {
    return tokens.length === 2 && (tokens[1] === '-v' || tokens[1] === '--version');
  }

  if (bin === 'bun') {
    return tokens.length === 2 && (tokens[1] === '-v' || tokens[1] === '--version');
  }

  if (bin === 'pnpm') {
    if (tokens.length === 2 && (tokens[1] === '-v' || tokens[1] === '--version')) return true;
    if (['ls', 'list', 'why', 'root'].includes(tokens[1])) return true;
    return false;
  }

  if (bin === 'npm') {
    if (tokens.length === 2 && (tokens[1] === '-v' || tokens[1] === '--version')) return true;
    if (tokens[1] === 'version') {
      const args = tokens.slice(2);
      return args.every(a => a.startsWith('-'));
    }
    if (['ls', 'list', 'view', 'info', 'show', 'explain', 'why', 'prefix', 'root', 'help'].includes(tokens[1])) {
      return true;
    }
    return false;
  }

  if (bin === 'cargo') {
    const sub = tokens[1];
    if (['-V', '--version'].includes(sub)) return true;
    if (sub === 'clippy') {
      return !tokens.slice(2).some(a => a === '--fix' || a.startsWith('--fix='));
    }
    if (['check', 'metadata'].includes(sub)) return true;
    return false;
  }

  if (bin === 'git') {
    const sub = tokens[1];
    if (!sub || ['-v', '--version'].includes(sub)) return true;

    if (sub === 'diff') {
      const rest = tokens.slice(2);
      const mutatingDiffFlags = ['--output', '-o'];
      if (rest.some(a => mutatingDiffFlags.some(m => a === m || a.startsWith(m + '=')))) return false;
      return true;
    }

    if (['status', 'log', 'show', 'rev-parse', 'describe', 'check-ref-format', 'cat-file'].includes(sub)) {
      return true;
    }

    if (sub === 'config') {
      const rest = tokens.slice(2);
      const isListing = rest.some(a => ['-l', '--list', '--get', '--get-all', '--get-regexp'].includes(a));
      const isMutating = rest.some(a => ['--unset', '--unset-all', '--add', '--replace-all'].includes(a));
      return isListing && !isMutating;
    }

    if (sub === 'branch') {
      const rest = tokens.slice(2);
      const mutatingFlags = ['-d', '-D', '--delete', '-m', '-M', '--move', '-c', '-C', '--copy', '-u', '--set-upstream-to', '--unset-upstream', '--edit-description'];
      if (rest.some(a => mutatingFlags.includes(a))) return false;
      const optionTakingArg = new Set(['--contains', '--no-contains', '--merged', '--no-merged', '--points-at', '--sort', '-l', '--list']);
      for (let i = 0; i < rest.length; i++) {
        const token = rest[i];
        if (token.startsWith('-')) continue;
        const prev = rest[i - 1];
        if (prev && optionTakingArg.has(prev)) continue;
        return false;
      }
      return true;
    }

    if (sub === 'remote') {
      const rest = tokens.slice(2);
      if (rest.length === 0) return true;
      if (rest.length === 1 && (rest[0] === '-v' || rest[0] === '--verbose')) return true;
      if (rest[0] === 'show' || rest[0] === 'get-url') {
        const mutatingSub = ['add', 'remove', 'rm', 'rename', 'set-url', 'set-head', 'prune', 'update'];
        return !rest.some(a => mutatingSub.includes(a));
      }
      return false;
    }

    if (sub === 'tag') {
      const rest = tokens.slice(2);
      const mutatingFlags = ['-d', '-D', '--delete', '-a', '-s', '-u', '-f', '--force'];
      if (rest.some(a => mutatingFlags.includes(a))) return false;
      if (rest.length === 0) return true;
      if (rest.every(a => a.startsWith('-'))) return true;
      return false;
    }

    return false;
  }

  return false;
}

function isReadOnlyCommand(cmd) {
  if (!cmd || typeof cmd !== 'string') return false;
  if (/>/.test(cmd)) return false;
  const parts = cmd.split(/[;&|]+/).map(s => s.trim()).filter(Boolean);
  if (parts.length === 0) return false;
  return parts.every(part => isSingleCommandReadOnly(part));
}

async function targetsFor(name,args) {
  const cwd=await safePath(args.cwd||roots[0],roots[0],roots);
  let values;
  if(single.has(name))values=[args.path];
  else if(name==='copy_path'||name==='move_path'){
    const source=await safePath(args.sourcePath,cwd,roots),destination=await safePath(args.destinationPath,cwd,roots);
    if(inside(source,destination)||inside(destination,source))throw new Error('Overlapping copy/move paths rejected');
    values=name==='move_path'?[source,destination]:[destination];
  }else throw new Error('Unknown mutating tool; use a reviewed job');
  const paths=await Promise.all(values.map(v=>safePath(v,cwd,roots)));
  if(paths.some(p=>roots.includes(p)))throw new Error('Workspace-root mutation rejected');
  return paths;
}
const original=McpServer.prototype.registerTool;
McpServer.prototype.registerTool=function(name,config,callback){
  callbacks.set(name,{config,callback});config.description+=instructions;
  let handler=callback;
  if(['write_stdin','terminate_process','read_process'].includes(name))handler=wrapped(async(args,extra)=>{
    if(processOwners.get(args.sessionId)!==owner(extra))throw new Error('Process not owned by this OAuth client');
    // Input and termination continue an already checkpointed process, not a new command.
    return callback(args,extra);
  });
  else if(name==='list_processes')handler=wrapped(async(args,extra)=>{
    const client=owner(extra);
    const res=await callback(args,extra);
    if(res?.structuredContent?.processes && Array.isArray(res.structuredContent.processes)){
      const filtered=res.structuredContent.processes.filter(p=>processOwners.get(p.sessionId)===client);
      return result({processes:filtered});
    }
    return res;
  });
  else if(deferred.has(name))handler=wrapped(async(args,extra)=>{
    if(name==='exec_command' && (args?.readOnly===true || isReadOnlyCommand(args?.cmd))){
      const res = await callback(args,extra);
      if(res?.structuredContent?.sessionId){
        processOwners.set(res.structuredContent.sessionId, owner(extra));
      }
      return res;
    }
    throw new Error(`Use submit_job with tool=${name}, arguments and unique requestKey; full backup precedes execution asynchronously.`);
  });
  else if(config.annotations?.readOnlyHint!==true)handler=wrapped(async(args,extra)=>{
    return gate(name,async()=>{
      const proof=await fastSnapshot(await targetsFor(name,args),{signal:extra?.signal,tool:name});
      await proof.verify();extra?.signal?.throwIfAborted();
      const output=await callback(args,extra);
      return {...output,structuredContent:{...output.structuredContent,checkpoint:{id:proof.id,count:proof.count,totalBytes:proof.totalBytes}}};
    },extra?.signal);
  });
  const registration=original.call(this,name,config,handler);
  if(!registrations.has(this)){
    registrations.add(this);const meta=config._meta;
    original.call(this,'inspect_command',{
      description:'Run a fast read-only query (such as git status, git diff, git log, git branch, pwd, cargo check) immediately without full backup.'+instructions,
      inputSchema:{
        cmd:z.string().optional().describe('Read-only shell command to execute'),
        command:z.string().optional().describe('Read-only shell command to execute (alias for cmd)'),
        workdir:z.string().optional().describe('Working directory (defaults to /workspace/musu-bee)')
      },
      annotations:{readOnlyHint:true,destructiveHint:false,idempotentHint:true,openWorldHint:false},
      _meta:meta
    },wrapped(async(args,extra)=>{
      const cwd=await safePath(args.workdir||roots[0],roots[0],roots);
      const cmd = args.cmd || args.command;
      if (!cmd) throw new Error('cmd or command argument is required');
      if(!isReadOnlyCommand(cmd)){
        throw new Error('inspect_command only allows read-only queries (git status, diff, log, show, branch, pwd, cargo check, node -v, etc.). For mutating commands, use submit_job.');
      }
      const res = await callbacks.get('exec_command').callback({cmd,workdir:cwd,yieldTimeMs:10000},extra);
      if(res?.structuredContent?.sessionId){
        processOwners.set(res.structuredContent.sessionId, owner(extra));
      }
      return res;
    }));
    original.call(this,'git_status',{
      description:'Get git working tree status cleanly and safely without shell interpolation.'+instructions,
      inputSchema:{
        workdir:z.string().optional().describe('Working directory (defaults to /workspace/musu-bee)'),
        short:z.boolean().optional().default(true).describe('Give the output in the short-format'),
        branch:z.boolean().optional().default(true).describe('Show the branch and tracking info even in short-format'),
        untracked:z.enum(['all','normal','no']).optional().default('normal').describe('Show untracked files mode')
      },
      annotations:{readOnlyHint:true,destructiveHint:false,idempotentHint:true,openWorldHint:false},
      _meta:meta
    },wrapped(async(args,extra)=>{
      owner(extra);
      const cwd=await safePath(args.workdir||roots[0],roots[0],roots);
      const gitArgs=['status'];
      if(args.short!==false)gitArgs.push('--short');
      if(args.branch!==false)gitArgs.push('-b');
      if(args.untracked)gitArgs.push(`--untracked-files=${args.untracked}`);
      return result(await runGit(gitArgs,cwd,extra?.signal));
    }));
    original.call(this,'git_log',{
      description:'Get commit logs cleanly and safely without shell interpolation.'+instructions,
      inputSchema:{
        workdir:z.string().optional().describe('Working directory (defaults to /workspace/musu-bee)'),
        maxCount:z.number().int().min(1).max(200).optional().default(20).describe('Limit the number of commits to output'),
        oneline:z.boolean().optional().default(true).describe('Shorthand for "--pretty=oneline --abbrev-commit"'),
        revision:z.string().optional().describe('Revision or branch (e.g. HEAD, origin/main, commit SHA)'),
        path:z.string().optional().describe('Filter log by path')
      },
      annotations:{readOnlyHint:true,destructiveHint:false,idempotentHint:true,openWorldHint:false},
      _meta:meta
    },wrapped(async(args,extra)=>{
      owner(extra);
      const cwd=await safePath(args.workdir||roots[0],roots[0],roots);
      const count=Math.min(Math.max(1,args.maxCount||20),200);
      const gitArgs=['log','-n',String(count)];
      if(args.oneline!==false)gitArgs.push('--oneline');
      if(args.revision){
        if(!/^[a-zA-Z0-9_./~^@-]+$/.test(args.revision)||args.revision.startsWith('-')){
          throw new Error('Invalid revision format');
        }
        gitArgs.push(args.revision);
      }
      if(args.path){
        await safePath(args.path,cwd,roots);
        gitArgs.push('--',args.path);
      }
      return result(await runGit(gitArgs,cwd,extra?.signal));
    }));
    original.call(this,'git_diff',{
      description:'Show changes between commits, commit and working tree, etc., cleanly capturing output in memory without file writes.'+instructions,
      inputSchema:{
        workdir:z.string().optional().describe('Working directory (defaults to /workspace/musu-bee)'),
        cached:z.boolean().optional().default(false).describe('View the changes staged for the next commit'),
        commit:z.string().optional().describe('Target commit or revision to compare against (e.g. HEAD, HEAD~1)'),
        path:z.string().optional().describe('Limit the diff to the named path'),
        stat:z.boolean().optional().default(false).describe('Generate a diffstat instead of full diff')
      },
      annotations:{readOnlyHint:true,destructiveHint:false,idempotentHint:true,openWorldHint:false},
      _meta:meta
    },wrapped(async(args,extra)=>{
      owner(extra);
      const cwd=await safePath(args.workdir||roots[0],roots[0],roots);
      const gitArgs=['diff'];
      if(args.cached)gitArgs.push('--cached');
      if(args.stat)gitArgs.push('--stat');
      if(args.commit){
        if(!/^[a-zA-Z0-9_./~^@-]+$/.test(args.commit)||args.commit.startsWith('-')){
          throw new Error('Invalid commit revision format');
        }
        gitArgs.push(args.commit);
      }
      if(args.path){
        await safePath(args.path,cwd,roots);
        gitArgs.push('--',args.path);
      }
      return result(await runGit(gitArgs,cwd,extra?.signal));
    }));
    original.call(this,'git_branch',{
      description:'List branches cleanly and safely without shell interpolation.'+instructions,
      inputSchema:{
        workdir:z.string().optional().describe('Working directory (defaults to /workspace/musu-bee)'),
        all:z.boolean().optional().default(false).describe('List both remote-tracking branches and local branches'),
        remotes:z.boolean().optional().default(false).describe('List the remote-tracking branches')
      },
      annotations:{readOnlyHint:true,destructiveHint:false,idempotentHint:true,openWorldHint:false},
      _meta:meta
    },wrapped(async(args,extra)=>{
      owner(extra);
      const cwd=await safePath(args.workdir||roots[0],roots[0],roots);
      const gitArgs=['branch','--no-color'];
      if(args.all)gitArgs.push('-a');
      else if(args.remotes)gitArgs.push('-r');
      return result(await runGit(gitArgs,cwd,extra?.signal));
    }));
    original.call(this,'submit_job',{description:'Queue full code AND wiki backup then execution; returns job ID immediately. Reuse requestKey for safe retry; poll get_job. Shell can access other mounted paths, which this snapshot does not protect.'+instructions,inputSchema:{requestKey:z.string().min(1).max(128),tool:z.enum(['checkpoint','exec_command','run_script','apply_patch','copy_path','move_path','remove_path']),arguments:z.record(z.string(),z.unknown()).default({})},annotations:{readOnlyHint:false,destructiveHint:true,idempotentHint:true,openWorldHint:true},_meta:meta},wrapped(async(args,extra)=>{
      const client=owner(extra),record=callbacks.get(args.tool);
      const parsed=args.tool==='checkpoint'?{}:z.object(record.config.inputSchema).parse(args.arguments);
      if(parsed.cwd)await safePath(parsed.cwd,roots[0],roots);
      if(parsed.workdir)await safePath(parsed.workdir,roots[0],roots);
      if(['copy_path','move_path','remove_path'].includes(args.tool))await targetsFor(args.tool,parsed);
      const isReadOnly = (args.tool === 'exec_command' && (parsed.readOnly === true || isReadOnlyCommand(parsed.cmd))) ||
                         (args.tool === 'run_script' && parsed.readOnly === true);
      return result(await jobs.submit(client,args.requestKey,{tool:args.tool,arguments:parsed},async({signal,update})=>{
        const executionExtra={...extra,signal};
        const runTool = async () => {
          let proof = null;
          if (!isReadOnly) {
            let lastProgress=0;
            proof=await snapshot(roots,{full:true,signal,tool:args.tool,progress:p=>{if(Date.now()-lastProgress>2000){lastProgress=Date.now();void update({progress:p}).catch(()=>{});}}});
            await update({state:'ready',checkpoint:{id:proof.id,count:proof.count,totalBytes:proof.totalBytes}});
            signal.throwIfAborted();
            if(args.tool==='checkpoint')return {checkpoint:{id:proof.id,count:proof.count,totalBytes:proof.totalBytes}};
          } else {
            await update({state:'ready',checkpoint:null});
            signal.throwIfAborted();
          }
          await update({state:'running'});
          const value=await record.callback(['exec_command','run_script'].includes(args.tool)?{...parsed,yieldTimeMs:0,maxOutputBytes:65536}:parsed,executionExtra);
          if(value.isError)throw new Error(value.structuredContent?.error||'Tool execution failed');
          const output=value.structuredContent;
          if(output?.sessionId){processOwners.set(output.sessionId,client);await update({process:{sessionId:output.sessionId,running:output.running}});}
          if(output?.sessionId&&(output.running||output.hasMore)){
            try{
              let current=output,tail=output.output||'';
              while(current.running||current.hasMore){
                signal.throwIfAborted();
                const next=await callbacks.get('read_process').callback({
                  sessionId:output.sessionId,
                  afterSeq:current.nextSeq||0,
                  waitMs:current.running?1000:0,
                  maxOutputBytes:65536
                },executionExtra);
                if(next.isError)throw new Error('Process result unavailable');
                current=next.structuredContent;
                tail=(tail+(current.output||'')).slice(-65536);
              }
              return {...current,output:tail};
            }catch(e){if(signal.aborted)await callbacks.get('terminate_process').callback({sessionId:output.sessionId,signal:'SIGTERM',graceMs:1000},executionExtra);throw e;}
          }
          return output;
        };
        return isReadOnly ? runTool() : gate(args.tool, runTool, signal);
      }));
    }));
    original.call(this,'get_job',{description:'Get your job state, progress, checkpoint and bounded result. No source scan.',inputSchema:{jobId:z.string().uuid()},annotations:{readOnlyHint:true},_meta:meta},wrapped(async(args,extra)=>result(jobs.get(args.jobId,owner(extra)))));
    original.call(this,'cancel_job',{description:'Cancel your queued/backup job or stop its managed process. Does not roll back effects.',inputSchema:{jobId:z.string().uuid()},annotations:{readOnlyHint:false,destructiveHint:true},_meta:meta},wrapped(async(args,extra)=>result(await jobs.cancel(args.jobId,owner(extra)))));
  }
  return registration;
};
