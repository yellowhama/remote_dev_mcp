import fs from 'node:fs/promises';
import { constants } from 'node:fs';
import path from 'node:path';
import { createHash, randomUUID } from 'node:crypto';

export const inside = (root, value) => value === root || value.startsWith(root + path.sep);
const excluded = new Set(['node_modules','target','.next','.git','.cache','test-results','playwright-report','musu-bee-backups','llm-wiki-backups','.cargo-target-claude','.cargo-target-claude-tauri','.local-build','dependency-cache','.turbo']);
const stamp = s => `${s.dev}:${s.ino}:${s.size}:${s.mtimeMs}:${s.ctimeMs}:${s.mode}`;
export async function safePath(value, cwd, roots) {
  const file = path.resolve(cwd, value);
  const root = roots.find(r => inside(r, file));
  if (!root) throw new Error('Path outside editable code/wiki roots');
  if (await fs.realpath(root) !== root) throw new Error('Workspace root must be canonical');
  let cursor = root;
  for (const part of path.relative(root, file).split(path.sep).filter(Boolean)) {
    cursor = path.join(cursor, part);
    try { if ((await fs.lstat(cursor)).isSymbolicLink()) throw new Error('Symlink mutation path rejected'); }
    catch (e) { if (e.code === 'ENOENT') break; throw e; }
  }
  return file;
}
export async function digestFile(file, signal) {
  const h = createHash('sha256');
  const handle = await fs.open(file, constants.O_RDONLY | (constants.O_NOFOLLOW ?? 0));
  try {
    if (!(await handle.stat()).isFile()) throw new Error('Expected regular file');
    const buffer = Buffer.alloc(1024 * 1024);
    for (;;) { signal?.throwIfAborted(); const { bytesRead } = await handle.read(buffer); if (!bytesRead) break; h.update(buffer.subarray(0,bytesRead)); }
    return h.digest('hex');
  } finally { await handle.close(); }
}

export function snapshotter({ roots, backupRoot, maxFiles=100000, maxFileBytes=256*1024*1024, maxTotalBytes=16*1024**3, workers=8 }) {
  roots = roots.map(r => path.resolve(r));
  for (const n of [maxFiles,maxFileBytes,maxTotalBytes,workers]) if (!Number.isSafeInteger(n)||n<1) throw new Error('Invalid snapshot limit');
  if (workers>64) throw new Error('Too many workers');
  return async function snapshot(targets, { signal, full=false, tool='file-change', progress=()=>{} }={}) {
    const id = `${Date.now()}-${randomUUID()}`;
    const entries = Object.create(null), versions = new Map(), pending = [];
    let count=0,totalBytes=0,done=0;
    const seen = new Set(), directories=[];
    async function walk(file) {
      signal?.throwIfAborted();
      if (seen.has(file)) return; seen.add(file);
      if(seen.size>maxFiles*4)throw new Error('Snapshot entry limit exceeded');
      let s;
      try { s=await fs.lstat(file); } catch(e) { if(e.code!=='ENOENT')throw e; entries[file]={absent:true};versions.set(file,null);return; }
      if(s.isSymbolicLink()) {
        if(!full)throw new Error('Symlink target rejected');
        entries[file]={link:await fs.readlink(file)};versions.set(file,stamp(s));return;
      }
      if(s.isDirectory()) {
        entries[file]={directory:true,mode:s.mode};versions.set(file,stamp(s));
        // Directory traversal is bounded by visited entries as well as regular files.
        if(seen.size>maxFiles*4)throw new Error('Snapshot entry limit exceeded');
        directories.push(file);
        return;
      }
      if(!s.isFile())throw new Error('Unsupported snapshot entry');
      if(++count>maxFiles||s.size>maxFileBytes||(totalBytes+=s.size)>maxTotalBytes)throw new Error(`Snapshot limit exceeded at ${file}; files=${count}, bytes=${totalBytes}`);
      versions.set(file,stamp(s));pending.push({file,s});
    }
    for(const target of targets) await walk(await safePath(target,roots[0],roots));
    while(directories.length){
      const batch=directories.splice(0,workers);
      const scans=await Promise.allSettled(batch.map(async(dir)=>{
        const children=await fs.readdir(dir,{withFileTypes:true});
        for(const child of children)if(!full||!excluded.has(child.name))await walk(path.join(dir,child.name));
      }));
      const error=scans.find(r=>r.status==='rejected');if(error)throw error.reason;
      progress({phase:'scanning',count,totalBytes});
    }
    await fs.mkdir(path.join(backupRoot,'objects'),{recursive:true});
    await fs.mkdir(path.join(backupRoot,'manifests'),{recursive:true});
    const verified=new Map();
    let next=0,failed=false;
    const outcomes=await Promise.allSettled(Array.from({length:workers},async()=>{
      try {
        while(!failed&&next<pending.length){
          const {file,s}=pending[next++];signal?.throwIfAborted();
          await safePath(file,roots[0],roots);
          let succeeded=false,lastError;
          for(let attempt=0;attempt<(full?3:1)&&!succeeded;attempt++){
          const temp=path.join(backupRoot,'objects',`.${id}-${randomUUID()}.tmp`);
          let input,output;
          try {
            input=await fs.open(file,constants.O_RDONLY|(constants.O_NOFOLLOW??0));
            const actual=await input.stat();
            if(!full&&stamp(actual)!==stamp(s))throw new Error(`Source changed before backup: ${file}`);
            if(!actual.isFile()||actual.size>maxFileBytes)throw new Error(`Snapshot file limit exceeded: ${file}`);
            const hash=createHash('sha256'),buffer=Buffer.alloc(1024*1024);let size=0;
            for(;;){signal?.throwIfAborted();const {bytesRead}=await input.read(buffer);if(!bytesRead)break;size+=bytesRead;if(size>actual.size){if(full)break;throw new Error('Source grew during backup');}hash.update(buffer.subarray(0,bytesRead));}
            if(size!==actual.size||stamp(await input.stat())!==stamp(actual)){if(!full)throw new Error(`Source changed during backup: ${file}`);}
            const sha256=hash.digest('hex'),object=path.join(backupRoot,'objects',`${sha256}.backup`);
            if(!verified.has(sha256))verified.set(sha256,(async()=>{
              try { if(await digestFile(object,signal)!==sha256)throw new Error('Corrupt backup object'); }
              catch(e){
                if(e.code!=='ENOENT')throw e;
                const source=await fs.open(file,constants.O_RDONLY|(constants.O_NOFOLLOW??0));
                try{
                  if(!full&&stamp(await source.stat())!==stamp(actual))throw new Error(`Source changed before object write: ${file}`);
                  output=await fs.open(temp,'wx',0o600);
                  const secondHash=createHash('sha256');let copied=0;
                  for(;;){signal?.throwIfAborted();const {bytesRead}=await source.read(buffer);if(!bytesRead)break;copied+=bytesRead;if(copied>actual.size){if(full)break;throw new Error('Source grew during object write');}const chunk=buffer.subarray(0,bytesRead);secondHash.update(chunk);await output.writeFile(chunk);}
                  if(copied!==actual.size||stamp(await source.stat())!==stamp(actual)||secondHash.digest('hex')!==sha256){if(!full)throw new Error(`Source changed during object write: ${file}`);}
                  await output.sync();await output.close();output=null;
                }finally{await source.close();}
                await fs.rename(temp,object);
              }
            })());
            try{await verified.get(sha256);}catch(e){verified.delete(sha256);throw e;}
            entries[file]={sha256,size,mode:actual.mode};
            done++; if(done%100===0)progress({phase:'backing_up',done,count,totalBytes});
            succeeded=true;
          }catch(e){
            if(full&&e.code==='ENOENT'){
              entries[file]={absent:true};
              done++;if(done%100===0)progress({phase:'backing_up',done,count,totalBytes});
              succeeded=true;
            }else{lastError=e;if(!full||attempt===2)throw e;}
          }
          finally{await input?.close();await output?.close();await fs.rm(temp,{force:true});}
          }
          if(!succeeded)throw lastError;
        }
      }catch(e){failed=true;throw e;}
    }));
    const failure=outcomes.find(r=>r.status==='rejected');if(failure)throw failure.reason;
    signal?.throwIfAborted();
    const manifest={version:2,id,tool,roots,full,createdAt:new Date().toISOString(),entries,count,totalBytes};
    const temp=path.join(backupRoot,'manifests',`${id}.tmp`),dest=path.join(backupRoot,'manifests',`${id}.json`);
    await fs.writeFile(temp,JSON.stringify(manifest),{flag:'wx',mode:0o600});await fs.rename(temp,dest);
    return {id,count,totalBytes,async verify(){for(const [file,version] of versions){if(!entries[file]?.link)await safePath(file,roots[0],roots);let current;try{current=stamp(await fs.lstat(file));}catch(e){if(e.code!=='ENOENT')throw e;current=null;}if(current!==version)throw new Error(`Source changed before mutation: ${file}`);}}};
  };
}

export async function restoreToNewDirectory(manifest,backupRoot,destination) {
  // Never overwrite an existing restore target or the original roots.
  destination=path.resolve(destination);
  destination=path.join(await fs.realpath(path.dirname(destination)),path.basename(destination));
  if(manifest.roots.some(r=>inside(r,destination)||inside(destination,r)))throw new Error('Restore must be outside source roots');
  await fs.mkdir(destination,{recursive:false});
  for(const [original,entry] of Object.entries(manifest.entries)){
    const root=manifest.roots.find(r=>inside(r,original));if(!root)throw new Error('Invalid manifest path');
    const target=path.resolve(destination,String(manifest.roots.indexOf(root)),path.relative(root,original));
    if(!inside(destination,target))throw new Error('Restore path escaped');
    if(entry.absent)continue;
    if(entry.link)continue; // Deliberately never recreate links into live sources.
    if(entry.directory){await fs.mkdir(target,{recursive:true});continue;}
    if(!/^[a-f0-9]{64}$/.test(entry.sha256))throw new Error('Invalid object hash');
    const object=path.join(backupRoot,'objects',`${entry.sha256}.backup`);
    if(await digestFile(object)!==entry.sha256)throw new Error('Corrupt restore object');
    await fs.mkdir(path.dirname(target),{recursive:true});await fs.copyFile(object,target,constants.COPYFILE_EXCL);
    if(await digestFile(target)!==entry.sha256)throw new Error('Restore readback mismatch');
    await fs.chmod(target,entry.mode&0o777);
  }
}
