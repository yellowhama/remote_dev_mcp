import fs from 'node:fs/promises';
import path from 'node:path';
import { createHash, randomUUID } from 'node:crypto';
const terminal = new Set(['succeeded','failed','cancelled','interrupted_unknown']);
function canonical(value) {
  if(Array.isArray(value))return value.map(canonical);
  if(value&&typeof value==='object')return Object.fromEntries(Object.keys(value).sort().map(k=>[k,canonical(value[k])]));
  return value;
}
const digest=value=>createHash('sha256').update(JSON.stringify(canonical(value))).digest('hex');
export function determineTerminalState(result) {
  if (!result) return 'succeeded';
  if (result.isError || result.error) return 'failed';

  const isProcess = (
    result.sessionId !== undefined ||
    result.exitCode !== undefined ||
    result.signal !== undefined ||
    result.timedOut !== undefined ||
    result.completed !== undefined
  );

  if (isProcess) {
    if (result.timedOut === true) return 'failed';
    if (result.signal != null) return 'failed';
    if (result.completed === false) return 'failed';
    if (result.exitCode !== 0) return 'failed';
    return 'succeeded';
  }

  return 'succeeded';
}

export async function openJobs(directory,{maxPending=4,maxRecords=2000}={}) {
  await fs.mkdir(directory,{recursive:true});
  const jobs=new Map(),controllers=new Map();let queue=Promise.resolve(),admission=Promise.resolve();
  const writes=new Map();
  async function save(job){
    const payload=JSON.stringify(job),id=job.id;
    const write=(writes.get(id)||Promise.resolve()).then(async()=>{
      const file=path.join(directory,`${id}.json`),tmp=`${file}.${randomUUID()}.tmp`;
      const h=await fs.open(tmp,'wx',0o600);try{await h.writeFile(payload);await h.sync();}finally{await h.close();}
      await fs.rename(tmp,file);
    });
    writes.set(id,write.catch(()=>{}));await write;
  }
  for(const name of await fs.readdir(directory)){
    if(!/^[a-f0-9-]{36}\.json$/.test(name))continue;
    const job=JSON.parse(await fs.readFile(path.join(directory,name),'utf8'));
    if(job.id+'.json'!==name||typeof job.owner!=='string')throw new Error('Invalid job state');
    if(!terminal.has(job.state)){job.state='interrupted_unknown';job.error='Server restarted; execution may have had effects. Inspect before submitting a new key.';await save(job);}
    jobs.set(job.id,job);
  }
  function owned(id,owner){const job=jobs.get(id);if(!owner||!job||job.owner!==owner)throw new Error('Job not found');return job;}
  function view(job){const {owner,keyHash,payloadHash,...result}=job;return structuredClone(result);}
  return {
    get(id,owner){return view(owned(id,owner));},
    async cancel(id,owner){const job=owned(id,owner);if(terminal.has(job.state))return view(job);controllers.get(id)?.abort(new Error('Job cancelled by owner'));return view(job);},
    async submit(owner,key,payload,operation){
      if(!owner||typeof key!=='string'||key.length<1||key.length>128)throw new Error('Authenticated owner and requestKey required');
      // Serialize key lookup + disk admission, so simultaneous retries cannot double-submit.
      const accepted=admission.then(async()=>{
        const keyHash=digest([owner,key]),payloadHash=digest(payload);
        const previous=[...jobs.values()].find(j=>j.keyHash===keyHash);
        if(previous){if(previous.payloadHash!==payloadHash)throw new Error('requestKey conflicts with different arguments');return {job:previous,existing:true};}
        if(jobs.size>=maxRecords)throw new Error('Job history full; operator archival required');
        if([...jobs.values()].filter(j=>!terminal.has(j.state)).length>=maxPending)throw new Error('Job queue full');
        const job={id:randomUUID(),owner,keyHash,payloadHash,tool:payload.tool,state:'queued',createdAt:new Date().toISOString()};
        await save(job);jobs.set(job.id,job);return {job,existing:false};
      });
      admission=accepted.catch(()=>{});
      const {job,existing}=await accepted;if(existing)return view(job);
      const controller=new AbortController();controllers.set(job.id,controller);
      queue=queue.then(async()=>{
        try{
          controller.signal.throwIfAborted();
          const update=async(data)=>{Object.assign(job,data,{updatedAt:new Date().toISOString()});await save(job);};
          await update({state:'backing_up'});
          const result=await operation({signal:controller.signal,update});
          controller.signal.throwIfAborted();
          const state = determineTerminalState(result);
          await update({
            state,
            result,
            ...(state === 'failed' && result?.error ? { error: String(result.error).slice(0, 2000) } : {})
          });
        }catch(e){job.state=controller.signal.aborted?'cancelled':'failed';job.error=String(e.message).slice(0,2000);try{await save(job);}catch(err){console.error('Job terminal state write failed:',err.message);}}
        finally{controllers.delete(job.id);}
      }).catch(e=>console.error('Job queue failed:',e.message));
      return view(job);
    },
    async idle(){await admission;await queue;}
  };
}
