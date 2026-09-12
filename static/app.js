const $=id=>document.getElementById(id);
let ws=null,stream=null,ctx=null,processor=null,source=null,running=false,fileBlob=null,history=[];
const HISTORY_KEY="voiceguard_history_v1";
function setConn(ok,text){$('connDot').className=ok?'on':'off';$('connText').textContent=text;$('engineStatus').textContent=ok?'Online / Ready':'Engine unavailable';}
function pct(v){const n=Number(v);return Number.isFinite(n)?Math.max(0,Math.min(100,Math.round(n*100)/100)):0}
function saveLocalHistory(d, source){
 try{
  const arr=JSON.parse(localStorage.getItem(HISTORY_KEY)||"[]");
  arr.unshift({timestamp:new Date().toISOString(),source:source||"scan",name:d.filename||"",verdict:d.verdict||"Unknown",level:d.level||"warning",ai_probability:Number(d.synthetic_probability||0),aasist_probability:Number(d.aasist_probability||0),for_probability:Number(d.for_probability||0),confidence:Number(d.confidence||0)});
  localStorage.setItem(HISTORY_KEY,JSON.stringify(arr.slice(0,250)));
 }catch(e){}
}
function fmtTime(v){try{return new Date(v).toLocaleString()}catch(e){return "—"}}
function renderHistory(items){
 const body=$("historyBody"); if(!body)return;
 if(!items.length){body.innerHTML='<tr><td colspan="8">No scan history yet.</td></tr>';return;}
 body.innerHTML=items.map(x=>`<tr><td>${fmtTime(x.timestamp)}</td><td>${String(x.source||'').replaceAll('<','&lt;')}</td><td>${String(x.name||'').replaceAll('<','&lt;')}</td><td><span class="history-pill ${x.level||'warning'}">${String(x.verdict||'Unknown')}</span></td><td>${Number(x.ai_probability||0).toFixed(1)}%</td><td>${Number(x.aasist_probability||0).toFixed(1)}%</td><td>${Number(x.for_probability||0).toFixed(1)}%</td><td>${Number(x.confidence||0).toFixed(1)}%</td></tr>`).join('');
}
async function loadHistory(){
 try{const r=await fetch('/api/history?limit=50');const d=await r.json();let items=d.items||[];
  const local=JSON.parse(localStorage.getItem(HISTORY_KEY)||"[]");
  if(!items.length && local.length) items=local;
  renderHistory(items); $('historyStatus').textContent=items.length?`${items.length} recent scan(s) · metadata only`:"No scans recorded yet";
 }catch(e){try{renderHistory(JSON.parse(localStorage.getItem(HISTORY_KEY)||"[]"));$('historyStatus').textContent='Browser history';}catch(_){} }
}
function addHistoryFromResult(d, source){saveLocalHistory(d,source);loadHistory();}
function render(d){
 if(!d||d.type!=='result')return;
 const ai=pct(d.synthetic_probability), h=pct(d.human_probability), conf=pct(d.confidence);
 const aas=pct(d.aasist_probability), fo=pct(d.for_probability);
 const fusion=pct(d.fusion_probability ?? d.synthetic_probability);
 $('verdict').textContent=d.verdict||'Analysis complete';
 $('prob').textContent=ai+'%'; $('human').textContent=h+'%'; $('ai').textContent=ai+'%'; $('confidence').textContent=conf+'%';
 $('humanBar').style.width=h+'%';$('aiBar').style.width=ai+'%';$('confidenceBar').style.width=conf+'%';
 $('gauge').style.setProperty('--p',ai+'%');
 $('aasistScore').textContent=aas+'%';$('forScore').textContent=fo+'%';$('fusionScore').textContent=fusion+'%';
 $('aasistBar').style.width=aas+'%';$('forBar').style.width=fo+'%';
 const level=d.level||'safe'; const label=level==='danger'?'HIGH RISK':level==='warning'?'REVIEW':'LOW RISK';
 $('state').textContent=label;$('state').className='risk-pill '+level;$('captureOrb').className='capture-orb '+level;
 $('resultTag').textContent=label; const rb=$('resultBanner');rb.className='result-banner '+level;$('resultBannerText').textContent=level==='danger'?'Synthetic-voice risk detected':level==='warning'?'Mixed signals — review recommended':'No strong synthetic signal detected';
 $('verdictKicker').textContent=level==='danger'?'SYNTHETIC SIGNAL DETECTED':level==='warning'?'MIXED / UNCERTAIN SIGNAL':'BONA-FIDE SIGNAL';
 $('verdictDetail').textContent=level==='danger'?'Multiple signals indicate characteristics associated with synthetic or spoofed speech.':level==='warning'?'The model signals are not sufficiently aligned. Treat this sample as suspicious and review it.':'The current signals are consistent with genuine speech.';
 $('explainTitle').textContent=level==='danger'?'Why VoiceGuard raised the alert':level==='warning'?'Why VoiceGuard marked this for review':'Why VoiceGuard marked this likely human';
 $('explainText').textContent=level==='danger'?'The calibrated FoR classifier crossed 55.5% AI-risk probability. AASIST remains visible as corroborating evidence.':level==='warning'?'The sample did not produce a reliable primary-model decision. Verify independently.':'The calibrated FoR classifier remained below the 55.5% AI-risk boundary, so VoiceGuard favors genuine speech.';
 $('pathA').textContent='AASIST '+aas+'%';$('pathF').textContent='FoR '+fo+'%';$('pathResult').textContent=label;
 const rec=$('recommendation'); rec.className='recommendation '+level;
 if(level==='danger'){ $('recommendationTitle').textContent='Do not trust the voice alone'; $('recommendationText').textContent='Treat this sample as a high-risk synthetic-voice signal. Verify the speaker using a trusted second channel before acting on requests.'; }
 else if(level==='warning'){ $('recommendationTitle').textContent='Verify before taking action'; $('recommendationText').textContent='The model signals are mixed or uncertain. Ask for an independent verification before sharing sensitive information or approving a request.'; }
 else { $('recommendationTitle').textContent='No strong synthetic signal detected'; $('recommendationText').textContent='The current evidence favors genuine speech, but a low-risk score is not proof of speaker identity.'; }
 $('verdict').parentElement.parentElement.classList.remove('scan-loading');
 $('stream').textContent='Live';$('stream').className='online-tag on';$('signalText').textContent='Streaming analysis active';
 $('activity').classList.add('active');setTimeout(()=>$('activity').classList.remove('active'),900);
}
function connect(){
 return new Promise((resolve,reject)=>{
  const proto=location.protocol==='https:'?'wss':'ws';
  ws=new WebSocket(`${proto}://${location.host}/ws/live`);ws.binaryType='arraybuffer';
  ws.onopen=()=>{setConn(true,'Live engine connected'); if(ws) ws.send(JSON.stringify({type:'config',sampleRate:ctx?.sampleRate||16000,mode:window.__vgMode||'live'})); resolve()};
  ws.onerror=()=>{setConn(false,'Engine connection error');reject(new Error('Could not connect to live engine'))};
  ws.onclose=()=>{if(running)setConn(false,'Connection interrupted — retrying')};
  ws.onmessage=e=>{if(typeof e.data==='string'){try{const d=JSON.parse(e.data);if(d.type==='result')render(d);if(d.type==='error')showError(d.message)}catch(err){}}};
 });
}
let toastTimer=null;
function showError(msg){
 const t=$('toast');$('toastTitle').textContent='Live engine';$('toastText').textContent=msg||'Something went wrong.';$('toastIcon').textContent='!';t.classList.add('show');clearTimeout(toastTimer);toastTimer=setTimeout(()=>t.classList.remove('show'),4200);
}
function setCaptureStatus(mode,text){const el=$('captureStatus'), wrap=el.parentElement;wrap.className='capture-status '+(mode||'');el.textContent=text;wrap.querySelector('small').textContent=mode==='live'?'Audio stream is active':mode==='warning'?'Connection needs attention':'Waiting for an audio source';}

function downsample(buf,inRate,outRate){if(inRate===outRate)return buf;const ratio=inRate/outRate,n=Math.round(buf.length/ratio),out=new Float32Array(n);for(let i=0;i<n;i++){const start=Math.floor(i*ratio),end=Math.min(Math.floor((i+1)*ratio),buf.length);let sum=0,c=0;for(let j=start;j<end;j++){sum+=buf[j];c++}out[i]=c?sum/c:0}return out}
function sendPCM(data){if(!ws||ws.readyState!==1)return;const ds=downsample(data,ctx.sampleRate,16000),pcm=new Int16Array(ds.length);for(let i=0;i<ds.length;i++){const v=Math.max(-1,Math.min(1,ds[i]));pcm[i]=v<0?v*32768:v*32767}ws.send(pcm.buffer)}
async function startCapture(mode){
 if(running)return;
 try{
  window.__vgMode=mode;await connect();let opts=mode==='system'?await navigator.mediaDevices.getDisplayMedia({video:true,audio:true}):await navigator.mediaDevices.getUserMedia({audio:{channelCount:1,echoCancellation:false,noiseSuppression:false,autoGainControl:false}});
  stream=opts;const track=stream.getAudioTracks()[0];if(!track)throw new Error('No audio track was shared. Select a tab/window with audio or use microphone mode.');
  ctx=new (window.AudioContext||window.webkitAudioContext)();source=ctx.createMediaStreamSource(stream);processor=ctx.createScriptProcessor(4096,1,1);
  processor.onaudioprocess=e=>sendPCM(e.inputBuffer.getChannelData(0));source.connect(processor);processor.connect(ctx.destination);running=true;
  $('micBtn').disabled=true;$('systemBtn').disabled=true;$('stopBtn').disabled=false;$('captureOrb').classList.add('live');$('modeText').textContent=mode==='system'?'Call/tab audio is being monitored':'Microphone is being monitored';setCaptureStatus('live',mode==='system'?'Monitoring call/tab':'Monitoring microphone');$('stream').textContent='Live';$('stream').className='online-tag on';setConn(true,'Monitoring live audio');track.onended=stopCapture;
 }catch(e){stopCapture();showError(e.message||'Could not start audio capture.')}
}
function stopCapture(){
 running=false;if(processor){try{processor.disconnect()}catch(e){}}if(source){try{source.disconnect()}catch(e){}}if(ctx){try{ctx.close()}catch(e){}}if(stream)stream.getTracks().forEach(t=>t.stop());if(ws){try{ws.close()}catch(e){}}
 ws=null;processor=source=ctx=stream=null;$('micBtn').disabled=false;$('systemBtn').disabled=false;$('stopBtn').disabled=true;$('captureOrb').className='capture-orb';$('modeText').textContent='Ready to monitor';setCaptureStatus('', 'Ready');$('stream').textContent='Offline';$('stream').className='online-tag';$('signalText').textContent='Awaiting audio';setConn(false,'Engine idle');
}
$('micBtn').onclick=()=>startCapture('mic');$('systemBtn').onclick=()=>startCapture('system');$('stopBtn').onclick=stopCapture;
$('file').onchange=e=>selectFile(e.target.files[0]);
function selectFile(f){
 fileBlob=f||null;if(fileBlob && !((fileBlob.type||'').toLowerCase()==='audio/wav' || /\.wav$/i.test(fileBlob.name))){fileBlob=null;showError('Please choose a WAV audio file.');}$('uploadBtn').disabled=!fileBlob;
 if(fileBlob){$('fileName').textContent=fileBlob.name;$('fileMeta').textContent=`${(fileBlob.size/1024).toFixed(1)} KB · WAV sample`;$('fileState').textContent='READY';$('fileState').style.color='var(--mint)';}
 else{$('fileName').textContent='No file selected';$('fileMeta').textContent='Choose a sample to begin';$('fileState').textContent='—';}
}
['dragenter','dragover'].forEach(ev=>$('dropzone').addEventListener(ev,e=>{e.preventDefault();$('dropzone').classList.add('drag')}));
['dragleave','drop'].forEach(ev=>$('dropzone').addEventListener(ev,e=>{e.preventDefault();$('dropzone').classList.remove('drag')}));
$('dropzone').addEventListener('drop',e=>{if(e.dataTransfer.files[0])selectFile(e.dataTransfer.files[0])});
$('uploadBtn').onclick=async()=>{
 if(!fileBlob)return;
 $('uploadBtn').disabled=true;$('uploadBtn').textContent='Analyzing…';$('fileState').textContent='SCANNING';$('fileState').style.color='var(--amber)';
 $('resultTag').textContent='ANALYZING';$('verdict').textContent='Analyzing voice…';$('verdict').parentElement.parentElement.classList.add('scan-loading');$('verdictKicker').textContent='PROCESSING';$('verdictDetail').textContent='Preprocessing audio and running the detection pipeline.';
 try{const fd=new FormData();fd.append('audio',fileBlob,fileBlob.name);const r=await fetch('/api/analyze',{method:'POST',body:fd});const d=await r.json();if(!r.ok)throw Error(d.error||'Analysis failed');render({...d,type:'result'}); addHistoryFromResult(d,'upload'); $('fileState').textContent='COMPLETE';$('fileState').style.color='var(--mint)';}
 catch(e){$('fileState').textContent='ERROR';$('fileState').style.color='var(--red)';showError(e.message)}
 finally{$('uploadBtn').disabled=!fileBlob;$('uploadBtn').innerHTML='Analyze sample <span>→</span>'}
};
$('refreshHistory').onclick=loadHistory; $('clearHistory').onclick=async()=>{if(!confirm('Clear server scan history and browser history?'))return; try{await fetch('/api/history',{method:'DELETE'});}catch(e){} localStorage.removeItem(HISTORY_KEY); loadHistory();}; loadHistory();
fetch('/api/model-status').then(r=>r.json()).then(d=>{setConn(!!d.ready,d.ready?'AI model ready':'Model loading');if(!d.ready)showError('The AI model is still loading. Try again in a moment.');}).catch(()=>setConn(false,'Engine unavailable'));
const activity=$('activity');for(let i=0;i<52;i++){const s=document.createElement('span');activity.appendChild(s)}
