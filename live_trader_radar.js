/* =====================================================================
   BTC SHADOW TRADER v1.0 — Phase 1 autonomous trading brain (NO REAL ORDERS)
   Makes every decision a live bot would make on Kalshi 15-min BTC markets —
   entries, sizing, exits, settlement — but LOGS trades instead of placing
   them. Purpose: prove positive expectancy before any dollar is at risk.

   EDGE STACK (from PANews 1.05M-trade study + our sentinel work):
     E1 late-window convergence: settlement = 60s average → progressively
        locked-in; fair value becomes near-certain while book lags
     E2 panic-liquidity capture: rest shadow bids below fair into the
        documented retail panic-exit flow (median exit 0.247)
     E3 upstream sentinel: Binance perp flow leads spot/BRTI by 30-120s;
        gates entries, triggers confirmed-reversal exits
     E4 selectivity: trade ONLY when edge > fees + cushion; stricter in the
        8:45-9:30 ET high-variance sub-window; most windows = no trade
     E5 machine risk control: hard daily stop, fixed size, consecutive-loss
        bench, no averaging down, no hope-holds

   Zero dependencies. Deploy as its own Render service:
     Start Command: node shadow_trader.js
   Endpoints: /health /selftest /status /report /log /halt?on=1|0
   ===================================================================== */
'use strict';
const http = require('http');
const fs = require('fs');
const { URL } = require('url');

const PORT = Number(process.env.PORT || 10000);
const VERSION = 'shadow-trader-1.9';
const KALSHI_BASE = (process.env.KALSHI_BASE || 'https://api.elections.kalshi.com/trade-api/v2').replace(/\/+$/,'');
const LOG_PATH = process.env.LOG_PATH || '/tmp/shadow_trades.jsonl';

/* ------------------------------ config ------------------------------ */
const CFG = {
  CONTRACTS: Number(process.env.CONTRACTS || 10),          // shadow size per trade
  DAILY_LOSS_LIMIT: Number(process.env.DAILY_LOSS_LIMIT || 200), // $ hard stop (shadow)
  MAX_CONSEC_LOSSES: Number(process.env.MAX_CONSEC_LOSSES || 4), // bench for the day
  EDGE_MIN_TAKER: Number(process.env.EDGE_MIN_TAKER || 0.06),    // fair-vs-price cushion, normal
  EDGE_MIN_TAKER_HV: Number(process.env.EDGE_MIN_TAKER_HV || 0.10), // high-variance sub-window
  MAKER_EDGE_MIN: Number(process.env.MAKER_EDGE_MIN || 0.08),    // rest bids this far below fair
  MAKER_WINDOW_S: Number(process.env.MAKER_WINDOW_S || 180),     // panic-capture active in final N s
  SENT_VETO: Number(process.env.SENT_VETO || 40),                // |perp pressure| that vetoes opposing entry
  EXIT_SENT: Number(process.env.EXIT_SENT || 30),                // adverse sentinel needed for reversal exit
  EXIT_FAIR_DROP: Number(process.env.EXIT_FAIR_DROP || 0.25),    // + fair collapse vs entry to confirm
  TAKER_FEE_K: Number(process.env.TAKER_FEE_K || 0.07),          // Kalshi taker: 0.07*P*(1-P)/contract
  MAKER_FEE: Number(process.env.MAKER_FEE || 0.003),             // $/contract maker
  MIN_TAU_ENTER: Number(process.env.MIN_TAU_ENTER || 8),         // no fresh entries in final N s
  MAX_TAU_ENTER: Number(process.env.MAX_TAU_ENTER || 600),       // ignore markets >10 min out
  TRADE_ALL_HOURS: !/^(1|true|yes)$/i.test(process.env.PRIME_ONLY||''), // 24/7 by default; PRIME_ONLY=1 restores gate
  PRIME_START: process.env.PRIME_START || '05:30',               // PT
  PRIME_END: process.env.PRIME_END || '09:00',                   // PT
  HV_START: process.env.HV_START || '05:45',                     // PT (= 8:45 ET)
  HV_END: process.env.HV_END || '06:30',                         // PT (= 9:30 ET)
};

/* ----------------------------- helpers ----------------------------- */
function clamp(x,lo,hi){const n=Number(x);return Number.isFinite(n)?Math.max(lo,Math.min(hi,n)):lo;}
function round(x,d=4){const n=Number(x);return Number.isFinite(n)?Number(n.toFixed(d)):null;}
function erf(x){const s=x<0?-1:1;x=Math.abs(x);const t=1/(1+0.3275911*x);const y=1-(((((1.061405429*t-1.453152027)*t)+1.421413741)*t-0.284496736)*t+0.254829592)*t*Math.exp(-x*x);return s*y;}
function normCdf(x){return 0.5*(1+erf(x/Math.SQRT2));}
function cors(res){res.setHeader('Access-Control-Allow-Origin','*');res.setHeader('Access-Control-Allow-Methods','GET,POST,OPTIONS');res.setHeader('Access-Control-Allow-Headers','Content-Type');res.setHeader('Cache-Control','no-store');}
function send(res,code,obj){cors(res);res.statusCode=code;res.setHeader('Content-Type','application/json; charset=utf-8');res.end(JSON.stringify(obj));}
async function fetchJson(url,timeoutMs=3500){
  const ac=new AbortController();const t=setTimeout(()=>{try{ac.abort();}catch(_){}} ,timeoutMs);
  try{const r=await fetch(url,{signal:ac.signal,headers:{accept:'application/json'}});
    if(!r.ok)throw new Error('HTTP '+r.status);return await r.json();}
  finally{clearTimeout(t);}
}
function ptClock(){ // minutes-since-midnight in America/Los_Angeles
  try{
    const p=new Intl.DateTimeFormat('en-US',{timeZone:'America/Los_Angeles',hour12:false,hour:'2-digit',minute:'2-digit'}).formatToParts(new Date());
    const h=Number(p.find(x=>x.type==='hour').value), m=Number(p.find(x=>x.type==='minute').value);
    return h*60+m;
  }catch(_){return null;}
}
const hm=s=>{const[a,b]=String(s).split(':').map(Number);return a*60+b;};
function windowState(nowMin){ // pure for tests
  if(nowMin===null)return{inPrime:true,inHV:false};
  const inPrime=nowMin>=hm(CFG.PRIME_START)&&nowMin<hm(CFG.PRIME_END);
  const inHV=nowMin>=hm(CFG.HV_START)&&nowMin<hm(CFG.HV_END);
  return {inPrime,inHV};
}
function sessionTag(nowMin){ // PT buckets for per-session P&L breakdown
  if(nowMin===null)return'unknown';
  if(nowMin<hm('05:30'))return'overnight';
  if(nowMin<hm('09:00'))return'prime';
  if(nowMin<hm('13:00'))return'midday';
  return'evening';
}


/* --------------------- fees (Kalshi model) --------------------- */
function takerFee(price,qty){return CFG.TAKER_FEE_K*price*(1-price)*qty;}
function makerFee(qty){return CFG.MAKER_FEE*qty;}

/* --------------- BRTI proxy tape (Coinbase/Kraken/Bitstamp) --------------- */
const TAPE=[]; // [ts, price] rolling ~20 min
let lastTapeErr=null;
async function pollSpot(){
  const now=Date.now();
  const [cb,kr,bs]=await Promise.all([
    fetchJson('https://api.exchange.coinbase.com/products/BTC-USD/ticker').then(j=>Number(j.price)).catch(()=>null),
    fetchJson('https://api.kraken.com/0/public/Ticker?pair=XBTUSD').then(j=>{const k=Object.keys(j.result||{})[0];return k?Number(j.result[k].c[0]):null;}).catch(()=>null),
    fetchJson('https://www.bitstamp.net/api/v2/ticker/btcusd/').then(j=>Number(j.last)).catch(()=>null)
  ]);
  const vals=[cb,kr,bs].filter(v=>Number.isFinite(v)&&v>0).sort((a,b)=>a-b);
  if(!vals.length){lastTapeErr='no spot venue reachable';return;}
  const px=vals[Math.floor(vals.length/2)]; // median
  TAPE.push([now,px]);
  const cut=now-20*60*1000; while(TAPE.length&&TAPE[0][0]<cut)TAPE.shift();
  lastTapeErr=null;
}
function tapeNow(){return TAPE.length?TAPE[TAPE.length-1][1]:null;}
function tapeVolBps(){ // realized vol in bps/sqrt-sec from last ~5 min
  const cut=Date.now()-300000; const w=TAPE.filter(t=>t[0]>=cut);
  if(w.length<10)return 0.45;
  const r=[]; for(let i=1;i<w.length;i++){const dt=Math.max(0.5,(w[i][0]-w[i-1][0])/1000);
    const v=(w[i][1]-w[i-1][1])/w[i-1][1]*1e4/Math.sqrt(dt); if(Number.isFinite(v))r.push(v);}
  if(r.length<5)return 0.45;
  const m=r.reduce((a,b)=>a+b,0)/r.length;
  return clamp(Math.sqrt(r.reduce((a,b)=>a+(b-m)*(b-m),0)/(r.length-1)),0.12,4);
}
function tapeDrift(){ // bps/sec EWMA over last 90s
  const cut=Date.now()-90000; const w=TAPE.filter(t=>t[0]>=cut);
  if(w.length<6)return 0;
  let num=0,den=0;const now=w[w.length-1][0];
  for(let i=1;i<w.length;i++){const dt=Math.max(0.5,(w[i][0]-w[i-1][0])/1000);
    const r=(w[i][1]-w[i-1][1])/w[i-1][1]*1e4/dt;const age=(now-w[i][0])/1000;const wt=Math.pow(0.5,age/25);
    if(Number.isFinite(r)){num+=wt*r;den+=wt;}}
  return den?clamp(num/den,-3,3):0;
}
function tapeLastAt(ts){ // last proxy print at or just before ts
  for(let i=TAPE.length-1;i>=0;i--){if(TAPE[i][0]<=ts+1500)return TAPE[i][1];}
  return null;
}
function tapeAvg(fromTs,toTs){ // time-weighted avg over [fromTs,toTs]
  const w=TAPE.filter(t=>t[0]>=fromTs-3000&&t[0]<=toTs+1000);
  if(w.length<2)return null;
  let sum=0,dur=0;
  for(let i=1;i<w.length;i++){const dt=(w[i][0]-w[i-1][0])/1000;sum+=w[i-1][1]*dt;dur+=dt;}
  return dur>0?sum/dur:null;
}

/* --------------- upstream sentinel (Binance perp, compact) --------------- */
function ewmaZ(a){let m=null,v=null;return{update(x){if(m===null){m=x;v=1e-9;return 0;}const d=x-m;m+=a*d;v=(1-a)*(v+a*d*d);return d/Math.sqrt(Math.max(v,1e-9));}};}
const z2s=z=>clamp(z/3.5,-1,1)*100;
const SENT={started:false,lastOk:0,lastAggId:null,trades:[],depthHist:[],curDepth:{bid:0,ask:0},perpMid:null,spotMid:null,basisEwma:null,
  z:{div:ewmaZ(0.03),burst:ewmaZ(0.03),basis:ewmaZ(0.03)},read:{ok:false,error:'warming up',pressure:0}};
function sentCompute(){
  const now=Date.now();
  const cT=now-90000;while(SENT.trades.length&&SENT.trades[0][0]<cT)SENT.trades.shift();
  const cD=now-300000;while(SENT.depthHist.length&&SENT.depthHist[0][0]<cD)SENT.depthHist.shift();
  if(SENT.trades.length<10||SENT.depthHist.length<8||!Number.isFinite(SENT.perpMid))
    return{ok:false,error:'warming up',pressure:0};
  let net=0;for(const t of SENT.trades)net+=t[1];
  const p0=SENT.trades[0][2],p1=SENT.trades[SENT.trades.length-1][2];
  const div=net/1e6-((p1-p0)/p0)*20000;
  const cvdDiv=z2s(SENT.z.div.update(div));
  let b30=0;const c30=now-30000;
  for(let i=SENT.trades.length-1;i>=0&&SENT.trades[i][0]>=c30;i--)b30+=SENT.trades[i][1];
  const burst=z2s(SENT.z.burst.update(b30/1e6));
  const med=a=>{const b=[...a].sort((x,y)=>x-y);return b[Math.floor(b.length/2)]||1e-6;};
  const bidR=SENT.curDepth.bid/Math.max(med(SENT.depthHist.map(d=>d[1])),1e-6);
  const askR=SENT.curDepth.ask/Math.max(med(SENT.depthHist.map(d=>d[2])),1e-6);
  const pull=clamp((bidR-askR)*100,-100,100);
  let basisS=0;
  if(Number.isFinite(SENT.spotMid)){
    const basis=SENT.perpMid-SENT.spotMid;
    if(SENT.basisEwma===null)SENT.basisEwma=basis;
    SENT.basisEwma+=0.05*(basis-SENT.basisEwma);
    basisS=z2s(SENT.z.basis.update(basis-SENT.basisEwma));
  }
  const pressure=clamp(0.35*cvdDiv+0.20*burst+0.30*pull+0.15*basisS,-100,100);
  const stale=(now-SENT.lastOk)>12000;
  return{ok:!stale,error:stale?'stale':null,pressure:Math.round(pressure),
    components:{cvdDiv:Math.round(cvdDiv),burst:Math.round(burst),bookPull:Math.round(pull),basis:Math.round(basisS)}};
}
async function sentPoll(){
  const now=Date.now();
  let any=false;
  try{
    if((SENT.failN||0)<3){ // primary: Binance perp (leads spot) — geo-blocked from some US hosts
      const aggUrl='https://fapi.binance.com/fapi/v1/aggTrades?symbol=BTCUSDT'+(SENT.lastAggId?('&fromId='+(SENT.lastAggId+1)+'&limit=500'):'&limit=300');
      const[trades,depth,pBT,sBT]=await Promise.all([
        fetchJson(aggUrl).catch(()=>null),
        fetchJson('https://fapi.binance.com/fapi/v1/depth?symbol=BTCUSDT&limit=10').catch(()=>null),
        fetchJson('https://fapi.binance.com/fapi/v1/ticker/bookTicker?symbol=BTCUSDT').catch(()=>null),
        fetchJson('https://api.binance.com/api/v3/ticker/bookTicker?symbol=BTCUSDT').catch(()=>null)
      ]);
      if(Array.isArray(trades)){for(const t of trades){const p=+t.p,q=+t.q;if(!Number.isFinite(p)||!Number.isFinite(q))continue;
        SENT.trades.push([+t.T||now,(t.m?-1:1)*p*q,p]);SENT.lastAggId=Math.max(SENT.lastAggId||0,+t.a||0);}any=true;}
      if(depth&&Array.isArray(depth.bids)){const s=x=>x.reduce((a,y)=>a+(+y[1]||0),0);
        SENT.curDepth={bid:s(depth.bids),ask:s(depth.asks)};SENT.depthHist.push([now,SENT.curDepth.bid,SENT.curDepth.ask]);any=true;}
      if(pBT&&pBT.bidPrice)SENT.perpMid=(+pBT.bidPrice+ +pBT.askPrice)/2;
      if(sBT&&sBT.bidPrice)SENT.spotMid=(+sBT.bidPrice+ +sBT.askPrice)/2;
      if(any){SENT.failN=0;SENT.venue='binance-perp';}
      else SENT.failN=(SENT.failN||0)+1;
    }
    if(!any&&(SENT.failN||0)>=3){ // fallback: Coinbase spot flow (always reachable from US)
      if(SENT.venue!=='coinbase-spot'){SENT.lastAggId=null;SENT.trades.length=0;SENT.venue='coinbase-spot';}
      const[trades,book]=await Promise.all([
        fetchJson('https://api.exchange.coinbase.com/products/BTC-USD/trades?limit=100').catch(()=>null),
        fetchJson('https://api.exchange.coinbase.com/products/BTC-USD/book?level=2').catch(()=>null)
      ]);
      if(Array.isArray(trades)){
        for(const t of trades){const p=+t.price,q=+t.size,id=+t.trade_id;
          if(!Number.isFinite(p)||!Number.isFinite(q))continue;
          if(SENT.lastAggId&&Number.isFinite(id)&&id<=SENT.lastAggId)continue;
          const signed=(t.side==='sell'?1:-1)*p*q; // maker sold => taker BOUGHT
          SENT.trades.push([Date.parse(t.time)||now,signed,p]);
          if(Number.isFinite(id))SENT.lastAggId=Math.max(SENT.lastAggId||0,id);}
        any=true;
      }
      if(book&&Array.isArray(book.bids)&&Array.isArray(book.asks)){
        const bb=+((book.bids[0]||[])[0]),ba=+((book.asks[0]||[])[0]);
        if(Number.isFinite(bb)&&Number.isFinite(ba)){
          const mid=(bb+ba)/2,band=mid*0.0006;let bd=0,ad=0;
          for(const b of book.bids){const p=+b[0],sz=+b[1];if(mid-p<=band)bd+=sz;else break;}
          for(const a of book.asks){const p=+a[0],sz=+a[1];if(p-mid<=band)ad+=sz;else break;}
          SENT.curDepth={bid:bd,ask:ad};SENT.depthHist.push([now,bd,ad]);
          SENT.perpMid=mid;SENT.spotMid=mid;any=true;
        }
      }
    }
    if(any)SENT.lastOk=now;
  }catch(_){}
  SENT.read=sentCompute();
  if(SENT.read)SENT.read.venue=SENT.venue||null;
}
function ensureSentinel(){if(SENT.started)return;SENT.started=true;sentPoll();const t=setInterval(sentPoll,2500);if(t.unref)t.unref();}

/* --------------------- Kalshi market discovery --------------------- */
let mktCache={t:0,data:null};
const DISC={ts:0,err:null,totalMarkets:0,btcCount:0,nearestCloseSec:null,picked:null};
function parseStrike(m){
  for(const c of[m.floor_strike,m.cap_strike,m.strike]){const n=Number(c);if(Number.isFinite(n)&&n>0)return n;}
  const tail=String(m.ticker||'').split('-').pop()||'';const n=Number(tail.replace(/[^0-9.]/g,''));
  return Number.isFinite(n)&&n>0?n:NaN;
}
function closeMs(m){ // Kalshi markets carry close_ts (sec) OR ISO close_time depending on series
  const s=Number(m.close_ts);if(Number.isFinite(s)&&s>0)return s*1000;
  for(const f of[m.close_time,m.expected_expiration_time,m.expiration_time]){
    const t=Date.parse(f||'');if(Number.isFinite(t))return t;}
  return 0;
}
async function discoverMarket(refPrice){
  const now=Date.now();
  const cacheMs=(DISC.err&&/429/.test(DISC.err))?25000:8000; // back off when rate-limited
  if(mktCache.data&&now-mktCache.t<cacheMs)return mktCache.data;
  if(!mktCache.data&&now-mktCache.t<cacheMs&&mktCache.t>0)return null;
  const s=Math.floor(now/1000);
  // primary: exact series query — small, precise, cheap
  let j=await fetchJson(KALSHI_BASE+'/markets?series_ticker=KXBTC15M&status=open&limit=20')
    .catch(e=>({__err:String((e&&e.message)||e)}));
  let via='series';
  if(!j||j.__err||!Array.isArray(j.markets)||!j.markets.length){
    const firstErr=j&&j.__err?j.__err:null;
    // fallback: broad time-windowed scan (original method)
    j=await fetchJson(KALSHI_BASE+'/markets?status=open&limit=200&min_close_ts='+s+'&max_close_ts='+(s+16*60))
      .catch(e=>({__err:String((e&&e.message)||e)}));
    via='broad'+(firstErr?(' (series: '+firstErr+')'):'');
  }
  DISC.ts=now;
  DISC.via=via;
  DISC.err=j&&j.__err?j.__err:(j?null:'null response');
  const all=Array.isArray(j&&j.markets)?j.markets:[];
  DISC.totalMarkets=all.length;
  const btc=all.filter(m=>/BTC/i.test(String(m.ticker||'')+' '+String(m.title||'')))
    .map(m=>({m,c:closeMs(m)})).filter(x=>x.c>now+3000);
  DISC.btcCount=btc.length;
  DISC.nearestCloseSec=btc.length?Math.round((Math.min(...btc.map(x=>x.c))-now)/1000):null;
  if(!btc.length){DISC.picked=null;mktCache={t:now,data:null};return null;}
  btc.sort((a,b)=>a.c-b.c);
  const firstClose=btc[0].c;
  let win=btc.filter(x=>x.c===firstClose).map(x=>x.m);
  if(!win.length){mktCache={t:now,data:null};return null;}
  if(Number.isFinite(refPrice))win.sort((a,b)=>Math.abs(parseStrike(a)-refPrice)-Math.abs(parseStrike(b)-refPrice));
  const m=win[0];
  const c2=v=>{const n=Number(v);return Number.isFinite(n)&&n>0&&n<100?n/100:null;};
  const data={ticker:m.ticker,strike:parseStrike(m),closeTs:firstClose,title:m.title||'',
    quotes:{yesBid:c2(m.yes_bid),yesAsk:c2(m.yes_ask),noBid:c2(m.no_bid),noAsk:c2(m.no_ask)}};
  DISC.picked=data.ticker;
  mktCache={t:now,data};return data;
}
let obCache={t:0,ticker:'',data:null};
function normalizeBook(j){ // Kalshi ships two shapes: orderbook_fp (dollar strings) or legacy orderbook (cents)
  const fp=j&&j.orderbook_fp, legacy=j&&j.orderbook;
  const src=fp||legacy; if(!src)return null;
  const norm=a=>(Array.isArray(a)?a:[]).filter(x=>Array.isArray(x)&&x.length>=2)
    .map(x=>[Number(x[0])/(fp?1:100),Number(x[1])])
    .filter(x=>Number.isFinite(x[0])&&x[0]>0&&x[0]<1&&Number.isFinite(x[1]));
  return {yes:norm(fp?src.yes_dollars:src.yes), no:norm(fp?src.no_dollars:src.no)};
}
async function getBook(ticker,fallbackQuotes){
  const now=Date.now();
  if(obCache.data&&obCache.ticker===ticker&&now-obCache.t<1500)return obCache.data;
  const j=await fetchJson(KALSHI_BASE+'/markets/'+encodeURIComponent(ticker)+'/orderbook?depth=10').catch(()=>null);
  const nb=normalizeBook(j);
  const yes=nb?nb.yes:[], no=nb?nb.no:[];
  const bestYesBid=yes.length?Math.max(...yes.map(x=>x[0])):null;
  const bestNoBid=no.length?Math.max(...no.map(x=>x[0])):null;
  let data={yesBid:bestYesBid, yesAsk:bestNoBid!==null?round(1-bestNoBid,2):null,
    noBid:bestNoBid, noAsk:bestYesBid!==null?round(1-bestYesBid,2):null,
    yesDepth:yes.reduce((a,x)=>a+x[1],0), noDepth:no.reduce((a,x)=>a+x[1],0), source:'orderbook'};
  const empty=data.yesBid===null&&data.yesAsk===null&&data.noBid===null;
  if(empty&&fallbackQuotes&&(fallbackQuotes.yesBid!==null||fallbackQuotes.yesAsk!==null)){
    data={yesBid:fallbackQuotes.yesBid,yesAsk:fallbackQuotes.yesAsk,
      noBid:fallbackQuotes.noBid,noAsk:fallbackQuotes.noAsk,yesDepth:0,noDepth:0,source:'listing'};
  }else if(empty){data.source='none';}
  obCache={t:now,ticker,data};return data;
}

/* --------------------- fair value engine (E1 core) --------------------- */
/* P(settlement avg over final 60s > strike).
   tau > 60: terminal distn of the average; effective horizon = (tau-60)+20s
             (var of a 60s BM average adds T/3 = 20s of variance).
   tau <= 60: locked-average math. S=(sumKnown + mFuture*r)/60.
             Need mFuture > (60K - sumKnown)/r. mFuture ~ N(p + drift*r/2, sig^2*r/3). */
function computeFair(o){
  const {price,strike,tauSec,volBps,driftBps,knownAvg,knownDur}=o;
  if(!Number.isFinite(price)||!Number.isFinite(strike))return null;
  const sigUsdPerSqrtSec=(volBps/1e4)*price;
  const driftUsdPerSec=(driftBps/1e4)*price;
  let mean,sd;
  if(tauSec>60){
    const h=(tauSec-60)+20;
    mean=price+driftUsdPerSec*Math.min(tauSec,120)*0.5; // damped drift projection
    sd=Math.max(1e-6,sigUsdPerSqrtSec*Math.sqrt(h));
    return clamp(1-normCdf((strike-mean)/sd),0.005,0.995);
  }
  const r=Math.max(0.5,tauSec);
  const e=clamp(Number.isFinite(knownDur)?knownDur:60-r,0,60-r+0.01)||Math.max(0,60-r);
  const kAvg=Number.isFinite(knownAvg)?knownAvg:price;
  const sumKnown=kAvg*e;
  const reqFutureMean=(60*strike-sumKnown)/r;
  mean=price+driftUsdPerSec*r*0.5;
  sd=Math.max(1e-6,sigUsdPerSqrtSec*Math.sqrt(r/3));
  return clamp(1-normCdf((reqFutureMean-mean)/sd),0.001,0.999);
}

/* --------------------- decision engine (E2-E4) --------------------- */
function decideEntry(o){
  const {fair,book,tauSec,inHV,sentPressure,haveOpen,ticker,lockout}=o;
  if(haveOpen)return{action:'NONE',reason:'position open'};
  if(!book||fair===null)return{action:'NONE',reason:'no data'};
  if(tauSec<CFG.MIN_TAU_ENTER)return{action:'NONE',reason:'too close to expiry'};
  const locked=lockout&&ticker&&lockout.ticker===ticker;
  const vetoAt=tauSec<=300?25:CFG.SENT_VETO; // late window: respect upstream flow harder
  const edgeMin=inHV?CFG.EDGE_MIN_TAKER_HV:CFG.EDGE_MIN_TAKER;
  // taker YES
  if(Number.isFinite(book.yesAsk)&&book.yesAsk>0.02&&book.yesAsk<0.98){
    const gross=fair-book.yesAsk;
    const net=gross-takerFee(book.yesAsk,1);
    if(net>=edgeMin){
      if(locked&&lockout.side==='YES')return{action:'NONE',reason:'reversal lockout (YES) this window'};
      if(sentPressure<=-vetoAt)return{action:'NONE',reason:'YES edge but perp pressure down (veto @'+vetoAt+')'};
      return{action:'BUY_YES',mode:'taker',px:book.yesAsk,fair,netEdge:round(net,3),reason:'fair '+round(fair,3)+' vs ask '+book.yesAsk};
    }
  }
  // taker NO
  if(Number.isFinite(book.noAsk)&&book.noAsk>0.02&&book.noAsk<0.98){
    const gross=(1-fair)-book.noAsk;
    const net=gross-takerFee(book.noAsk,1);
    if(net>=edgeMin){
      if(locked&&lockout.side==='NO')return{action:'NONE',reason:'reversal lockout (NO) this window'};
      if(sentPressure>=vetoAt)return{action:'NONE',reason:'NO edge but perp pressure up (veto @'+vetoAt+')'};
      return{action:'BUY_NO',mode:'taker',px:book.noAsk,fair,netEdge:round(net,3),reason:'fair(no) '+round(1-fair,3)+' vs ask '+book.noAsk};
    }
  }
  // maker panic-capture (final window only): rest a YES bid well below fair
  if(tauSec<=CFG.MAKER_WINDOW_S&&fair>=0.35&&fair<=0.9&&!(locked&&lockout.side==='YES')){
    const bid=round(Math.max(0.02,fair-CFG.MAKER_EDGE_MIN),2);
    if(Number.isFinite(book.yesAsk)&&bid<book.yesAsk)
      return{action:'POST_YES_BID',mode:'maker',px:bid,fair,netEdge:round(fair-bid-CFG.MAKER_FEE,3),reason:'panic-capture bid '+bid+' vs fair '+round(fair,3)};
  }
  return{action:'NONE',reason:'no edge ≥ '+edgeMin};
}
function decideExit(o){ // firm stay-in: exit ONLY on confirmed reversal
  const {pos,fair,sentPressure,tauSec}=o;
  if(!pos)return{exit:false};
  const adverse=pos.side==='YES'?(sentPressure<=-CFG.EXIT_SENT):(sentPressure>=CFG.EXIT_SENT);
  const posFair=pos.side==='YES'?fair:1-fair;
  const collapsed=(pos.entryFair-posFair)>=CFG.EXIT_FAIR_DROP;
  if(adverse&&collapsed&&tauSec>3)return{exit:true,reason:'confirmed reversal: perp '+sentPressure+', fair '+round(pos.entryFair,2)+'→'+round(posFair,2)};
  return{exit:false};
}

/* --------------------- risk cage (E5) --------------------- */
function makeCage(){
  return{
    day:null,realized:0,consecLosses:0,manualHalt:false,
    roll(){const d=new Date().toISOString().slice(0,10);if(d!==this.day){this.day=d;this.realized=0;this.consecLosses=0;}},
    record(pnl){this.roll();this.realized+=pnl;if(pnl<0)this.consecLosses++;else if(pnl>0)this.consecLosses=0;},
    halted(){this.roll();
      if(this.manualHalt)return'manual halt';
      if(this.realized<=-Math.abs(CFG.DAILY_LOSS_LIMIT))return'daily loss limit';
      if(this.consecLosses>=CFG.MAX_CONSEC_LOSSES)return'consecutive losses';
      return null;}
  };
}
const cage=makeCage();

/* --------------------- shadow book-keeping --------------------- */
const STATE={pos:null,pendingMaker:null,lastReversal:null,trades:[],reconcile:[],lastStatus:null,lastErr:null,ticks:0};
function logLine(obj){try{fs.appendFileSync(LOG_PATH,JSON.stringify(obj)+'\n');}catch(_){}}
function openPos(mkt,side,mode,px,fair,tauSec){
  const qty=STATE.inHV?Math.max(1,Math.floor(CFG.CONTRACTS/2)):CFG.CONTRACTS;
  const fees=mode==='taker'?takerFee(px,qty):makerFee(qty);
  STATE.pos={ticker:mkt.ticker,strike:mkt.strike,closeTs:mkt.closeTs,side,mode,px,qty,fees,
    entryFair:side==='YES'?fair:1-fair,entryTs:Date.now(),entryTau:tauSec,session:sessionTag(ptClock())};
  logLine({ev:'OPEN',...STATE.pos});
}
function closePos(reason,exitPx,settled,won,extra){
  const p=STATE.pos;if(!p)return;
  let pnl;
  if(settled){pnl=p.qty*((won?1:0)-p.px)-p.fees;}
  else{const fee=takerFee(exitPx,p.qty);pnl=p.qty*(exitPx-p.px)-p.fees-fee;}
  const rec={ev:'CLOSE',ticker:p.ticker,side:p.side,mode:p.mode,entryPx:p.px,exitPx:settled?(won?1:0):exitPx,
    qty:p.qty,pnl:round(pnl,2),reason,settled:!!settled,entryFair:round(p.entryFair,3),
    entryTau:p.entryTau,session:p.session||'unknown',ts:Date.now(),...(extra||{})};
  STATE.trades.push(rec);cage.record(pnl);logLine(rec);
  if(settled)STATE.reconcile.push({ticker:p.ticker,ourWin:won,side:p.side,checkedAt:0});
  else STATE.lastReversal={ticker:p.ticker,side:p.side,ts:Date.now()};
  STATE.pos=null;
}

/* --------------------- main loop --------------------- */
async function tick(){
  STATE.ticks++;
  await pollSpot().catch(()=>{});
  ensureSentinel();
  const price=tapeNow();
  const sent=SENT.read||{ok:false,pressure:0};
  const nowMin=ptClock();
  const w=windowState(nowMin);STATE.inHV=w.inHV;
  const haltReason=cage.halted();
  let mkt=null,book=null,fair=null,tauSec=null,decision={action:'NONE',reason:'idle'};
  try{
    mkt=await discoverMarket(price);
    // settle any expired position FIRST — never depends on discovery succeeding
    if(STATE.pos&&Date.now()>STATE.pos.closeTs){
      const avg=tapeAvg(STATE.pos.closeTs-60000,STATE.pos.closeTs);
      const lastPx=tapeLastAt(STATE.pos.closeTs);
      const won=avg!==null?(STATE.pos.side==='YES'?avg>STATE.pos.strike:avg<=STATE.pos.strike):null;
      closePos('settlement (proxy avg '+round(avg,2)+')',null,true,!!won,
        {settleAvg60:round(avg,2),settleLast:round(lastPx,2),
         margin:avg!==null?round(avg-STATE.pos.strike,2):null,strike:STATE.pos.strike});
    }
    if(STATE.lastReversal&&mkt&&STATE.lastReversal.ticker!==mkt.ticker)STATE.lastReversal=null;
    // cancel a resting shadow bid the moment its window rolls over
    if(STATE.pendingMaker&&(!mkt||STATE.pendingMaker.ticker!==mkt.ticker)){
      logLine({ev:'MAKER_CANCEL',ticker:STATE.pendingMaker.ticker,why:'window rolled'});
      STATE.pendingMaker=null;
    }
    if(mkt&&Number.isFinite(mkt.strike)){
      tauSec=(mkt.closeTs-Date.now())/1000;
      book=await getBook(mkt.ticker,mkt.quotes);
      const avgStart=mkt.closeTs-60000;
      const knownDur=clamp((Date.now()-avgStart)/1000,0,60);
      const knownAvg=knownDur>1?tapeAvg(avgStart,Date.now()):null;
      fair=computeFair({price,strike:mkt.strike,tauSec,volBps:tapeVolBps(),driftBps:tapeDrift(),knownAvg,knownDur});
      // maker fill check
      if(STATE.pendingMaker&&STATE.pendingMaker.ticker===mkt.ticker){
        const pm=STATE.pendingMaker;
        if(tauSec<8||Math.abs((fair??0)-pm.fairAtPost)>0.12){STATE.pendingMaker=null;logLine({ev:'MAKER_CANCEL',ticker:pm.ticker,why:'stale/fair moved'});}
        else if(Number.isFinite(book.yesAsk)&&book.yesAsk<=pm.px){ // panic seller crossed into us
          STATE.pendingMaker=null;openPos(mkt,'YES','maker',pm.px,fair,tauSec);
        }
      }
      // exits (never gated by halt — always allowed to reduce risk)
      if(STATE.pos&&STATE.pos.ticker===mkt.ticker&&tauSec>0){
        const ex=decideExit({pos:STATE.pos,fair,sentPressure:sent.pressure||0,tauSec});
        if(ex.exit){
          const px=STATE.pos.side==='YES'?(book.yesBid??Math.max(0.01,fair-0.03)):(book.noBid??Math.max(0.01,1-fair-0.03));
          closePos(ex.reason,px,false,null);
        }
      }
      // entries
      const gated=haltReason?('halted: '+haltReason):((!CFG.TRADE_ALL_HOURS&&!w.inPrime)?'outside prime window':(!sent.ok&&tauSec<180?'sentinel warming (late-window entries blocked)':null));
      if(!gated&&tauSec>0&&tauSec<=CFG.MAX_TAU_ENTER&&!STATE.pendingMaker){
        decision=decideEntry({fair,book,tauSec,inHV:w.inHV,sentPressure:sent.pressure||0,haveOpen:!!STATE.pos,ticker:mkt.ticker,lockout:STATE.lastReversal});
        if(decision.action==='BUY_YES')openPos(mkt,'YES','taker',decision.px,fair,tauSec);
        else if(decision.action==='BUY_NO')openPos(mkt,'NO','taker',decision.px,fair,tauSec);
        else if(decision.action==='POST_YES_BID'){STATE.pendingMaker={ticker:mkt.ticker,px:decision.px,fairAtPost:fair,ts:Date.now()};logLine({ev:'MAKER_POST',ticker:mkt.ticker,px:decision.px,fair:round(fair,3)});}
      }else if(gated){decision={action:'NONE',reason:gated};}
    }
    STATE.lastErr=null;
  }catch(e){STATE.lastErr=String(e.message||e);}
  // reconcile vs actual Kalshi results (ground truth)
  const rc=STATE.reconcile.find(r=>Date.now()-r.checkedAt>30000);
  if(rc){rc.checkedAt=Date.now();
    fetchJson(KALSHI_BASE+'/markets/'+encodeURIComponent(rc.ticker)).then(j=>{
      const result=j&&j.market&&j.market.result;
      if(result==='yes'||result==='no'){
        const actualWin=rc.side==='YES'?result==='yes':result==='no';
        const match=actualWin===rc.ourWin;
        logLine({ev:'RECONCILE',ticker:rc.ticker,kalshiResult:result,ourWin:rc.ourWin,match});
        let rec=null;
        for(let i=STATE.trades.length-1;i>=0;i--){if(STATE.trades[i].ticker===rc.ticker&&STATE.trades[i].settled){rec=STATE.trades[i];break;}}
        if(rec){rec.kalshiResult=result;
          if(!match){const np=truthPnl(rec,actualWin);
            logLine({ev:'CORRECTION',ticker:rc.ticker,oldPnl:rec.pnl,newPnl:np,margin:rec.margin??null});
            rec.pnlOriginal=rec.pnl;rec.pnl=np;rec.corrected=true;}}
        STATE.reconcile=STATE.reconcile.filter(x=>x!==rc);
      }
    }).catch(()=>{});
  }
  STATE.lastStatus={ts:Date.now(),price:round(price,2),market:mkt?{ticker:mkt.ticker,strike:mkt.strike,tauSec:round(tauSec,0)}:null,
    discovery:{...DISC},
    book,fair:fair===null?null:round(fair,3),sentinel:{ok:sent.ok,pressure:sent.pressure||0,venue:sent.venue||null},
    volBps:round(tapeVolBps(),3),driftBps:round(tapeDrift(),4),
    window:{inPrime:w.inPrime,inHV:w.inHV},halt:haltReason,decision,
    position:STATE.pos?{ticker:STATE.pos.ticker,side:STATE.pos.side,px:STATE.pos.px,qty:STATE.pos.qty,mode:STATE.pos.mode}:null,
    pendingMaker:STATE.pendingMaker?{px:STATE.pendingMaker.px}:null,
    tapeErr:lastTapeErr,err:STATE.lastErr};
}

function truthPnl(rec,actualWin){ // recompute a settled trade's P&L from Kalshi's official result
  const fee=rec.mode==='taker'?takerFee(rec.entryPx,rec.qty):CFG.MAKER_FEE*rec.qty;
  return Math.round((rec.qty*((actualWin?1:0)-rec.entryPx)-fee)*100)/100;
}

/* --------------------- reporting --------------------- */
function report(){
  const t=STATE.trades;
  const n=t.length,wins=t.filter(x=>x.pnl>0).length;
  const pnl=t.reduce((a,x)=>a+x.pnl,0);
  const settledN=t.filter(x=>x.settled).length;
  const byMode={};
  for(const x of t){byMode[x.mode]=byMode[x.mode]||{n:0,pnl:0};byMode[x.mode].n++;byMode[x.mode].pnl=round(byMode[x.mode].pnl+x.pnl,2);}
  const bySession={};
  for(const x of t){const s=x.session||'unknown';bySession[s]=bySession[s]||{n:0,wins:0,pnl:0};
    bySession[s].n++;if(x.pnl>0)bySession[s].wins++;bySession[s].pnl=round(bySession[s].pnl+x.pnl,2);}
  return{version:VERSION,mode:'24/7'+(CFG.TRADE_ALL_HOURS?'':' (PRIME_ONLY)'),
    trades:n,wins,winRate:n?round(wins/n,3):null,totalPnl:round(pnl,2),
    avgPnlPerTrade:n?round(pnl/n,2):null,settled:settledN,reversalExits:n-settledN,corrections:t.filter(x=>x.corrected).length,
    byMode,bySession,todayRealized:round(cage.realized,2),consecLosses:cage.consecLosses,halt:cage.halted(),
    last10:t.slice(-10)};
}

/* --------------------- self-test (pure, offline) --------------------- */
function runSelfTest(){
  const C=[];
  // 1-2: locked-average convergence extremes
  const w1=computeFair({price:62200,strike:62050,tauSec:10,volBps:0.6,driftBps:0,knownAvg:62200,knownDur:50});
  C.push({name:'locked avg: near-certain win → fair>0.99',pass:w1>0.99,got:round(w1,4)});
  const l1=computeFair({price:61900,strike:62050,tauSec:10,volBps:0.6,driftBps:0,knownAvg:61900,knownDur:50});
  C.push({name:'locked avg: near-certain loss → fair<0.01',pass:l1<0.01,got:round(l1,4)});
  // 3: mid-window fair is sane
  const m1=computeFair({price:62050,strike:62050,tauSec:300,volBps:0.6,driftBps:0,knownAvg:null,knownDur:0});
  C.push({name:'ATM mid-window → fair≈0.5',pass:m1>0.4&&m1<0.6,got:round(m1,3)});
  // 4: taker fee model
  const f=takerFee(0.5,1);
  C.push({name:'taker fee @0.50 = 0.0175',pass:Math.abs(f-0.0175)<1e-9,got:round(f,4)});
  // 5: convergence edge fires BUY_YES
  const d1=decideEntry({fair:0.96,book:{yesAsk:0.84,noAsk:0.2,yesBid:0.8,noBid:0.14},tauSec:40,inHV:false,sentPressure:0,haveOpen:false});
  C.push({name:'fair .96 vs ask .84 → BUY_YES',pass:d1.action==='BUY_YES',got:d1.action+' '+(d1.netEdge??'')});
  // 6: no trade when edge under cushion
  const d2=decideEntry({fair:0.87,book:{yesAsk:0.84,noAsk:0.2,yesBid:0.8,noBid:0.14},tauSec:400,inHV:false,sentPressure:0,haveOpen:false});
  C.push({name:'thin edge → no trade (selectivity)',pass:d2.action==='NONE',got:d2.action});
  // 7: HV window demands more edge
  const d3=decideEntry({fair:0.92,book:{yesAsk:0.84,noAsk:0.2,yesBid:0.8,noBid:0.14},tauSec:40,inHV:true,sentPressure:0,haveOpen:false});
  C.push({name:'same edge blocked in high-variance window',pass:d3.action==='NONE',got:d3.action});
  // 8: sentinel vetoes entry against upstream flow
  const d4=decideEntry({fair:0.96,book:{yesAsk:0.84,noAsk:0.2,yesBid:0.8,noBid:0.14},tauSec:40,inHV:false,sentPressure:-55,haveOpen:false});
  C.push({name:'perp pressure down vetoes YES buy',pass:d4.action==='NONE',got:d4.action});
  // 9: panic-capture maker bid posted late-window
  const d5=decideEntry({fair:0.62,book:{yesAsk:0.6,noAsk:0.5,yesBid:0.42,noBid:0.4},tauSec:120,inHV:false,sentPressure:0,haveOpen:false});
  C.push({name:'late window → panic-capture bid below fair',pass:d5.action==='POST_YES_BID'&&d5.px<0.62,got:d5.action+' @'+d5.px});
  // 10: reversal exit needs BOTH adverse perp AND fair collapse
  const posA={side:'YES',entryFair:0.9};
  const eA=decideExit({pos:posA,fair:0.6,sentPressure:-45,tauSec:60});
  const eB=decideExit({pos:posA,fair:0.85,sentPressure:-45,tauSec:60});
  C.push({name:'confirmed reversal exits; wiggle does not',pass:eA.exit===true&&eB.exit===false,got:eA.exit+'/'+eB.exit});
  // 11-12: risk cage
  const cg=makeCage();cg.record(-30);cg.record(-30);cg.record(-30);cg.record(-30);
  C.push({name:'cage: 4 consec losses → halted',pass:cg.halted()==='consecutive losses',got:String(cg.halted())});
  const cg2=makeCage();cg2.record(-250);
  C.push({name:'cage: daily loss limit → halted',pass:cg2.halted()==='daily loss limit',got:String(cg2.halted())});
  // 13: session tagging for 24/7 P&L breakdown
  const st=[sessionTag(120),sessionTag(400),sessionTag(700),sessionTag(1200)].join(',');
  C.push({name:'session tags: overnight/prime/midday/evening',pass:st==='overnight,prime,midday,evening',got:st});
  // 14: Kalshi close-time parsing handles both epoch and ISO formats
  const cA=closeMs({close_ts:1784392000}),cB=closeMs({close_time:'2026-07-18T16:00:00Z'});
  C.push({name:'closeMs parses close_ts and ISO close_time',pass:cA===1784392000000&&cB===Date.parse('2026-07-18T16:00:00Z'),got:cA+','+cB});
  // 15: book parser handles orderbook_fp (dollar strings) AND legacy orderbook (cents)
  const nbA=normalizeBook({orderbook_fp:{yes_dollars:[['0.1500','100.00'],['0.4200','13.00']],no_dollars:[['0.5600','17.00']]}});
  const nbB=normalizeBook({orderbook:{yes:[[15,100],[42,13]],no:[[56,17]]}});
  const okA=nbA&&Math.max(...nbA.yes.map(x=>x[0]))===0.42&&Math.max(...nbA.no.map(x=>x[0]))===0.56;
  const okB=nbB&&Math.max(...nbB.yes.map(x=>x[0]))===0.42;
  C.push({name:'normalizeBook parses fp-dollars and legacy-cents',pass:!!(okA&&okB),got:JSON.stringify(nbA&&nbA.yes)});
  // 16: reversal lockout — after exiting NO on reversal, same-side re-entry in that window is banned
  const d6=decideEntry({fair:0.4,book:{yesAsk:0.8,noAsk:0.22,yesBid:0.75,noBid:0.18},tauSec:139,inHV:false,sentPressure:0,haveOpen:false,ticker:'T1',lockout:{ticker:'T1',side:'NO',ts:Date.now()}});
  C.push({name:'reversal lockout blocks same-side re-entry',pass:d6.action==='NONE'&&/lockout/.test(d6.reason),got:d6.action+' '+d6.reason});
  // 17: late window entry against upstream flow (+30) is vetoed at the tighter threshold
  const d7=decideEntry({fair:0.4,book:{yesAsk:0.8,noAsk:0.22,yesBid:0.75,noBid:0.18},tauSec:139,inHV:false,sentPressure:30,haveOpen:false,ticker:'T2',lockout:null});
  C.push({name:'late-window flow veto at 25 (trade-3 case)',pass:d7.action==='NONE'&&/veto/.test(d7.reason),got:d7.action+' '+d7.reason});
  // 18: Kalshi-truth correction — tonight's mismatch (NO @0.79 scored win, actually lost)
  const tp=truthPnl({mode:'taker',entryPx:0.79,qty:10},false);
  C.push({name:'truthPnl flips phantom win to real loss',pass:Math.abs(tp-(-8.02))<0.02,got:tp});
  const failed=C.filter(c=>!c.pass);
  return{ok:failed.length===0,version:VERSION,passed:C.length-failed.length,total:C.length,checks:C};
}

/* --------------------- HTTP --------------------- */
const server=http.createServer(async(req,res)=>{
  const u=new URL(req.url,`http://${req.headers.host}`);
  if(req.method==='OPTIONS'){cors(res);res.statusCode=204;return res.end();}
  try{
    if(u.pathname==='/health')return send(res,200,{ok:true,version:VERSION,service:'btc-shadow-trader',
      mode:'SHADOW (no real orders)',tapeLen:TAPE.length,sentinel:SENT.read&&SENT.read.ok?'live':'warming',
      halt:cage.halted(),ts:Date.now()});
    if(u.pathname==='/selftest'){const r=runSelfTest();return send(res,r.ok?200:500,r);}
    if(u.pathname==='/status')return send(res,200,STATE.lastStatus||{ok:false,error:'first tick pending'});
    if(u.pathname==='/report')return send(res,200,report());
    if(u.pathname==='/log'){cors(res);res.setHeader('Content-Type','text/plain');
      try{return res.end(fs.readFileSync(LOG_PATH,'utf8'));}catch(_){return res.end('');}}
    if(u.pathname==='/halt'){const on=u.searchParams.get('on');cage.manualHalt=on==='1'||on==='true';
      return send(res,200,{ok:true,manualHalt:cage.manualHalt});}
    return send(res,404,{ok:false,error:'NOT_FOUND'});
  }catch(e){return send(res,500,{ok:false,error:String(e.message||e)});}
});
if(require.main===module){
  server.listen(PORT,()=>console.log(`${VERSION} SHADOW MODE on ${PORT}`));
  const t=setInterval(()=>tick().catch(e=>{STATE.lastErr=String(e.message||e);}),2000);
  if(t.unref)t.unref();
  tick().catch(()=>{});
}
module.exports={computeFair,decideEntry,decideExit,takerFee,makerFee,makeCage,runSelfTest,windowState,sessionTag,tapeAvg};
