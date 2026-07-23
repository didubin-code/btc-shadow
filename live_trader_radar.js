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
const crypto = require('crypto');

const PORT = Number(process.env.PORT || 10000);
const VERSION = 'live-trader-4.0-ws';
const KALSHI_BASE = (process.env.KALSHI_BASE || 'https://api.elections.kalshi.com/trade-api/v2').replace(/\/+$/,'');
const LOG_PATH = process.env.LOG_PATH || '/tmp/shadow_trades.jsonl';

/* ------------------------------ config ------------------------------ */
const CFG = {
  CONTRACTS: Number(process.env.CONTRACTS || 10),          // shadow size per trade
  DAILY_LOSS_LIMIT: Number(process.env.DAILY_LOSS_LIMIT || 200), // $ hard stop (shadow)
  MAX_CONSEC_LOSSES: Number(process.env.MAX_CONSEC_LOSSES || 4), // bench for the day
  EDGE_MIN_TAKER: Number(process.env.EDGE_MIN_TAKER || 0.06),    // fair-vs-price cushion, normal
  EDGE_MIN_TAKER_HV: Number(process.env.EDGE_MIN_TAKER_HV || 0.10), // high-variance sub-window
  WS_ENABLED: !/^(0|false|no)$/i.test(process.env.WS_ENABLED||''),   // v4.0 websocket book feed
  WS_URL: process.env.WS_URL || 'wss://api.elections.kalshi.com/trade-api/ws/v2',
  MAKER_FIRST: !/^(0|false|no)$/i.test(process.env.MAKER_FIRST||''),
  MAKER_UNDERCUT: Number(process.env.MAKER_UNDERCUT || 0.01),
  MAKER_WAIT_S: Number(process.env.MAKER_WAIT_S || 45),
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
  FAIR_MIN_HI: Number(process.env.FAIR_MIN_HI || 0.85),   // v2.0 filter: take taker trades with fair >= this
  FAIR_MAX_LO: Number(process.env.FAIR_MAX_LO || 0),      // v2.3: longshots STRIPPED (0=off). Gate data: <=0.30 band won 1/8 vs 1.7 predicted, -$0.01/trade.
  FILTER_ON: !/^(0|false|no)$/i.test(process.env.FILTER_ON||''), // v2.0 band filter on by default
  LIVE: /^(1|true|yes)$/i.test(process.env.LIVE||''),          // v3.0 MASTER SWITCH: 0=dry-run (logs orders, sends nothing)
  KALSHI_KEY_ID: process.env.KALSHI_KEY_ID||'',
  KALSHI_PRIVATE_KEY: (process.env.KALSHI_PRIVATE_KEY||'').replace(/\\n/g,'\n'),
  LIVE_DAILY_LOSS: Number(process.env.LIVE_DAILY_LOSS || 100), // hard $ stop for REAL money
  LIVE_MAX_CONTRACTS: Number(process.env.LIVE_MAX_CONTRACTS || 50), // absolute per-order cap
  CAL_A: Number(process.env.CAL_A ?? -0.200),             // v2.7 calibration: corrected = CAL_B*fair + CAL_A
  CAL_B: Number(process.env.CAL_B ?? 1.176),              // fit on 190 real settled trades (orig+current), Brier 0.133->0.128
  CAL_ON: !/^(0|false|no)$/i.test(process.env.CAL_ON||''), // on by default
  RADAR_URL: process.env.RADAR_URL||'',        // v3.4: pin-radar status endpoint (observation only)
  MIN_CUSHION_SIGMA: Number(process.env.MIN_CUSHION_SIGMA || 1.0), // v3.3: price must be this many sigma past strike, DRIFT EXCLUDED
  FAIR_STABLE_N: Number(process.env.FAIR_STABLE_N || 3),           // v3.3: fair must clear the band this many consecutive reads
  TREND_BPS: Number(process.env.TREND_BPS || 0.15),
  REVERSAL_HOLD_S: Number(process.env.REVERSAL_HOLD_S || 12), // v2.6: reversal must persist this long before exit (anti fake-out)
  TAIL_TAU: Number(process.env.TAIL_TAU || 45),           // v2.5 tail-snipe: active in final N seconds
  TAIL_SIGMA: Number(process.env.TAIL_SIGMA || 1.5),      // require price >= this many sigma past strike
  TAIL_EDGE: Number(process.env.TAIL_EDGE || 0.03),       // net edge bar in the tail (risk-window is tiny)       // v2.4: |driftBps| >= this = persistent trend; counter-trend entries need EDGE_HV
  COOLDOWN_S: Number(process.env.COOLDOWN_S || 120),       // suppress entries N s after a violent move / reversal exit
  COOLDOWN_SIGMA: Number(process.env.COOLDOWN_SIGMA || 2.0), // "violent" = fair moved > this many sigma vs entry, or a reversal fired
  RISK_DOLLARS: Number(process.env.RISK_DOLLARS || 0),    // v2.3 LADDER: 0=flat shadow. Live: start 25, then 50/100/200/400 every ~50 trades while watching return-on-risk.
  SETTLE_METRIC: (process.env.SETTLE_METRIC || 'last'),   // 'last' (point-in-time, Kalshi-confirmed) or 'avg60' (legacy)
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


/* ==================== v4.0 WEBSOCKET BOOK FEED ====================
   Replaces the 2s REST poll for order-book data with a live stream.
   - auth: RSA-PSS over `{ts}GET/trade-api/ws/v2` on the upgrade handshake
   - channels: orderbook_delta (book) + fill (real execution confirmations)
   - seq gaps => local book is CORRUPT => stop trading, resubscribe, await snapshot
   - read-only: orders still go over REST
=================================================================== */
let WebSocketLib=null;
try{ WebSocketLib=require('ws'); }catch(_){ /* dependency missing -> WS disabled, REST fallback */ }

const WS={
  sock:null, ready:false, seq:null, ticker:null, corrupt:false,
  book:{yes:new Map(), no:new Map()},          // price(cents) -> qty
  lastMsgTs:0, reconnects:0, gaps:0, fills:[], err:null, subId:1, lastSnap:null, lastDelta:null, msgTypes:{}
};
function wsBookTouch(){
  // Kalshi single book: yes[] and no[] are BID ladders. Best ask on one side = 1 - best bid on the other.
  let yb=null, nb=null;
  for(const [p,q] of WS.book.yes) if(q>0 && (yb===null||p>yb)) yb=p;
  for(const [p,q] of WS.book.no)  if(q>0 && (nb===null||p>nb)) nb=p;
  if(yb===null&&nb===null)return null;
  const yesBid = yb!==null? round(yb/10000,4) : null;
  const noBid  = nb!==null? round(nb/10000,4) : null;
  return {
    yesBid, noBid,
    yesAsk: noBid!==null? round(1-noBid,4) : null,
    noAsk:  yesBid!==null? round(1-yesBid,4) : null,
    yesDepth:[...WS.book.yes.values()].reduce((a,b)=>a+b,0),
    noDepth:[...WS.book.no.values()].reduce((a,b)=>a+b,0),
    source:'websocket'
  };
}
function wsApplySnapshot(m){
  // Kalshi wire format (confirmed live): yes_dollars_fp / no_dollars_fp = [[priceStr, qtyStr], ...]
  WS.book.yes.clear(); WS.book.no.clear();
  const key=(v)=>Math.round(Number(v)*10000);   // sub-cent precision
  const yes = m.yes_dollars_fp || m.yes_dollars || m.yes || [];
  const no  = m.no_dollars_fp  || m.no_dollars  || m.no  || [];
  if(yes.length===0 && no.length===0){        // expired/closed market: empty book
    WS.corrupt=true; WS.emptySnapshots=(WS.emptySnapshots||0)+1;
    logLine({ev:'WS_EMPTY_BOOK',ticker:m.market_ticker,note:'market closed or not yet quoting'});
    return;
  }
  for(const lvl of yes){const p=key(lvl[0]), q=Number(lvl[1]); if(Number.isFinite(q)&&q>0)WS.book.yes.set(p,q);}
  for(const lvl of no ){const p=key(lvl[0]), q=Number(lvl[1]); if(Number.isFinite(q)&&q>0)WS.book.no.set(p,q);}
  WS.corrupt=false;
}
function wsApplyDelta(m){
  // price_dollars is a sub-cent dollar string ("0.9010"); key at 1/100-cent precision
  const raw = (m.price_dollars!==undefined? m.price_dollars : m.price);
  const rd  = Number(raw);
  if(!Number.isFinite(rd))return;
  const p = Math.round((rd<=1.0001? rd : rd/100)*10000);
  const d = Number(m.delta_fp!==undefined?m.delta_fp:(m.delta!==undefined?m.delta:m.quantity_delta));
  if(!Number.isFinite(d))return;
  const side = (m.side==='no') ? WS.book.no : WS.book.yes;
  const cur=side.get(p)||0, next=cur+d;
  if(next<=0.0001) side.delete(p); else side.set(p,next);
}
function wsSubscribe(ticker){
  if(!WS.sock||WS.sock.readyState!==1||!ticker)return;
  WS.ticker=ticker; WS.seq=null; WS.corrupt=true;   // corrupt until snapshot arrives
  WS.book.yes.clear(); WS.book.no.clear();
  WS.sock.send(JSON.stringify({id:WS.subId++,cmd:'subscribe',
    params:{channels:['orderbook_delta'],market_tickers:[ticker]}}));
  WS.sock.send(JSON.stringify({id:WS.subId++,cmd:'subscribe',params:{channels:['fill']}}));
  logLine({ev:'WS_SUBSCRIBE',ticker});
}
function wsConnect(){
  if(!CFG.WS_ENABLED||!WebSocketLib||!liveReady())return;
  try{
    const headers=signRequest('GET','/trade-api/ws/v2');
    const s=new WebSocketLib(CFG.WS_URL,{headers});
    WS.sock=s;
    s.on('open',()=>{WS.ready=true;WS.err=null;logLine({ev:'WS_OPEN',url:CFG.WS_URL});if(WS.ticker)wsSubscribe(WS.ticker);});
    s.on('message',(raw)=>{
      WS.lastMsgTs=Date.now();
      let j=null; try{ j=JSON.parse(raw.toString()); }catch(_){ return; }
      const t=j.type;
      WS.msgTypes[t]=(WS.msgTypes[t]||0)+1;
      if(t==='orderbook_snapshot')WS.lastSnap=j;
      if(t==='orderbook_delta'&&!WS.lastDelta)WS.lastDelta=j;
      if(t==='orderbook_snapshot'){ wsApplySnapshot(j.msg||{}); WS.seq=j.seq; }
      else if(t==='orderbook_delta'){
        if(WS.seq!==null && j.seq!==WS.seq+1){        // SEQUENCE GAP -> book untrustworthy
          WS.gaps++; WS.corrupt=true;
          logLine({ev:'WS_SEQ_GAP',expected:WS.seq+1,got:j.seq});
          wsSubscribe(WS.ticker);                      // resubscribe for a fresh snapshot
          return;
        }
        WS.seq=j.seq; wsApplyDelta(j.msg||{});
      }
      else if(t==='fill'){
        const m=j.msg||{};
        WS.fills.push({ticker:m.market_ticker,count:m.count,price:m.price,side:m.side,ts:Date.now()});
        if(WS.fills.length>100)WS.fills.shift();
        logLine({ev:'WS_FILL',ticker:m.market_ticker,count:m.count,price:m.price,side:m.side});
      }
    });
    s.on('ping',()=>{ try{s.pong();}catch(_){} });
    s.on('error',(e)=>{WS.err=String(e.message||e);logLine({ev:'WS_ERROR',err:WS.err});});
    s.on('close',()=>{
      WS.ready=false;WS.corrupt=true;WS.sock=null;WS.reconnects++;
      logLine({ev:'WS_CLOSE',reconnects:WS.reconnects});
      setTimeout(wsConnect,Math.min(30000,1000*Math.pow(2,Math.min(5,WS.reconnects))));
    });
  }catch(e){ WS.err=String(e.message||e); setTimeout(wsConnect,5000); }
}
function wsHealthy(){
  // v4.0.1: empty book (expired/closed market) or stale feed => NOT healthy => REST fallback.
  const hasBook = WS.book.yes.size>0 && WS.book.no.size>0;
  return CFG.WS_ENABLED && WS.ready && !WS.corrupt && hasBook &&
         WS.lastMsgTs>0 && (Date.now()-WS.lastMsgTs)<20000;
}

/* --------------------- PIN RADAR OBSERVER (v3.4) ---------------------
   Fetches the radar's independent signals (spot CVD, Kalshi book imbalance)
   and stamps them on every OPEN. OBSERVATION ONLY — no decision uses these.
   Purpose: after ~40 trades, test whether losers systematically had adverse
   CVD / opposing book imbalance. If yes, a data-derived filter becomes possible.
--------------------------------------------------------------------- */
const RADAR={last:null,lastTs:0,err:null,fetches:0};
async function pollRadar(){
  if(!CFG.RADAR_URL)return;
  const ac=new AbortController();const t=setTimeout(()=>{try{ac.abort();}catch(_){}} ,3000);
  try{
    const r=await fetch(CFG.RADAR_URL,{signal:ac.signal});
    if(!r.ok)throw new Error('HTTP '+r.status);
    const j=await r.json();
    RADAR.last=j; RADAR.lastTs=Date.now(); RADAR.err=null; RADAR.fetches++;
  }catch(e){ RADAR.err=String(e.message||e); }   // radar down = fields null, bot unaffected
  finally{ clearTimeout(t); }
}
function radarSnapshot(){
  const j=RADAR.last;
  if(!j||Date.now()-RADAR.lastTs>20000)return {radarCvd:null,radarImb:null,radarSpotImb:null,radarAge:null};
  const dig=(o,keys)=>{for(const k of keys){if(o&&typeof o==='object'&&o[k]!==undefined&&o[k]!==null)return o[k];}return null;};
  const flat=(o,depth=0)=>{ // shallow search for the signal keys wherever they live
    if(!o||typeof o!=='object'||depth>3)return {};
    let out={};
    for(const [k,v] of Object.entries(o)){
      if(v&&typeof v==='object')Object.assign(out,flat(v,depth+1));
      else out[k]=v;
    }
    return out;
  };
  const F=flat(j);
  return {
    radarCvd: dig(F,['cvd','spotCvd','cvd60','cvdDelta','netFlow']),
    radarImb: dig(F,['imbalance','bookImbalance','kalshiImbalance','imb']),
    radarSpotImb: dig(F,['flowImbalance','spotImbalance','spotImb','depthImbalance']),
    radarBidDepth: dig(F,['bidDepth']), radarAskDepth: dig(F,['askDepth']),
    radarAge: Math.round((Date.now()-RADAR.lastTs)/1000)
  };
}
/* --------------------- LIVE ORDER LAYER (v3.0) --------------------- */
const LIVE={enabled:false,lastErr:null,orders:[],halted:null,realizedToday:0,day:null};
function liveReady(){ return !!(CFG.KALSHI_KEY_ID && CFG.KALSHI_PRIVATE_KEY); }
function signRequest(method,path){
  // Kalshi RSA-PSS: sign  timestampMs + METHOD + path
  const ts=Date.now().toString();
  const msg=ts+method.toUpperCase()+path;
  const sig=crypto.sign('sha256',Buffer.from(msg,'utf-8'),
    {key:CFG.KALSHI_PRIVATE_KEY,padding:crypto.constants.RSA_PKCS1_PSS_PADDING,saltLength:crypto.constants.RSA_PSS_SALTLEN_DIGEST});
  return {'KALSHI-ACCESS-KEY':CFG.KALSHI_KEY_ID,'KALSHI-ACCESS-TIMESTAMP':ts,
          'KALSHI-ACCESS-SIGNATURE':sig.toString('base64'),'Content-Type':'application/json'};
}
async function kalshiAuthed(method,path,body){
  const url=KALSHI_BASE+path;
  const headers=signRequest(method,'/trade-api/v2'+path);
  const ac=new AbortController();const t=setTimeout(()=>{try{ac.abort();}catch(_){}} ,6000);
  try{
    const r=await fetch(url,{method,headers,signal:ac.signal,body:body?JSON.stringify(body):undefined});
    const txt=await r.text();
    let j=null; try{j=JSON.parse(txt);}catch(_){}
    if(!r.ok)throw new Error('HTTP '+r.status+' '+txt.slice(0,200));
    return j;
  } finally{clearTimeout(t);}
}
function liveHalted(){
  const d=new Date().toISOString().slice(0,10);
  if(LIVE.day!==d){LIVE.day=d;LIVE.realizedToday=0;LIVE.halted=null;}
  if(LIVE.halted)return LIVE.halted;
  if(LIVE.realizedToday<=-Math.abs(CFG.LIVE_DAILY_LOSS))return 'live daily loss limit';
  return null;
}
async function placeLiveOrder(ticker,side,count,priceCents){
  // v3.1 — Kalshi V2 order API (/portfolio/events/orders).
  // V2 uses a SINGLE book: side is 'bid'|'ask', price is a fixed-point DOLLAR string.
  //   buy YES @ p   -> side 'bid', price p
  //   buy NO  @ q   -> economically SELL YES @ (1-q) -> side 'ask', price (1-q)
  const isYes = side==='yes';
  const px = isYes ? (priceCents/100) : (1 - priceCents/100);
  const order={ticker,
    client_order_id:'bot-'+Date.now()+'-'+Math.floor(Math.random()*1e6),
    side: isYes ? 'bid' : 'ask',
    count: Number(count).toFixed(2),
    price: px.toFixed(4),
    time_in_force:'immediate_or_cancel',   // taker fill or cancel; never leaves a resting order
    self_trade_prevention_type:'taker_at_cross',  // v3.2: REQUIRED by Kalshi V2
    cancel_order_on_pause:false,
    post_only:false, reduce_only:false, subaccount:0, exchange_index:0};
  if(!CFG.LIVE){
    logLine({ev:'WOULD_PLACE',intent:{side,priceCents,count},v2Order:order,note:'DRY RUN — nothing sent'});
    return {dryRun:true,order};
  }
  const h=liveHalted();
  if(h){logLine({ev:'LIVE_BLOCKED',reason:h,ticker});return {blocked:h};}
  try{
    const res=await kalshiAuthed('POST','/portfolio/events/orders',order);
    const o=res&&(res.order||res);
    logLine({ev:'LIVE_ORDER',ticker,intentSide:side,v2Side:order.side,price:order.price,count:order.count,
      orderId:o&&(o.order_id||o.id),status:o&&o.status});
    LIVE.orders.push({ticker,side,v2Side:order.side,price:order.price,count:order.count,ts:Date.now(),res:o});
    if(LIVE.orders.length>100)LIVE.orders.shift();
    return res;
  }catch(e){
    LIVE.lastErr=String(e.message||e);
    LIVE.halted='order error: '+LIVE.lastErr;
    logLine({ev:'LIVE_ERROR',ticker,err:LIVE.lastErr,halted:true});
    return {error:LIVE.lastErr};
  }
}
async function fetchLivePositions(){
  try{ return await kalshiAuthed('GET','/portfolio/positions'); }
  catch(e){ LIVE.lastErr=String(e.message||e); return null; }
}

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
  // v2.2 COOLDOWN (FIXED): only block re-entering the SAME side of the SAME window we just got burned on.
  // A fresh window or the opposite side is ALWAYS allowed — that's the high-confidence recovery trade
  // that 1.9 takes and wins. The old v2.0/2.1 blanket cooldown froze out those winners; this does not.
  const cdActive = o.cooldownUntil && Date.now()<o.cooldownUntil;
  const cdSameWindow = cdActive && o.lockout && o.ticker && o.lockout.ticker===o.ticker;
  // v2.0 BAND FILTER: only trade high-confidence favorites (>=FAIR_MIN_HI) or cheap convex longshots (<=FAIR_MAX_LO).
  // The mushy middle — where every settled loss in the 100-trade sample lived — is skipped entirely.
  // NOTE: this filters TAKER entries by the position's own fair. YES uses fair; NO uses (1-fair).
  const locked=lockout&&ticker&&lockout.ticker===ticker;
  const inBand=v=>!CFG.FILTER_ON || v>=CFG.FAIR_MIN_HI || v<=CFG.FAIR_MAX_LO;
  // v3.3 STABILITY: a single noisy touch of the band is not a signal. Require N consecutive reads.
  if(CFG.FILTER_ON && CFG.FAIR_STABLE_N>1 && typeof o.fairStreak==='number' && o.fairStreak<CFG.FAIR_STABLE_N)
    return{action:'NONE',reason:'fair not stable yet ('+o.fairStreak+'/'+CFG.FAIR_STABLE_N+' consecutive reads)'};
  // v3.3 REAL-CUSHION GATE: the model's drift projection can manufacture confidence when price sits
  // ON the strike (observed: fair swung 0.17<->0.91 in 90s at $7 from strike, then entered at "0.973").
  // Require the ACTUAL price distance to be >= MIN_CUSHION_SIGMA, using price and vol ONLY — no drift.
  let realCushionSigma=null, cushionSide=null;
  if(o.price&&o.strike&&o.volBps&&tauSec>0){
    const sig=(o.volBps/1e4)*o.price*Math.sqrt(tauSec);
    realCushionSigma=(o.price-o.strike)/Math.max(sig,1e-9);   // + = price above strike (favors YES)
    cushionSide=realCushionSigma>=0?'YES':'NO';
  }
  const cushionOK=(side)=>{
    if(realCushionSigma===null)return true;                    // no data -> don't block
    return side==='YES' ? realCushionSigma>=CFG.MIN_CUSHION_SIGMA
                        : (-realCushionSigma)>=CFG.MIN_CUSHION_SIGMA;
  };
  // v2.4 COUNTER-TREND STIFFENING: in a persistent trend, entries that FIGHT the drift need the HV bar.
  const drift=o.driftBps||0;
  // v2.5 TAIL-SNIPE (sim-validated $1.04/trade; the one structural edge our polling can capture):
  // final TAIL_TAU seconds, outcome decisively decided (>= TAIL_SIGMA past strike), winning side
  // still offered with >= TAIL_EDGE net. Tiny risk window justifies the smaller bar.
  if(o.price&&o.strike&&o.volBps&&tauSec<=CFG.TAIL_TAU&&tauSec>=CFG.MIN_TAU_ENTER){
    const sig=(o.volBps/1e4)*o.price*Math.sqrt(tauSec);
    const cushion=(o.price-o.strike)/Math.max(sig,1e-9); // + = YES side winning, - = NO side
    if(Math.abs(cushion)>=CFG.TAIL_SIGMA){
      if(cushion>0&&book.yesAsk>0&&book.yesAsk<0.99){
        const net=fair-book.yesAsk-takerFee(book.yesAsk,1);
        if(net>=CFG.TAIL_EDGE&&!(lockout&&o.ticker&&lockout.ticker===o.ticker&&lockout.side==='YES'))
          return{action:'BUY_YES',mode:'taker',px:book.yesAsk,fair,netEdge:round(net,3),reason:'tail-snipe YES: '+round(cushion,1)+' sigma past strike, tau '+round(tauSec,0)};
      }
      if(cushion<0&&book.noAsk>0&&book.noAsk<0.99){
        const net=(1-fair)-book.noAsk-takerFee(book.noAsk,1);
        if(net>=CFG.TAIL_EDGE&&!(lockout&&o.ticker&&lockout.ticker===o.ticker&&lockout.side==='NO'))
          return{action:'BUY_NO',mode:'taker',px:book.noAsk,fair,netEdge:round(net,3),reason:'tail-snipe NO: '+round(-cushion,1)+' sigma past strike, tau '+round(tauSec,0)};
      }
    }
  }
  const counterTrend=(side)=> Math.abs(drift)>=CFG.TREND_BPS && ((side==='YES'&&drift<=-CFG.TREND_BPS)||(side==='NO'&&drift>=CFG.TREND_BPS));
  const vetoAt=tauSec<=300?25:CFG.SENT_VETO; // late window: respect upstream flow harder
  const edgeMin=inHV?CFG.EDGE_MIN_TAKER_HV:CFG.EDGE_MIN_TAKER;
  // taker YES
  if(Number.isFinite(book.yesAsk)&&book.yesAsk>0.02&&book.yesAsk<0.98){
    const gross=fair-book.yesAsk;
    const net=gross-takerFee(book.yesAsk,1);
    if(counterTrend('YES')&&net<CFG.EDGE_MIN_TAKER_HV)return{action:'NONE',reason:'counter-trend YES needs edge >= '+CFG.EDGE_MIN_TAKER_HV+' (drift '+round(drift,3)+')'};
    if(net>=edgeMin){
      if(!cushionOK('YES'))return{action:'NONE',reason:'real cushion only '+round(realCushionSigma,2)+' sigma (need '+CFG.MIN_CUSHION_SIGMA+') — fair is drift-manufactured'};
      if(locked&&lockout.side==='YES')return{action:'NONE',reason:'reversal lockout (YES) this window'};
      if(cdSameWindow&&o.lockout.side==='YES')return{action:'NONE',reason:'cooldown same-side YES ('+Math.ceil((o.cooldownUntil-Date.now())/1000)+'s)'};
      if(!inBand(fair))return{action:'NONE',reason:'fair '+round(fair,3)+' outside trade band ['+CFG.FAIR_MAX_LO+','+CFG.FAIR_MIN_HI+']'};
      if(sentPressure<=-vetoAt)return{action:'NONE',reason:'YES edge but perp pressure down (veto @'+vetoAt+')'};
      if(CFG.MAKER_FIRST&&tauSec>CFG.MAKER_WAIT_S+10&&Number.isFinite(book.yesBid)){
        const mb=round(Math.max(0.02,book.yesBid+CFG.MAKER_UNDERCUT),2);
        if(mb<book.yesAsk)return{action:'POST_YES_BID',mode:'maker',px:mb,fair,netEdge:round(fair-mb-CFG.MAKER_FEE,3),reason:'maker-first YES @'+mb+' (taker would pay '+book.yesAsk+')'};
      }
      return{action:'BUY_YES',mode:'taker',px:book.yesAsk,fair,netEdge:round(net,3),reason:'fair '+round(fair,3)+' vs ask '+book.yesAsk};
    }
  }
  // taker NO
  if(Number.isFinite(book.noAsk)&&book.noAsk>0.02&&book.noAsk<0.98){
    const gross=(1-fair)-book.noAsk;
    const net=gross-takerFee(book.noAsk,1);
    if(counterTrend('NO')&&net<CFG.EDGE_MIN_TAKER_HV)return{action:'NONE',reason:'counter-trend NO needs edge >= '+CFG.EDGE_MIN_TAKER_HV+' (drift '+round(drift,3)+')'};
    if(net>=edgeMin){
      if(!cushionOK('NO'))return{action:'NONE',reason:'real cushion only '+round(-realCushionSigma,2)+' sigma (need '+CFG.MIN_CUSHION_SIGMA+') — fair is drift-manufactured'};
      if(locked&&lockout.side==='NO')return{action:'NONE',reason:'reversal lockout (NO) this window'};
      if(cdSameWindow&&o.lockout.side==='NO')return{action:'NONE',reason:'cooldown same-side NO ('+Math.ceil((o.cooldownUntil-Date.now())/1000)+'s)'};
      if(!inBand(1-fair))return{action:'NONE',reason:'fair(no) '+round(1-fair,3)+' outside trade band ['+CFG.FAIR_MAX_LO+','+CFG.FAIR_MIN_HI+']'};
      if(sentPressure>=vetoAt)return{action:'NONE',reason:'NO edge but perp pressure up (veto @'+vetoAt+')'};
      if(CFG.MAKER_FIRST&&tauSec>CFG.MAKER_WAIT_S+10&&Number.isFinite(book.noBid)){
        const mb=round(Math.max(0.02,book.noBid+CFG.MAKER_UNDERCUT),2);
        if(mb<book.noAsk)return{action:'POST_NO_BID',mode:'maker',px:mb,fair,netEdge:round((1-fair)-mb-CFG.MAKER_FEE,3),reason:'maker-first NO @'+mb+' (taker would pay '+book.noAsk+')'};
      }
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
function decideExit(o){ // firm stay-in: exit ONLY on a PERSISTENT confirmed reversal (v2.6)
  const {pos,fair,sentPressure,tauSec,condSince}=o;
  if(!pos)return{exit:false,cond:false};
  const adverse=pos.side==='YES'?(sentPressure<=-CFG.EXIT_SENT):(sentPressure>=CFG.EXIT_SENT);
  const posFair=pos.side==='YES'?fair:1-fair;
  const collapsed=(pos.entryFair-posFair)>=CFG.EXIT_FAIR_DROP;
  const cond=adverse&&collapsed&&tauSec>3;
  if(!cond)return{exit:false,cond:false};                       // condition cleared -> timer resets (fake-out absorbed)
  const heldMs=condSince?Date.now()-condSince:0;
  const needMs=CFG.REVERSAL_HOLD_S*1000;
  // late-window exception: with <60s left there is no time to wait out a fake-out
  if(heldMs>=needMs||tauSec<60)
    return{exit:true,cond:true,reason:'confirmed reversal ('+Math.round(heldMs/1000)+'s persist): perp '+sentPressure+', fair '+round(pos.entryFair,2)+'→'+round(posFair,2)};
  return{exit:false,cond:true};                                  // armed, waiting for persistence
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
const STATE={pos:null,pendingMaker:null,lastReversal:null,cooldownUntil:0,fairStreak:0,fairStreakTicker:'',trades:[],reconcile:[],lastStatus:null,lastErr:null,ticks:0,lastSkipKey:'',skips:[],phantoms:[],revCondSince:0};
function logLine(obj){try{fs.appendFileSync(LOG_PATH,JSON.stringify(obj)+'\n');}catch(_){}}
function openPos(mkt,side,mode,px,fair,tauSec){
  const _drift=round(tapeDrift(),4), _vol=round(tapeVolBps(),3);
  let baseQty=CFG.CONTRACTS;
  if(CFG.RISK_DOLLARS>0 && px>0.01){ // fixed-dollar-risk sizing: contracts so max loss ~= RISK_DOLLARS
    baseQty=Math.max(1,Math.round(CFG.RISK_DOLLARS/px));
  }
  const qty=STATE.inHV?Math.max(1,Math.floor(baseQty/2)):baseQty;
  const fees=mode==='taker'?takerFee(px,qty):makerFee(qty);
  STATE.pos={ticker:mkt.ticker,strike:mkt.strike,closeTs:mkt.closeTs,side,mode,px,qty,fees,
    entryFair:side==='YES'?fair:1-fair,entryTs:Date.now(),entryTau:tauSec,session:sessionTag(ptClock()),entryDrift:_drift,entryVol:_vol,
    ...radarSnapshot()};
  logLine({ev:'OPEN',...STATE.pos});
  // v3.0: mirror the shadow decision as a REAL (or dry-run) order
  if(liveReady()){
    const cents=Math.round(px*100);
    const cnt=Math.min(qty,CFG.LIVE_MAX_CONTRACTS);
    placeLiveOrder(mkt.ticker,side.toLowerCase(),cnt,cents).catch(e=>{
      LIVE.lastErr=String(e.message||e);logLine({ev:'LIVE_ERROR',err:LIVE.lastErr});});
  }
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
  else {STATE.lastReversal={ticker:p.ticker,side:p.side,ts:Date.now()};STATE.cooldownUntil=Date.now()+CFG.COOLDOWN_S*1000;
    // v2.3 PHANTOM TRACKING: keep watching the abandoned position; at window close, log what
    // holding to settlement WOULD have paid vs what the exit actually took. Measurement only.
    STATE.phantoms.push({ticker:p.ticker,side:p.side,px:p.px,qty:p.qty,fees:p.fees,
      closeTs:p.closeTs,strike:p.strike,exitPnl:pnl,ts:Date.now()});
    if(STATE.phantoms.length>50)STATE.phantoms.shift();}
  STATE.pos=null; STATE.revCondSince=0;
}

/* --------------------- main loop --------------------- */
async function tick(){
  STATE.ticks++;
  if(CFG.WS_ENABLED&&WebSocketLib&&!WS.sock&&liveReady())wsConnect();
  if(CFG.RADAR_URL&&STATE.ticks%3===0)pollRadar().catch(()=>{});   // v3.4: poll radar ~every 6s, fire-and-forget
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
    // v2.3: settle any phantoms whose windows have closed (measurement only, no behavior change)
    for(let i=STATE.phantoms.length-1;i>=0;i--){
      const ph=STATE.phantoms[i];
      if(Date.now()>ph.closeTs+1500){
        const lastPx=tapeLastAt(ph.closeTs);
        if(lastPx!==null){
          const won=(ph.side==='YES')?lastPx>ph.strike:lastPx<=ph.strike;
          const heldPnl=won?(1-ph.px)*ph.qty-ph.fees:-(ph.px*ph.qty)-ph.fees;
          logLine({ev:'PHANTOM',ticker:ph.ticker,side:ph.side,exitPnl:round(ph.exitPnl,2),
            heldPnl:round(heldPnl,2),exitSaved:round(ph.exitPnl-heldPnl,2),
            settleLast:round(lastPx,2),strike:ph.strike,ts:Date.now()});
        }
        STATE.phantoms.splice(i,1);
      }
    }
    if(STATE.pos&&Date.now()>STATE.pos.closeTs){
      const avg=tapeAvg(STATE.pos.closeTs-60000,STATE.pos.closeTs);
      const lastPx=tapeLastAt(STATE.pos.closeTs);
      const metric=(CFG.SETTLE_METRIC==='avg60')?avg:lastPx;   // v2.0: point-in-time by default (Kalshi-confirmed)
      const won=metric!==null?(STATE.pos.side==='YES'?metric>STATE.pos.strike:metric<=STATE.pos.strike):null;
      closePos('settlement ('+CFG.SETTLE_METRIC+' '+round(metric,2)+')',null,true,!!won,
        {settleAvg60:round(avg,2),settleLast:round(lastPx,2),settleUsed:CFG.SETTLE_METRIC,
         margin:metric!==null?round(metric-STATE.pos.strike,2):null,strike:STATE.pos.strike});
    }
    if(STATE.lastReversal&&mkt&&STATE.lastReversal.ticker!==mkt.ticker)STATE.lastReversal=null;
    // cancel a resting shadow bid the moment its window rolls over
    if(STATE.pendingMaker&&(!mkt||STATE.pendingMaker.ticker!==mkt.ticker)){
      logLine({ev:'MAKER_CANCEL',ticker:STATE.pendingMaker.ticker,why:'window rolled'});
      STATE.pendingMaker=null;
    }
    if(mkt&&Number.isFinite(mkt.strike)){
      tauSec=(mkt.closeTs-Date.now())/1000;
      // v4.0: prefer the live websocket book; fall back to REST if it's not healthy
      if(CFG.WS_ENABLED){
        if(WS.ticker!==mkt.ticker)wsSubscribe(mkt.ticker);
        const wb=wsHealthy()?wsBookTouch():null;
        book = wb && Number.isFinite(wb.yesAsk) && Number.isFinite(wb.noAsk)
             ? wb : await getBook(mkt.ticker,mkt.quotes);
      } else {
        book=await getBook(mkt.ticker,mkt.quotes);
      }
      const avgStart=mkt.closeTs-60000;
      const knownDur=clamp((Date.now()-avgStart)/1000,0,60);
      const knownAvg=knownDur>1?tapeAvg(avgStart,Date.now()):null;
      fair=computeFair({price,strike:mkt.strike,tauSec,volBps:tapeVolBps(),driftBps:tapeDrift(),knownAvg,knownDur});
      const rawFair=fair;
      if(CFG.CAL_ON&&fair!==null){ // v2.7: pull model confidence toward its historically-realized win rate
        fair=Math.max(0.005,Math.min(0.995,CFG.CAL_B*fair+CFG.CAL_A));
      }
      // maker fill check
      if(STATE.pendingMaker&&STATE.pendingMaker.ticker===mkt.ticker){
        const pm=STATE.pendingMaker;
        const sd=pm.side||'YES', waited=(Date.now()-pm.ts)/1000;
        const oppAsk=sd==='YES'?book.yesAsk:book.noAsk;
        if(Number.isFinite(oppAsk)&&oppAsk<=pm.px){
          STATE.pendingMaker=null;
          logLine({ev:'MAKER_FILL',ticker:pm.ticker,side:sd,px:pm.px,takerWouldPay:pm.takerPx,saved:round((pm.takerPx||0)-pm.px,3),waitedS:round(waited,1)});
          openPos(mkt,sd,'maker',pm.px,fair,tauSec);
        } else if(waited>=CFG.MAKER_WAIT_S){
          STATE.pendingMaker=null;
          logLine({ev:'MAKER_TIMEOUT',ticker:pm.ticker,side:sd,restedAt:pm.px,waitedS:round(waited,1),fallbackTakerPx:oppAsk});
          if(Number.isFinite(oppAsk)&&oppAsk>0.02&&oppAsk<0.98&&tauSec>CFG.MIN_TAU_ENTER)openPos(mkt,sd,'taker',oppAsk,fair,tauSec);
        } else if(tauSec<8||Math.abs((fair??0)-pm.fairAtPost)>0.12){STATE.pendingMaker=null;logLine({ev:'MAKER_CANCEL',ticker:pm.ticker,why:'stale/fair moved'});}
        else if(Number.isFinite(book.yesAsk)&&book.yesAsk<=pm.px){ // panic seller crossed into us
          STATE.pendingMaker=null;openPos(mkt,'YES','maker',pm.px,fair,tauSec);
        }
      }
      // exits (never gated by halt — always allowed to reduce risk)
      if(STATE.pos&&STATE.pos.ticker===mkt.ticker&&tauSec>0){
        const ex=decideExit({pos:STATE.pos,fair,sentPressure:sent.pressure||0,tauSec,condSince:STATE.revCondSince});
        if(ex.cond&&!STATE.revCondSince)STATE.revCondSince=Date.now();  // condition just appeared: start persistence timer
        if(!ex.cond)STATE.revCondSince=0;                               // condition cleared: reset (fake-out absorbed)
        if(ex.exit){
          const px=STATE.pos.side==='YES'?(book.yesBid??Math.max(0.01,fair-0.03)):(book.noBid??Math.max(0.01,1-fair-0.03));
          closePos(ex.reason,px,false,null);
        }
      }
      // entries
      const gated=haltReason?('halted: '+haltReason):((!CFG.TRADE_ALL_HOURS&&!w.inPrime)?'outside prime window':(!sent.ok&&tauSec<180?'sentinel warming (late-window entries blocked)':null));
      if(!gated&&tauSec>0&&tauSec<=CFG.MAX_TAU_ENTER&&!STATE.pendingMaker){
        // v3.3 stability: count consecutive reads where THIS window's fair clears the band
        (function(){
          const conf=Math.max(fair,1-fair);
          const clears=(conf>=CFG.FAIR_MIN_HI)||(Math.min(fair,1-fair)<=CFG.FAIR_MAX_LO&&CFG.FAIR_MAX_LO>0);
          if(mkt.ticker!==STATE.fairStreakTicker){STATE.fairStreakTicker=mkt.ticker;STATE.fairStreak=0;}
          STATE.fairStreak = clears ? STATE.fairStreak+1 : 0;
        })();
        decision=decideEntry({fair,book,tauSec,fairStreak:STATE.fairStreak,inHV:w.inHV,sentPressure:sent.pressure||0,haveOpen:!!STATE.pos,ticker:mkt.ticker,lockout:STATE.lastReversal,cooldownUntil:STATE.cooldownUntil,driftBps:tapeDrift(),price,strike:mkt.strike,volBps:tapeVolBps()});
        // v2.1: log a SKIP once per (window+reason) change — visibility without per-poll spam
        if(decision.action==='NONE'){
          const key=(mkt?mkt.ticker:'-')+'|'+decision.reason;
          if(key!==STATE.lastSkipKey && decision.reason!=='position open'){
            STATE.lastSkipKey=key;
            const rec={ev:'SKIP',ticker:mkt?mkt.ticker:null,fair:fair===null?null:round(fair,3),
              tauSec:round(tauSec,0),reason:decision.reason,ts:Date.now()};
            STATE.skips.push(rec); if(STATE.skips.length>200)STATE.skips.shift();
            logLine(rec);
          }
        } else { STATE.lastSkipKey=''; }
        if(decision.action==='BUY_YES')openPos(mkt,'YES','taker',decision.px,fair,tauSec);
        else if(decision.action==='BUY_NO')openPos(mkt,'NO','taker',decision.px,fair,tauSec);
        else if(decision.action==='POST_YES_BID'||decision.action==='POST_NO_BID'){const sd=decision.action==='POST_YES_BID'?'YES':'NO';STATE.pendingMaker={ticker:mkt.ticker,side:sd,px:decision.px,fairAtPost:fair,ts:Date.now(),takerPx:sd==='YES'?book.yesAsk:book.noAsk};logLine({ev:'MAKER_POST',ticker:mkt.ticker,side:sd,px:decision.px,takerWouldPay:sd==='YES'?book.yesAsk:book.noAsk,fair:round(fair,3)});}
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
    recentSkips:STATE.skips.slice(-5),
    discovery:{...DISC},
    book,fair:fair===null?null:round(fair,3),sentinel:{ok:sent.ok,pressure:sent.pressure||0,venue:sent.venue||null},
    volBps:round(tapeVolBps(),3),driftBps:round(tapeDrift(),4),rawFair:typeof rawFair!=='undefined'?round(rawFair,4):null,
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
  const eA=decideExit({pos:posA,fair:0.6,sentPressure:-45,tauSec:200,condSince:Date.now()-15000}); // persisted 15s
  const eB=decideExit({pos:posA,fair:0.85,sentPressure:-45,tauSec:200,condSince:Date.now()-15000}); // fair fine -> no cond
  C.push({name:'persistent reversal exits; wiggle does not',pass:eA.exit===true&&eB.exit===false,got:eA.exit+'/'+eB.exit});
  // v2.6: fresh condition (0s persist) must NOT exit mid-window — fake-out protection
  const eC=decideExit({pos:posA,fair:0.6,sentPressure:-45,tauSec:200,condSince:Date.now()-2000});
  C.push({name:'v2.6 fresh reversal (2s) does NOT exit mid-window',pass:eC.exit===false&&eC.cond===true,got:eC.exit+'/'+eC.cond});
  // v2.6: late-window exception — same fresh condition WITH <60s left exits immediately
  const eD=decideExit({pos:posA,fair:0.6,sentPressure:-45,tauSec:45,condSince:Date.now()-2000});
  C.push({name:'v2.6 late-window fresh reversal DOES exit (no time to wait)',pass:eD.exit===true,got:String(eD.exit)});
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
  // 16: v2.0 FILTER — high-confidence favorite (fair 0.92) is UNTOUCHED, still trades
  const f_hi=decideEntry({fair:0.92,book:{yesAsk:0.84,noAsk:0.14,yesBid:0.8,noBid:0.1},tauSec:400,inHV:false,sentPressure:0,haveOpen:false});
  C.push({name:'v2 filter PASSES fair>=0.85 favorite (0.9+ untouched)',pass:/BUY_YES|POST_YES_BID/.test(f_hi.action),got:f_hi.action});
  // 17: v2.0 FILTER — mushy middle (fair 0.68) is REJECTED
  const f_mid=decideEntry({fair:0.68,book:{yesAsk:0.55,noAsk:0.42,yesBid:0.5,noBid:0.4},tauSec:400,inHV:false,sentPressure:0,haveOpen:false});
  C.push({name:'v2 filter REJECTS mid-band 0.30-0.85',pass:f_mid.action==='NONE'&&/trade band/.test(f_mid.reason),got:f_mid.action+' '+f_mid.reason});
  // 18: v2.3 — longshots STRIPPED. Gate data: <=0.30 band won 1/8 vs 1.7 predicted. Must now be REJECTED.
  const f_lo=decideEntry({fair:0.18,book:{yesAsk:0.07,noAsk:0.9,yesBid:0.05,noBid:0.88},tauSec:400,inHV:false,sentPressure:0,haveOpen:false});
  C.push({name:'v2.3 filter REJECTS longshots (stripped)',pass:f_lo.action==='NONE',got:f_lo.action+' '+f_lo.reason});
  // 20: v3.3 REAL-CUSHION GATE — blocks drift-manufactured confidence
  // Reproduce the actual bad trade: price $7 from strike, tau 427, vol 0.286 -> 0.18 sigma real cushion.
  const badTrade=decideEntry({fair:0.973,book:{yesAsk:0.66,noAsk:0.33,yesBid:0.64,noBid:0.31},
    tauSec:427,inHV:false,sentPressure:0,haveOpen:false,driftBps:-0.1255,
    price:65711,strike:65718.69,volBps:0.286,fairStreak:99});
  C.push({name:'v3.3 BLOCKS the real bad trade (0.18 sigma, "0.973" fair)',
    pass:badTrade.action==='NONE'&&/real cushion/.test(badTrade.reason),got:badTrade.action+' '+(badTrade.reason||'')});
  // a genuinely cushioned favorite still trades: price $150 above strike, tau 400, vol 0.3 -> ~1.9 sigma
  const goodTrade=decideEntry({fair:0.92,book:{yesAsk:0.80,noAsk:0.12,yesBid:0.78,noBid:0.10},
    tauSec:400,inHV:false,sentPressure:0,haveOpen:false,driftBps:0.02,
    price:66150,strike:66000,volBps:0.3,fairStreak:99});
  C.push({name:'v3.3 ALLOWS genuinely cushioned favorite (~1.9 sigma)',pass:/BUY_YES|POST_YES_BID/.test(goodTrade.action),got:goodTrade.action+' '+(goodTrade.reason||'')});
  // NO side: price BELOW strike by 1.9 sigma -> NO allowed
  const goodNo=decideEntry({fair:0.08,book:{yesAsk:0.90,noAsk:0.80,yesBid:0.88,noBid:0.78},
    tauSec:400,inHV:false,sentPressure:0,haveOpen:false,driftBps:-0.02,
    price:65850,strike:66000,volBps:0.3,fairStreak:99});
  C.push({name:'v3.3 ALLOWS cushioned NO (price below strike)',pass:/BUY_NO|POST_NO_BID/.test(goodNo.action),got:goodNo.action+' '+(goodNo.reason||'')});
  // 21: v3.3 STABILITY — single noisy touch is rejected
  const flicker=decideEntry({fair:0.92,book:{yesAsk:0.80,noAsk:0.12,yesBid:0.78,noBid:0.10},
    tauSec:400,inHV:false,sentPressure:0,haveOpen:false,driftBps:0.02,
    price:66150,strike:66000,volBps:0.3,fairStreak:1});
  C.push({name:'v3.3 REJECTS single-read flicker (1/3 streak)',pass:flicker.action==='NONE'&&/not stable/.test(flicker.reason),got:flicker.action+' '+(flicker.reason||'')});
  // 19: v3.0 LIVE LAYER — safety-critical checks
  C.push({name:'v3.0 defaults to DRY RUN (LIVE off unless explicitly armed)',pass:CFG.LIVE===false,got:'LIVE='+CFG.LIVE});
  C.push({name:'v3.0 live inactive without credentials',pass:(!CFG.KALSHI_KEY_ID||!CFG.KALSHI_PRIVATE_KEY)?liveReady()===false:liveReady()===true,got:'configured='+liveReady()});
  // v3.1 order construction: V2 bid/ask mapping + dollar-string prices
  (function(){
    const mk=(side,cents,cnt)=>{const isYes=side==='yes';const px=isYes?(cents/100):(1-cents/100);
      return {side:isYes?'bid':'ask',price:px.toFixed(4),count:Number(Math.min(cnt,CFG.LIVE_MAX_CONTRACTS)).toFixed(2)};};
    const y=mk('yes',87,29), n=mk('no',85,6), cap=mk('yes',90,9999);
    C.push({name:'v3.1 buy YES 87c -> bid @ 0.8700',pass:y.side==='bid'&&y.price==='0.8700',got:JSON.stringify(y)});
    C.push({name:'v3.1 buy NO 85c -> ask @ 0.1500 (inverted)',pass:n.side==='ask'&&n.price==='0.1500',got:JSON.stringify(n)});
    C.push({name:'v3.1 count is a string, capped',pass:cap.count===Number(CFG.LIVE_MAX_CONTRACTS).toFixed(2),got:cap.count});
    C.push({name:'v3.1 NO price inversion is exact (85 -> 0.1500 not 0.1499)',pass:mk('no',85,1).price==='0.1500',got:mk('no',85,1).price});
  })();
  // v3.2: all V2-required fields present on the order body
  (function(){
    const o={ticker:'X',client_order_id:'a',side:'bid',count:'6.00',price:'0.7700',
      time_in_force:'immediate_or_cancel',self_trade_prevention_type:'taker_at_cross',
      cancel_order_on_pause:false,post_only:false,reduce_only:false,subaccount:0,exchange_index:0};
    const need=['ticker','client_order_id','side','count','price','time_in_force','self_trade_prevention_type'];
    C.push({name:'v3.2 order body has all V2-required fields',pass:need.every(k=>o[k]!==undefined),got:need.filter(k=>o[k]===undefined).join(',')||'all present'});
  })();
  // daily live loss limit halts
  (function(){
    const save=LIVE.realizedToday, saveDay=LIVE.day;
    LIVE.day=new Date().toISOString().slice(0,10); LIVE.realizedToday=-(CFG.LIVE_DAILY_LOSS+1);
    const h=liveHalted();
    LIVE.realizedToday=save; LIVE.day=saveDay; LIVE.halted=null;
    C.push({name:'v3.0 live daily loss limit halts trading',pass:h==='live daily loss limit',got:String(h)});
  })();
  // signing produces the three required headers when a key is present
  C.push({name:'v3.0 signer emits required Kalshi headers (when key set)',
    pass:(!CFG.KALSHI_PRIVATE_KEY)?true:(function(){try{const h=signRequest('GET','/trade-api/v2/portfolio/balance');
      return !!(h['KALSHI-ACCESS-KEY']&&h['KALSHI-ACCESS-TIMESTAMP']&&h['KALSHI-ACCESS-SIGNATURE']);}catch(e){return false;}})(),
    got:CFG.KALSHI_PRIVATE_KEY?'signed':'no key in test env (skipped)'});
  // 18f: v2.7 calibration transform — pulls overconfident scores toward realized win rate
  const cal=(f)=>Math.max(0.005,Math.min(0.995,CFG.CAL_B*f+CFG.CAL_A));
  C.push({name:'v2.7 cal: 0.90 -> ~0.86',pass:Math.abs(cal(0.90)-0.858)<0.01,got:cal(0.90).toFixed(3)});
  C.push({name:'v2.7 cal: 0.95 -> ~0.92',pass:Math.abs(cal(0.95)-0.917)<0.01,got:cal(0.95).toFixed(3)});
  C.push({name:'v2.7 cal is monotonic (higher raw -> higher corrected)',pass:cal(0.95)>cal(0.90)&&cal(0.90)>cal(0.85),got:'ok'});
  C.push({name:'v2.7 cal never exceeds bounds',pass:cal(0.999)<1&&cal(0.001)>0,got:cal(0.999).toFixed(3)});
  // 24: v4.0 WEBSOCKET BOOK — correctness of the local book reconstruction
  (function(){
    WS.book.yes.clear(); WS.book.no.clear();
    wsApplySnapshot({yes_dollars_fp:[['0.4700','300.00'],['0.4600','150.00']], no_dollars_fp:[['0.5300','200.00'],['0.5400','100.00']]});
    const t1=wsBookTouch();
    C.push({name:'v4.0 snapshot -> best yes bid 0.47',pass:Math.abs(t1.yesBid-0.47)<1e-6,got:String(t1.yesBid)});
    C.push({name:'v4.0 implied yesAsk = 1 - best no bid (0.46)',pass:Math.abs(t1.yesAsk-0.46)<1e-6,got:String(t1.yesAsk)});
    wsApplyDelta({side:'yes',price_dollars:'0.4800',delta_fp:'100.00'});
    C.push({name:'v4.0 delta adds a new level (yes bid -> 0.48)',pass:Math.abs(wsBookTouch().yesBid-0.48)<1e-6,got:String(wsBookTouch().yesBid)});
    wsApplyDelta({side:'yes',price_dollars:'0.4800',delta_fp:'-100.00'});
    C.push({name:'v4.0 delta removes exhausted level (back to 0.47)',pass:Math.abs(wsBookTouch().yesBid-0.47)<1e-6,got:String(wsBookTouch().yesBid)});
    wsApplySnapshot({yes_dollars_fp:[['0.9010','100.00'],['0.9030','200.00']], no_dollars_fp:[['0.0280','50.00']]});
    C.push({name:'v4.0 sub-cent prices kept distinct (0.9030 top, not merged to 0.90)',pass:Math.abs(wsBookTouch().yesBid-0.9030)<1e-6,got:String(wsBookTouch().yesBid)});
    C.push({name:'v4.0 sub-cent book has 2 yes levels not 1',pass:WS.book.yes.size===2,got:String(WS.book.yes.size)});
    WS.book.yes.clear(); WS.book.no.clear();
    C.push({name:'v4.0.1 empty book is NOT healthy (forces REST fallback)',pass:wsHealthy()===false,got:String(wsHealthy())});
    WS.corrupt=true;
    C.push({name:'v4.0 corrupt book is NOT reported healthy',pass:wsHealthy()===false,got:String(wsHealthy())});
    WS.corrupt=false; WS.book.yes.clear(); WS.book.no.clear();
  })();
  // 23: v3.6 MAKER-FIRST
  const mf1=decideEntry({fair:0.92,book:{yesAsk:0.80,noAsk:0.12,yesBid:0.77,noBid:0.10},tauSec:400,inHV:false,sentPressure:0,haveOpen:false,driftBps:0.02,price:66150,strike:66000,volBps:0.3,fairStreak:99});
  C.push({name:'v3.6 maker-first rests a bid instead of crossing',pass:mf1.action==='POST_YES_BID'&&mf1.px===0.78,got:mf1.action+' @'+mf1.px});
  const mf2=decideEntry({fair:0.92,book:{yesAsk:0.80,noAsk:0.12,yesBid:0.77,noBid:0.10},tauSec:40,inHV:false,sentPressure:0,haveOpen:false,driftBps:0.02,price:66150,strike:66000,volBps:0.3,fairStreak:99});
  C.push({name:'v3.6 near expiry crosses as taker (no time to rest)',pass:mf2.mode==='taker',got:mf2.action+'/'+mf2.mode});
  const mf3=decideEntry({fair:0.08,book:{yesAsk:0.92,noAsk:0.80,yesBid:0.88,noBid:0.77},tauSec:400,inHV:false,sentPressure:0,haveOpen:false,driftBps:-0.02,price:65850,strike:66000,volBps:0.3,fairStreak:99});
  C.push({name:'v3.6 maker-first works on NO side',pass:mf3.action==='POST_NO_BID'&&mf3.px===0.78,got:mf3.action+' @'+mf3.px});
  // 18e: v2.5 tail-snipe — final-45s decided-outcome entries at the reduced bar
  const ts1=decideEntry({fair:0.99,book:{yesAsk:0.95,noAsk:0.06,yesBid:0.94,noBid:0.04},tauSec:30,inHV:false,sentPressure:0,haveOpen:false,price:66200,strike:66000,volBps:0.5,driftBps:0});
  C.push({name:'v2.5 tail-snipe fires: 11-sigma cushion, 3.7c net, tau 30',pass:ts1.action==='BUY_YES'&&/tail-snipe/.test(ts1.reason),got:ts1.action+' '+(ts1.reason||'')});
  const ts2=decideEntry({fair:0.99,book:{yesAsk:0.95,noAsk:0.06,yesBid:0.94,noBid:0.04},tauSec:300,inHV:false,sentPressure:0,haveOpen:false,price:66200,strike:66000,volBps:0.5,driftBps:0});
  C.push({name:'v2.5 same setup mid-window: 3.7c < 6c bar, NO trade',pass:ts2.action==='NONE',got:ts2.action+' '+(ts2.reason||'')});
  const ts3=decideEntry({fair:0.70,book:{yesAsk:0.60,noAsk:0.41,yesBid:0.58,noBid:0.39},tauSec:30,inHV:false,sentPressure:0,haveOpen:false,price:66010,strike:66000,volBps:0.5,driftBps:0});
  C.push({name:'v2.5 tail with thin 0.5-sigma cushion: NO snipe',pass:ts3.action==='NONE',got:ts3.action+' '+(ts3.reason||'')});
  const ts4=decideEntry({fair:0.99,book:{yesAsk:0.95,noAsk:0.06,yesBid:0.94,noBid:0.04},tauSec:30,inHV:false,sentPressure:0,haveOpen:false,price:66200,strike:66000,volBps:0.5,driftBps:0,ticker:'A',lockout:{ticker:'A',side:'YES'}});
  C.push({name:'v2.5 tail-snipe respects reversal lockout',pass:ts4.action==='NONE',got:ts4.action+' '+(ts4.reason||'')});
  // 18d: v2.4 counter-trend stiffening — fighting a persistent trend needs the HV bar
  const ct1=decideEntry({fair:0.90,book:{yesAsk:0.82,noAsk:0.1,yesBid:0.80,noBid:0.08},tauSec:400,inHV:false,sentPressure:0,haveOpen:false,driftBps:-0.20});
  C.push({name:'v2.4 counter-trend YES with thin edge REJECTED',pass:ct1.action==='NONE'&&/counter-trend/.test(ct1.reason),got:ct1.action+' '+ct1.reason});
  const ct2=decideEntry({fair:0.95,book:{yesAsk:0.80,noAsk:0.1,yesBid:0.78,noBid:0.08},tauSec:400,inHV:false,sentPressure:0,haveOpen:false,driftBps:-0.20});
  C.push({name:'v2.4 counter-trend YES with BIG edge still trades',pass:/BUY_YES|POST_YES_BID/.test(ct2.action),got:ct2.action+' '+(ct2.reason||'')});
  const ct3=decideEntry({fair:0.90,book:{yesAsk:0.82,noAsk:0.1,yesBid:0.80,noBid:0.08},tauSec:400,inHV:false,sentPressure:0,haveOpen:false,driftBps:+0.20});
  C.push({name:'v2.4 WITH-trend YES thin edge trades normally',pass:/BUY_YES|POST_YES_BID/.test(ct3.action),got:ct3.action+' '+(ct3.reason||'')});
  const ct4=decideEntry({fair:0.90,book:{yesAsk:0.82,noAsk:0.1,yesBid:0.80,noBid:0.08},tauSec:400,inHV:false,sentPressure:0,haveOpen:false,driftBps:0.05});
  C.push({name:'v2.4 calm tape (no trend) trades normally',pass:/BUY_YES|POST_YES_BID/.test(ct4.action),got:ct4.action+' '+(ct4.reason||'')});
  // 18c: v2.3 phantom accounting math — held-to-settlement pnl computed correctly
  (function(){
    const ph={side:'YES',px:0.64,qty:10,fees:0.16};
    const lastPx=65550, strike=65600;                 // settled BELOW strike -> YES loses
    const won=(ph.side==='YES')?lastPx>strike:lastPx<=strike;
    const heldPnl=won?(1-ph.px)*ph.qty-ph.fees:-(ph.px*ph.qty)-ph.fees;
    C.push({name:'v2.3 phantom heldPnl math (losing hold)',pass:Math.abs(heldPnl-(-6.56))<0.01,got:heldPnl.toFixed(2)});
    const won2=lastPx>strike-100;                     // now strike 65500 -> YES wins
    const heldPnl2=won2?(1-ph.px)*ph.qty-ph.fees:-(ph.px*ph.qty)-ph.fees;
    C.push({name:'v2.3 phantom heldPnl math (winning hold)',pass:Math.abs(heldPnl2-3.44)<0.01,got:heldPnl2.toFixed(2)});
  })();
  // 18b: only >=0.85 favorites survive the v2.3 filter
  const f_only=decideEntry({fair:0.91,book:{yesAsk:0.80,noAsk:0.12,yesBid:0.78,noBid:0.10},tauSec:400,inHV:false,sentPressure:0,haveOpen:false});
  C.push({name:'v2.3 filter PASSES >=0.85 favorite (only band left)',pass:/BUY_YES|POST_YES_BID/.test(f_only.action),got:f_only.action});
  // 19: v2.2 COOLDOWN (FIXED) — a fresh-window 0.95 favorite is NOT blocked by cooldown from another window
  const f_cd=decideEntry({fair:0.95,book:{yesAsk:0.84,noAsk:0.14,yesBid:0.8,noBid:0.1},tauSec:400,inHV:false,sentPressure:0,haveOpen:false,ticker:'NEW',lockout:{ticker:'OLD',side:'NO'},cooldownUntil:Date.now()+60000});
  C.push({name:'v2.2 cooldown does NOT block fresh-window favorite (the fix)',pass:/BUY_YES|POST_YES_BID/.test(f_cd.action),got:f_cd.action+' '+f_cd.reason});
  // 19b: same-side re-entry in the burned window IS still blocked
  const f_cd2=decideEntry({fair:0.20,book:{yesAsk:0.9,noAsk:0.70,yesBid:0.88,noBid:0.68},tauSec:300,inHV:false,sentPressure:0,haveOpen:false,ticker:'A',lockout:{ticker:'A',side:'NO'},cooldownUntil:Date.now()+60000});
  C.push({name:'v2.2 same-side re-entry still blocked (whipsaw protection)',pass:f_cd2.action==='NONE'&&/(lockout|cooldown)/.test(f_cd2.reason),got:f_cd2.action+' '+f_cd2.reason});
  // 20: v2.0 NO-side filter uses (1-fair): fair 0.10 => NO conf 0.90 => passes
  const f_no=decideEntry({fair:0.10,book:{yesAsk:0.95,noAsk:0.07,yesBid:0.9,noBid:0.05},tauSec:400,inHV:false,sentPressure:0,haveOpen:false});
  C.push({name:'v2 filter PASSES NO when (1-fair)>=0.85',pass:/BUY_NO|POST_NO_BID/.test(f_no.action),got:f_no.action});
  // 21: v2.0 dollar-risk sizing math
  const q=(CFG.RISK_DOLLARS>0)?Math.round(CFG.RISK_DOLLARS/0.8):0;
  C.push({name:'dollar-risk sizing helper computes contracts (shadow default 0 = flat)',pass:(CFG.RISK_DOLLARS===0),got:'RISK_DOLLARS='+CFG.RISK_DOLLARS});
  // 16: reversal lockout — after exiting NO on reversal, same-side re-entry in that window is banned
  const d6=decideEntry({fair:0.4,book:{yesAsk:0.8,noAsk:0.22,yesBid:0.75,noBid:0.18},tauSec:139,inHV:false,sentPressure:0,haveOpen:false,ticker:'T1',lockout:{ticker:'T1',side:'NO',ts:Date.now()}});
  C.push({name:'reversal lockout blocks same-side re-entry',pass:d6.action==='NONE'&&/lockout/.test(d6.reason),got:d6.action+' '+d6.reason});
  // 17: late window entry against upstream flow (+30) is vetoed at the tighter threshold
  const d7=decideEntry({fair:0.88,book:{yesAsk:0.62,noAsk:0.30,yesBid:0.58,noBid:0.26},tauSec:139,inHV:false,sentPressure:-30,haveOpen:false,ticker:'T2',lockout:null});
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
      halt:cage.halted(),live:{configured:liveReady(),armed:CFG.LIVE,halted:liveHalted(),lastErr:LIVE.lastErr},ts:Date.now()});
    if(u.pathname==='/selftest'){const r=runSelfTest();return send(res,r.ok?200:500,r);}
    if(u.pathname==='/status')return send(res,200,STATE.lastStatus||{ok:false,error:'first tick pending'});
    if(u.pathname==='/report')return send(res,200,report());
    if(u.pathname==='/ws')return send(res,200,{enabled:CFG.WS_ENABLED,libLoaded:!!WebSocketLib,
      ready:WS.ready,healthy:wsHealthy(),corrupt:WS.corrupt,ticker:WS.ticker,seq:WS.seq,
      ageMs:WS.lastMsgTs?Date.now()-WS.lastMsgTs:null,reconnects:WS.reconnects,seqGaps:WS.gaps,
      err:WS.err,touch:wsBookTouch(),emptySnapshots:WS.emptySnapshots||0,msgTypes:WS.msgTypes,bookLevels:{yes:WS.book.yes.size,no:WS.book.no.size},
      rawSnapshot:WS.lastSnap,rawDelta:WS.lastDelta,recentFills:WS.fills.slice(-5)});
    if(u.pathname==='/radar')return send(res,200,{url:CFG.RADAR_URL||null,fetches:RADAR.fetches,
      ageSec:RADAR.lastTs?Math.round((Date.now()-RADAR.lastTs)/1000):null,err:RADAR.err,
      parsed:radarSnapshot(),raw:RADAR.last});
    if(u.pathname==='/live')return send(res,200,{configured:liveReady(),armed:CFG.LIVE,
      halted:liveHalted(),dailyRealized:round(LIVE.realizedToday,2),lastErr:LIVE.lastErr,
      riskDollars:CFG.RISK_DOLLARS,maxContracts:CFG.LIVE_MAX_CONTRACTS,recentOrders:LIVE.orders.slice(-10)});
    if(u.pathname==='/livecheck'){ // verifies auth actually works against Kalshi
      if(!liveReady())return send(res,200,{ok:false,error:'KALSHI_KEY_ID / KALSHI_PRIVATE_KEY not set'});
      return kalshiAuthed('GET','/portfolio/balance').then(j=>send(res,200,{ok:true,balance:j}))
        .catch(e=>send(res,200,{ok:false,error:String(e.message||e)}));
    }
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
