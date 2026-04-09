/* ============================================================
   HOSPITAL MUNICIPAL DE MALANJE
   Sistema de Gestão de Medicamentos v2.0
   - Credenciais armazenadas com hash SHA-256 (nunca expostas)
   - Gestão de Utilizadores
   - Sincronização Offline ↔ Google Sheets
   - Gráficos de consumo (Chart.js)
   - Pesquisa por data e tipo de movimentação
   - Exportação/Importação exclusivamente em XLSX
   ============================================================ */

'use strict';

// ===================== XLSX PURO (sem CDN, sem dependências) =====================
// Motor XLSX completo: ZIP não-comprimido + OOXML.
// Funciona 100% offline, sem SheetJS nem qualquer biblioteca externa.
const XLSXio = (() => {
  const enc = new TextEncoder();
  const dec = new TextDecoder();

  /* --- Primitivos ZIP --- */
  function u16(n){ return [n&0xff,(n>>8)&0xff]; }
  function u32(n){ return [n&0xff,(n>>8)&0xff,(n>>16)&0xff,(n>>24)&0xff]; }
  function cat(...a){ const b=new Uint8Array(a.reduce((s,x)=>s+(x.length||x.byteLength||0),0));let o=0;for(const x of a){b.set(x instanceof Uint8Array?x:new Uint8Array(x),o);o+=x.length||x.byteLength;}return b; }
  function crc32(d){
    if(!crc32._t){crc32._t=new Uint32Array(256);for(let i=0;i<256;i++){let c=i;for(let j=0;j<8;j++)c=c&1?0xEDB88320^(c>>>1):c>>>1;crc32._t[i]=c;}}
    let c=0xFFFFFFFF;for(let i=0;i<d.length;i++)c=crc32._t[(c^d[i])&0xFF]^(c>>>8);return(c^0xFFFFFFFF)>>>0;
  }

  /* --- Escrever ZIP (stored, sem compressão) --- */
  function zipWrite(files){
    // files: Map<string, Uint8Array>
    const locals=[], offsets=[], list=[...files.entries()];
    let off=0;
    for(const[name,data]of list){
      const nb=enc.encode(name), crc=crc32(data);
      offsets.push(off);
      const lh=cat([0x50,0x4B,0x03,0x04],u16(20),u16(0),u16(0),u16(0),u16(0),u32(crc),u32(data.length),u32(data.length),u16(nb.length),u16(0),nb,data);
      locals.push(lh); off+=lh.length;
    }
    const cds=[]; let cdSz=0;
    list.forEach(([name,data],i)=>{
      const nb=enc.encode(name), crc=crc32(data);
      const cd=cat([0x50,0x4B,0x01,0x02],u16(20),u16(20),u16(0),u16(0),u16(0),u16(0),u32(crc),u32(data.length),u32(data.length),u16(nb.length),u16(0),u16(0),u16(0),u16(0),u32(0),u32(offsets[i]),nb);
      cds.push(cd); cdSz+=cd.length;
    });
    const eocd=cat([0x50,0x4B,0x05,0x06],u16(0),u16(0),u16(list.length),u16(list.length),u32(cdSz),u32(off),u16(0));
    return cat(...locals,...cds,eocd);
  }

  /* --- Descompressão DEFLATE (para ficheiros Excel) --- */
  /* --- DEFLATE via pipeThrough — método correcto, sem deadlock, funciona no Chrome mobile --- */
  async function inflateRaw(data){
    const src = data instanceof Uint8Array ? data : new Uint8Array(data);

    if(typeof DecompressionStream !== 'undefined'){
      // Método principal: pipeThrough com Blob.stream() — sem deadlock
      if(typeof Response !== 'undefined'){
        for(const fmt of ['deflate-raw','deflate']){
          try{
            const blob = new Blob([src]);
            const stream = blob.stream().pipeThrough(new DecompressionStream(fmt));
            const buf = await new Response(stream).arrayBuffer();
            const out = new Uint8Array(buf);
            if(out.length > 0) return out;
          }catch(e){}
        }
      }

      // Fallback: write/read manual com Promise.all
      for(const fmt of ['deflate-raw','deflate']){
        try{
          const ds = new DecompressionStream(fmt);
          const writer = ds.writable.getWriter();
          const reader = ds.readable.getReader();
          const writeP = (async()=>{ try{ await writer.write(src); await writer.close(); }catch(e){} })();
          const readP  = (async()=>{ const c=[]; try{ while(true){ const{done,value}=await reader.read(); if(done)break; if(value&&value.length)c.push(value); } }catch(e){} return c; })();
          const [,chunks] = await Promise.all([writeP, readP]);
          if(chunks.length){
            const total = chunks.reduce((s,c)=>s+c.length,0);
            if(total > 0){
              const out = new Uint8Array(total);
              let off=0; for(const c of chunks){out.set(c,off);off+=c.length;}
              return out;
            }
          }
        }catch(e){}
      }
    }

    // Último recurso: DEFLATE puro JS
    return inflatePure(src);
  }

  /* --- Pure-JS DEFLATE (fallback para browsers sem DecompressionStream) ---
     USA Uint8Array em vez de Array.push() — 10-50x mais rápido para ficheiros grandes */
  function inflatePure(data){
    const src = data instanceof Uint8Array ? data : new Uint8Array(data);
    let pos=0, bits=0, buf=0;

    function rb(n){
      while(bits<n){ buf|=src[pos++]<<bits; bits+=8; }
      const v=buf&((1<<n)-1); buf>>>=n; bits-=n; return v;
    }
    function align(){ bits=0; buf=0; }

    // Usa lookup array (Int32Array) em vez de Map — muito mais rápido
    function buildTree(lens){
      const maxBits = Math.max(0, ...lens);
      if(maxBits === 0) return { lut: new Int32Array(0), maxBits: 0 };
      const counts = new Int32Array(maxBits+1);
      for(const l of lens) if(l) counts[l]++;
      const nextCode = new Int32Array(maxBits+2);
      for(let i=1; i<=maxBits; i++) nextCode[i+1]=(nextCode[i]+counts[i])<<1;
      // LUT indexada por (len << 9 | reversedCode)
      const lutSize = 1 << maxBits;
      const lut = new Int32Array(lutSize).fill(-1);
      for(let sym=0; sym<lens.length; sym++){
        const l=lens[sym]; if(!l) continue;
        const code = nextCode[l]++;
        // Inverter bits do código para lookup directo
        let rcode=0;
        for(let i=0;i<l;i++) rcode=(rcode<<1)|((code>>i)&1);
        // Preencher todas as entradas que partilham este prefixo
        const step = 1 << l;
        for(let k=rcode; k<lutSize; k+=step) lut[k] = (l<<16)|sym;
      }
      return { lut, maxBits };
    }

    function readSym(tree){
      // Peek maxBits sem consumir
      while(bits < tree.maxBits){ buf|=src[pos++]<<bits; bits+=8; }
      const entry = tree.lut[buf & ((1<<tree.maxBits)-1)];
      if(entry < 0) throw new Error('inflate: símbolo inválido');
      const len = entry >>> 16;
      buf >>>= len; bits -= len;
      return entry & 0xFFFF;
    }

    // Tabelas DEFLATE fixas
    const FIXED_LIT_LENS = new Uint8Array(288);
    for(let i=0;i<144;i++) FIXED_LIT_LENS[i]=8;
    for(let i=144;i<256;i++) FIXED_LIT_LENS[i]=9;
    for(let i=256;i<280;i++) FIXED_LIT_LENS[i]=7;
    for(let i=280;i<288;i++) FIXED_LIT_LENS[i]=8;
    const FIXED_DIST_LENS = new Uint8Array(32).fill(5);
    const fixedLit  = buildTree([...FIXED_LIT_LENS]);
    const fixedDist = buildTree([...FIXED_DIST_LENS]);

    const LENGTH_BASE =[3,4,5,6,7,8,9,10,11,13,15,17,19,23,27,31,35,43,51,59,67,83,99,115,131,163,195,227,258];
    const LENGTH_EXTRA=[0,0,0,0,0,0,0, 0, 1, 1, 1, 1, 2, 2, 2, 2, 3, 3, 3, 3, 4, 4, 4,  4,  5,  5,  5,  5,  0];
    const DIST_BASE =[1,2,3,4,5,7,9,13,17,25,33,49,65,97,129,193,257,385,513,769,1025,1537,2049,3073,4097,6145,8193,12289,16385,24577];
    const DIST_EXTRA=[0,0,0,0,1,1,2, 2, 3, 3, 4, 4, 5, 5,  6,  6,  7,  7,  8,  8,   9,   9,  10,  10,  11,  11,  12,   12,   13,   13];

    // Pré-alocar buffer de saída — evita milhões de Array.push()
    let outBuf = new Uint8Array(Math.max(src.length * 6, 65536));
    let outPos = 0;

    function growOut(){
      const bigger = new Uint8Array(outBuf.length * 2);
      bigger.set(outBuf);
      outBuf = bigger;
    }

    let bfinal = 0;
    do {
      bfinal = rb(1);
      const btype = rb(2);
      if(btype === 0){
        align();
        const len = rb(16); rb(16);
        if(outPos + len > outBuf.length) { while(outPos+len > outBuf.length) growOut(); }
        for(let i=0; i<len; i++) outBuf[outPos++] = rb(8);
      } else {
        let litTree, distTree;
        if(btype === 1){
          litTree = fixedLit; distTree = fixedDist;
        } else {
          const hlit=rb(5)+257, hdist=rb(5)+1, hclen=rb(4)+4;
          const clOrder=[16,17,18,0,8,7,9,6,10,5,11,4,12,3,13,2,14,1,15];
          const clLens = new Array(19).fill(0);
          for(let i=0;i<hclen;i++) clLens[clOrder[i]]=rb(3);
          const clTree = buildTree(clLens);
          const allLens = [];
          while(allLens.length < hlit+hdist){
            const s = readSym(clTree);
            if(s<16){ allLens.push(s); }
            else if(s===16){ const rep=allLens[allLens.length-1]; for(let i=rb(2)+3;i--;) allLens.push(rep); }
            else if(s===17){ for(let i=rb(3)+3;i--;) allLens.push(0); }
            else { for(let i=rb(7)+11;i--;) allLens.push(0); }
          }
          litTree  = buildTree(allLens.slice(0, hlit));
          distTree = buildTree(allLens.slice(hlit));
        }
        while(true){
          const sym = readSym(litTree);
          if(sym === 256) break;
          if(sym < 256){
            if(outPos >= outBuf.length) growOut();
            outBuf[outPos++] = sym;
          } else {
            const li = sym - 257;
            const length = LENGTH_BASE[li] + rb(LENGTH_EXTRA[li]);
            const di = readSym(distTree);
            const dist = DIST_BASE[di] + rb(DIST_EXTRA[di]);
            const start = outPos - dist;
            while(outPos + length > outBuf.length) growOut();
            // Copiar byte a byte (seguro mesmo com overlap)
            for(let i=0; i<length; i++) outBuf[outPos++] = outBuf[start+i];
          }
        }
      }
    } while(!bfinal);

    return outBuf.subarray(0, outPos);
  }

  /* --- Ler ZIP (suporta stored E deflate — compatível com Excel) --- */
  async function zipRead(buf){
    const b=buf instanceof Uint8Array?buf:new Uint8Array(buf);
    const v=new DataView(b.buffer,b.byteOffset,b.byteLength);
    let epos=-1;
    for(let i=b.length-22;i>=0;i--){if(b[i]===0x50&&b[i+1]===0x4B&&b[i+2]===0x05&&b[i+3]===0x06){epos=i;break;}}
    if(epos<0)throw new Error('ZIP inválido');
    const cnt=v.getUint16(epos+8,true), cdOff=v.getUint32(epos+16,true);
    const files=new Map(); let p=cdOff;
    for(let i=0;i<cnt;i++){
      if(b[p]!==0x50||b[p+1]!==0x4B||b[p+2]!==0x01||b[p+3]!==0x02)break;
      const nl=v.getUint16(p+28,true), el=v.getUint16(p+30,true), cl2=v.getUint16(p+32,true);
      const method=v.getUint16(p+10,true); // compression method from central directory
      const lo=v.getUint32(p+42,true);
      const name=dec.decode(b.slice(p+46,p+46+nl));
      const lnl=v.getUint16(lo+26,true), lel=v.getUint16(lo+28,true);
      const csz=v.getUint32(lo+20,true); // compressed size
      const usz=v.getUint32(lo+22,true); // uncompressed size (unused but kept for reference)
      const ds=lo+30+lnl+lel;
      const compressed=b.slice(ds,ds+csz);
      if(method===0){
        files.set(name,compressed);
      } else if(method===8){
        // DEFLATE — used by Excel when saving
        const decompressed=await inflateRaw(compressed);
        files.set(name,decompressed);
      } else {
        // Unknown compression — skip but log
        console.warn(`ZIP: entrada '${name}' usa método de compressão ${method} não suportado`);
        files.set(name, new Uint8Array(0));
      }
      p+=46+nl+el+cl2;
    }
    return files;
  }

  /* --- Utilitários OOXML --- */
  function esc(s){return String(s??'').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');}
  function colLetter(c){let s='',n=c+1;while(n>0){n--;s=String.fromCharCode(65+n%26)+s;n=Math.floor(n/26);}return s;}
  function cellRef(c,r){return colLetter(c)+(r+1);}

  /* --- Escrever XLSX --- */
  function write(sheets){
    // sheets: [{name:string, data:object[]}]
    const ss=[], ssMap=new Map();
    function si(v){const k=String(v??'');if(ssMap.has(k))return ssMap.get(k);const i=ss.length;ss.push(k);ssMap.set(k,i);return i;}

    const files=new Map();

    // [Content_Types].xml
    let ct=`<?xml version="1.0" encoding="UTF-8" standalone="yes"?><Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types"><Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/><Default Extension="xml" ContentType="application/xml"/><Override PartName="/xl/workbook.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/><Override PartName="/xl/styles.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.styles+xml"/><Override PartName="/xl/sharedStrings.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sharedStrings+xml"/>`;
    sheets.forEach((_,i)=>ct+=`<Override PartName="/xl/worksheets/sheet${i+1}.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>`);
    ct+=`</Types>`;
    files.set('[Content_Types].xml',enc.encode(ct));

    files.set('_rels/.rels',enc.encode(`<?xml version="1.0" encoding="UTF-8" standalone="yes"?><Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships"><Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="xl/workbook.xml"/></Relationships>`));

    let wbx=`<?xml version="1.0" encoding="UTF-8" standalone="yes"?><workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships"><sheets>`;
    sheets.forEach((s,i)=>wbx+=`<sheet name="${esc(s.name)}" sheetId="${i+1}" r:id="rId${i+1}"/>`);
    wbx+=`</sheets></workbook>`;
    files.set('xl/workbook.xml',enc.encode(wbx));

    let wr=`<?xml version="1.0" encoding="UTF-8" standalone="yes"?><Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">`;
    sheets.forEach((_,i)=>wr+=`<Relationship Id="rId${i+1}" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet${i+1}.xml"/>`);
    wr+=`<Relationship Id="rId${sheets.length+1}" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/sharedStrings" Target="sharedStrings.xml"/></Relationships>`;
    files.set('xl/_rels/workbook.xml.rels',enc.encode(wr));

    files.set('xl/styles.xml',enc.encode(`<?xml version="1.0" encoding="UTF-8" standalone="yes"?><styleSheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main"><fonts count="1"><font><sz val="11"/><name val="Calibri"/></font></fonts><fills count="2"><fill><patternFill patternType="none"/></fill><fill><patternFill patternType="gray125"/></fill></fills><borders count="1"><border><left/><right/><top/><bottom/><diagonal/></border></borders><cellStyleXfs count="1"><xf numFmtId="0" fontId="0" fillId="0" borderId="0"/></cellStyleXfs><cellXfs count="1"><xf numFmtId="0" fontId="0" fillId="0" borderId="0" xfId="0"/></cellXfs></styleSheet>`));

    // Worksheets
    sheets.forEach((sheet,si2)=>{
      const rows=sheet.data&&sheet.data.length?sheet.data:[{'info':'Sem dados'}];
      const keys=Object.keys(rows[0]);
      let ws=`<?xml version="1.0" encoding="UTF-8" standalone="yes"?><worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main"><sheetData>`;
      // cabeçalho
      ws+=`<row r="1">`;
      keys.forEach((k,ci)=>ws+=`<c r="${cellRef(ci,0)}" t="s"><v>${si(k)}</v></c>`);
      ws+=`</row>`;
      // dados
      rows.forEach((row,ri)=>{
        ws+=`<row r="${ri+2}">`;
        keys.forEach((k,ci)=>{
          const val=row[k]; const addr=cellRef(ci,ri+1);
          if(val===null||val===undefined||val===''){ws+=`<c r="${addr}"/>`;
          }else if(typeof val==='number'){ws+=`<c r="${addr}"><v>${val}</v></c>`;
          }else{ws+=`<c r="${addr}" t="s"><v>${si(val)}</v></c>`;}
        });
        ws+=`</row>`;
      });
      ws+=`</sheetData></worksheet>`;
      files.set(`xl/worksheets/sheet${si2+1}.xml`,enc.encode(ws));
    });

    // sharedStrings — tem de ser construído depois de todos os worksheets
    let ssx=`<?xml version="1.0" encoding="UTF-8" standalone="yes"?><sst xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" count="${ss.length}" uniqueCount="${ss.length}">`;
    ss.forEach(s=>ssx+=`<si><t xml:space="preserve">${esc(s)}</t></si>`);
    ssx+=`</sst>`;
    files.set('xl/sharedStrings.xml',enc.encode(ssx));

    return zipWrite(files);
  }

  /* ================================================================
     LEITOR XLSX VIA REGEX — sem DOMParser, sem namespaces, sem erros.
     Funciona com qualquer ficheiro gerado pelo Microsoft Excel.
     ================================================================ */
  async function read(arrayBuffer){
    const files = await zipRead(new Uint8Array(arrayBuffer));

    /* Converter bytes para texto */
    function getText(bytes){
      if(!bytes || !bytes.length) return '';
      try { return dec.decode(bytes); } catch(e){ return ''; }
    }

    /* Procurar ficheiro no ZIP sem diferenciar maiúsculas/minúsculas */
    function getFile(path){
      if(files.has(path)) return files.get(path);
      const lc = path.toLowerCase().replace(/\\/g,'/');
      for(const [k,v] of files.entries()){
        if(k.toLowerCase().replace(/\\/g,'/')===lc) return v;
      }
      // só o nome do ficheiro
      const fname = path.split('/').pop().toLowerCase();
      for(const [k,v] of files.entries()){
        if(k.split('/').pop().toLowerCase()===fname) return v;
      }
      return null;
    }

    /* Descodificar entidades XML */
    function unesc(s){
      return String(s||'')
        .replace(/&amp;/g,'&').replace(/&lt;/g,'<').replace(/&gt;/g,'>')
        .replace(/&quot;/g,'"').replace(/&apos;/g,"'").replace(/&#(\d+);/g,(_,n)=>String.fromCharCode(+n));
    }

    /* Obter valor de um atributo de uma tag (tolera namespaces) */
    function attr(tag, name){
      // casa: name="val" ou ns:name="val"
      const re = new RegExp(`(?:^|\\s)(?:[\\w]+:)?${name}\\s*=\\s*"([^"]*)"`, 'i');
      const m  = tag.match(re);
      return m ? unesc(m[1]) : '';
    }

    /* Extrair blocos entre tags (ex: <si>...</si>) */
    function blocks(xml, tag){
      const re = new RegExp(`<${tag}(?:\\s[^>]*)?(?:/>|>([\\s\\S]*?)</${tag}>)`,'gi');
      const result = [];
      let m;
      while((m=re.exec(xml))!==null) result.push(m[1]||'');
      return result;
    }

    /* ── 1. Shared Strings — tokenizer sem regex para suportar SS grandes ── */
    const ssList = [];
    const ssXml = getText(getFile('xl/sharedStrings.xml'));
    if(ssXml){
      // Tokenizer character-by-character — sem backtracking
      let inSi=false, inT=false, parts=[], si=0, ssLen=ssXml.length;
      while(si<ssLen){
        if(ssXml[si]==='<'){
          let end=si+1; while(end<ssLen&&ssXml[end]!=='>') end++;
          const raw=ssXml.slice(si+1,end).trim();
          si=end+1;
          if(raw.startsWith('!')||raw.startsWith('?')) continue;
          const isClose=raw.startsWith('/');
          const name=(isClose?raw.slice(1):raw).split(/[\s/]/)[0].toLowerCase().replace(/^[\w]+:/,'');
          if(!isClose&&name==='si'){inSi=true;parts=[];}
          else if(isClose&&name==='si'){if(inSi)ssList.push(parts.join(''));inSi=false;inT=false;}
          else if(!isClose&&name==='t'&&inSi) inT=true;
          else if(isClose&&name==='t') inT=false;
        } else {
          const end=ssXml.indexOf('<',si);
          const text=end===-1?ssXml.slice(si):ssXml.slice(si,end);
          if(inT&&inSi) parts.push(text);
          si=end===-1?ssLen:end;
        }
      }
    }

    /* ── 2. Relações workbook → ficheiros de folha ── */
    const sheetFiles = new Map(); // rId → fullPath
    const relXml = getText(getFile('xl/_rels/workbook.xml.rels'));
    if(relXml){
      const relRe = /<[Rr]elationship\s([^/?>]+)/gi;
      let rm;
      while((rm=relRe.exec(relXml))!==null){
        const tag = rm[1];
        const id     = attr(tag,'Id');
        const target = attr(tag,'Target');
        const type   = attr(tag,'Type');
        if(!id || !target) continue;
        if(type.includes('worksheet')||target.toLowerCase().includes('sheet')){
          let path = target;
          if(path.startsWith('/'))      path = path.slice(1);
          else if(!path.startsWith('xl/')) path = 'xl/'+path;
          sheetFiles.set(id, path);
        }
      }
    }

    /* ── 3. Nomes das folhas no workbook.xml ── */
    const wbXml = getText(getFile('xl/workbook.xml'));
    if(!wbXml) return {};

    const sheetDefs = []; // [{name, rId}]
    const sheetTagRe = /<[Ss]heet\s([^/?>]+)/gi;
    let sm;
    while((sm=sheetTagRe.exec(wbXml))!==null){
      const tag = sm[1];
      const name = attr(tag,'name') || `Sheet${sheetDefs.length+1}`;
      // rId pode ser r:id="rId1" ou id="rId1"
      const rId  = attr(tag,'r:id') || attr(tag,'id') || `rId${sheetDefs.length+1}`;
      sheetDefs.push({name, rId});
    }

    /* ── 4. Ler cada folha ── */
    /* ── Tokenizer XML sem regex — suporta 100.000+ linhas ── */
    function xmlTok(xml) {
      const tokens = []; const len = xml.length; let i = 0;
      while (i < len) {
        if (xml[i] === '<') {
          let end = i + 1; let inStr = false; let sc = '';
          while (end < len) { const c = xml[end]; if (inStr){if(c===sc)inStr=false;} else if(c==='"'||c==="'"){inStr=true;sc=c;} else if(c==='>') break; end++; }
          const raw = xml.slice(i+1, end).trim(); i = end + 1;
          if (raw.startsWith('!')||raw.startsWith('?')) continue;
          const isSelf = raw.endsWith('/'); const isClose = raw.startsWith('/');
          const body = isClose ? raw.slice(1) : (isSelf ? raw.slice(0,-1) : raw);
          const sp = body.search(/[\s/]/); const name = (sp===-1?body:body.slice(0,sp)).toLowerCase().replace(/^[\w]+:/,'');
          const aStr = sp===-1?'':body.slice(sp); const attrs = {};
          const aRe = /([\w:]+)\s*=\s*(?:"([^"]*)"|'([^']*)')/g; let am;
          while((am=aRe.exec(aStr))!==null){ const k=am[1].toLowerCase().replace(/^[\w]+:/,''); attrs[k]=am[2]!==undefined?am[2]:am[3]; }
          if (isClose) tokens.push({type:'close',name}); else if(isSelf) tokens.push({type:'self',name,attrs}); else tokens.push({type:'open',name,attrs});
        } else {
          const end = xml.indexOf('<',i); const text = end===-1?xml.slice(i):xml.slice(i,end);
          if(text) tokens.push({type:'text',text}); i=end===-1?len:end;
        }
      }
      return tokens;
    }

    function readSheet(wsPath, idx){
      const wsXml = getText(getFile(wsPath) || getFile(`xl/worksheets/sheet${idx+1}.xml`));
      if(!wsXml) return [];
      const tokens = xmlTok(wsXml);
      const rows = [];
      let inSD = false, curRow = null, curCellRef = '', curCellType = '';
      let readV = false, readIS = false, curText = '', curCell = null;

      for (const tok of tokens) {
        if (!inSD) { if(tok.type==='open'&&tok.name==='sheetdata') inSD=true; continue; }
        if (tok.type==='close'&&tok.name==='sheetdata') break;

        if (tok.type==='open'&&tok.name==='row') {
          const r=parseInt(tok.attrs.r||'0',10); if(r>0){curRow={ri:r-1,cells:{}}; rows.push(curRow);}
        } else if(tok.type==='close'&&tok.name==='row') {
          curRow=null;
        } else if((tok.type==='open'||tok.type==='self')&&tok.name==='c') {
          curCellRef=tok.attrs.r||''; curCellType=tok.attrs.t||''; curText=''; curCell=null; readV=false; readIS=false;
          if(tok.type==='self'&&curRow&&curCellRef){const colS=curCellRef.replace(/[0-9]/g,'').toUpperCase();let ci=0;for(let k=0;k<colS.length;k++)ci=ci*26+(colS.charCodeAt(k)-64);curRow.cells[ci-1]='';}
        } else if(tok.type==='close'&&tok.name==='c') {
          if(curRow&&curCellRef){const colS=curCellRef.replace(/[0-9]/g,'').toUpperCase();let ci=0;for(let k=0;k<colS.length;k++)ci=ci*26+(colS.charCodeAt(k)-64);curRow.cells[ci-1]=curCell!==null?curCell:'';}
          curCellRef=''; curCell=null; readV=false; readIS=false;
        } else if(tok.type==='open'&&tok.name==='v') { readV=true; curText=''; }
        else if(tok.type==='close'&&tok.name==='v') {
          readV=false;
          if(curCellType==='s'){const idx2=parseInt(curText,10);curCell=isNaN(idx2)?'':(ssList[idx2]??'');}
          else if(curCellType==='b'){curCell=curText.trim()==='1';}
          else if(curCellType==='str'){curCell=unesc(curText);}
          else{const n=Number(curText.trim());curCell=(curText.trim()!==''&&!isNaN(n))?n:unesc(curText);}
        } else if(tok.type==='open'&&tok.name==='is') { readIS=true; curText=''; }
        else if(tok.type==='close'&&tok.name==='is') { readIS=false; curCell=unesc(curText); }
        else if(tok.type==='text'&&(readV||readIS)) { curText+=tok.text; }
      }

      if (!rows.length) return [];
      let maxCol = 0;
      for (const row of rows) for (const ci of Object.keys(row.cells)) { const n=Number(ci); if(n+1>maxCol) maxCol=n+1; }
      if (!maxCol) return [];
      const hdrRow = rows[0]; const headers = [];
      for (let c=0;c<maxCol;c++) headers.push(String(hdrRow.cells[c]??'').trim());
      if (!headers.some(h=>h!=='')) return [];
      const result = [];
      for (let i=1;i<rows.length;i++) {
        const row=rows[i]; const obj={}; let hasVal=false;
        for (let c=0;c<headers.length;c++) { const h=headers[c]; const v=row.cells[c]??''; if(h!==''){obj[h]=v; if(v!==''&&v!==null&&v!==undefined)hasVal=true;} }
        if(hasVal) result.push(obj);
      }
      return result;
    }

    /* ── 5. Montar resultado ── */
    const result={};
    sheetDefs.forEach(({name,rId},i)=>{
      const wsPath = sheetFiles.get(rId) || `xl/worksheets/sheet${i+1}.xml`;
      result[name] = readSheet(wsPath, i);
    });
    return result;
  }

  /* --- API pública --- */
  function download(sheets, filename){
    // sheets: [{name:string, data:object[]}]
    try{
      const bytes=write(sheets);
      const blob=new Blob([bytes],{type:'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'});
      const url=URL.createObjectURL(blob);
      const a=document.createElement('a');
      a.href=url; a.download=filename;
      document.body.appendChild(a); a.click(); document.body.removeChild(a);
      setTimeout(()=>URL.revokeObjectURL(url),3000);
      return true;
    }catch(e){toast('error','Erro no download XLSX',e.message);return false;}
  }

  return {write, read, download};
})();
// ===================== FIM XLSX PURO =====================

// ===================== XLSX WEB WORKER =====================
// O Web Worker executa o parsing pesado numa thread separada,
// evitando que o browser congele durante a importação.
// O código do worker é embutido como Blob URL — sem ficheiros externos.

const XLSX_WORKER_CODE = `
'use strict';
const dec = new TextDecoder();

// ── DEFLATE via DecompressionStream ──
async function inflateRaw(data) {
  const bytes = data instanceof Uint8Array ? data : new Uint8Array(data);
  if (typeof DecompressionStream !== 'undefined') {
    for (const fmt of ['deflate-raw','deflate']) {
      try {
        const blob = new Blob([bytes]);
        const stream = blob.stream().pipeThrough(new DecompressionStream(fmt));
        const buf = await new Response(stream).arrayBuffer();
        return new Uint8Array(buf);
      } catch(e) {}
    }
    try {
      const ds = new DecompressionStream('deflate-raw');
      const writer = ds.writable.getWriter();
      const reader = ds.readable.getReader();
      const writeP = (async()=>{ try{ await writer.write(bytes); await writer.close(); }catch(e){} })();
      const readP  = (async()=>{ const c=[]; try{ while(true){ const{done,value}=await reader.read(); if(done)break; if(value)c.push(value); } }catch(e){} return c; })();
      const [,chunks] = await Promise.all([writeP, readP]);
      const total = chunks.reduce((s,c)=>s+c.length,0);
      const out = new Uint8Array(total); let off=0;
      for(const c of chunks){ out.set(c,off); off+=c.length; }
      if(out.length > 0) return out;
    } catch(e3) {}
  }
  return inflatePure(bytes);
}

// ── DEFLATE puro JS (fallback) ──
function inflatePure(data) {
  const src = data instanceof Uint8Array ? data : new Uint8Array(data);
  let pos=0,bits=0,buf=0;
  function rb(n){ while(bits<n){buf|=src[pos++]<<bits;bits+=8;} const v=buf&((1<<n)-1);buf>>>=n;bits-=n;return v; }
  function align(){ bits=0;buf=0; }
  function buildTree(lens){
    const maxBits=Math.max(0,...lens);
    if(!maxBits) return{lut:new Int32Array(0),maxBits:0};
    const counts=new Int32Array(maxBits+1);
    for(const l of lens) if(l) counts[l]++;
    const nc=new Int32Array(maxBits+2);
    for(let i=1;i<=maxBits;i++) nc[i+1]=(nc[i]+counts[i])<<1;
    const lut=new Int32Array(1<<maxBits).fill(-1);
    for(let sym=0;sym<lens.length;sym++){
      const l=lens[sym]; if(!l) continue;
      const code=nc[l]++;
      let rc=0; for(let i=0;i<l;i++) rc=(rc<<1)|((code>>i)&1);
      for(let k=rc;k<(1<<maxBits);k+=(1<<l)) lut[k]=(l<<16)|sym;
    }
    return{lut,maxBits};
  }
  function readSym(tree){
    while(bits<tree.maxBits){buf|=src[pos++]<<bits;bits+=8;}
    const e=tree.lut[buf&((1<<tree.maxBits)-1)];
    if(e<0) throw new Error('bad sym');
    buf>>>=(e>>>16); bits-=(e>>>16); return e&0xFFFF;
  }
  const FLL=new Uint8Array(288);
  for(let i=0;i<144;i++)FLL[i]=8; for(let i=144;i<256;i++)FLL[i]=9;
  for(let i=256;i<280;i++)FLL[i]=7; for(let i=280;i<288;i++)FLL[i]=8;
  const fL=buildTree([...FLL]), fD=buildTree([...new Uint8Array(32).fill(5)]);
  const LB=[3,4,5,6,7,8,9,10,11,13,15,17,19,23,27,31,35,43,51,59,67,83,99,115,131,163,195,227,258];
  const LE=[0,0,0,0,0,0,0,0,1,1,1,1,2,2,2,2,3,3,3,3,4,4,4,4,5,5,5,5,0];
  const DB=[1,2,3,4,5,7,9,13,17,25,33,49,65,97,129,193,257,385,513,769,1025,1537,2049,3073,4097,6145,8193,12289,16385,24577];
  const DE=[0,0,0,0,1,1,2,2,3,3,4,4,5,5,6,6,7,7,8,8,9,9,10,10,11,11,12,12,13,13];
  let ob=new Uint8Array(Math.max(src.length*6,65536)), op=0;
  function grow(){ const b=new Uint8Array(ob.length*2); b.set(ob); ob=b; }
  let bfinal=0;
  do {
    bfinal=rb(1); const bt=rb(2);
    if(bt===0){ align(); const len=rb(16);rb(16); while(op+len>ob.length)grow(); for(let i=0;i<len;i++)ob[op++]=rb(8); }
    else {
      let lT,dT;
      if(bt===1){lT=fL;dT=fD;}
      else {
        const hl=rb(5)+257,hd=rb(5)+1,hc=rb(4)+4;
        const co=[16,17,18,0,8,7,9,6,10,5,11,4,12,3,13,2,14,1,15];
        const cl=new Array(19).fill(0); for(let i=0;i<hc;i++)cl[co[i]]=rb(3);
        const ct=buildTree(cl); const al=[];
        while(al.length<hl+hd){
          const s=readSym(ct);
          if(s<16)al.push(s);
          else if(s===16){const r=al[al.length-1];for(let i=rb(2)+3;i--;)al.push(r);}
          else if(s===17){for(let i=rb(3)+3;i--;)al.push(0);}
          else{for(let i=rb(7)+11;i--;)al.push(0);}
        }
        lT=buildTree(al.slice(0,hl)); dT=buildTree(al.slice(hl));
      }
      while(true){
        const sym=readSym(lT); if(sym===256)break;
        if(sym<256){ if(op>=ob.length)grow(); ob[op++]=sym; }
        else {
          const li=sym-257, len=LB[li]+rb(LE[li]);
          const di=readSym(dT), dist=DB[di]+rb(DE[di]);
          const st=op-dist; while(op+len>ob.length)grow();
          for(let i=0;i<len;i++)ob[op++]=ob[st+i];
        }
      }
    }
  } while(!bfinal);
  return ob.subarray(0,op);
}

// ── Leitura do ZIP ──
async function zipRead(buf){
  const b=buf instanceof Uint8Array?buf:new Uint8Array(buf);
  const v=new DataView(b.buffer,b.byteOffset,b.byteLength);
  let ep=-1;
  for(let i=b.length-22;i>=0;i--){if(b[i]===0x50&&b[i+1]===0x4B&&b[i+2]===0x05&&b[i+3]===0x06){ep=i;break;}}
  if(ep<0) throw new Error('ZIP invalido');
  const cnt=v.getUint16(ep+8,true), cdOff=v.getUint32(ep+16,true);
  const files=new Map(); let p=cdOff;
  for(let i=0;i<cnt;i++){
    if(b[p]!==0x50||b[p+1]!==0x4B||b[p+2]!==0x01||b[p+3]!==0x02)break;
    const nl=v.getUint16(p+28,true),el=v.getUint16(p+30,true),cl=v.getUint16(p+32,true);
    const method=v.getUint16(p+10,true), lo=v.getUint32(p+42,true);
    const name=dec.decode(b.slice(p+46,p+46+nl));
    const lnl=v.getUint16(lo+26,true),lel=v.getUint16(lo+28,true);
    const csz=v.getUint32(lo+20,true), ds=lo+30+lnl+lel;
    const compressed=b.slice(ds,ds+csz);
    files.set(name, method===8 ? await inflateRaw(compressed) : compressed);
    p+=46+nl+el+cl;
  }
  return files;
}

// ── XML tokenizer sem regex — lida com 100.000+ linhas ──
function xmlTokenize(xml) {
  // Retorna array de tokens: {type:'open'|'close'|'self'|'text', name, attrs:{}, text}
  const tokens = [];
  const len = xml.length;
  let i = 0;
  while (i < len) {
    if (xml[i] === '<') {
      // tag
      const start = i + 1;
      // procurar fim da tag
      let end = i + 1;
      let inStr = false;
      let strChar = '';
      while (end < len) {
        const c = xml[end];
        if (inStr) { if (c === strChar) inStr = false; }
        else if (c === '"' || c === "'") { inStr = true; strChar = c; }
        else if (c === '>') break;
        end++;
      }
      const raw = xml.slice(start, end).trim();
      i = end + 1;
      if (raw.startsWith('!') || raw.startsWith('?')) continue;
      const isSelf = raw.endsWith('/');
      const isClose = raw.startsWith('/');
      const content = isClose ? raw.slice(1) : (isSelf ? raw.slice(0,-1) : raw);
      // parse name and attrs
      const spIdx = content.search(/[\s/]/);
      const name = (spIdx === -1 ? content : content.slice(0, spIdx)).toLowerCase().replace(/^[\w]+:/,'');
      const attrStr = spIdx === -1 ? '' : content.slice(spIdx);
      const attrs = {};
      const attrRe = /([\w:]+)\s*=\s*(?:"([^"]*)"|'([^']*)')/g;
      let am;
      while ((am = attrRe.exec(attrStr)) !== null) {
        const key = am[1].toLowerCase().replace(/^[\w]+:/,'');
        attrs[key] = am[2] !== undefined ? am[2] : am[3];
      }
      if (isClose) tokens.push({type:'close', name});
      else if (isSelf) tokens.push({type:'self', name, attrs});
      else tokens.push({type:'open', name, attrs});
    } else {
      // text node
      const end = xml.indexOf('<', i);
      const text = end === -1 ? xml.slice(i) : xml.slice(i, end);
      if (text) tokens.push({type:'text', text});
      i = end === -1 ? len : end;
    }
  }
  return tokens;
}

// ── Parsear sharedStrings.xml ──
function parseSharedStrings(xml) {
  const list = [];
  const tokens = xmlTokenize(xml);
  let inSi = false, inT = false, curParts = [];
  for (const tok of tokens) {
    if (tok.type === 'open' && tok.name === 'si') { inSi = true; curParts = []; }
    else if (tok.type === 'close' && tok.name === 'si') { list.push(curParts.join('')); inSi = false; inT = false; }
    else if (inSi && tok.type === 'open' && tok.name === 't') inT = true;
    else if (inSi && tok.type === 'close' && tok.name === 't') inT = false;
    else if (inSi && inT && tok.type === 'text') curParts.push(tok.text);
    else if (inSi && tok.type === 'self' && tok.name === 't') {} // vazio
  }
  return list;
}

// ── Descodificar entidades XML ──
function unesc(s) {
  return String(s||'')
    .replace(/&amp;/g,'&').replace(/&lt;/g,'<').replace(/&gt;/g,'>')
    .replace(/&quot;/g,'"').replace(/&apos;/g,"'")
    .replace(/&#(\d+);/g,(_,n)=>String.fromCharCode(+n));
}

// ── Converter referência de célula (ex: "AB12") em índice de coluna ──
function colToIdx(ref) {
  const colStr = ref.replace(/[0-9]/g,'').toUpperCase();
  let ci = 0;
  for (let k = 0; k < colStr.length; k++) ci = ci * 26 + (colStr.charCodeAt(k) - 64);
  return ci - 1;
}

// ── Parsear uma folha de cálculo usando tokenizer ──
function parseSheet(xml, ssList) {
  if (!xml) return [];
  const tokens = xmlTokenize(xml);
  const rows = []; // array de {ri, cells:{ci:val}}
  let inSheetData = false;
  let curRow = null;
  let curCell = null;
  let curCellType = '';
  let curCellRef = '';
  let readingV = false;
  let readingInlineStr = false;
  let curText = '';

  for (const tok of tokens) {
    if (!inSheetData) {
      if (tok.type === 'open' && tok.name === 'sheetdata') inSheetData = true;
      continue;
    }
    if (tok.type === 'close' && tok.name === 'sheetdata') break;

    if (tok.type === 'open' && tok.name === 'row') {
      const r = parseInt(tok.attrs.r || '0', 10);
      if (r > 0) { curRow = { ri: r - 1, cells: {} }; rows.push(curRow); }
    } else if (tok.type === 'close' && tok.name === 'row') {
      curRow = null;
    } else if ((tok.type === 'open' || tok.type === 'self') && tok.name === 'c') {
      curCellRef = tok.attrs.r || '';
      curCellType = tok.attrs.t || '';
      curCell = null; curText = ''; readingV = false; readingInlineStr = false;
      if (tok.type === 'self') {
        // empty cell
        if (curRow && curCellRef) {
          const ci = colToIdx(curCellRef);
          if (ci >= 0) curRow.cells[ci] = '';
        }
        curCellRef = '';
      }
    } else if (tok.type === 'close' && tok.name === 'c') {
      if (curRow && curCellRef) {
        const ci = colToIdx(curCellRef);
        if (ci >= 0) curRow.cells[ci] = curCell !== null ? curCell : '';
      }
      curCell = null; curText = ''; readingV = false; readingInlineStr = false; curCellRef = '';
    } else if (tok.type === 'open' && tok.name === 'v') {
      readingV = true; curText = '';
    } else if (tok.type === 'close' && tok.name === 'v') {
      readingV = false;
      if (curCellType === 's') {
        const idx = parseInt(curText, 10);
        curCell = isNaN(idx) ? '' : (ssList[idx] ?? '');
      } else if (curCellType === 'b') {
        curCell = curText.trim() === '1';
      } else if (curCellType === 'str') {
        curCell = unesc(curText);
      } else {
        const n = Number(curText.trim());
        curCell = (curText.trim() !== '' && !isNaN(n)) ? n : unesc(curText);
      }
    } else if (tok.type === 'open' && tok.name === 'is') {
      readingInlineStr = true; curText = '';
    } else if (tok.type === 'close' && tok.name === 'is') {
      readingInlineStr = false;
      curCell = unesc(curText);
    } else if (tok.type === 'open' && tok.name === 't') {
      curText = '';
    } else if (tok.type === 'close' && tok.name === 't') {
      // handled by text node
    } else if (tok.type === 'text') {
      if (readingV || readingInlineStr) curText += tok.text;
    }
  }

  if (!rows.length) return [];

  // Determinar max coluna
  let maxCol = 0;
  for (const row of rows) {
    for (const ci of Object.keys(row.cells)) {
      const n = Number(ci);
      if (n + 1 > maxCol) maxCol = n + 1;
    }
  }
  if (maxCol === 0) return [];

  // Extrair cabeçalho
  const hdrRow = rows[0];
  const headers = [];
  for (let c = 0; c < maxCol; c++) {
    headers.push(String(hdrRow.cells[c] ?? '').trim());
  }
  if (!headers.some(h => h !== '')) return [];

  // Converter linhas restantes para objectos
  const result = [];
  for (let i = 1; i < rows.length; i++) {
    const row = rows[i];
    const obj = {};
    let hasValue = false;
    for (let c = 0; c < headers.length; c++) {
      const h = headers[c];
      const val = row.cells[c] ?? '';
      if (h !== '') {
        obj[h] = val;
        if (val !== '' && val !== null && val !== undefined) hasValue = true;
      }
    }
    if (hasValue) result.push(obj);
  }
  return result;
}

// ── Leitura principal do XLSX ──
async function readXLSX(arrayBuffer) {
  const files = await zipRead(new Uint8Array(arrayBuffer));

  function getText(bytes) {
    try { return bytes && bytes.length ? dec.decode(bytes) : ''; } catch(e) { return ''; }
  }
  function getFile(path) {
    if (files.has(path)) return files.get(path);
    const lc = path.toLowerCase().replace(/\\\\/g,'/');
    for (const [k,v] of files.entries()) {
      if (k.toLowerCase().replace(/\\\\/g,'/') === lc) return v;
    }
    const fn = path.split('/').pop().toLowerCase();
    for (const [k,v] of files.entries()) {
      if (k.split('/').pop().toLowerCase() === fn) return v;
    }
    return null;
  }

  // Shared strings
  const ssXml = getText(getFile('xl/sharedStrings.xml'));
  const ssList = ssXml ? parseSharedStrings(ssXml) : [];

  // Relações das folhas
  const sheetFiles = new Map();
  const relXml = getText(getFile('xl/_rels/workbook.xml.rels'));
  if (relXml) {
    const rRe = /<[Rr]elationship\s([^/?>]+)/gi; let rm;
    while ((rm = rRe.exec(relXml)) !== null) {
      const idM = rm[1].match(/\bId\s*=\s*"([^"]*)"/i);
      const tgtM = rm[1].match(/\bTarget\s*=\s*"([^"]*)"/i);
      const typeM = rm[1].match(/\bType\s*=\s*"([^"]*)"/i);
      if (!idM || !tgtM) continue;
      const type = typeM ? typeM[1] : '';
      const target = tgtM[1];
      if (type.includes('worksheet') || target.toLowerCase().includes('sheet')) {
        let path = target;
        if (path.startsWith('/')) path = path.slice(1);
        else if (!path.startsWith('xl/')) path = 'xl/' + path;
        sheetFiles.set(idM[1], path);
      }
    }
  }

  // Nomes das folhas
  const wbXml = getText(getFile('xl/workbook.xml'));
  if (!wbXml) return {};
  const sheetDefs = [];
  const stRe = /<[Ss]heet\s([^/?>]+)/gi; let stm;
  while ((stm = stRe.exec(wbXml)) !== null) {
    const tag = stm[1];
    const nameM = tag.match(/\bname\s*=\s*"([^"]*)"/i);
    const rIdM = tag.match(/\br:id\s*=\s*"([^"]*)"/i) || tag.match(/\brid\s*=\s*"([^"]*)"/i) || tag.match(/\bid\s*=\s*"([^"]*)"/i);
    sheetDefs.push({
      name: nameM ? nameM[1] : ('Sheet' + (sheetDefs.length+1)),
      rId: rIdM ? rIdM[1] : ('rId' + (sheetDefs.length+1))
    });
  }

  const result = {};
  for (let i = 0; i < sheetDefs.length; i++) {
    const { name, rId } = sheetDefs[i];
    const wsPath = sheetFiles.get(rId) || ('xl/worksheets/sheet' + (i+1) + '.xml');
    const wsXml = getText(getFile(wsPath) || getFile('xl/worksheets/sheet' + (i+1) + '.xml'));
    self.postMessage({ status: 'progress', msg: 'A processar folha "' + name + '" (' + (i+1) + '/' + sheetDefs.length + ')...' });
    result[name] = parseSheet(wsXml, ssList);
  }
  return result;
}

// ── Handler do Worker ──
self.onmessage = async function(e) {
  try {
    self.postMessage({ status: 'progress', msg: 'A descomprimir ficheiro ZIP...' });
    const sheets = await readXLSX(e.data);
    self.postMessage({ status: 'progress', msg: 'A serializar JSON...' });
    const json = JSON.stringify(sheets);
    self.postMessage({ status: 'done', json });
  } catch(err) {
    self.postMessage({ status: 'error', msg: err.message || 'Erro desconhecido' });
  }
};

`;

/* Criar e reutilizar URL do worker */
let _xlsxWorkerUrl = null;
function getXlsxWorkerUrl(){
  if(!_xlsxWorkerUrl){
    const blob = new Blob([XLSX_WORKER_CODE], {type:'application/javascript'});
    _xlsxWorkerUrl = URL.createObjectURL(blob);
  }
  return _xlsxWorkerUrl;
}

/* Função principal: usa Worker para converter XLSX → JSON sem travar o browser */
function parseXLSXInWorker(arrayBuffer){
  return new Promise((resolve, reject)=>{
    let worker;
    try { worker = new Worker(getXlsxWorkerUrl()); }
    catch(e){ reject(new Error('Web Worker não suportado: '+e.message)); return; }

    const timeout = setTimeout(()=>{
      worker.terminate();
      reject(new Error('Tempo limite excedido (60s). Ficheiro demasiado grande ou corrompido.'));
    }, 60000);

    worker.onmessage = (e)=>{
      const d = e.data;
      if(d.status === 'progress'){
        console.log('[XLSX Worker]', d.msg);
      } else if(d.status === 'done'){
        clearTimeout(timeout);
        worker.terminate();
        try { resolve(JSON.parse(d.json)); }
        catch(err){ reject(new Error('Erro ao parsear JSON do worker: '+err.message)); }
      } else if(d.status === 'error'){
        clearTimeout(timeout);
        worker.terminate();
        reject(new Error(d.msg));
      }
    };
    worker.onerror = (e)=>{
      clearTimeout(timeout);
      worker.terminate();
      reject(new Error('Worker error: '+(e.message||'desconhecido')));
    };
    // NÃO transferir (não passar [arrayBuffer]) — o worker recebe uma cópia
    // e o buffer original fica disponível para o fallback XLSXio.read()
    worker.postMessage(arrayBuffer);
  });
}
// ===================== FIM XLSX WEB WORKER =====================

// ===================== SVG ICONS =====================
const ICONS = {
  medical:`<img src="${typeof BANDMED_ICON_B64!=='undefined'?BANDMED_ICON_B64:''}" style="width:20px;height:20px;object-fit:contain;vertical-align:middle;" alt="BANDMED">`,
  dashboard:`<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="3" y="3" width="7" height="7" rx="1"/><rect x="14" y="3" width="7" height="7" rx="1"/><rect x="3" y="14" width="7" height="7" rx="1"/><rect x="14" y="14" width="7" height="7" rx="1"/></svg>`,
  pill:`<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M10.5 20H4a2 2 0 0 1-2-2V5c0-1.1.9-2 2-2h3.93a2 2 0 0 1 1.66.9l.82 1.2a2 2 0 0 0 1.66.9H20a2 2 0 0 1 2 2v3"/><circle cx="18" cy="18" r="4"/><path d="m15.4 20.6 5.2-5.2"/></svg>`,
  supplier:`<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M3 9h18v10a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2V9z"/><path d="M3 9V6a2 2 0 0 1 2-2h14a2 2 0 0 1 2 2v3"/><path d="M12 12v5"/><path d="M8 12v5"/><path d="M16 12v5"/></svg>`,
  shelf:`<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="2" y="3" width="20" height="3" rx="1"/><rect x="2" y="10" width="20" height="3" rx="1"/><rect x="2" y="17" width="20" height="3" rx="1"/><path d="M6 6v4M10 6v4M14 6v4M18 6v4M6 13v4M10 13v4M14 13v4M18 13v4"/></svg>`,
  lot:`<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M20 7H4a2 2 0 0 0-2 2v9a2 2 0 0 0 2 2h16a2 2 0 0 0 2-2V9a2 2 0 0 0-2-2z"/><path d="M16 21V5a2 2 0 0 0-2-2h-4a2 2 0 0 0-2 2v16"/></svg>`,
  movement:`<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M7 16V4m0 0L3 8m4-4 4 4"/><path d="M17 8v12m0 0 4-4m-4 4-4-4"/></svg>`,
  alert:`<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M10.29 3.86 1.82 18a2 2 0 0 0 1.71 3h16.94a2 2 0 0 0 1.71-3L13.71 3.86a2 2 0 0 0-3.42 0z"/><line x1="12" y1="9" x2="12" y2="13"/><line x1="12" y1="17" x2="12.01" y2="17"/></svg>`,
  report:`<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z"/><polyline points="14 2 14 8 20 8"/><line x1="16" y1="13" x2="8" y2="13"/><line x1="16" y1="17" x2="8" y2="17"/><polyline points="10 9 9 9 8 9"/></svg>`,
  database:`<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><ellipse cx="12" cy="5" rx="9" ry="3"/><path d="M21 12c0 1.66-4 3-9 3s-9-1.34-9-3"/><path d="M3 5v14c0 1.66 4 3 9 3s9-1.34 9-3V5"/></svg>`,
  plus:`<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><line x1="12" y1="5" x2="12" y2="19"/><line x1="5" y1="12" x2="19" y2="12"/></svg>`,
  edit:`<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M11 4H4a2 2 0 0 0-2 2v14a2 2 0 0 0 2 2h14a2 2 0 0 0 2-2v-7"/><path d="M18.5 2.5a2.121 2.121 0 0 1 3 3L12 15l-4 1 1-4 9.5-9.5z"/></svg>`,
  trash:`<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polyline points="3 6 5 6 21 6"/><path d="M19 6l-1 14a2 2 0 0 1-2 2H8a2 2 0 0 1-2-2L5 6"/><path d="M10 11v6M14 11v6"/><path d="M9 6V4a1 1 0 0 1 1-1h4a1 1 0 0 1 1 1v2"/></svg>`,
  eye:`<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M1 12s4-8 11-8 11 8 11 8-4 8-11 8-11-8-11-8z"/><circle cx="12" cy="12" r="3"/></svg>`,
  search:`<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="11" cy="11" r="8"/><line x1="21" y1="21" x2="16.65" y2="16.65"/></svg>`,
  download:`<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"/><polyline points="7 10 12 15 17 10"/><line x1="12" y1="15" x2="12" y2="3"/></svg>`,
  upload:`<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"/><polyline points="17 8 12 3 7 8"/><line x1="12" y1="3" x2="12" y2="15"/></svg>`,
  logout:`<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M9 21H5a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h4"/><polyline points="16 17 21 12 16 7"/><line x1="21" y1="12" x2="9" y2="12"/></svg>`,
  x:`<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><line x1="18" y1="6" x2="6" y2="18"/><line x1="6" y1="6" x2="18" y2="18"/></svg>`,
  check:`<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polyline points="20 6 9 17 4 12"/></svg>`,
  chevron_left:`<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polyline points="15 18 9 12 15 6"/></svg>`,
  chevron_right:`<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polyline points="9 18 15 12 9 6"/></svg>`,
  bell:`<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M18 8A6 6 0 0 0 6 8c0 7-3 9-3 9h18s-3-2-3-9"/><path d="M13.73 21a2 2 0 0 1-3.46 0"/></svg>`,
  user:`<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M20 21v-2a4 4 0 0 0-4-4H8a4 4 0 0 0-4 4v2"/><circle cx="12" cy="7" r="4"/></svg>`,
  users:`<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M17 21v-2a4 4 0 0 0-4-4H5a4 4 0 0 0-4 4v2"/><circle cx="9" cy="7" r="4"/><path d="M23 21v-2a4 4 0 0 0-3-3.87"/><path d="M16 3.13a4 4 0 0 1 0 7.75"/></svg>`,
  lock:`<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="3" y="11" width="18" height="11" rx="2" ry="2"/><path d="M7 11V7a5 5 0 0 1 10 0v4"/></svg>`,
  unlock:`<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="3" y="11" width="18" height="11" rx="2" ry="2"/><path d="M7 11V7a5 5 0 0 1 9.9-1"/></svg>`,
  eye_off:`<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M17.94 17.94A10.07 10.07 0 0 1 12 20c-7 0-11-8-11-8a18.45 18.45 0 0 1 5.06-5.94"/><path d="M9.9 4.24A9.12 9.12 0 0 1 12 4c7 0 11 8 11 8a18.5 18.5 0 0 1-2.16 3.19"/><line x1="1" y1="1" x2="23" y2="23"/></svg>`,
  refresh:`<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polyline points="23 4 23 10 17 10"/><polyline points="1 20 1 14 7 14"/><path d="M3.51 9a9 9 0 0 1 14.85-3.36L23 10M1 14l4.64 4.36A9 9 0 0 0 20.49 15"/></svg>`,
  calendar:`<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="3" y="4" width="18" height="18" rx="2" ry="2"/><line x1="16" y1="2" x2="16" y2="6"/><line x1="8" y1="2" x2="8" y2="6"/><line x1="3" y1="10" x2="21" y2="10"/></svg>`,
  package:`<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><line x1="16.5" y1="9.4" x2="7.5" y2="4.21"/><path d="M21 16V8a2 2 0 0 0-1-1.73l-7-4a2 2 0 0 0-2 0l-7 4A2 2 0 0 0 3 8v8a2 2 0 0 0 1 1.73l7 4a2 2 0 0 0 2 0l7-4A2 2 0 0 0 21 16z"/><polyline points="3.27 6.96 12 12.01 20.73 6.96"/><line x1="12" y1="22.08" x2="12" y2="12"/></svg>`,
  arrow_up:`<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><line x1="12" y1="19" x2="12" y2="5"/><polyline points="5 12 12 5 19 12"/></svg>`,
  arrow_down:`<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><line x1="12" y1="5" x2="12" y2="19"/><polyline points="19 12 12 19 5 12"/></svg>`,
  barcode:`<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M3 5v14M8 5v14M12 5v14M17 5v14M21 5v14M3 5h2M3 19h2M19 5h2M19 19h2"/></svg>`,
  map_pin:`<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M21 10c0 7-9 13-9 13s-9-6-9-13a9 9 0 0 1 18 0z"/><circle cx="12" cy="10" r="3"/></svg>`,
  phone:`<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M22 16.92v3a2 2 0 0 1-2.18 2 19.79 19.79 0 0 1-8.63-3.07A19.5 19.5 0 0 1 4.07 11.5 19.79 19.79 0 0 1 1 2.84A2 2 0 0 1 3 .66h3a2 2 0 0 1 2 1.72 12.84 12.84 0 0 0 .7 2.81 2 2 0 0 1-.45 2.11L7.09 8.91a16 16 0 0 0 6 6l1.27-1.27a2 2 0 0 1 2.11-.45 12.84 12.84 0 0 0 2.81.7A2 2 0 0 1 22 16.92z"/></svg>`,
  mail:`<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M4 4h16c1.1 0 2 .9 2 2v12c0 1.1-.9 2-2 2H4c-1.1 0-2-.9-2-2V6c0-1.1.9-2 2-2z"/><polyline points="22,6 12,13 2,6"/></svg>`,
  info:`<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="10"/><line x1="12" y1="16" x2="12" y2="12"/><line x1="12" y1="8" x2="12.01" y2="8"/></svg>`,
  settings:`<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="3"/><path d="M19.4 15a1.65 1.65 0 0 0 .33 1.82l.06.06a2 2 0 0 1-2.83 2.83l-.06-.06a1.65 1.65 0 0 0-1.82-.33 1.65 1.65 0 0 0-1 1.51V21a2 2 0 0 1-4 0v-.09A1.65 1.65 0 0 0 9 19.4a1.65 1.65 0 0 0-1.82.33l-.06.06a2 2 0 0 1-2.83-2.83l.06-.06A1.65 1.65 0 0 0 4.68 15a1.65 1.65 0 0 0-1.51-1H3a2 2 0 0 1 0-4h.09A1.65 1.65 0 0 0 4.6 9a1.65 1.65 0 0 0-.33-1.82l-.06-.06a2 2 0 0 1 2.83-2.83l.06.06A1.65 1.65 0 0 0 9 4.68a1.65 1.65 0 0 0 1-1.51V3a2 2 0 0 1 4 0v.09a1.65 1.65 0 0 0 1 1.51 1.65 1.65 0 0 0 1.82-.33l.06-.06a2 2 0 0 1 2.83 2.83l-.06.06A1.65 1.65 0 0 0 19.4 9a1.65 1.65 0 0 0 1.51 1H21a2 2 0 0 1 0 4h-.09a1.65 1.65 0 0 0-1.51 1z"/></svg>`,
  trending_up:`<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polyline points="23 6 13.5 15.5 8.5 10.5 1 18"/><polyline points="17 6 23 6 23 12"/></svg>`,
  list:`<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><line x1="8" y1="6" x2="21" y2="6"/><line x1="8" y1="12" x2="21" y2="12"/><line x1="8" y1="18" x2="21" y2="18"/><line x1="3" y1="6" x2="3.01" y2="6"/><line x1="3" y1="12" x2="3.01" y2="12"/><line x1="3" y1="18" x2="3.01" y2="18"/></svg>`,
  clock:`<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="10"/><polyline points="12 6 12 12 16 14"/></svg>`,
  tag:`<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M20.59 13.41l-7.17 7.17a2 2 0 0 1-2.83 0L2 12V2h10l8.59 8.59a2 2 0 0 1 0 2.82z"/><line x1="7" y1="7" x2="7.01" y2="7"/></svg>`,
  layers:`<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polygon points="12 2 2 7 12 12 22 7 12 2"/><polyline points="2 17 12 22 22 17"/><polyline points="2 12 12 17 22 12"/></svg>`,
  filter:`<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polygon points="22 3 2 3 10 12.46 10 19 14 21 14 12.46 22 3"/></svg>`,
  box:`<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M21 16V8a2 2 0 0 0-1-1.73l-7-4a2 2 0 0 0-2 0l-7 4A2 2 0 0 0 3 8v8a2 2 0 0 0 1 1.73l7 4a2 2 0 0 0 2 0l7-4A2 2 0 0 0 21 16z"/></svg>`,
  money:`<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><line x1="12" y1="1" x2="12" y2="23"/><path d="M17 5H9.5a3.5 3.5 0 0 0 0 7h5a3.5 3.5 0 0 1 0 7H6"/></svg>`,
  money_in:`<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="2" y="5" width="20" height="14" rx="2"/><path d="M12 10v4m0 0-2-2m2 2 2-2"/><path d="M6 9h2M16 9h2"/></svg>`,
  money_out:`<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="2" y="5" width="20" height="14" rx="2"/><path d="M12 14v-4m0 0-2 2m2-2 2 2"/><path d="M6 15h2M16 15h2"/></svg>`,
  profit:`<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polyline points="22 7 13.5 15.5 8.5 10.5 2 17"/><polyline points="16 7 22 7 22 13"/><circle cx="12" cy="12" r="1" fill="currentColor"/></svg>`,
  kit_box:`<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M21 16V8a2 2 0 0 0-1-1.73l-7-4a2 2 0 0 0-2 0l-7 4A2 2 0 0 0 3 8v8a2 2 0 0 0 1 1.73l7 4a2 2 0 0 0 2 0l7-4A2 2 0 0 0 21 16z"/><polyline points="3.27 6.96 12 12.01 20.73 6.96"/><line x1="12" y1="22.08" x2="12" y2="12"/><line x1="8" y1="10" x2="16" y2="10"/></svg>`,
  activity:`<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polyline points="22 12 18 12 15 21 9 3 6 12 2 12"/></svg>`,
  pie_chart:`<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M21.21 15.89A10 10 0 1 1 8 2.83"/><path d="M22 12A10 10 0 0 0 12 2v10z"/></svg>`,
  bar_chart:`<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><line x1="18" y1="20" x2="18" y2="10"/><line x1="12" y1="20" x2="12" y2="4"/><line x1="6" y1="20" x2="6" y2="14"/><line x1="2" y1="20" x2="22" y2="20"/></svg>`,
  sync:`<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polyline points="23 4 23 10 17 10"/><polyline points="1 20 1 14 7 14"/><path d="M3.51 9a9 9 0 0 1 14.85-3.36L23 10M1 14l4.64 4.36A9 9 0 0 0 20.49 15"/></svg>`,
  cloud:`<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M18 10h-1.26A8 8 0 1 0 9 20h9a5 5 0 0 0 0-10z"/></svg>`,
  shield:`<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M12 22s8-4 8-10V5l-8-3-8 3v7c0 6 8 10 8 10z"/></svg>`,
};

function icon(name, cls='') {
  return `<span class="${cls}" style="display:inline-flex;align-items:center;">${ICONS[name]||''}</span>`;
}

// ===================== SEARCH FOCUS UTILITY =====================
// Restores focus + cursor to a search input after full-page re-render
function refocus(id) {
  requestAnimationFrame(() => {
    const el = document.getElementById(id);
    if (el) { el.focus(); const len = el.value.length; el.setSelectionRange(len, len); }
  });
}

// ===================== CRYPTO UTILITIES =====================
async function hashPassword(pwd) {
  const encoder = new TextEncoder();
  const data = encoder.encode(pwd);
  const hashBuffer = await crypto.subtle.digest('SHA-256', data);
  const hashArray = Array.from(new Uint8Array(hashBuffer));
  return hashArray.map(b => b.toString(16).padStart(2, '0')).join('');
}

// ===================== DATABASE =====================
const DB_KEY = 'hmm_db_v2';
const SETTINGS_KEY = 'hmm_settings_v2';

// Load/save global settings (theme, db_mode, etc.)
function loadSettings() {
  try { return JSON.parse(localStorage.getItem(SETTINGS_KEY)||'{}'); } catch { return {}; }
}
function saveSettings(s) { localStorage.setItem(SETTINGS_KEY, JSON.stringify(s)); }
let appSettings = loadSettings();

// ---- THEME (Lite / Dark) ----
function applyTheme(theme) {
  appSettings.theme = theme || 'dark';
  saveSettings(appSettings);
  document.body.classList.toggle('theme-light', appSettings.theme === 'light');
  const btn = document.getElementById('theme-toggle-btn');
  if (btn) {
    btn.title = appSettings.theme === 'light' ? 'Modo Escuro' : 'Modo Lite (Claro)';
    btn.innerHTML = appSettings.theme === 'light'
      ? `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M21 12.79A9 9 0 1 1 11.21 3 7 7 0 0 0 21 12.79z"/></svg>`
      : `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="5"/><line x1="12" y1="1" x2="12" y2="3"/><line x1="12" y1="21" x2="12" y2="23"/><line x1="4.22" y1="4.22" x2="5.64" y2="5.64"/><line x1="18.36" y1="18.36" x2="19.78" y2="19.78"/><line x1="1" y1="12" x2="3" y2="12"/><line x1="21" y1="12" x2="23" y2="12"/><line x1="4.22" y1="19.78" x2="5.64" y2="18.36"/><line x1="18.36" y1="5.64" x2="19.78" y2="4.22"/></svg>`;
  }
}
function toggleTheme() { applyTheme(appSettings.theme === 'light' ? 'dark' : 'light'); }

// ---- XLSX FILE DATABASE ----
let xlsxDirHandle = null; // File System Access API directory handle
const XLSX_DB_FILENAME = 'bandmed_database.xlsx';
const DB_MODE_KEY = 'hmm_db_mode'; // 'localstorage' | 'xlsx' | 'indexeddb' | 'firebase'

function getDbMode() { return localStorage.getItem(DB_MODE_KEY) || 'localstorage'; }
function setDbMode(m) { localStorage.setItem(DB_MODE_KEY, m); }

// ===================== INDEXEDDB BACKEND (suporta 12.000+ registos) =====================
const IDB_NAME = 'BandMedGestDB';
const IDB_STORE = 'data';
const IDB_KEY = 'main';

// Contador de escritas IDB em curso — usado pelo beforeunload para avisar o utilizador
let _idbPending = 0;

// Cache da conexão IDB para evitar abrir/fechar repetidamente
let _idbConnectionPromise = null;

function openIDB() {
  if (_idbConnectionPromise) return _idbConnectionPromise;
  _idbConnectionPromise = new Promise((resolve, reject) => {
    const req = indexedDB.open(IDB_NAME, 1);
    req.onupgradeneeded = (e) => {
      const dbInst = e.target.result;
      if (!dbInst.objectStoreNames.contains(IDB_STORE)) {
        dbInst.createObjectStore(IDB_STORE);
      }
    };
    req.onsuccess = (e) => {
      const conn = e.target.result;
      // Limpar cache se a ligação for fechada externamente
      conn.onclose = () => { _idbConnectionPromise = null; };
      conn.onversionchange = () => { conn.close(); _idbConnectionPromise = null; };
      resolve(conn);
    };
    req.onerror = (e) => {
      _idbConnectionPromise = null;
      reject(e.target.error);
    };
    req.onblocked = () => {
      _idbConnectionPromise = null;
      reject(new Error('IndexedDB bloqueado por outra aba. Feche outras abas do sistema e tente novamente.'));
    };
  });
  return _idbConnectionPromise;
}

async function saveToIDB(data) {
  _idbPending++;
  try {
    const idb = await openIDB();
    await new Promise((resolve, reject) => {
      const tx = idb.transaction(IDB_STORE, 'readwrite');
      const store = tx.objectStore(IDB_STORE);
      // Cópia profunda para evitar erros de objectos não-serializáveis
      const toSave = JSON.parse(JSON.stringify(data));
      const req = store.put(toSave, IDB_KEY);
      req.onerror = (e) => reject(e.target.error);
      tx.oncomplete = () => resolve(true);
      tx.onerror = (e) => reject(e.target.error);
      tx.onabort = (e) => reject(e.target.error || new Error('Transacção IDB abortada'));
    });
    console.log('[IDB] Dados guardados com sucesso no IndexedDB.');
  } catch(e) {
    console.error('[IDB] Erro ao guardar:', e);
    throw e;
  } finally {
    _idbPending = Math.max(0, _idbPending - 1);
  }
}

async function loadFromIDB() {
  try {
    const idb = await openIDB();
    return await new Promise((resolve, reject) => {
      const tx = idb.transaction(IDB_STORE, 'readonly');
      const store = tx.objectStore(IDB_STORE);
      const req = store.get(IDB_KEY);
      req.onsuccess = (e) => resolve(e.target.result || null);
      req.onerror = (e) => reject(e.target.error);
    });
  } catch(e) {
    console.warn('[IDB] Erro ao carregar:', e);
    return null;
  }
}

async function clearIDB() {
  try {
    const idb = await openIDB();
    return new Promise((resolve) => {
      const tx = idb.transaction(IDB_STORE, 'readwrite');
      tx.objectStore(IDB_STORE).delete(IDB_KEY);
      tx.oncomplete = () => resolve();
      tx.onerror = () => resolve();
    });
  } catch(e) {
    console.warn('[IDB] Erro ao limpar:', e);
  }
}

// Verificar se o IDB tem dados guardados (sem carregar o payload completo)
async function idbHasData() {
  try {
    const idb = await openIDB();
    return await new Promise((resolve) => {
      const tx = idb.transaction(IDB_STORE, 'readonly');
      const store = tx.objectStore(IDB_STORE);
      const req = store.count();
      req.onsuccess = (e) => resolve(e.target.result > 0);
      req.onerror = () => resolve(false);
    });
  } catch(e) { return false; }
}

// ===================== FIREBASE REALTIME DB (modo armazenamento) =====================
let _firebaseStoreConfig = JSON.parse(localStorage.getItem('hmm_firebase_store_cfg') || 'null');

function saveFirebaseStoreCfg(cfg) {
  _firebaseStoreConfig = cfg;
  localStorage.setItem('hmm_firebase_store_cfg', JSON.stringify(cfg));
}
function getFirebaseStoreCfg() {
  return _firebaseStoreConfig || JSON.parse(localStorage.getItem('hmm_firebase_store_cfg') || 'null');
}

async function saveToFirebaseStore(data) {
  const cfg = getFirebaseStoreCfg();
  if (!cfg || !cfg.url) throw new Error('Firebase não configurado');
  const url = `${cfg.url}/bandmed_data.json${cfg.key ? '?auth=' + cfg.key : ''}`;
  const payload = JSON.parse(JSON.stringify(data));
  const r = await fetch(url, {
    method: 'PUT',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload)
  });
  if (!r.ok) throw new Error('Firebase: erro HTTP ' + r.status);
  return true;
}

async function loadFromFirebaseStore() {
  const cfg = getFirebaseStoreCfg();
  if (!cfg || !cfg.url) return null;
  const url = `${cfg.url}/bandmed_data.json${cfg.key ? '?auth=' + cfg.key : ''}`;
  const r = await fetch(url);
  if (!r.ok) return null;
  return await r.json();
}



// ===================== NETWORK STATUS MANAGER =====================
let _isOnline = navigator.onLine;

function _netColor(online) { return online ? '#00b894' : '#e74c3c'; }
function _netBg(online)    { return online ? 'rgba(0,184,148,0.12)' : 'rgba(231,76,60,0.12)'; }
function _netBorder(online){ return online ? 'rgba(0,184,148,0.3)'  : 'rgba(231,76,60,0.25)'; }

function updateNetworkStatusUI() {
  const online = _isOnline;
  const cfg = getCloudCfg ? getCloudCfg() : null;
  const color = _netColor(online);
  const bg    = _netBg(online);
  const bdr   = _netBorder(online);

  // ── Header badge ──
  const hBadge = document.getElementById('header-net-badge');
  const hDot   = document.getElementById('header-net-dot');
  const hLabel = document.getElementById('header-net-label');
  if (hBadge) { hBadge.style.background=bg; hBadge.style.color=color; hBadge.style.border='1px solid '+bdr; }
  if (hDot)   { hDot.style.background=color; hDot.style.boxShadow='0 0 6px '+color+'99'; }
  if (hLabel) {
    if (online && cfg && getDbMode && getDbMode()==='cloud') hLabel.textContent = 'Online · ' + cloudDbTypeName(cfg.type);
    else if (online) hLabel.textContent = 'Online';
    else hLabel.textContent = 'Offline';
  }

  // ── Login screen status ──
  const lDot   = document.getElementById('login-net-dot');
  const lLabel = document.getElementById('login-net-label');
  const lWrap  = document.getElementById('login-net-status');
  if (lDot)   { lDot.style.background = color; lDot.style.boxShadow = '0 0 5px '+color+'88'; }
  if (lLabel) {
    if (online && cfg) lLabel.textContent = '☁️ Online · ' + cloudDbTypeName(cfg.type) + ' activo';
    else if (online)   lLabel.textContent = '🌐 Online · Modo local activo';
    else               lLabel.textContent = 'Offline · Dados armazenados localmente com segurança';
  }
  if (lWrap) lWrap.style.color = online ? color : 'var(--text-muted)';
}

async function handleGoOnline() {
  _isOnline = true;
  updateNetworkStatusUI();
  const cfg = getCloudCfg();
  const mode = getDbMode();
  if (cfg && mode === 'cloud') {
    toast('info', '🌐 Ligação restabelecida', 'A sincronizar dados locais com a nuvem (merge inteligente)...');
    await syncLocalToCloud();
  } else if (cfg && (mode === 'indexeddb' || mode === 'localstorage' || mode === 'xlsx')) {
    // Mesmo em modo local, se há config de nuvem, faz merge automático ao recuperar internet
    toast('info', '🌐 Ligação restabelecida', 'A sincronizar dados locais com a nuvem (merge inteligente)...');
    await syncLocalToCloud();
  } else if (!cfg) {
    // Sem configuração de nuvem: mostrar wizard automaticamente (se ainda não estiver aberto)
    if (!document.getElementById('cloud-setup-overlay') && !sessionStorage.getItem('hmm_cloud_wizard_skipped')) {
      showCloudSetupWizard();
    }
  }
}

function handleGoOffline() {
  _isOnline = false;
  updateNetworkStatusUI();
  if (getDbMode() === 'cloud') {
    toast('warning', '🔴 Sem ligação à internet', 'Os dados serão guardados localmente e sincronizados quando a ligação for restabelecida.');
  } else {
    toast('warning', '🔴 Sem ligação à internet', 'A continuar em modo local.');
  }
}

window.addEventListener('online',  () => handleGoOnline());
window.addEventListener('offline', () => handleGoOffline());

// ===================== MOTOR DE MERGE INTELIGENTE =====================
// Regras:
//   - INSERT: registo local que não existe na nuvem → adiciona na nuvem
//   - UPDATE: registo existe em ambos → vence o _updatedAt mais recente
//   - DELETE: registo marcado com ativo:false na fonte mais recente → propaga
//   - NUNCA apaga toda a BD na nuvem para reescrever
//
// Suporte: IndexedDB, localStorage, XLSX → Nuvem (Firebase RTDB / Supabase / REST)

const SYNC_TABLES = ['produtos','fornecedores','prateleiras','lotes','movimentacoes','kits','usuarios','logs'];

/**
 * Faz merge de dois arrays de registos pelo campo id.
 * @param {Array} localArr   - registos locais (IndexedDB / localStorage / XLSX)
 * @param {Array} cloudArr   - registos da nuvem
 * @returns {{ merged: Array, stats: {inserted:number, updated:number, skipped:number} }}
 */
function mergeRecords(localArr, cloudArr) {
  const stats = { inserted: 0, updated: 0, skipped: 0 };
  // Filtrar registos inválidos (sem id ou id nulo) antes do merge
  const safeCloud = (cloudArr || []).filter(r => r != null && r.id != null && r.id !== '');
  const safeLocal = (localArr  || []).filter(r => r != null && r.id != null && r.id !== '');

  // Indexar nuvem por id
  const cloudMap = new Map();
  safeCloud.forEach(r => cloudMap.set(String(r.id), r));

  // Indexar local por id
  const localMap = new Map();
  safeLocal.forEach(r => localMap.set(String(r.id), r));

  // Começar com cópia da nuvem (base)
  const result = new Map(cloudMap);

  for (const [id, localRec] of localMap) {
    if (!result.has(id)) {
      // Não existe na nuvem → INSERT
      result.set(id, { ...localRec });
      stats.inserted++;
    } else {
      const cloudRec = result.get(id);
      const localTs  = localRec._updatedAt  ? new Date(localRec._updatedAt).getTime()  : 0;
      const cloudTs  = cloudRec._updatedAt  ? new Date(cloudRec._updatedAt).getTime() : 0;
      if (localTs > cloudTs) {
        // Local é mais recente → UPDATE (mantém ativo:false se foi apagado localmente)
        result.set(id, { ...cloudRec, ...localRec });
        stats.updated++;
      } else {
        // Nuvem é igual ou mais recente → não alterar (skip)
        stats.skipped++;
      }
    }
  }

  return { merged: Array.from(result.values()), stats };
}

/**
 * Aplica o resultado do merge de volta à BD local (cloud → local).
 * Não destrói dados locais mais recentes.
 */
function mergeCloudIntoLocal(cloudData) {
  if (!cloudData) return { inserted:0, updated:0, skipped:0 };
  const totalStats = { inserted:0, updated:0, skipped:0 };
  SYNC_TABLES.forEach(table => {
    if (!Array.isArray(cloudData[table])) return;
    const { merged, stats } = mergeRecords(cloudData[table], db.data[table] || []);
    // Aqui a "nuvem" é a fonte autoritativa só quando mais recente
    // então invertemos: local vence se for mais recente, nuvem vence se for mais recente
    // mergeRecords já faz isso — o resultado merged é o estado correcto
    db.data[table] = merged;
    totalStats.inserted += stats.inserted;
    totalStats.updated  += stats.updated;
    totalStats.skipped  += stats.skipped;
  });
  return totalStats;
}

/**
 * Produz o payload a enviar para a nuvem após merge.
 * Recebe os dados actuais da nuvem e faz merge com os dados locais.
 * Retorna o objecto mesclado pronto para ser guardado na nuvem.
 */
function buildMergedCloudPayload(cloudData) {
  const result = JSON.parse(JSON.stringify(DEFAULT_DB));
  SYNC_TABLES.forEach(table => {
    let cloudArr = (cloudData && Array.isArray(cloudData[table])) ? cloudData[table] : [];
    // Filtrar nulos e registos sem id (Firebase arrays esparsos)
    cloudArr = cloudArr.filter(r => r != null && r.id != null && r.id !== '');
    const localArr = (db.data[table] || []).filter(r => r != null && r.id != null && r.id !== '');
    const { merged } = mergeRecords(localArr, cloudArr);
    result[table] = merged;
  });
  return result;
}

// ── Sincronizar dados locais → nuvem (MERGE inteligente) ──
async function syncLocalToCloud() {
  const cfg = getCloudCfg();
  if (!cfg) return;
  try {
    toast('info', '☁️ A sincronizar com a nuvem...', 'A ler dados actuais da nuvem...');

    // 1. Ler dados actuais da nuvem (para não os perder)
    let cloudData = null;
    try { cloudData = await CloudDB.loadAll(); } catch(e) { console.warn('[Sync] loadAll falhou:', e); }

    // Garantir que cloudData tem estrutura válida mesmo se nuvem está vazia
    if (!cloudData || typeof cloudData !== 'object') cloudData = {};
    SYNC_TABLES.forEach(t => {
      if (!Array.isArray(cloudData[t])) cloudData[t] = [];
      // Filtrar nulos que o Firebase pode guardar em arrays esparsos
      cloudData[t] = cloudData[t].filter(r => r != null && r.id != null && r.id !== '');
    });

    // Igualmente limpar dados locais de registos inválidos antes do merge
    SYNC_TABLES.forEach(t => {
      if (Array.isArray(db.data[t])) {
        db.data[t] = db.data[t].filter(r => r != null && r.id != null && r.id !== '');
      }
    });

    // 2. Fazer merge: local + nuvem → payload final
    const mergedPayload = buildMergedCloudPayload(cloudData);

    // 3. Contar estatísticas para feedback
    let totalInserted = 0, totalUpdated = 0;
    SYNC_TABLES.forEach(table => {
      const cloudArr = cloudData[table] || [];
      const localArr = db.data[table] || [];
      const { stats } = mergeRecords(localArr, cloudArr);
      totalInserted += stats.inserted;
      totalUpdated  += stats.updated;
    });

    // 4. Guardar na nuvem (PUT com dados mesclados)
    await CloudDB.saveAll(mergedPayload);

    // 5. Actualizar dados locais com o estado mesclado (para ficar em sincronia)
    const localUsers = db.data.usuarios;
    db.data = { ...JSON.parse(JSON.stringify(DEFAULT_DB)), ...mergedPayload };
    // Preservar utilizadores locais se não há na nuvem
    if (localUsers.length && !mergedPayload.usuarios.length) db.data.usuarios = localUsers;

    // 6. Cache local
    try { localStorage.setItem(DB_KEY, JSON.stringify(db.data)); } catch(e) {
      try { localStorage.setItem(DB_KEY, JSON.stringify({usuarios:db.data.usuarios})); } catch(e2) {}
    }
    if (getDbMode() === 'indexeddb') saveToIDB(db.data).catch(()=>{});

    toast('success', '✅ Sincronizado com a nuvem!',
      `Adicionados: ${totalInserted} | Actualizados: ${totalUpdated} | Sem conflito: OK`);
    if (currentPage && typeof navigateTo === 'function') navigateTo(currentPage);
  } catch(e) {
    console.error('[Sync] syncLocalToCloud erro:', e);
    toast('error', 'Sincronização falhou', 'Verifique a ligação e tente novamente. ' + (e.message || String(e)));
  }
}

// ── Sincronizar nuvem → dados locais (MERGE inteligente) ──
async function syncCloudToLocal() {
  const cfg = getCloudCfg();
  if (!cfg) return;
  try {
    toast('info', '☁️ A carregar dados da nuvem...', 'A fazer merge inteligente...');
    let cloudData = await CloudDB.loadAll();
    if (cloudData) {
      // Sanitizar dados da nuvem antes do merge (Firebase pode retornar null em arrays esparsos)
      if (typeof cloudData === 'object') {
        SYNC_TABLES.forEach(t => {
          if (!Array.isArray(cloudData[t])) cloudData[t] = cloudData[t] ? Object.values(cloudData[t]).filter(r => r != null) : [];
          cloudData[t] = cloudData[t].filter(r => r != null && r.id != null && r.id !== '');
        });
      }
      // Merge: nuvem vence apenas onde for mais recente que local
      const stats = mergeCloudIntoLocal(cloudData);
      try { localStorage.setItem(DB_KEY, JSON.stringify(db.data)); } catch(e) {
        try { localStorage.setItem(DB_KEY, JSON.stringify({usuarios:db.data.usuarios})); } catch(e2) {}
      }
      if (getDbMode() === 'indexeddb') saveToIDB(db.data).catch(()=>{});
      toast('success', '☁️ Nuvem mesclada com dados locais',
        `Novos: ${stats.inserted} | Actualizados: ${stats.updated} | Mantidos locais: ${stats.skipped}`);
    } else {
      toast('warning', 'Nuvem sem dados', 'Não foram encontrados dados na nuvem para carregar.');
    }
    if (currentPage && typeof navigateTo === 'function') navigateTo(currentPage);
  } catch(e) {
    toast('warning', 'Carregamento da nuvem falhou', 'A usar dados locais em cache.');
  }
}
// ===================== FIM MOTOR DE MERGE INTELIGENTE =====================

// ===================== CLOUD DATABASE (Universal: Firebase / Supabase / REST) =====================
// Detecta automaticamente o tipo de BD a partir do URL.
// Suportado: Firebase RTDB, Supabase, ou qualquer REST API com URL+Key.

const CLOUD_CFG_KEY = 'hmm_cloud_cfg';
let _cloudCfg = null;

function getCloudCfg() {
  if (_cloudCfg) return _cloudCfg;
  try { _cloudCfg = JSON.parse(localStorage.getItem(CLOUD_CFG_KEY) || 'null'); return _cloudCfg; } catch { return null; }
}
function saveCloudCfg(cfg) {
  _cloudCfg = cfg;
  localStorage.setItem(CLOUD_CFG_KEY, JSON.stringify(cfg));
}
function clearCloudCfg() {
  _cloudCfg = null;
  localStorage.removeItem(CLOUD_CFG_KEY);
}
function detectCloudDbType(url) {
  if (!url) return 'generic';
  const u = url.toLowerCase();
  if (u.includes('firebaseio.com')) return 'firebase';
  if (u.includes('supabase.co') || u.includes('supabase.io')) return 'supabase';
  return 'generic';
}
function cloudDbTypeName(type) {
  return type === 'firebase' ? 'Firebase RTDB' : type === 'supabase' ? 'Supabase' : 'Base de Dados REST';
}

// ── Adaptador Cloud DB ──────────────────────────────────────────────
const CloudDB = {
  get cfg() { return getCloudCfg(); },

  _head(extra) {
    const cfg = this.cfg || {};
    const h = { 'Content-Type': 'application/json' };
    if (cfg.type === 'supabase' && cfg.key) {
      h['apikey'] = cfg.key;
      h['Authorization'] = 'Bearer ' + cfg.key;
    } else if (cfg.key) {
      h['Authorization'] = 'Bearer ' + cfg.key;
    }
    return extra ? { ...h, ...extra } : h;
  },

  _url(path) {
    const cfg = this.cfg || {};
    if (cfg.type === 'firebase') return `${cfg.url}/${path}.json${cfg.key ? '?auth=' + cfg.key : ''}`;
    if (cfg.type === 'supabase') return `${cfg.url}/rest/v1/${path}`;
    return `${cfg.url}/${path}`;
  },

  async testConnection() {
    try {
      const cfg = this.cfg;
      const ctrl = new AbortController();
      const tid = setTimeout(() => ctrl.abort(), 8000);
      let r;
      if (cfg.type === 'firebase') {
        r = await fetch(this._url('bandmed_ping'), { signal: ctrl.signal });
      } else if (cfg.type === 'supabase') {
        r = await fetch(this._url('bandmed_store?limit=1'), { headers: this._head(), signal: ctrl.signal });
        clearTimeout(tid);
        // 401/403 = wrong key; anything else = connection ok (table may not exist yet)
        return r.status !== 401 && r.status !== 403;
      } else {
        r = await fetch(this._url('bandmed_ping'), { headers: this._head(), signal: ctrl.signal });
      }
      clearTimeout(tid);
      return r.status < 500;
    } catch { return false; }
  },

  async checkUsersExist() {
    try { const u = await this.loadUsers(); return u && u.length > 0; } catch { return false; }
  },

  async loadAll() {
    const cfg = this.cfg;
    try {
      let raw = null;
      if (cfg.type === 'firebase') {
        const r = await fetch(this._url('bandmed_data'));
        if (!r.ok) return null;
        raw = await r.json();
      } else if (cfg.type === 'supabase') {
        const r = await fetch(this._url('bandmed_store?store_key=eq.data'), { headers: this._head() });
        if (!r.ok) return null;
        const rows = await r.json();
        raw = (Array.isArray(rows) && rows.length) ? rows[0].store_value : null;
      } else {
        const r = await fetch(this._url('bandmed_data'), { headers: this._head() });
        if (!r.ok) return null;
        raw = await r.json();
      }
      // Normalizar: Firebase guarda arrays como objectos {0:{...},1:{...}}
      // Converter qualquer tabela que seja objecto (não-array) em array
      if (raw && typeof raw === 'object') {
        const TABLES = ['produtos','fornecedores','prateleiras','lotes','movimentacoes','kits','usuarios','logs'];
        TABLES.forEach(t => {
          if (raw[t] != null && !Array.isArray(raw[t]) && typeof raw[t] === 'object') {
            raw[t] = Object.values(raw[t]).filter(r => r != null);
          }
          if (!Array.isArray(raw[t])) raw[t] = [];
        });
      }
      return raw;
    } catch { return null; }
  },

  async saveAll(data) {
    const cfg = this.cfg;
    const payload = JSON.parse(JSON.stringify(data));
    if (cfg.type === 'firebase') {
      const r = await fetch(this._url('bandmed_data'), { method:'PUT', headers:this._head(), body:JSON.stringify(payload) });
      if (!r.ok) throw new Error('HTTP ' + r.status);
      return true;
    }
    if (cfg.type === 'supabase') {
      const r = await fetch(this._url('bandmed_store'), {
        method:'POST',
        headers: this._head({'Prefer':'resolution=merge-duplicates,return=minimal'}),
        body: JSON.stringify({store_key:'data', store_value:payload})
      });
      if (!r.ok) throw new Error('Supabase HTTP ' + r.status);
      return true;
    }
    const r = await fetch(this._url('bandmed_data'), { method:'PUT', headers:this._head(), body:JSON.stringify(payload) });
    if (!r.ok) throw new Error('HTTP ' + r.status);
    return true;
  },

  async loadUsers() {
    const cfg = this.cfg;
    try {
      if (cfg.type === 'firebase') {
        const r = await fetch(this._url('bandmed_usuarios'));
        if (!r.ok) return [];
        const data = await r.json();
        if (!data) return [];
        return Array.isArray(data) ? data : Object.values(data).filter(Boolean);
      }
      if (cfg.type === 'supabase') {
        const r = await fetch(this._url('bandmed_store?store_key=eq.usuarios'), { headers:this._head() });
        if (!r.ok) return [];
        const rows = await r.json();
        const val = (Array.isArray(rows) && rows.length) ? rows[0].store_value : null;
        return Array.isArray(val) ? val : [];
      }
      const r = await fetch(this._url('bandmed_usuarios'), { headers:this._head() });
      if (!r.ok) return [];
      const data = await r.json();
      return Array.isArray(data) ? data : Object.values(data||{}).filter(Boolean);
    } catch { return []; }
  },

  async saveUsers(users) {
    const cfg = this.cfg;
    if (cfg.type === 'firebase') {
      const obj = {};
      users.forEach(u => { obj['u' + u.id] = u; });
      const r = await fetch(this._url('bandmed_usuarios'), { method:'PUT', headers:this._head(), body:JSON.stringify(obj) });
      return r.ok;
    }
    if (cfg.type === 'supabase') {
      const r = await fetch(this._url('bandmed_store'), {
        method:'POST',
        headers: this._head({'Prefer':'resolution=merge-duplicates,return=minimal'}),
        body: JSON.stringify({store_key:'usuarios', store_value:users})
      });
      return r.ok;
    }
    const r = await fetch(this._url('bandmed_usuarios'), { method:'PUT', headers:this._head(), body:JSON.stringify(users) });
    return r.ok;
  },

  async pushLog(log) {
    const cfg = this.cfg;
    try {
      if (cfg.type === 'firebase') {
        // Firebase POST cria entrada com ID único automático
        const fbUrl = `${cfg.url}/bandmed_logs.json${cfg.key ? '?auth=' + cfg.key : ''}`;
        await fetch(fbUrl, { method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify(log) });
        return true;
      }
      if (cfg.type === 'supabase') {
        const r = await fetch(this._url('bandmed_store'), {
          method:'POST',
          headers: this._head({'Prefer':'resolution=merge-duplicates,return=minimal'}),
          body: JSON.stringify({store_key:'log_' + Date.now(), store_value:log})
        });
        return r.ok;
      }
      const r = await fetch(this._url('bandmed_logs'), { method:'POST', headers:this._head(), body:JSON.stringify(log) });
      return r.ok;
    } catch { return false; }
  }
};

// ── Gerar senha admin aleatória ──
function generateAdminPassword() {
  const chars = 'ABCDEFGHJKLMNPQRSTUVWXYZabcdefghjkmnpqrstuvwxyz23456789@#!';
  return Array.from({length:12}, () => chars[Math.floor(Math.random()*chars.length)]).join('');
}

// ── Criar utilizador administrador na nuvem ──
async function createCloudAdmin() {
  const adminUsername = 'admin';
  const adminPwd = generateAdminPassword();
  const hashed = await hashPassword(adminPwd);
  const adminUser = { id:1, username:adminUsername, senha:hashed, nome:'Administrador', funcao:'Administrador', ativo:true, criadoEm:new Date().toISOString() };
  try {
    await CloudDB.saveUsers([adminUser]);
    db.data.usuarios = [adminUser];
    try { localStorage.setItem(DB_KEY, JSON.stringify({usuarios:[adminUser]})); } catch(e) {}
    showAdminCredentials(adminUsername, adminPwd);
  } catch(e) {
    toast('error','Erro ao criar administrador', e.message);
    showScreen('login');
  }
}

// ── Mostrar credenciais admin por 30 segundos ──
function showAdminCredentials(username, password) {
  let secs = 30;
  const overlay = document.createElement('div');
  overlay.id = 'admin-cred-overlay';
  overlay.style.cssText = 'position:fixed;inset:0;background:rgba(0,0,0,0.92);z-index:9999;display:flex;align-items:center;justify-content:center;padding:20px;box-sizing:border-box;';
  const render = () => {
    overlay.innerHTML = `
      <div style="background:var(--card-bg);border-radius:16px;padding:32px;max-width:420px;width:100%;box-shadow:0 24px 48px rgba(0,0,0,0.5);border:2px solid var(--accent);text-align:center;">
        <div style="font-size:36px;margin-bottom:10px;">🔑</div>
        <div style="font-size:17px;font-weight:700;color:var(--text-primary);margin-bottom:4px;">Conta Administrador Criada!</div>
        <div style="font-size:12px;color:var(--text-muted);margin-bottom:20px;">Guarde estes dados agora — <strong style="color:#e74c3c;">não serão mostrados novamente</strong></div>
        <div style="background:rgba(0,0,0,0.35);border-radius:12px;padding:18px;margin-bottom:18px;text-align:left;">
          <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:12px;padding-bottom:12px;border-bottom:1px solid var(--border);">
            <span style="font-size:12px;color:var(--text-muted);">Utilizador</span>
            <span style="font-size:15px;font-weight:700;color:var(--accent);font-family:monospace;">${username}</span>
          </div>
          <div style="display:flex;justify-content:space-between;align-items:center;">
            <span style="font-size:12px;color:var(--text-muted);">Senha</span>
            <span style="font-size:15px;font-weight:700;color:#e74c3c;font-family:monospace;letter-spacing:2px;">${password}</span>
          </div>
        </div>
        <div style="margin-bottom:18px;">
          <div style="display:inline-flex;align-items:center;gap:8px;background:rgba(231,76,60,0.12);border:1px solid rgba(231,76,60,0.3);border-radius:20px;padding:7px 18px;">
            <span style="font-size:20px;font-weight:800;color:#e74c3c;min-width:26px;text-align:center;">${secs}</span>
            <span style="font-size:12px;color:var(--text-muted);">segundos restantes</span>
          </div>
        </div>
        <div style="font-size:11px;color:var(--text-muted);margin-bottom:14px;">Após o login, crie outros utilizadores na secção <strong>Utilizadores</strong>.</div>
        <button class="btn btn-primary" style="width:100%;font-size:13px;" onclick="document.getElementById('admin-cred-overlay').remove();showScreen('login');">
          ✓ Já guardei — Ir para Login
        </button>
      </div>`;
  };
  render();
  document.body.appendChild(overlay);
  const iv = setInterval(() => {
    secs--;
    if (secs <= 0) { clearInterval(iv); overlay.remove(); showScreen('login'); toast('info','Sessão pronta','Faça login com as credenciais de administrador.'); }
    else render();
  }, 1000);
}

async function testCloudConnectionUI() {
  const cfg = getCloudCfg();
  if (!cfg) { toast('error','Sem configuração de nuvem'); return; }
  toast('info','A testar ligação ' + cloudDbTypeName(cfg.type) + '...');
  const ok = await CloudDB.testConnection();
  if (ok) toast('success','Nuvem acessível!','Ligação ' + cloudDbTypeName(cfg.type) + ' estabelecida com sucesso.');
  else toast('error','Nuvem inacessível','Verifique a ligação à internet e as credenciais.');
}

async function saveCloudCfgFromUI(url, key) {
  url = (url||'').trim().replace(/\/+$/, '');
  key = (key||'').trim();
  if (!url || !key) { toast('error','URL e chave são obrigatórios'); return; }
  const type = detectCloudDbType(url);
  const cfg = { url, key, type, connectedAt: new Date().toISOString() };
  _cloudCfg = cfg;
  const ok = await CloudDB.testConnection();
  if (!ok) { toast('error','Ligação falhou','Verifique o URL e a chave.'); _cloudCfg = null; return; }
  saveCloudCfg(cfg);
  setDbMode('cloud');
  toast('success','Nuvem configurada!', cloudDbTypeName(type) + ' activo.');
  renderBaseDados();
}

// ── Assistente de configuração da nuvem (wizard) ──
function showCloudSetupWizard() {
  // Don't show if already configured or overlay exists
  if (document.getElementById('cloud-setup-overlay')) return;
  const overlay = document.createElement('div');
  overlay.id = 'cloud-setup-overlay';
  overlay.style.cssText = 'position:fixed;inset:0;background:rgba(0,0,0,0.87);z-index:9998;display:flex;align-items:center;justify-content:center;padding:20px;box-sizing:border-box;';
  overlay.innerHTML = `
    <div style="background:var(--card-bg);border-radius:16px;padding:28px;max-width:510px;width:100%;box-shadow:0 24px 48px rgba(0,0,0,0.45);border:1px solid var(--border);">
      <div style="text-align:center;margin-bottom:22px;">
        <div style="width:54px;height:54px;border-radius:50%;background:linear-gradient(135deg,#00b894,#0a2463);display:flex;align-items:center;justify-content:center;margin:0 auto 10px;font-size:24px;">☁️</div>
        <div style="font-size:17px;font-weight:700;color:var(--text-primary);">Ligação à Internet Detectada</div>
        <div style="font-size:12px;color:var(--text-muted);margin-top:6px;">Configure a base de dados em nuvem para acesso multi-dispositivo.<br>Pode ignorar e usar armazenamento local.</div>
      </div>
      <div style="margin-bottom:14px;">
        <label style="font-size:11px;font-weight:600;color:var(--text-secondary);display:block;margin-bottom:5px;">URL da Base de Dados <span style="color:#e74c3c;">*</span></label>
        <input id="cld-url" class="field-input" placeholder="https://projecto.firebaseio.com  ou  https://xxx.supabase.co" style="width:100%;box-sizing:border-box;font-size:12px;" oninput="onCloudUrlInput(this.value)">
        <div id="cld-type-hint" style="font-size:10px;color:var(--text-muted);margin-top:3px;">Compatível com Firebase RTDB, Supabase ou qualquer REST API</div>
      </div>
      <div style="margin-bottom:16px;">
        <label style="font-size:11px;font-weight:600;color:var(--text-secondary);display:block;margin-bottom:5px;">Chave de Acesso / API Key <span style="color:#e74c3c;">*</span></label>
        <div style="position:relative;">
          <input id="cld-key" class="field-input" type="password" placeholder="Token ou chave de autenticação..." style="width:100%;box-sizing:border-box;padding-right:40px;font-size:12px;">
          <button onclick="var i=document.getElementById('cld-key');i.type=i.type==='password'?'text':'password'" style="position:absolute;right:10px;top:50%;transform:translateY(-50%);background:none;border:none;color:var(--text-muted);cursor:pointer;font-size:15px;">👁</button>
        </div>
      </div>
      <div id="cld-sb-hint" style="display:none;padding:10px 12px;background:rgba(61,213,152,0.07);border:1px dashed rgba(61,213,152,0.3);border-radius:8px;font-size:11px;color:var(--text-muted);margin-bottom:14px;">
        <strong style="color:#3DD598;">Supabase detectado.</strong> Execute este SQL no <em>SQL Editor</em> do Supabase antes de continuar:<br>
        <code style="display:block;margin-top:6px;background:rgba(0,0,0,0.3);padding:8px 10px;border-radius:6px;font-size:10px;line-height:1.6;white-space:pre;">CREATE TABLE IF NOT EXISTS bandmed_store (
  store_key text PRIMARY KEY,
  store_value jsonb
);
ALTER TABLE bandmed_store ENABLE ROW LEVEL SECURITY;
CREATE POLICY "allow_all" ON bandmed_store
  FOR ALL USING (true) WITH CHECK (true);</code>
      </div>
      <div id="cld-status" style="display:none;padding:9px 13px;border-radius:8px;font-size:12px;margin-bottom:14px;"></div>
      <div style="display:flex;gap:10px;">
        <button class="btn btn-secondary" style="flex:1;font-size:12px;" onclick="skipCloudSetupWizard()">Ignorar — Usar Local</button>
        <button id="cld-btn" class="btn btn-primary" style="flex:2;font-size:12px;" onclick="submitCloudSetup()">☁ Conectar e Guardar</button>
      </div>
    </div>`;
  document.body.appendChild(overlay);
}

function onCloudUrlInput(val) {
  const type = detectCloudDbType(val.trim());
  const hint = document.getElementById('cld-type-hint');
  const sbHint = document.getElementById('cld-sb-hint');
  if (!hint || !sbHint) return;
  if (val.trim().length > 10) {
    hint.textContent = '✓ Tipo detectado: ' + cloudDbTypeName(type);
    hint.style.color = 'var(--accent)';
    sbHint.style.display = type === 'supabase' ? 'block' : 'none';
  } else {
    hint.textContent = 'Compatível com Firebase RTDB, Supabase ou qualquer REST API';
    hint.style.color = 'var(--text-muted)';
    sbHint.style.display = 'none';
  }
}

function setCloudStatus(type, msg) {
  const el = document.getElementById('cld-status');
  if (!el) return;
  el.style.display = 'block';
  const c = {error:'#e74c3c', info:'var(--info)', success:'var(--accent)', warning:'#FFA000'}[type]||'var(--info)';
  el.style.background = c + '18';
  el.style.border = '1px solid ' + c + '45';
  el.style.color = c;
  el.textContent = msg;
}

function skipCloudSetupWizard() {
  document.getElementById('cloud-setup-overlay')?.remove();
  // Mark as skipped so wizard doesn't appear on next startup this session
  sessionStorage.setItem('hmm_cloud_wizard_skipped', '1');
  if (db.data.usuarios.length === 0) { showScreen('setup'); setupFirstRun(); }
  else showScreen('login');
}

async function submitCloudSetup() {
  const url = (document.getElementById('cld-url')?.value || '').trim().replace(/\/+$/, '');
  const key = (document.getElementById('cld-key')?.value || '').trim();
  if (!url) { setCloudStatus('error','O URL da base de dados é obrigatório.'); return; }
  if (!key) { setCloudStatus('error','A chave de acesso é obrigatória.'); return; }
  const btn = document.getElementById('cld-btn');
  if (btn) { btn.disabled = true; btn.textContent = 'A conectar...'; }
  const type = detectCloudDbType(url);
  const cfg = { url, key, type, connectedAt: new Date().toISOString() };
  _cloudCfg = cfg; // temp before saving
  setCloudStatus('info', 'A testar ligação ' + cloudDbTypeName(type) + '...');
  const ok = await CloudDB.testConnection();
  if (!ok) {
    setCloudStatus('error','Não foi possível conectar. Verifique o URL e a chave de acesso.');
    if (btn) { btn.disabled=false; btn.textContent='☁ Conectar e Guardar'; }
    _cloudCfg = null;
    return;
  }
  setCloudStatus('info','Ligação estabelecida! A verificar utilizadores...');
  saveCloudCfg(cfg);
  setDbMode('cloud');
  const hasUsers = await CloudDB.checkUsersExist();
  document.getElementById('cloud-setup-overlay')?.remove();
  if (hasUsers) {
    const cloudUsers = await CloudDB.loadUsers();
    if (cloudUsers && cloudUsers.length) {
      db.data.usuarios = cloudUsers;
      try { localStorage.setItem(DB_KEY, JSON.stringify({usuarios:cloudUsers})); } catch(e) {}
    }
    toast('success', cloudDbTypeName(type) + ' conectado!', 'Utilizadores carregados. Faça login.');
    showScreen('login');
  } else {
    toast('info','A criar conta de administrador...','Aguarde...');
    await createCloudAdmin();
  }
}

// ── Reconectar nuvem automaticamente ao iniciar ──
async function cloudAutoReconnect(onDone) {
  const cfg = getCloudCfg();
  if (!cfg) { onDone && onDone(false); return; }
  _cloudCfg = cfg;
  try {
    const cloudUsers = await CloudDB.loadUsers();
    if (cloudUsers && cloudUsers.length) {
      db.data.usuarios = cloudUsers;
      try { localStorage.setItem(DB_KEY, JSON.stringify({usuarios:cloudUsers})); } catch(e) {}
      toast('success', cloudDbTypeName(cfg.type) + ' reconectado', 'Sessão restabelecida automaticamente.');
      onDone && onDone(true);
      return;
    }
  } catch(e) { console.warn('[Cloud] Auto-reconnect falhou:', e); }
  toast('warning','Nuvem inacessível','A usar dados locais em cache.');
  onDone && onDone(false);
}

// ── Desligar da nuvem ──
function disconnectCloud() {
  clearCloudCfg();
  setDbMode('localstorage');
  toast('info','Desligado da nuvem','Modo local activado.');
  renderBaseDados();
}

async function pickXlsxFolder() {
  if (!window.showDirectoryPicker) {
    toast('error','Não suportado','O seu browser não suporta selecção de pasta. Use Chrome ou Edge.');
    return false;
  }
  try {
    xlsxDirHandle = await window.showDirectoryPicker({ mode:'readwrite' });
    localStorage.setItem('hmm_xlsx_dir_name', xlsxDirHandle.name);
    setDbMode('xlsx');
    toast('success','Pasta seleccionada',`Base de dados: ${xlsxDirHandle.name}/${XLSX_DB_FILENAME}`);
    return true;
  } catch(e) {
    if (e.name !== 'AbortError') toast('error','Erro ao seleccionar pasta', e.message);
    return false;
  }
}

async function reconnectXlsxFolder() {
  if (!window.showDirectoryPicker) return false;
  try {
    xlsxDirHandle = await window.showDirectoryPicker({ mode:'readwrite' });
    toast('success','Pasta reconectada', xlsxDirHandle.name);
    return true;
  } catch(e) {
    if (e.name !== 'AbortError') toast('warning','Reconexão cancelada','A usar localStorage temporariamente.');
    return false;
  }
}

async function writeXlsxDb(data) {
  if (!xlsxDirHandle) return false;
  try {
    const tables = ['produtos','fornecedores','prateleiras','lotes','movimentacoes','kits'];
    const sheets = tables.map(t => ({ name: t.charAt(0).toUpperCase()+t.slice(1), data: data[t] || [] }));
    sheets.push({ name:'Meta', data:[{'Versao':'3.0','Exportado':new Date().toISOString()}] });
    const bytes = XLSXio.write(sheets);
    const fh = await xlsxDirHandle.getFileHandle(XLSX_DB_FILENAME, { create: true });
    const writable = await fh.createWritable();
    await writable.write(bytes);
    await writable.close();
    return true;
  } catch(e) {
    console.warn('writeXlsxDb error:', e);
    return false;
  }
}

async function readXlsxDb() {
  if (!xlsxDirHandle) return null;
  try {
    const fh = await xlsxDirHandle.getFileHandle(XLSX_DB_FILENAME);
    const file = await fh.getFile();
    const buf = await file.arrayBuffer();
    // Usar Web Worker para não travar o browser
    let sheets;
    try {
      sheets = await parseXLSXInWorker(buf);
    } catch(e) {
      console.warn('readXlsxDb worker falhou, fallback directo:', e.message);
      sheets = await XLSXio.read(buf);
    }
    const tableMap = {
      'Produtos':'produtos','Fornecedores':'fornecedores','Prateleiras':'prateleiras',
      'Lotes':'lotes','Movimentacoes':'movimentacoes','Kits':'kits'
    };
    const result = JSON.parse(JSON.stringify(DEFAULT_DB));
    for (const [sn, rows] of Object.entries(sheets)) {
      const key = tableMap[sn];
      if (key && Array.isArray(rows) && rows.length && !rows[0].info) {
        result[key] = rows.map(r => {
          const obj = {...r};
          if (obj.id) obj.id = Number(obj.id);
          if (key === 'kits' && typeof obj.componentes === 'string') {
            try { obj.componentes = JSON.parse(obj.componentes); } catch { obj.componentes = []; }
          }
          return obj;
        });
      }
    }
    return result;
  } catch(e) {
    if (e.name !== 'NotFoundError') console.warn('readXlsxDb error:', e);
    return null;
  }
}

// Empty database — credentials are NEVER in source code
const DEFAULT_DB = {
  usuarios: [],        // Populated via first-run setup wizard
  produtos: [],
  fornecedores: [],
  prateleiras: [],
  lotes: [],
  movimentacoes: [],
  kits: [],
  logs: []             // System activity logs
};

// Agendador de merge com debounce: evita fazer merge na nuvem em cada save() rápido
let _mergeCloudTimer = null;
function _scheduleMergeToCloud() {
  if (_mergeCloudTimer) clearTimeout(_mergeCloudTimer);
  _mergeCloudTimer = setTimeout(async () => {
    _mergeCloudTimer = null;
    try { await syncLocalToCloud(); } catch(e) { console.warn('[Cloud] merge agendado falhou:', e); }
  }, 3000); // 3 segundos de debounce
}

class Database {
  constructor() { this.data = this.load(); }
  load() {
    try {
      const mode = getDbMode();
      // Modo IndexedDB: carregar apenas utilizadores do localStorage (para login rápido).
      // Os dados completos serão carregados assincronamente por loadAsync().
      if (mode === 'indexeddb') {
        const raw = localStorage.getItem(DB_KEY);
        const base = JSON.parse(JSON.stringify(DEFAULT_DB));
        if (raw) {
          const parsed = JSON.parse(raw);
          // Manter apenas os utilizadores do localStorage para autenticação
          if (parsed && parsed.usuarios && parsed.usuarios.length) {
            base.usuarios = parsed.usuarios;
          }
        }
        return base;
      }
      // Para outros modos, carregar dados completos do localStorage normalmente
      const raw = localStorage.getItem(DB_KEY);
      if (raw) {
        const parsed = JSON.parse(raw);
        const base = JSON.parse(JSON.stringify(DEFAULT_DB));
        return { ...base, ...parsed };
      }
      return JSON.parse(JSON.stringify(DEFAULT_DB));
    } catch { return JSON.parse(JSON.stringify(DEFAULT_DB)); }
  }
  // Carregamento assíncrono para IDB e Firebase
  async loadAsync() {
    const mode = getDbMode();
    try {
      if (mode === 'indexeddb') {
        const idbData = await loadFromIDB();
        if (idbData) {
          // Preservar utilizadores do localStorage (mais recentes / autenticados)
          const lsUsers = (() => {
            try {
              const raw = localStorage.getItem(DB_KEY);
              if (!raw) return [];
              const p = JSON.parse(raw);
              return (p && p.usuarios) ? p.usuarios : [];
            } catch(e) { return []; }
          })();
          this.data = { ...JSON.parse(JSON.stringify(DEFAULT_DB)), ...idbData };
          if (lsUsers.length) this.data.usuarios = lsUsers;
          console.log('[IDB] loadAsync: dados carregados do IndexedDB — ' +
            Object.entries(this.data).map(([k,v]) => `${k}:${Array.isArray(v)?v.length:'-'}`).join(', '));
          return true;
        } else {
          console.warn('[IDB] loadAsync: IndexedDB vazio. O modo IndexedDB está activo mas não há dados guardados.');
          return false;
        }
      } else if (mode === 'firebase') {
        const fbData = await loadFromFirebaseStore();
        if (fbData) {
          const users = this.data.usuarios;
          this.data = { ...JSON.parse(JSON.stringify(DEFAULT_DB)), ...fbData };
          this.data.usuarios = users.length ? users : this.data.usuarios;
          return true;
        }
      } else if (mode === 'cloud') {
        const cfg = getCloudCfg();
        if (cfg) {
          _cloudCfg = cfg;
          const cloudData = await CloudDB.loadAll();
          if (cloudData) {
            this.data = { ...JSON.parse(JSON.stringify(DEFAULT_DB)), ...cloudData };
          }
          const cloudUsers = await CloudDB.loadUsers();
          if (cloudUsers && cloudUsers.length) {
            this.data.usuarios = cloudUsers;
            try { localStorage.setItem(DB_KEY, JSON.stringify({usuarios:cloudUsers})); } catch(e) {}
          }
          return !!(cloudData || (cloudUsers && cloudUsers.length));
        }
      }
    } catch(e) {
      console.warn('[DB] loadAsync falhou:', e);
    }
    return false;
  }
  save() {
    const mode = getDbMode();
    const usersOnly = { usuarios: this.data.usuarios };

    // --- Guardar sempre no backend activo primeiro ---
    if (mode === 'xlsx' && xlsxDirHandle) {
      writeXlsxDb(this.data).catch(()=>{});
    } else if (mode === 'indexeddb') {
      // IDB é o backend principal — guardar dados completos lá
      saveToIDB(this.data).catch(e => console.warn('[IDB] save falhou:', e));
      // No localStorage guardar apenas utilizadores (para login rápido)
      try { localStorage.setItem(DB_KEY, JSON.stringify(usersOnly)); } catch(e) {}
      return; // IDB trata de tudo, não precisamos de mais nada
    } else if (mode === 'firebase') {
      saveToFirebaseStore(this.data).catch(e => console.warn('[Firebase] save falhou:', e));
      try { localStorage.setItem(DB_KEY, JSON.stringify(usersOnly)); } catch(e) {}
      return;
    } else if (mode === 'cloud') {
      const cfg = getCloudCfg();
      if (cfg && _isOnline) {
        // Online: guardar localmente e agendar merge na nuvem (debounced para não fazer merge em cada tecla)
        _scheduleMergeToCloud();
      }
      // Sempre manter cache local completa para funcionar offline
      try { localStorage.setItem(DB_KEY, JSON.stringify(this.data)); } catch(e) {
        try { localStorage.setItem(DB_KEY, JSON.stringify(usersOnly)); } catch(e2) {}
      }
      return;
    }

    // --- Modo localStorage: tentar guardar dados completos ---
    try {
      localStorage.setItem(DB_KEY, JSON.stringify(this.data));
    } catch(lsErr) {
      // LocalStorage cheio — guardar apenas utilizadores para não perder o login
      // NÃO mudar o modo automaticamente: o utilizador deve decidir na secção Base de Dados
      console.warn('[DB] LocalStorage cheio. Dados completos não foram guardados.');
      try { localStorage.setItem(DB_KEY, JSON.stringify(usersOnly)); } catch(e) {
        console.error('[DB] Impossível guardar mesmo só utilizadores:', e);
      }
      // Aviso único por sessão para não spam
      if (!this._lsFullWarned) {
        this._lsFullWarned = true;
        toast('warning', 'Armazenamento quase cheio',
          'O LocalStorage está cheio e alguns dados podem não ter sido guardados. Vá a Base de Dados → IndexedDB para activar um armazenamento sem limites.');
      }
    }
  }
  async loadFromXlsx() {
    const xdata = await readXlsxDb();
    if (xdata) {
      // Keep usuarios from localStorage (security)
      const users = this.data.usuarios;
      this.data = { ...JSON.parse(JSON.stringify(DEFAULT_DB)), ...xdata };
      this.data.usuarios = users;
    }
  }
  nextId(table) {
    const items = this.data[table];
    return items.length ? Math.max(...items.map(i => Number(i.id)||0)) + 1 : 1;
  }
  getAll(table, includeDeleted=false) {
    return (this.data[table]||[]).filter(r => includeDeleted || r.ativo !== false);
  }
  getById(table, id) { return (this.data[table]||[]).find(r => Number(r.id) === Number(id)); }
  insert(table, item) {
    item.id = this.nextId(table);
    item.ativo = true;
    item._updatedAt = new Date().toISOString();
    this.data[table].push(item);
    this.save();
    if (table !== 'logs') addLog('insert', table, item.id, item);
    return item;
  }
  update(table, id, updates) {
    const idx = this.data[table].findIndex(r => Number(r.id) === Number(id));
    if (idx === -1) return false;
    updates._updatedAt = new Date().toISOString();
    this.data[table][idx] = { ...this.data[table][idx], ...updates };
    this.save();
    if (table !== 'logs') addLog('update', table, id, updates);
    return true;
  }
  remove(table, id) { return this.update(table, id, { ativo: false }); }
  clear() {
    this.data = { ...JSON.parse(JSON.stringify(DEFAULT_DB)), usuarios: this.data.usuarios };
    this.save();
  }
  getStock(produtoId) {
    const movs = this.getAll('movimentacoes').filter(m => Number(m.produto_id) === Number(produtoId));
    const entradas = movs.filter(m=>(m.tipo||'').toUpperCase()==='ENTRADA').reduce((s,m)=>s+(Number(m.quantidade)||0),0);
    const saidas = movs.filter(m=>(m.tipo||'').toUpperCase()==='SAÍDA').reduce((s,m)=>s+(Number(m.quantidade)||0),0);
    return { entradas, saidas, stock: entradas - saidas };
  }
  getLoteStock(loteId) {
    const movs = this.getAll('movimentacoes').filter(m => Number(m.lote_id) === Number(loteId));
    const entradas = movs.filter(m=>(m.tipo||'').toUpperCase()==='ENTRADA').reduce((s,m)=>s+(Number(m.quantidade)||0),0);
    const saidas = movs.filter(m=>(m.tipo||'').toUpperCase()==='SAÍDA').reduce((s,m)=>s+(Number(m.quantidade)||0),0);
    return entradas - saidas;
  }
  getShelfCount(prateleiraId) {
    return this.getAll('produtos').filter(p=>Number(p.prateleira_id)===Number(prateleiraId)).length;
  }
  getShelfProducts(prateleiraId) {
    return this.getAll('produtos').filter(p=>Number(p.prateleira_id)===Number(prateleiraId));
  }
}

const db = new Database();

// ===================== LOGGING SYSTEM =====================
const TABLE_LABELS = {
  produtos: 'Produtos', fornecedores: 'Fornecedores', prateleiras: 'Prateleiras',
  lotes: 'Lotes', movimentacoes: 'Movimentações', kits: 'Kits', usuarios: 'Utilizadores'
};
const ACTION_LABELS = {
  insert: 'Criação', update: 'Actualização', remove: 'Eliminação',
  login: 'Login', logout: 'Logout', view: 'Visualização', export: 'Exportação', clear: 'Limpeza'
};
const ACTION_COLORS = {
  insert: '#22c55e', update: '#3b82f6', remove: '#ef4444',
  login: '#8b5cf6', logout: '#f59e0b', view: '#6b7280', export: '#06b6d4', clear: '#f97316'
};

function addLog(action, module, recordId, details) {
  const now = new Date();
  const log = {
    timestamp: now.toISOString(),
    date: now.toLocaleDateString('pt-PT'),
    time: now.toLocaleTimeString('pt-PT'),
    action,
    module: module || '',
    module_label: TABLE_LABELS[module] || module || '',
    record_id: recordId || null,
    user_id: currentUser?.id || null,
    user_name: currentUser?.nome || 'Sistema',
    user_role: currentUser?.funcao || '',
    details: details ? JSON.stringify(details).slice(0, 200) : '',
    description: buildLogDescription(action, module, recordId, details)
  };
  const existing = db.data.logs || [];
  log.id = existing.length ? Math.max(...existing.map(l => Number(l.id)||0)) + 1 : 1;
  log.ativo = true;
  db.data.logs.push(log);
  // Keep max 2000 logs (remove oldest)
  if (db.data.logs.length > 2000) db.data.logs = db.data.logs.slice(-2000);
  db.save();
  // Sincronizar log com a nuvem em tempo real
  if (getDbMode() === 'cloud' && getCloudCfg()) {
    CloudDB.pushLog(log).catch(() => {});
  }
}

function buildLogDescription(action, module, recordId, details) {
  const mod = TABLE_LABELS[module] || module || '';
  const user = currentUser?.nome || 'Sistema';
  if (action === 'insert') return `${user} criou um registo em ${mod} (ID: ${recordId})`;
  if (action === 'update') {
    if (details && details.ativo === false) return `${user} eliminou um registo em ${mod} (ID: ${recordId})`;
    return `${user} actualizou um registo em ${mod} (ID: ${recordId})`;
  }
  if (action === 'remove') return `${user} eliminou um registo em ${mod} (ID: ${recordId})`;
  if (action === 'login') return `Utilizador "${details?.nome || details?.username || user}" iniciou sessão`;
  if (action === 'logout') return `Utilizador "${user}" encerrou sessão`;
  if (action === 'export') return `${user} exportou dados de ${mod}`;
  if (action === 'clear') return `${user} limpou dados de ${mod}`;
  return `${user} — ${ACTION_LABELS[action]||action} em ${mod}`;
}

// ===================== APP STATE =====================
let currentUser = null;
let currentPage = 'dashboard';
let sidebarCollapsed = false;
let editingId = null;
const chartInstances = {};

// ===================== UTILITIES =====================
function formatDate(d) {
  if (!d) return '—';
  const s = String(d);
  // Already in DD-MM-YYYY display format — return as-is
  if (/^\d{2}-\d{2}-\d{4}$/.test(s)) return s;
  // YYYY-MM-DD (old format) — reformat to DD-MM-YYYY
  if (/^\d{4}-\d{2}-\d{2}$/.test(s)) {
    const [y,m,day] = s.split('-');
    return `${day}-${m}-${y}`;
  }
  // Fallback — try locale format
  try { return new Date(d).toLocaleDateString('pt-AO',{day:'2-digit',month:'2-digit',year:'numeric'}); }
  catch { return s; }
}
function formatMoney(v) {
  if (!v && v !== 0) return '—';
  return Number(v).toLocaleString('pt-AO') + ' AOA';
}
function today() { const d=new Date(); return `${String(d.getDate()).padStart(2,'0')}-${String(d.getMonth()+1).padStart(2,'0')}-${d.getFullYear()}`; }
// Convert date input value (YYYY-MM-DD) to storage format (DD-MM-YYYY)
function dateInputToStorage(v) { if(!v) return today(); const p=v.split('-'); if(p.length===3&&p[0].length===4) return `${p[2]}-${p[1]}-${p[0]}`; return v; }
// Convert storage format (DD-MM-YYYY) to date input value (YYYY-MM-DD)
function storageToDateInput(v) { if(!v) return new Date().toISOString().split('T')[0]; const p=v.split('-'); if(p.length===3&&p[2].length===4) return `${p[2]}-${p[1]}-${p[0]}`; return v; }
function daysUntil(dateStr) {
  if (!dateStr) return Infinity;
  return Math.floor((new Date(dateStr) - new Date()) / 86400000);
}
function getLotStatus(validade, bloqueado) {
  if (bloqueado) return { label:'Bloqueado', cls:'badge-secondary' };
  const d = daysUntil(validade);
  if (d < 0) return { label:'Vencido', cls:'badge-danger' };
  if (d <= 90) return { label:'A Vencer', cls:'badge-warning' };
  return { label:'Activo', cls:'badge-success' };
}
function initials(name) {
  return (name||'U').split(' ').slice(0,2).map(w=>w[0]).join('').toUpperCase();
}
function destroyChart(id) {
  if (chartInstances[id]) { chartInstances[id].destroy(); delete chartInstances[id]; }
}

// Toast
function toast(type, title, msg='') {
  const iconMap = {success:'check',error:'x',warning:'alert',info:'info'};
  const container = document.getElementById('toast-container');
  const t = document.createElement('div');
  t.className = `toast t-${type}`;
  t.innerHTML = `
    <div class="toast-icon">${ICONS[iconMap[type]]||''}</div>
    <div class="toast-content">
      <div class="toast-title">${title}</div>
      ${msg?`<div class="toast-msg">${msg}</div>`:''}
    </div>
    <button class="toast-close">${ICONS.x}</button>`;
  t.querySelector('.toast-close').onclick = () => t.remove();
  container.appendChild(t);
  setTimeout(() => { t.style.opacity='0'; t.style.transform='translateX(20px)'; t.style.transition='all 0.3s'; setTimeout(()=>t.remove(),300); }, 4200);
}

// Confirm dialog
let confirmResolve = null;
function confirm(title, msg) {
  return new Promise(resolve => {
    confirmResolve = resolve;
    document.getElementById('confirm-title').textContent = title;
    document.getElementById('confirm-msg').textContent = msg;
    document.getElementById('confirm-overlay').classList.add('open');
  });
}

function setLoading(btn, loading) {
  if (loading) { btn.classList.add('loading'); btn.disabled = true; }
  else { btn.classList.remove('loading'); btn.disabled = false; }
}

// ===================== SPLASH =====================
function startSplash(onDone) {
  const messages = ['Inicializando sistema...','Verificando base de dados...','Carregando configurações...','Preparando interface...','Sistema pronto!'];
  const fill = document.getElementById('splash-fill');
  const label = document.getElementById('splash-label');
  let step = 0;
  const particles = document.getElementById('splash-particles');
  for (let i=0;i<20;i++) {
    const p = document.createElement('div');
    p.className = 'splash-particle';
    const size = Math.random()*8+4;
    p.style.cssText = `width:${size}px;height:${size}px;left:${Math.random()*100}%;background:${Math.random()>0.5?'rgba(0,184,148,0.3)':'rgba(10,36,99,0.5)'};animation-duration:${Math.random()*6+5}s;animation-delay:${Math.random()*4}s;--drift:${(Math.random()-0.5)*200}px;`;
    particles.appendChild(p);
  }
  const interval = setInterval(() => {
    step++;
    fill.style.width = Math.min((step/messages.length)*100,100)+'%';
    label.textContent = messages[Math.min(step,messages.length-1)];
    if (step >= messages.length) {
      clearInterval(interval);
      setTimeout(() => {
        document.getElementById('splash').style.opacity='0';
        document.getElementById('splash').style.transition='opacity 0.5s ease';
        setTimeout(() => {
          // First run check — override with callback if provided
          if (onDone) { onDone(); return; }
          if (db.data.usuarios.length === 0) {
            showScreen('setup');
            setupFirstRun();
          } else {
            showScreen('login');
          }
        }, 500);
      }, 500);
    }
  }, 550);
}

// ===================== SCREEN MANAGEMENT =====================
function showScreen(name) {
  document.querySelectorAll('.screen').forEach(s => s.classList.remove('active'));
  const screen = document.getElementById(name);
  if (screen) screen.classList.add('active');
  // Refresh network badge on every screen transition
  if (typeof updateNetworkStatusUI === 'function') updateNetworkStatusUI();
  // On login screen: show/hide cloud config prompt
  if (name === 'login') {
    setTimeout(() => {
      const cloudPrompt = document.getElementById('login-cloud-prompt');
      if (cloudPrompt) {
        const show = navigator.onLine && !getCloudCfg() && !sessionStorage.getItem('hmm_cloud_wizard_skipped');
        cloudPrompt.style.display = show ? 'flex' : 'none';
      }
    }, 50);
  }
}

// ===================== FIRST RUN SETUP =====================
function setupFirstRun() {
  const form = document.getElementById('setup-form');
  const errBox = document.getElementById('setup-error');
  form.addEventListener('submit', async (e) => {
    e.preventDefault();
    errBox.classList.remove('show');
    const nome = document.getElementById('setup-nome').value.trim();
    const username = document.getElementById('setup-username').value.trim();
    const pwd = document.getElementById('setup-pwd').value;
    const pwd2 = document.getElementById('setup-pwd2').value;
    if (!nome || !username) { errBox.textContent='Nome e utilizador são obrigatórios.'; errBox.classList.add('show'); return; }
    if (pwd.length < 6) { errBox.textContent='A senha deve ter pelo menos 6 caracteres.'; errBox.classList.add('show'); return; }
    if (pwd !== pwd2) { errBox.textContent='As senhas não coincidem.'; errBox.classList.add('show'); return; }
    if (db.data.usuarios.find(u=>u.username===username)) { errBox.textContent='Nome de utilizador já existe.'; errBox.classList.add('show'); return; }
    const btn = document.getElementById('setup-btn');
    setLoading(btn, true);
    const hashed = await hashPassword(pwd);
    db.insert('usuarios', { username, senha: hashed, nome, funcao:'Administrador' });
    setLoading(btn, false);
    toast('success','Conta criada!','Pode agora fazer login.');
    showScreen('login');
  });
}

// ===================== LOGIN =====================
function setupLogin() {
  const form = document.getElementById('login-form');
  const pwdInput = document.getElementById('pwd-input');
  const toggleBtn = document.getElementById('pwd-toggle');
  const errorBox = document.getElementById('login-error');

  toggleBtn.addEventListener('click', () => {
    const isText = pwdInput.type === 'text';
    pwdInput.type = isText ? 'password' : 'text';
    toggleBtn.innerHTML = isText ? ICONS.eye : ICONS.eye_off;
  });

  form.addEventListener('submit', async (e) => {
    e.preventDefault();
    const btn = document.getElementById('login-btn');
    const username = document.getElementById('user-input').value.trim();
    const pwd = pwdInput.value;
    errorBox.classList.remove('show');
    setLoading(btn, true);
    await new Promise(r=>setTimeout(r,700));

    // Find user and compare hashed password
    const hashed = await hashPassword(pwd);
    // Em modo cloud e online: re-sincronizar utilizadores da nuvem antes de verificar
    if (getDbMode() === 'cloud' && getCloudCfg() && _isOnline) {
      try {
        const freshUsers = await CloudDB.loadUsers();
        if (freshUsers && freshUsers.length) {
          db.data.usuarios = freshUsers;
          try { localStorage.setItem(DB_KEY, JSON.stringify(db.data)); } catch(e) {}
        }
      } catch(e) { console.warn('[Cloud] sync users no login falhou:', e); }
    }
    const found = db.data.usuarios.find(u => u.username === username && u.senha === hashed && u.ativo !== false);
    if (found) {
      currentUser = found;
      addLog('login', 'usuarios', found.id, { nome: found.nome, username: found.username, funcao: found.funcao });
      setLoading(btn, false);
      document.getElementById('login').style.opacity='0';
      document.getElementById('login').style.transition='opacity 0.4s ease';
      // Carregar dados async do backend activo ANTES de mostrar a app
      const mode = getDbMode();
      if (mode === 'indexeddb' || mode === 'firebase' || mode === 'cloud') {
        // Mostrar ecrã de carregamento enquanto IDB/Firebase carrega
        const loginEl = document.getElementById('login');
        if (loginEl) { loginEl.style.opacity='0'; loginEl.style.transition='opacity 0.3s ease'; }
        try {
          const loaded = await db.loadAsync();
          if (loaded && mode === 'indexeddb') console.log('[DB] IndexedDB carregado com sucesso');
          if (loaded && mode === 'firebase') console.log('[DB] Firebase carregado com sucesso');
        } catch(e) {
          console.warn('[initApp] loadAsync falhou:', e);
          toast('error', 'Erro ao carregar dados',
            'Não foi possível carregar os dados do ' + (mode === 'indexeddb' ? 'IndexedDB' : 'Firebase') + '. Verifique as definições em Base de Dados.');
        }
      } else {
        document.getElementById('login').style.opacity='0';
        document.getElementById('login').style.transition='opacity 0.4s ease';
      }
      setTimeout(() => {
        showScreen('app');
        initApp();
        const m = getDbMode();
        if (m === 'indexeddb') toast('success', 'IndexedDB activo', 'Todos os dados carregados da base de dados local.');
        if (m === 'firebase') toast('success', 'Firebase activo', 'Dados sincronizados da nuvem.');
      }, 400);
    } else {
      setLoading(btn, false);
      errorBox.textContent = 'Utilizador ou senha incorrectos.';
      errorBox.classList.add('show');
    }
  });
}

// ===================== APP INIT =====================

// Roles that can access restricted sections
const RESTRICTED_ROLES = ['Administrador', 'Técnico'];
function isPrivileged() { return RESTRICTED_ROLES.includes(currentUser?.funcao); }

function initApp() {
  // Atualizar elementos do header — com null check para compatibilidade mobile/desktop
  const setEl = (id, val) => { const el = document.getElementById(id); if(el) el.textContent = val; };
  setEl('header-user-name',   currentUser.nome);
  setEl('header-user-role',   currentUser.funcao);
  setEl('header-user-avatar', initials(currentUser.nome));
  setEl('sidebar-user-name',  currentUser.nome);
  setEl('sidebar-user-role',  currentUser.funcao);
  setEl('sidebar-user-avatar',initials(currentUser.nome));
  // Atualizar badge de rede no header ao entrar na app
  updateNetworkStatusUI();

  // ── Visibility by role ──────────────────────────────────────────
  const priv = isPrivileged();
  const isAdmin = currentUser.funcao === 'Administrador';

  // Base de Dados — Técnico e Administrador
  const navBD = document.getElementById('nav-basedados');
  if (navBD) navBD.style.display = priv ? 'flex' : 'none';

  // Utilizadores — apenas Administrador
  const navUsuarios = document.getElementById('nav-usuarios');
  if (navUsuarios) navUsuarios.style.display = isAdmin ? 'flex' : 'none';

  // Logs — Técnico e Administrador
  const navLogs = document.getElementById('nav-logs');
  if (navLogs) navLogs.style.display = priv ? 'flex' : 'none';

  // Sincronização — Técnico e Administrador
  const navSync = document.getElementById('nav-sincronizacao');
  if (navSync) navSync.style.display = priv ? 'flex' : 'none';
  // ────────────────────────────────────────────────────────────────

  // Apply saved theme
  applyTheme(appSettings.theme || 'dark');

  // Theme toggle button
  const themeBtn = document.getElementById('theme-toggle-btn');
  if (themeBtn) themeBtn.addEventListener('click', toggleTheme);

  setupSidebar();
  setupNavigation();
  updateAlertBadge();
  navigateTo('dashboard');
}

// ===================== SIDEBAR =====================
function setupSidebar() {
  const sidebar = document.getElementById('sidebar');
  // sidebar-toggle pode não existir na versão mobile (usa drawer)
  const toggleBtn = document.getElementById('sidebar-toggle');
  if (toggleBtn && sidebar) {
    toggleBtn.addEventListener('click', () => {
      sidebarCollapsed = !sidebarCollapsed;
      sidebar.classList.toggle('collapsed', sidebarCollapsed);
    });
  }
  const sidebarUser = document.getElementById('sidebar-user');
  if (sidebarUser) {
    sidebarUser.addEventListener('click', () => {
      confirm('Terminar Sessão','Deseja terminar a sessão actual?').then(ok => {
        if (ok) {
          addLog('logout', 'usuarios', currentUser?.id, null);
          currentUser = null;
          Object.keys(chartInstances).forEach(k => destroyChart(k));
          showScreen('login');
          const l = document.getElementById('login');
          if(l) l.style.opacity='1';
          const f = document.getElementById('login-form');
          if(f) f.reset();
          const e = document.getElementById('login-error');
          if(e) e.classList.remove('show');
        }
      });
    });
  }
}

// ===================== NAVIGATION =====================
function setupNavigation() {
  document.querySelectorAll('.nav-item').forEach(item => {
    item.addEventListener('click', () => {
      const page = item.dataset.page;
      if (page) navigateTo(page);
    });
  });
}

const PAGE_TITLES = {
  dashboard:'Dashboard', produtos:'Cadastro de Produtos', fornecedores:'Fornecedores',
  prateleiras:'Prateleiras', lotes:'Cadastro de Lotes', movimentacoes:'Entradas / Saídas',
  alertas:'Alertas', relatorios:'Relatórios', basedados:'Base de Dados',
  usuarios:'Utilizadores', sincronizacao:'Sincronização', kits:'Kits de Produtos',
  fichastock:'Gerar Ficha de Stock',
};

function navigateTo(page) {
  // ── Access control ──────────────────────────────────────────────
  const adminOnly = ['usuarios'];
  const privOnly  = ['basedados', 'sincronizacao', 'logs'];
  if (adminOnly.includes(page) && currentUser?.funcao !== 'Administrador') {
    toast('error', 'Acesso restrito', 'Esta secção é exclusiva do Administrador do sistema.');
    return;
  }
  if (privOnly.includes(page) && !isPrivileged()) {
    toast('error', 'Acesso restrito', 'Esta secção requer função de Técnico ou Administrador.');
    return;
  }
  // ───────────────────────────────────────────────────────────────
  currentPage = page;
  document.querySelectorAll('.nav-item').forEach(item => {
    item.classList.toggle('active', item.dataset.page === page);
  });
  document.getElementById('header-page-title').textContent = PAGE_TITLES[page]||page;
  const breadcrumb = document.getElementById('header-breadcrumb-current');
  if (breadcrumb) breadcrumb.textContent = PAGE_TITLES[page]||page;
  document.querySelectorAll('.page').forEach(p => p.classList.remove('active'));
  const pageEl = document.getElementById('page-'+page);
  if (pageEl) pageEl.classList.add('active');
  renderPage(page);
}

function renderPage(page) {
  const renders = {
    dashboard:renderDashboard, produtos:renderProdutos, fornecedores:renderFornecedores,
    prateleiras:renderPrateleiras, lotes:renderLotes, movimentacoes:renderMovimentacoes,
    alertas:renderAlertas, relatorios:renderRelatorios, basedados:renderBaseDados,
    usuarios:renderUsuarios, sincronizacao:renderSincronizacao, kits:renderKits,
    fichastock:renderFichaStock, logs:renderLogs,
  };
  if (renders[page]) renders[page]();
}

// ===================== ALERT BADGE =====================
function updateAlertBadge() {
  const alertas = getAlerts();
  const badge = document.getElementById('nav-badge-alertas');
  const headerBadge = document.getElementById('header-notif-badge');
  const count = alertas.length;
  if (badge) { badge.textContent = count||''; badge.style.display = count>0?'flex':'none'; }
  if (headerBadge) { headerBadge.textContent = count||''; headerBadge.style.display = count>0?'flex':'none'; }
}

function getAlerts() {
  const alerts = [];
  db.getAll('lotes').forEach(lot => {
    if (lot.bloqueado) return; // Lotes bloqueados não geram alertas de validade
    const d = daysUntil(lot.validade);
    const prod = db.getById('produtos', lot.produto_id);
    const name = prod ? prod.nome : `Produto #${lot.produto_id}`;
    if (d < 0) alerts.push({type:'err',icon:'alert',title:`Lote vencido: ${lot.numero_lote}`,desc:`${name} — Vencido há ${Math.abs(d)} dias`,time:formatDate(lot.validade)});
    else if (d <= 90) alerts.push({type:'warn',icon:'clock',title:`Lote a vencer: ${lot.numero_lote}`,desc:`${name} — Vence em ${d} dias`,time:formatDate(lot.validade)});
  });
  db.getAll('produtos').forEach(prod => {
    const {stock} = db.getStock(prod.id);
    if (prod.stock_minimo && stock <= prod.stock_minimo) {
      alerts.push({type:'warn',icon:'package',title:`Stock mínimo: ${prod.nome}`,desc:`Stock actual: ${stock} unid. (Mínimo: ${prod.stock_minimo})`,time:'Agora'});
    }
  });
  return alerts;
}

// ===================== STAT CARD =====================
function statCard(label, value, ico, color, rgb, sub) {
  return `<div class="stat-card">
    <div class="stat-icon" style="background:rgba(${rgb},0.15);color:${color};">${ICONS[ico]||''}</div>
    <div class="stat-info">
      <div class="stat-value" style="color:${color};">${value}</div>
      <div class="stat-label">${label}</div>
      <div class="stat-sub">${sub}</div>
    </div>
  </div>`;
}

// ===================== DASHBOARD PAGE =====================
let dashChartPeriod = 'month'; // 'day' | 'month' | 'year'
let dashChartDay   = new Date().toISOString().split('T')[0]; // YYYY-MM-DD
let dashChartMonth = new Date().getMonth();   // 0-11
let dashChartMYear = new Date().getFullYear(); // year for month mode

function renderDashboard() {
  const produtos = db.getAll('produtos');
  const fornecedores = db.getAll('fornecedores');
  const prateleiras = db.getAll('prateleiras');
  const lotes = db.getAll('lotes');
  const movs = db.getAll('movimentacoes');

  const totalEntradas = movs.filter(m=>(m.tipo||'').toUpperCase()==='ENTRADA').reduce((s,m)=>s+(Number(m.quantidade)||0),0);
  const totalSaidas = movs.filter(m=>(m.tipo||'').toUpperCase()==='SAÍDA').reduce((s,m)=>s+(Number(m.quantidade)||0),0);
  // Financial totals
  const totalGastoEntradas = movs.filter(m=>(m.tipo||'').toUpperCase()==='ENTRADA'&&Number(m.preco)>0).reduce((s,m)=>s+(Number(m.preco)||0)*(Number(m.quantidade)||0),0);
  const totalGastoSaidas = movs.filter(m=>(m.tipo||'').toUpperCase()==='SAÍDA'&&Number(m.preco)>0).reduce((s,m)=>s+(Number(m.preco)||0)*(Number(m.quantidade)||0),0);
  const lucroGanho = totalGastoSaidas - totalGastoEntradas;
  const kits = db.getAll('kits');
  const totalUsuarios = (db.data.usuarios||[]).length;
  const alerts = getAlerts();

  const topProd = produtos.map(p => {
    const {stock} = db.getStock(p.id);
    return {...p, stock};
  }).sort((a,b)=>b.stock-a.stock).slice(0,5);

  const _toISO = d => { if(!d) return ''; const p = String(d).split('-'); if(p.length===3&&p[2].length===4) return `${p[2]}-${p[1]}-${p[0]}`; return d; };
  const recentMovs = [...movs].sort((a,b)=>_toISO(b.data).localeCompare(_toISO(a.data))).slice(0,7);

  document.getElementById('page-dashboard').innerHTML = `
    <div class="stats-grid">
      ${statCard('Produtos',produtos.length,'pill','#00B894','0,184,148','Total cadastrados')}
      ${statCard('Total Entradas',totalEntradas,'arrow_up','#27AE60','39,174,96','Unidades entradas')}
      ${statCard('Total Saídas',totalSaidas,'arrow_down','#E74C3C','231,76,60','Unidades saídas')}
      ${statCard('Lotes',lotes.length,'lot','#3498DB','52,152,219','Lotes registados')}
      ${statCard('Prateleiras',prateleiras.length,'shelf','#9B59B6','155,89,182','Prateleiras activas')}
      ${statCard('Fornecedores',fornecedores.length,'supplier','#F39C12','243,156,18','Fornecedores activos')}
      ${statCard('Alertas',alerts.length,'alert',alerts.length>0?'#E74C3C':'#27AE60',alerts.length>0?'231,76,60':'39,174,96','Alertas activos')}
      ${statCard('Gastos Entradas (AOA)',formatMoney(totalGastoEntradas),'money_in','#16A085','22,160,133','Total gasto em entradas')}
      ${statCard('Gastos Saídas (AOA)',formatMoney(totalGastoSaidas),'money_out','#C0392B','192,57,43','Total gasto em saídas')}
      ${statCard('Lucro Ganho (AOA)',formatMoney(lucroGanho),lucroGanho>=0?'profit':'arrow_down',lucroGanho>=0?'#27AE60':'#E74C3C',lucroGanho>=0?'39,174,96':'231,76,60',lucroGanho>=0?'Resultado positivo':'Resultado negativo')}
      ${statCard('Kits de Produtos',kits.length,'kit_box','#8E44AD','142,68,173','Kits cadastrados')}
      ${statCard('Utilizadores',totalUsuarios,'users','#2980B9','41,128,185','Total no sistema')}
    </div>

    <!-- CHARTS ROW -->
    <div class="charts-row">
      <div class="chart-card" style="overflow:hidden;">
        <div class="chart-title" style="flex-wrap:wrap;gap:6px;margin-bottom:8px;">
          <span style="font-size:11px;font-weight:600;">${ICONS.bar_chart} Consumo de Medicamentos</span>
          <div style="display:flex;align-items:center;gap:6px;flex-wrap:wrap;margin-top:4px;width:100%;">
            <div class="chart-period-btns">
              <button class="chart-period-btn ${dashChartPeriod==='day'?'active':''}" onclick="setDashPeriod('day')">Dia</button>
              <button class="chart-period-btn ${dashChartPeriod==='month'?'active':''}" onclick="setDashPeriod('month')">Mês</button>
              <button class="chart-period-btn ${dashChartPeriod==='year'?'active':''}" onclick="setDashPeriod('year')">Ano</button>
            </div>
            ${dashChartPeriod==='day' ? `
              <input type="date" id="dash-day-picker" value="${dashChartDay}"
                style="background:var(--bg-input);border:1px solid var(--border);border-radius:6px;padding:3px 8px;font-size:11px;color:var(--text-primary);flex:1;min-width:0;"
                oninput="dashChartDay=this.value;updateConsumoChart()">
            ` : dashChartPeriod==='month' ? `
              <select id="dash-month-picker"
                style="background:var(--bg-input);border:1px solid var(--border);border-radius:6px;padding:3px 6px;font-size:11px;color:var(--text-primary);flex:1;min-width:0;"
                onchange="dashChartMonth=parseInt(this.value);updateConsumoChart()">
                ${['Jan','Fev','Mar','Abr','Mai','Jun','Jul','Ago','Set','Out','Nov','Dez'].map((m,i)=>`<option value="${i}" ${dashChartMonth===i?'selected':''}>${m}</option>`).join('')}
              </select>
              <input type="number" id="dash-myear-picker" value="${dashChartMYear}" min="2000" max="2099"
                style="background:var(--bg-input);border:1px solid var(--border);border-radius:6px;padding:3px 6px;font-size:11px;color:var(--text-primary);width:62px;"
                oninput="dashChartMYear=parseInt(this.value)||new Date().getFullYear();updateConsumoChart()">
            ` : ''}
          </div>
        </div>
        <div class="chart-container" style="height:200px;width:100%;max-width:100%;">
          <canvas id="chart-consumo" style="max-width:100%;"></canvas>
        </div>
      </div>
      <div class="chart-card" style="overflow:hidden;">
        <div class="chart-title" style="font-size:11px;">${ICONS.pie_chart} Distribuição por Grupo Farmacológico</div>
        <div class="chart-container" style="height:200px;width:100%;max-width:100%;">
          <canvas id="chart-grupos" style="max-width:100%;"></canvas>
        </div>
      </div>
    </div>
    <div class="charts-row">
      <div class="chart-card" style="overflow:hidden;">
        <div class="chart-title" style="font-size:11px;">${ICONS.pie_chart} Entradas vs Saídas</div>
        <div class="chart-container" style="height:200px;width:100%;max-width:100%;">
          <canvas id="chart-entradas-saidas" style="max-width:100%;"></canvas>
        </div>
      </div>
      <div class="chart-card" style="overflow:hidden;">
        <div class="chart-title" style="font-size:11px;">${ICONS.bar_chart} Top 5 Produtos por Stock</div>
        <div class="chart-container" style="height:200px;width:100%;max-width:100%;">
          <canvas id="chart-top-stock" style="max-width:100%;"></canvas>
        </div>
      </div>
    </div>

    <!-- TABLES ROW -->
    <div class="grid-2-1" style="gap:16px;margin-bottom:20px;">
      <div>
        <div class="dash-section-title">${ICONS.activity} Últimas Movimentações</div>
        <div class="table-wrap">
          <div class="tbl-scroll">
          <table>
            <thead><tr><th>Produto</th><th>Tipo</th><th>Qtd</th><th>Destino</th><th>Data</th></tr></thead>
            <tbody>
              ${recentMovs.length ? recentMovs.map(m => {
                const p = db.getById('produtos', m.produto_id);
                return `<tr>
                  <td class="td-name">${p?p.nome:'—'}</td>
                  <td><span class="badge ${(m.tipo||'').toUpperCase()==='ENTRADA'?'badge-success':'badge-danger'}">${m.tipo||'—'}</span></td>
                  <td class="font-bold">${m.quantidade}</td>
                  <td>${m.destino||'—'}</td>
                  <td>${formatDate(m.data)}</td>
                </tr>`;
              }).join('') : `<tr><td colspan="5" class="table-empty"><p>Sem movimentações registadas</p></td></tr>`}
            </tbody>
          </table>
          </div>
        </div>
      </div>
      <div>
        <div class="dash-section-title">${ICONS.alert} Alertas Recentes</div>
        <div class="table-wrap">
          ${alerts.slice(0,5).map(a => `
            <div class="alert-item">
              <div class="alert-icon ${a.type}">${ICONS[a.icon]||ICONS.alert}</div>
              <div class="alert-content">
                <div class="alert-title">${a.title}</div>
                <div class="alert-desc">${a.desc}</div>
                <div class="alert-time">${a.time}</div>
              </div>
            </div>
          `).join('') || `<div class="table-empty">${ICONS.check}<p>Sem alertas activos</p></div>`}
        </div>
      </div>
    </div>
  `;

  // Initialize charts after DOM is ready
  setTimeout(() => initDashboardCharts(movs, produtos), 0);
}

function setDashPeriod(period) {
  dashChartPeriod = period;
  // Reset to today/current month when switching modes
  if (period === 'day')   dashChartDay   = new Date().toISOString().split('T')[0];
  if (period === 'month') { dashChartMonth = new Date().getMonth(); dashChartMYear = new Date().getFullYear(); }
  renderDashboard();
}

function updateConsumoChart() {
  const movs = db.getAll('movimentacoes');
  const CHART_COLORS = {
    entrada: 'rgba(39,174,96,0.85)', saida: 'rgba(231,76,60,0.85)',
    border_entrada: '#27AE60', border_saida: '#E74C3C',
  };
  destroyChart('chart-consumo');
  const ctx1 = document.getElementById('chart-consumo');
  if (!ctx1) return;
  const { labels, entradas, saidas } = buildPeriodData(movs, dashChartPeriod);
  chartInstances['chart-consumo'] = new Chart(ctx1, {
    type: 'bar',
    data: {
      labels,
      datasets: [
        { label:'Entradas', data: entradas, backgroundColor: CHART_COLORS.entrada, borderColor: CHART_COLORS.border_entrada, borderWidth:1, borderRadius:4 },
        { label:'Saídas',   data: saidas,   backgroundColor: CHART_COLORS.saida,   borderColor: CHART_COLORS.border_saida,   borderWidth:1, borderRadius:4 }
      ]
    },
    options: {
      responsive:true, maintainAspectRatio:false,
      plugins:{ legend:{ labels:{ color:'#8BA7C7', font:{size:11} } } },
      scales:{
        x:{ ticks:{ color:'#5A7A9B', font:{size:10}, maxRotation: dashChartPeriod==='month'?45:0 }, grid:{ color:'rgba(30,58,95,0.5)' } },
        y:{ ticks:{ color:'#5A7A9B', font:{size:10} }, grid:{ color:'rgba(30,58,95,0.5)' }, beginAtZero:true }
      }
    }
  });
}

function initDashboardCharts(movs, produtos) {
  const CHART_COLORS = {
    entrada: 'rgba(39,174,96,0.85)', saida: 'rgba(231,76,60,0.85)',
    border_entrada: '#27AE60', border_saida: '#E74C3C',
  };
  const PALETTE = ['#00B894','#3498DB','#9B59B6','#F39C12','#E74C3C','#1ABC9C','#E67E22','#2ECC71'];

  Chart.defaults.color = '#8BA7C7';
  Chart.defaults.borderColor = '#1E3A5F';

  // --- Chart 1: Consumo por período ---
  destroyChart('chart-consumo');
  const ctx1 = document.getElementById('chart-consumo');
  if (ctx1) {
    const { labels, entradas, saidas } = buildPeriodData(movs, dashChartPeriod);
    chartInstances['chart-consumo'] = new Chart(ctx1, {
      type: 'bar',
      data: {
        labels,
        datasets: [
          { label:'Entradas', data: entradas, backgroundColor: CHART_COLORS.entrada, borderColor: CHART_COLORS.border_entrada, borderWidth:1, borderRadius:4 },
          { label:'Saídas', data: saidas, backgroundColor: CHART_COLORS.saida, borderColor: CHART_COLORS.border_saida, borderWidth:1, borderRadius:4 }
        ]
      },
      options: {
        responsive:true, maintainAspectRatio:false,
        plugins:{ legend:{ labels:{ color:'#8BA7C7', font:{size:11} } } },
        scales:{
          x:{ ticks:{ color:'#5A7A9B', font:{size:10} }, grid:{ color:'rgba(30,58,95,0.5)' } },
          y:{ ticks:{ color:'#5A7A9B', font:{size:10} }, grid:{ color:'rgba(30,58,95,0.5)' }, beginAtZero:true }
        }
      }
    });
  }

  // --- Chart 2: Distribuição por grupo farmacológico (doughnut) ---
  destroyChart('chart-grupos');
  const ctx2 = document.getElementById('chart-grupos');
  if (ctx2) {
    const grupos = {};
    produtos.forEach(p => {
      const g = p.grupo_farmacologico || 'Outro';
      if (!grupos[g]) grupos[g] = 0;
      const {stock} = db.getStock(p.id);
      grupos[g] += Math.max(0, stock);
    });
    const gLabels = Object.keys(grupos);
    const gValues = gLabels.map(k => grupos[k]);
    if (gLabels.length === 0) { gLabels.push('Sem dados'); gValues.push(1); }
    chartInstances['chart-grupos'] = new Chart(ctx2, {
      type: 'doughnut',
      data: {
        labels: gLabels,
        datasets:[{ data:gValues, backgroundColor:PALETTE.slice(0,gLabels.length), borderWidth:2, borderColor:'#112240' }]
      },
      options:{
        responsive:true, maintainAspectRatio:false, cutout:'65%',
        plugins:{ legend:{ position:'right', labels:{ color:'#8BA7C7', font:{size:10}, padding:8 } } }
      }
    });
  }

  // --- Chart 3: Entradas vs Saídas total (doughnut) ---
  destroyChart('chart-entradas-saidas');
  const ctx3 = document.getElementById('chart-entradas-saidas');
  if (ctx3) {
    const totalE = movs.filter(m=>(m.tipo||'').toUpperCase()==='ENTRADA').reduce((s,m)=>s+(Number(m.quantidade)||0),0);
    const totalS = movs.filter(m=>(m.tipo||'').toUpperCase()==='SAÍDA').reduce((s,m)=>s+(Number(m.quantidade)||0),0);
    const hasData = totalE > 0 || totalS > 0;
    chartInstances['chart-entradas-saidas'] = new Chart(ctx3, {
      type:'doughnut',
      data:{
        labels:['Entradas','Saídas'],
        datasets:[{ data: hasData ? [totalE,totalS] : [1,1], backgroundColor:[CHART_COLORS.entrada, CHART_COLORS.saida], borderWidth:2, borderColor:'#112240' }]
      },
      options:{
        responsive:true, maintainAspectRatio:false, cutout:'65%',
        plugins:{ legend:{ position:'right', labels:{ color:'#8BA7C7', font:{size:11}, padding:10 } } }
      }
    });
  }

  // --- Chart 4: Top 5 produtos por stock (horizontal bar) ---
  destroyChart('chart-top-stock');
  const ctx4 = document.getElementById('chart-top-stock');
  if (ctx4) {
    const topProd = produtos.map(p => {
      const {stock} = db.getStock(p.id);
      return { nome: p.nome, nomeShort: p.nome.length > 22 ? p.nome.substring(0,20)+'…' : p.nome, stock: Math.max(0, stock) };
    }).sort((a,b)=>b.stock-a.stock).slice(0,5);
    // Store full names globally so tooltip callback can access them
    window._topProdNames = topProd.map(p => p.nome);
    window._topProdStock = topProd.map(p => p.stock);
    chartInstances['chart-top-stock'] = new Chart(ctx4, {
      type:'bar',
      data:{
        labels: topProd.map(p=>p.nomeShort),
        datasets:[{ label:'Stock Actual', data: topProd.map(p=>p.stock), backgroundColor: PALETTE, borderWidth:1, borderRadius:4 }]
      },
      options:{
        indexAxis:'y', responsive:true, maintainAspectRatio:false,
        plugins:{
          legend:{ display:false },
          tooltip:{
            callbacks:{
              title: function(items) { return (window._topProdNames || [])[items[0].dataIndex] || items[0].label; },
              label: function(item) { return '  Stock: ' + item.raw + ' unidades'; }
            },
            bodyFont:{ size:12 }, titleFont:{ size:12, weight:'bold' },
            padding:10, displayColors:false
          }
        },
        scales:{
          x:{ ticks:{ color:'#5A7A9B', font:{size:10} }, grid:{ color:'rgba(30,58,95,0.5)' }, beginAtZero:true },
          y:{ ticks:{ color:'#8BA7C7', font:{size:10} }, grid:{ color:'rgba(30,58,95,0.5)' } }
        }
      }
    });
  }
}

function buildPeriodData(movs, period) {
  const now = new Date();
  let labels = [], entradas = [], saidas = [];

  if (period === 'day') {
    // Show 24 hourly bars for the selected day
    const selDate = dashChartDay || now.toISOString().split('T')[0];
    const _toISO2 = d => { if(!d) return ''; const p = String(d).split('-'); if(p.length===3&&p[2].length===4) return `${p[2]}-${p[1]}-${p[0]}`; return d; };
    const dayMovs = movs.filter(m => _toISO2(m.data) === selDate);
    // Group by hour
    for (let h = 0; h < 24; h++) {
      labels.push(String(h).padStart(2,'0') + 'h');
      // movimentacoes don't store hour, so spread evenly in a single "All day" bar at h=0
      // Show just one bar "Dia todo" if no hour info, else spread
      entradas.push(h === 12 ? dayMovs.filter(m=>(m.tipo||'').toUpperCase()==='ENTRADA').reduce((s,m)=>s+(Number(m.quantidade)||0),0) : 0);
      saidas.push(h === 12 ? dayMovs.filter(m=>(m.tipo||'').toUpperCase()==='SAÍDA').reduce((s,m)=>s+(Number(m.quantidade)||0),0) : 0);
    }
    // If no hour field, just show a clean single-day summary with one bar
    const totalE = dayMovs.filter(m=>(m.tipo||'').toUpperCase()==='ENTRADA').reduce((s,m)=>s+(Number(m.quantidade)||0),0);
    const totalS = dayMovs.filter(m=>(m.tipo||'').toUpperCase()==='SAÍDA').reduce((s,m)=>s+(Number(m.quantidade)||0),0);
    const [y,mo,d] = selDate.split('-');
    labels   = [selDate ? `${d}/${mo}/${y}` : 'Dia'];
    entradas = [totalE];
    saidas   = [totalS];

  } else if (period === 'month') {
    // Show each day of the selected month
    const selMonth = (typeof dashChartMonth === 'number') ? dashChartMonth : now.getMonth();
    const selYear  = dashChartMYear || now.getFullYear();
    const daysInMonth = new Date(selYear, selMonth + 1, 0).getDate();
    const _toISO3 = d => { if(!d) return ''; const p = String(d).split('-'); if(p.length===3&&p[2].length===4) return `${p[2]}-${p[1]}-${p[0]}`; return d; };
    for (let d = 1; d <= daysInMonth; d++) {
      const key = `${selYear}-${String(selMonth+1).padStart(2,'0')}-${String(d).padStart(2,'0')}`;
      labels.push(String(d));
      const dm = movs.filter(m => _toISO3(m.data) === key);
      entradas.push(dm.filter(m=>(m.tipo||'').toUpperCase()==='ENTRADA').reduce((s,m)=>s+(Number(m.quantidade)||0),0));
      saidas.push(dm.filter(m=>(m.tipo||'').toUpperCase()==='SAÍDA').reduce((s,m)=>s+(Number(m.quantidade)||0),0));
    }

  } else {
    // Last 5 years
    const _toISO4 = d => { if(!d) return ''; const p = String(d).split('-'); if(p.length===3&&p[2].length===4) return `${p[2]}-${p[1]}-${p[0]}`; return d; };
    for (let i=4; i>=0; i--) {
      const y = now.getFullYear()-i;
      labels.push(String(y));
      const yMovs = movs.filter(mv=>new Date(_toISO4(mv.data)).getFullYear()===y);
      entradas.push(yMovs.filter(m=>(m.tipo||'').toUpperCase()==='ENTRADA').reduce((s,m)=>s+(Number(m.quantidade)||0),0));
      saidas.push(yMovs.filter(m=>(m.tipo||'').toUpperCase()==='SAÍDA').reduce((s,m)=>s+(Number(m.quantidade)||0),0));
    }
  }
  return { labels, entradas, saidas };
}

// ===================== PRODUTOS PAGE =====================
let prodSearch = '';
function renderProdutos() {
  const prateleiras = db.getAll('prateleiras');
  let produtos = db.getAll('produtos');
  if (prodSearch) produtos = produtos.filter(p => p.nome.toLowerCase().includes(prodSearch.toLowerCase()) || (p.grupo_farmacologico||'').toLowerCase().includes(prodSearch.toLowerCase()) || (p.forma||'').toLowerCase().includes(prodSearch.toLowerCase()));
  produtos.sort((a, b) => (a.nome||'').localeCompare(b.nome||'', 'pt', {sensitivity:'base'}));

  document.getElementById('page-produtos').innerHTML = `
    <div class="page-header">
      <div>
        <div class="page-title">${ICONS.pill} Cadastro de Produtos</div>
        <div class="page-title-sub">Gerir medicamentos e stocks</div>
      </div>
      <div class="page-actions">
        <button class="btn btn-primary" onclick="openProdutoModal()">
          ${ICONS.plus}<span class="btn-text-content">Novo Produto</span>
        </button>
      </div>
    </div>
    <div class="table-wrap">
      <div class="table-header">
        <div class="table-title">${ICONS.list} Lista de Produtos <span class="chip">${produtos.length}</span></div>
        <div class="table-actions">
          <div class="search-wrap">
            <input class="search-input" id="search-produtos" placeholder="Pesquisar por nome, grupo ou forma..." value="${prodSearch}" oninput="prodSearch=this.value;filterProdutosTable()">
          </div>
        </div>
      </div>
      <div class="tbl-scroll">
      <table>
        <thead><tr>
          <th>Nome</th><th>Forma</th><th>Grupo Farmacológico</th><th>Prateleira</th>
          <th>Stock Mín.</th><th>Preço</th><th>Entradas</th><th>Saídas</th><th>Stock</th><th>Status</th><th>Acções</th>
        </tr></thead>
        <tbody id="tbody-produtos">
          ${produtos.length ? produtos.map(p => {
            const {entradas,saidas,stock} = db.getStock(p.id);
            const prat = prateleiras.find(s=>s.id===p.prateleira_id);
            const belowMin = p.stock_minimo && stock <= p.stock_minimo;
            return `<tr>
              <td class="td-name">${p.nome}</td>
              <td>${p.forma||'—'}</td>
              <td>${p.grupo_farmacologico||'—'}</td>
              <td>${prat?prat.nome:'—'}</td>
              <td>${p.stock_minimo||'—'}</td>
              <td>${p.preco?formatMoney(p.preco):'—'}</td>
              <td class="text-accent font-bold">${entradas}</td>
              <td class="text-danger font-bold">${saidas}</td>
              <td class="font-bold ${belowMin?'text-danger':'text-info'}">${stock}</td>
              <td><span class="badge ${belowMin?'badge-danger':'badge-success'}">${belowMin?'Stock Baixo':p.status||'Ativo'}</span></td>
              <td>
                <div style="display:flex;gap:5px;">
                  <button class="btn btn-secondary btn-icon" title="Editar" onclick="openProdutoModal(${p.id})">${ICONS.edit}</button>
                  <button class="btn btn-danger btn-icon" title="Eliminar" onclick="deleteProduto(${p.id})">${ICONS.trash}</button>
                </div>
              </td>
            </tr>`;
          }).join('') : `<tr><td colspan="11"><div class="table-empty">${ICONS.pill}<p>Nenhum produto cadastrado</p><p style="font-size:12px;color:var(--text-muted)">Clique em "Novo Produto" para começar</p></div></td></tr>`}
        </tbody>
      </table>
      </div>
    </div>

    <div class="modal-overlay" id="modal-produto">
      <div class="modal">
        <div class="modal-header">
          <div class="modal-title">${ICONS.pill} <span id="modal-prod-title">Novo Produto</span></div>
          <button class="modal-close" onclick="closeModal('modal-produto')">${ICONS.x}</button>
        </div>
        <div class="modal-body">
          <div class="form-grid form-grid-2">
            <div class="field-wrap form-grid-full">
              <label class="field-label">${ICONS.pill} Nome do Medicamento <span class="field-req">*</span></label>
              <input class="field-input" id="prod-nome" placeholder="Ex: Paracetamol 500mg" required>
            </div>
            <div class="field-wrap">
              <label class="field-label">${ICONS.tag} Forma Farmacêutica</label>
              <select class="field-select" id="prod-forma" onchange="toggleOutroField('prod-forma','prod-forma-outro')">
                <option value="">Seleccionar...</option>
                ${['Comprimido','Cápsula','Xarope','Injectável','Creme','Pomada','Supositório','Solução Oral','Gotas','Spray','Inalador','Sachê','Pó para Solução','Adesivo','Outro'].map(f=>`<option value="${f}">${f}</option>`).join('')}
              </select>
              <input class="field-input" id="prod-forma-outro" placeholder="Especifique a forma farmacêutica..."
                style="margin-top:8px;display:none;" oninput="this.value=this.value">
            </div>
            <div class="field-wrap">
              <label class="field-label">${ICONS.layers} Grupo Farmacológico</label>
              <select class="field-select" id="prod-grupo" onchange="toggleOutroField('prod-grupo','prod-grupo-outro')">
                <option value="">Seleccionar...</option>
                ${['Analgésico','Antibiótico','Anti-inflamatório','Antifúngico','Antiviral','Antiparasitário','Anti-hipertensivo','Antidiabético','Antiácido','Antihistamínico','Antidepressivo','Ansiolítico','Antiepiléptico','Cardiovascular','Diurético','Laxante','Vitamina/Suplemento','Anestésico','Antialérgico','Outro'].map(g=>`<option value="${g}">${g}</option>`).join('')}
              </select>
              <input class="field-input" id="prod-grupo-outro" placeholder="Especifique o grupo farmacológico..."
                style="margin-top:8px;display:none;" oninput="this.value=this.value">
            </div>
            <div class="field-wrap">
              <label class="field-label">${ICONS.shelf} Prateleira</label>
              <select class="field-select" id="prod-prateleira">
                <option value="">Sem prateleira</option>
                ${prateleiras.map(s=>`<option value="${s.id}">${s.nome} (${s.seccao})</option>`).join('')}
              </select>
            </div>
            <div class="field-wrap">
              <label class="field-label">${ICONS.package} Stock Mínimo</label>
              <input class="field-input" id="prod-stock-min" type="number" min="0" placeholder="Ex: 50">
            </div>
            <div class="field-wrap">
              <label class="field-label">${ICONS.money} Preço (AOA)</label>
              <input class="field-input" id="prod-preco" type="number" min="0" step="0.01" placeholder="Ex: 500.00">
            </div>
          </div>
        </div>
        <div class="modal-footer">
          <button class="btn btn-secondary" onclick="closeModal('modal-produto')">Cancelar</button>
          <button class="btn btn-primary" id="btn-save-produto" onclick="saveProduto()">
            <span class="btn-spin"></span><span class="btn-text-content">${ICONS.check} Guardar</span>
          </button>
        </div>
      </div>
    </div>
  `;
  refocus('search-produtos');
}

function openProdutoModal(id=null) {
  editingId = id;
  document.getElementById('modal-prod-title').textContent = id?'Editar Produto':'Novo Produto';

  // Known forma and grupo options
  const formas  = ['Comprimido','Cápsula','Xarope','Injectável','Creme','Pomada','Supositório','Solução Oral','Gotas','Spray','Inalador','Sachê','Pó para Solução','Adesivo','Outro',''];
  const grupos  = ['Analgésico','Antibiótico','Anti-inflamatório','Antifúngico','Antiviral','Antiparasitário','Anti-hipertensivo','Antidiabético','Antiácido','Antihistamínico','Antidepressivo','Ansiolítico','Antiepiléptico','Cardiovascular','Diurético','Laxante','Vitamina/Suplemento','Anestésico','Antialérgico','Outro',''];

  if (id) {
    const p = db.getById('produtos',id);
    if (p) {
      document.getElementById('prod-nome').value = p.nome||'';
      document.getElementById('prod-prateleira').value = p.prateleira_id||'';
      document.getElementById('prod-stock-min').value = p.stock_minimo||'';
      document.getElementById('prod-preco').value = p.preco||'';

      // Forma — if saved value is not in the list, it's a custom "Outro"
      const formaVal = p.forma||'';
      if (formaVal && !formas.includes(formaVal)) {
        document.getElementById('prod-forma').value = 'Outro';
        document.getElementById('prod-forma-outro').value = formaVal;
        document.getElementById('prod-forma-outro').style.display = 'block';
      } else {
        document.getElementById('prod-forma').value = formaVal;
        document.getElementById('prod-forma-outro').value = '';
        document.getElementById('prod-forma-outro').style.display = 'none';
      }

      // Grupo — same logic
      const grupoVal = p.grupo_farmacologico||'';
      if (grupoVal && !grupos.includes(grupoVal)) {
        document.getElementById('prod-grupo').value = 'Outro';
        document.getElementById('prod-grupo-outro').value = grupoVal;
        document.getElementById('prod-grupo-outro').style.display = 'block';
      } else {
        document.getElementById('prod-grupo').value = grupoVal;
        document.getElementById('prod-grupo-outro').value = '';
        document.getElementById('prod-grupo-outro').style.display = 'none';
      }
    }
  } else {
    ['prod-nome','prod-stock-min','prod-preco'].forEach(fid=>{ const el=document.getElementById(fid); if(el) el.value=''; });
    document.getElementById('prod-forma').value='';
    document.getElementById('prod-grupo').value='';
    document.getElementById('prod-prateleira').value='';
    document.getElementById('prod-forma-outro').value=''; document.getElementById('prod-forma-outro').style.display='none';
    document.getElementById('prod-grupo-outro').value=''; document.getElementById('prod-grupo-outro').style.display='none';
  }
  document.getElementById('modal-produto').classList.add('open');
}

async function saveProduto() {
  const nome = document.getElementById('prod-nome').value.trim();
  if (!nome) { toast('error','Nome obrigatório','Introduza o nome do medicamento'); return; }
  const btn = document.getElementById('btn-save-produto');
  setLoading(btn,true);
  await new Promise(r=>setTimeout(r,400));
  const data = {
    nome,
    forma: (()=>{ const s=document.getElementById('prod-forma').value; return s==='Outro'?(document.getElementById('prod-forma-outro').value.trim()||'Outro'):s; })(),
    grupo_farmacologico: (()=>{ const s=document.getElementById('prod-grupo').value; return s==='Outro'?(document.getElementById('prod-grupo-outro').value.trim()||'Outro'):s; })(),
    prateleira_id:parseInt(document.getElementById('prod-prateleira').value)||null,
    stock_minimo:parseInt(document.getElementById('prod-stock-min').value)||null,
    preco:parseFloat(document.getElementById('prod-preco').value)||null, status:'Ativo',
  };
  if (editingId) { db.update('produtos',editingId,data); toast('success','Produto actualizado'); }
  else { db.insert('produtos',data); toast('success','Produto cadastrado'); }
  setLoading(btn,false);
  closeModal('modal-produto');
  renderProdutos();
  updateAlertBadge();
}

async function deleteProduto(id) {
  const p = db.getById('produtos',id);
  const ok = await confirm('Eliminar Produto',`Deseja eliminar "${p?.nome}"?`);
  if (ok) { db.remove('produtos',id); toast('success','Produto eliminado'); renderProdutos(); }
}

// ===================== FORNECEDORES PAGE =====================
let fornSearch = '';
function renderFornecedores() {
  let forns = db.getAll('fornecedores');
  if (fornSearch) forns = forns.filter(f=>f.nome.toLowerCase().includes(fornSearch.toLowerCase())||(f.contacto||'').toLowerCase().includes(fornSearch.toLowerCase()));

  document.getElementById('page-fornecedores').innerHTML = `
    <div class="page-header">
      <div>
        <div class="page-title">${ICONS.supplier} Fornecedores</div>
        <div class="page-title-sub">Gerir fornecedores de medicamentos</div>
      </div>
      <div class="page-actions">
        <button class="btn btn-primary" onclick="openFornecedorModal()">${ICONS.plus}<span class="btn-text-content">Novo Fornecedor</span></button>
      </div>
    </div>
    <div class="table-wrap">
      <div class="table-header">
        <div class="table-title">${ICONS.list} Lista de Fornecedores <span class="chip">${forns.length}</span></div>
        <div class="table-actions">
          <div class="search-wrap">
            <input class="search-input" id="search-fornecedores" placeholder="Pesquisar..." value="${fornSearch}" oninput="fornSearch=this.value;filterFornecedoresTable()">
          </div>
        </div>
      </div>
      <div class="tbl-scroll">
      <table>
        <thead><tr><th>Nome</th><th>Contacto</th><th>Email</th><th>Telefone</th><th>Endereço</th><th>Acções</th></tr></thead>
        <tbody id="tbody-fornecedores">
          ${forns.length ? forns.map(f=>`<tr>
            <td class="td-name">${f.nome}</td>
            <td>${f.contacto||'—'}</td>
            <td>${f.email||'—'}</td>
            <td>${f.telefone||'—'}</td>
            <td>${f.endereco||'—'}</td>
            <td>
              <div style="display:flex;gap:5px;">
                <button class="btn btn-secondary btn-icon" onclick="openFornecedorModal(${f.id})">${ICONS.edit}</button>
                <button class="btn btn-danger btn-icon" onclick="deleteFornecedor(${f.id})">${ICONS.trash}</button>
              </div>
            </td>
          </tr>`).join('') : `<tr><td colspan="6"><div class="table-empty">${ICONS.supplier}<p>Nenhum fornecedor cadastrado</p></div></td></tr>`}
        </tbody>
      </table>
      </div>
    </div>

    <div class="modal-overlay" id="modal-forn">
      <div class="modal">
        <div class="modal-header">
          <div class="modal-title">${ICONS.supplier} <span id="modal-forn-title">Novo Fornecedor</span></div>
          <button class="modal-close" onclick="closeModal('modal-forn')">${ICONS.x}</button>
        </div>
        <div class="modal-body">
          <div class="form-grid form-grid-2">
            <div class="field-wrap form-grid-full">
              <label class="field-label">${ICONS.supplier} Nome da Empresa <span class="field-req">*</span></label>
              <input class="field-input" id="forn-nome" placeholder="Ex: MedDistrib Lda">
            </div>
            <div class="field-wrap">
              <label class="field-label">${ICONS.user} Contacto</label>
              <input class="field-input" id="forn-contacto" placeholder="Ex: João Silva">
            </div>
            <div class="field-wrap">
              <label class="field-label">${ICONS.mail} Email</label>
              <input class="field-input" id="forn-email" type="email" placeholder="Ex: empresa@mail.com">
            </div>
            <div class="field-wrap">
              <label class="field-label">${ICONS.phone} Telefone</label>
              <input class="field-input" id="forn-tel" placeholder="Ex: 923 000 000">
            </div>
            <div class="field-wrap form-grid-full">
              <label class="field-label">${ICONS.map_pin} Endereço</label>
              <input class="field-input" id="forn-end" placeholder="Ex: Luanda, Angola">
            </div>
          </div>
        </div>
        <div class="modal-footer">
          <button class="btn btn-secondary" onclick="closeModal('modal-forn')">Cancelar</button>
          <button class="btn btn-primary" id="btn-save-forn" onclick="saveFornecedor()">
            <span class="btn-spin"></span><span class="btn-text-content">${ICONS.check} Guardar</span>
          </button>
        </div>
      </div>
    </div>
  `;
  refocus('search-fornecedores');
}

function openFornecedorModal(id=null) {
  editingId = id;
  document.getElementById('modal-forn-title').textContent = id?'Editar Fornecedor':'Novo Fornecedor';
  if (id) {
    const f = db.getById('fornecedores',id);
    if (f) {
      document.getElementById('forn-nome').value=f.nome||'';
      document.getElementById('forn-contacto').value=f.contacto||'';
      document.getElementById('forn-email').value=f.email||'';
      document.getElementById('forn-tel').value=f.telefone||'';
      document.getElementById('forn-end').value=f.endereco||'';
    }
  } else {
    ['forn-nome','forn-contacto','forn-email','forn-tel','forn-end'].forEach(i=>document.getElementById(i).value='');
  }
  document.getElementById('modal-forn').classList.add('open');
}

async function saveFornecedor() {
  const nome = document.getElementById('forn-nome').value.trim();
  if (!nome) { toast('error','Nome obrigatório'); return; }
  const btn = document.getElementById('btn-save-forn');
  setLoading(btn,true);
  await new Promise(r=>setTimeout(r,400));
  const data = {
    nome, contacto:document.getElementById('forn-contacto').value,
    email:document.getElementById('forn-email').value,
    telefone:document.getElementById('forn-tel').value,
    endereco:document.getElementById('forn-end').value,
  };
  if (editingId) { db.update('fornecedores',editingId,data); toast('success','Fornecedor actualizado'); }
  else { db.insert('fornecedores',data); toast('success','Fornecedor cadastrado'); }
  setLoading(btn,false);
  closeModal('modal-forn');
  renderFornecedores();
}

async function deleteFornecedor(id) {
  const f = db.getById('fornecedores',id);
  const ok = await confirm('Eliminar Fornecedor',`Deseja eliminar "${f?.nome}"?`);
  if (ok) { db.remove('fornecedores',id); toast('success','Fornecedor eliminado'); renderFornecedores(); }
}

// ===================== PRATELEIRAS PAGE =====================
function renderPrateleiras() {
  const prateleiras = db.getAll('prateleiras');
  document.getElementById('page-prateleiras').innerHTML = `
    <div class="page-header">
      <div>
        <div class="page-title">${ICONS.shelf} Prateleiras</div>
        <div class="page-title-sub">Gerir localização e organização do depósito</div>
      </div>
      <div class="page-actions">
        <button class="btn btn-primary" onclick="openPrateleiraModal()">${ICONS.plus}<span class="btn-text-content">Nova Prateleira</span></button>
      </div>
    </div>
    <div class="table-wrap">
      <div class="table-header">
        <div class="table-title">${ICONS.list} Prateleiras <span class="chip">${prateleiras.length}</span></div>
      </div>
      <div class="tbl-scroll">
      <table>
        <thead><tr><th>Nome</th><th>Secção</th><th>Capacidade</th><th>Produtos</th><th>Ocupação</th><th>Acções</th></tr></thead>
        <tbody>
          ${prateleiras.length ? prateleiras.map(p=>{
            const count = db.getShelfCount(p.id);
            const pct = p.capacidade ? Math.min(100,Math.round((count/p.capacidade)*100)) : 0;
            return `<tr>
              <td class="td-name">${p.nome}</td>
              <td>${p.seccao||'—'}</td>
              <td>${p.capacidade||'—'}</td>
              <td class="font-bold text-accent">${count}</td>
              <td>
                <div style="display:flex;align-items:center;gap:8px;">
                  <div style="flex:1;height:6px;background:var(--border);border-radius:3px;">
                    <div style="height:6px;border-radius:3px;width:${pct}%;background:${pct>80?'var(--danger)':pct>50?'var(--warning)':'var(--accent)'};transition:width 0.4s;"></div>
                  </div>
                  <span style="font-size:11px;color:var(--text-muted);min-width:30px;">${pct}%</span>
                </div>
              </td>
              <td>
                <div style="display:flex;gap:5px;">
                  <button class="btn btn-secondary btn-icon" title="Ver Produtos" onclick="viewShelfProducts(${p.id})">${ICONS.eye}</button>
                  <button class="btn btn-secondary btn-icon" onclick="openPrateleiraModal(${p.id})">${ICONS.edit}</button>
                  <button class="btn btn-danger btn-icon" onclick="deletePrateleira(${p.id})">${ICONS.trash}</button>
                </div>
              </td>
            </tr>`;
          }).join('') : `<tr><td colspan="6"><div class="table-empty">${ICONS.shelf}<p>Nenhuma prateleira cadastrada</p></div></td></tr>`}
        </tbody>
      </table>
      </div>
    </div>

    <div class="modal-overlay" id="modal-prat">
      <div class="modal">
        <div class="modal-header">
          <div class="modal-title">${ICONS.shelf} <span id="modal-prat-title">Nova Prateleira</span></div>
          <button class="modal-close" onclick="closeModal('modal-prat')">${ICONS.x}</button>
        </div>
        <div class="modal-body">
          <div class="form-grid form-grid-2">
            <div class="field-wrap">
              <label class="field-label">Nome da Prateleira <span class="field-req">*</span></label>
              <input class="field-input" id="prat-nome" placeholder="Ex: Prateleira A1">
            </div>
            <div class="field-wrap">
              <label class="field-label">Secção</label>
              <input class="field-input" id="prat-seccao" placeholder="Ex: Secção A">
            </div>
            <div class="field-wrap">
              <label class="field-label">Capacidade (produtos)</label>
              <input class="field-input" id="prat-cap" type="number" min="1" placeholder="Ex: 100">
            </div>
          </div>
        </div>
        <div class="modal-footer">
          <button class="btn btn-secondary" onclick="closeModal('modal-prat')">Cancelar</button>
          <button class="btn btn-primary" id="btn-save-prat" onclick="savePrateleira()">
            <span class="btn-spin"></span><span class="btn-text-content">${ICONS.check} Guardar</span>
          </button>
        </div>
      </div>
    </div>

    <!-- Modal Ver Produtos da Prateleira -->
    <div class="modal-overlay" id="modal-shelf-products">
      <div class="modal modal-lg">
        <div class="modal-header">
          <div class="modal-title">${ICONS.shelf} <span id="modal-shelf-title">Produtos da Prateleira</span></div>
          <button class="modal-close" onclick="closeModal('modal-shelf-products')">${ICONS.x}</button>
        </div>
        <div class="modal-body" id="modal-shelf-body"></div>
        <div class="modal-footer">
          <button class="btn btn-secondary" onclick="closeModal('modal-shelf-products')">Fechar</button>
        </div>
      </div>
    </div>
  `;
}

function openPrateleiraModal(id=null) {
  editingId = id;
  document.getElementById('modal-prat-title').textContent = id?'Editar Prateleira':'Nova Prateleira';
  if (id) {
    const p = db.getById('prateleiras',id);
    if (p) {
      document.getElementById('prat-nome').value=p.nome||'';
      document.getElementById('prat-seccao').value=p.seccao||'';
      document.getElementById('prat-cap').value=p.capacidade||'';
    }
  } else {
    ['prat-nome','prat-seccao','prat-cap'].forEach(i=>document.getElementById(i).value='');
  }
  document.getElementById('modal-prat').classList.add('open');
}

async function savePrateleira() {
  const nome = document.getElementById('prat-nome').value.trim();
  if (!nome) { toast('error','Nome obrigatório'); return; }
  const btn = document.getElementById('btn-save-prat');
  setLoading(btn,true);
  await new Promise(r=>setTimeout(r,400));
  const data = { nome, seccao:document.getElementById('prat-seccao').value, capacidade:parseInt(document.getElementById('prat-cap').value)||null };
  if (editingId) { db.update('prateleiras',editingId,data); toast('success','Prateleira actualizada'); }
  else { db.insert('prateleiras',data); toast('success','Prateleira cadastrada'); }
  setLoading(btn,false); closeModal('modal-prat'); renderPrateleiras();
}

async function deletePrateleira(id) {
  const p = db.getById('prateleiras',id);
  const ok = await confirm('Eliminar Prateleira',`Deseja eliminar "${p?.nome}"?`);
  if (ok) { db.remove('prateleiras',id); toast('success','Prateleira eliminada'); renderPrateleiras(); }
}

function viewShelfProducts(pratId) {
  const prat = db.getById('prateleiras', pratId);
  const prods = db.getShelfProducts(pratId);
  const el = document.getElementById('modal-shelf-products');
  if (!el) return;
  document.getElementById('modal-shelf-title').textContent = `Produtos — ${prat?.nome||'Prateleira'}`;
  document.getElementById('modal-shelf-body').innerHTML = prods.length ? `
    <div class="tbl-scroll">
    <table>
      <thead><tr><th>Nome</th><th>Forma</th><th>Grupo</th><th>Stock Mín.</th><th>Stock Actual</th></tr></thead>
      <tbody>
        ${prods.map(p=>{
          const {stock}=db.getStock(p.id);
          return `<tr>
            <td class="td-name">${p.nome}</td>
            <td>${p.forma||'—'}</td>
            <td>${p.grupo_farmacologico||'—'}</td>
            <td>${p.stock_minimo||'—'}</td>
            <td class="font-bold ${stock>0?'text-info':'text-danger'}">${stock}</td>
          </tr>`;
        }).join('')}
      </tbody>
    </table>
    </div>
  ` : `<div class="table-empty" style="padding:32px;">${ICONS.shelf}<p>Nenhum produto nesta prateleira</p></div>`;
  el.classList.add('open');
}

// ===================== LOTES PAGE =====================
let loteSearch='', loteFilter='todos';
function renderLotes() {
  const produtos = db.getAll('produtos');
  const fornecedores = db.getAll('fornecedores');
  let lotes = db.getAll('lotes');
  if (loteSearch) lotes = lotes.filter(l => {
    const _sp = db.getById('produtos', l.produto_id);
    const _sf = db.getById('fornecedores', l.fornecedor_id);
    const _sq = loteSearch.toLowerCase();
    return l.numero_lote.toLowerCase().includes(_sq) ||
      (_sp?.nome||'').toLowerCase().includes(_sq) ||
      (_sf?.nome||'').toLowerCase().includes(_sq) ||
      (_sp?.grupo_farmacologico||'').toLowerCase().includes(_sq) ||
      (_sp?.forma||'').toLowerCase().includes(_sq);
  });
  if (loteFilter==='ativos') lotes = lotes.filter(l=>daysUntil(l.validade)>=0);
  if (loteFilter==='avencer') lotes = lotes.filter(l=>{ const d=daysUntil(l.validade); return d>=0&&d<=90; });
  if (loteFilter==='vencidos') lotes = lotes.filter(l=>daysUntil(l.validade)<0);
  lotes.sort((a, b) => {
    const nA = (db.getById('produtos', a.produto_id)?.nome || '').toLowerCase();
    const nB = (db.getById('produtos', b.produto_id)?.nome || '').toLowerCase();
    return nA.localeCompare(nB, 'pt', {sensitivity:'base'});
  });

  document.getElementById('page-lotes').innerHTML = `
    <div class="page-header">
      <div>
        <div class="page-title">${ICONS.lot} Cadastro de Lotes</div>
        <div class="page-title-sub">Gerir lotes e validades de medicamentos</div>
      </div>
      <div class="page-actions">
        <button class="btn btn-primary" onclick="openLoteModal()">${ICONS.plus}<span class="btn-text-content">Novo Lote</span></button>
      </div>
    </div>
    <div class="table-wrap">
      <div class="table-header">
        <div class="table-title">${ICONS.list} Lotes <span class="chip">${lotes.length}</span></div>
        <div class="table-actions">
          <div class="search-wrap">
            <input class="search-input" id="search-lotes" placeholder="Pesquisar por lote, produto, fornecedor, grupo ou forma..." value="${loteSearch}" oninput="loteSearch=this.value;renderLotesFiltered()">
          </div>
          <select class="select-filter" onchange="loteFilter=this.value;renderLotesFiltered(true)">
            <option value="todos" ${loteFilter==='todos'?'selected':''}>Todos</option>
            <option value="ativos" ${loteFilter==='ativos'?'selected':''}>Activos</option>
            <option value="avencer" ${loteFilter==='avencer'?'selected':''}>A Vencer</option>
            <option value="vencidos" ${loteFilter==='vencidos'?'selected':''}>Vencidos</option>
          </select>
        </div>
      </div>
      <div class="tbl-scroll">
      <table>
        <thead><tr><th>Nº Lote</th><th>Produto</th><th>Fornecedor</th><th>Qtd. Inicial</th><th>Stock Actual</th><th>Preço (AOA)</th><th>Validade</th><th>Dias Rest.</th><th>Código Barras</th><th>Status</th><th>Acções</th></tr></thead>
        <tbody id="tbody-lotes">
          ${lotes.length ? lotes.map(l=>{
            const prod=db.getById('produtos',l.produto_id);
            const forn=db.getById('fornecedores',l.fornecedor_id);
            const isBloqueado = !!l.bloqueado;
            const st=getLotStatus(l.validade, isBloqueado);
            const dias=daysUntil(l.validade);
            const stockActual = db.getLoteStock(l.id);
            return `<tr style="${isBloqueado?'opacity:0.65;background:var(--bg-tertiary,#f5f5f5);':''}">
              <td class="font-mono text-accent">${l.numero_lote}${isBloqueado?` <span style="color:var(--danger,#dc3545);font-size:10px;">🔒</span>`:''}</td>
              <td class="td-name">${prod?prod.nome:'—'}</td>
              <td>${forn?forn.nome:'—'}</td>
              <td class="font-bold">${l.quantidade||0}</td>
              <td class="font-bold ${stockActual<0?'text-danger':stockActual===0?'text-muted':'text-accent'}">${stockActual}</td>
              <td class="font-mono">${l.preco?formatMoney(l.preco):'—'}</td>
              <td>${formatDate(l.validade)}</td>
              <td class="${dias<0?'text-danger':dias<=90?'text-warning':'text-accent'}">${dias<0?`Há ${Math.abs(dias)}d`:dias===Infinity?'—':`${dias}d`}</td>
              <td class="font-mono text-muted">${l.codigo_barra||'—'}</td>
              <td><span class="badge ${st.cls}">${st.label}</span></td>
              <td>
                <div style="display:flex;gap:5px;">
                  <button class="btn btn-secondary btn-icon" title="${isBloqueado?'Desbloquear lote':'Bloquear lote'}" onclick="toggleLoteBloqueado(${l.id})" style="${isBloqueado?'color:var(--warning,#ffc107);':''}">${isBloqueado?ICONS.unlock:ICONS.lock}</button>
                  <button class="btn btn-secondary btn-icon" onclick="openLoteModal(${l.id})">${ICONS.edit}</button>
                  <button class="btn btn-danger btn-icon" onclick="deleteLote(${l.id})">${ICONS.trash}</button>
                </div>
              </td>
            </tr>`;
          }).join('') : `<tr><td colspan="11"><div class="table-empty">${ICONS.lot}<p>Nenhum lote encontrado</p></div></td></tr>`}
        </tbody>
      </table>
      </div>
    </div>

    <div class="modal-overlay" id="modal-lote">
      <div class="modal">
        <div class="modal-header">
          <div class="modal-title">${ICONS.lot} <span id="modal-lote-title">Novo Lote</span></div>
          <button class="modal-close" onclick="closeModal('modal-lote')">${ICONS.x}</button>
        </div>
        <div class="modal-body">
          <div class="form-grid form-grid-2">
            <div class="field-wrap">
              <label class="field-label">${ICONS.barcode} Número do Lote <span class="field-req">*</span></label>
              <input class="field-input" id="lote-numero" placeholder="Ex: LOT-2025-001">
            </div>
            <div class="field-wrap">
              <label class="field-label">${ICONS.pill} Produto <span class="field-req">*</span></label>
              <select class="field-select" id="lote-produto">
                <option value="">Seleccionar produto...</option>
                ${produtos.map(p=>`<option value="${p.id}">${p.nome}</option>`).join('')}
              </select>
            </div>
            <div class="field-wrap">
              <label class="field-label">${ICONS.supplier} Fornecedor</label>
              <select class="field-select" id="lote-fornecedor">
                <option value="">Seleccionar fornecedor...</option>
                ${fornecedores.map(f=>`<option value="${f.id}">${f.nome}</option>`).join('')}
              </select>
            </div>
            <div class="field-wrap">
              <label class="field-label">${ICONS.package} Quantidade</label>
              <input class="field-input" id="lote-quantidade" type="number" min="1" placeholder="Ex: 200">
            </div>
            <div class="field-wrap">
              <label class="field-label">${ICONS.calendar} Validade</label>
              <input class="field-input" id="lote-validade" type="date">
            </div>
            <div class="field-wrap">
              <label class="field-label">${ICONS.money} Preço Unitário (AOA)</label>
              <input class="field-input" id="lote-preco" type="number" min="0" step="0.01" placeholder="Opcional">
            </div>
            <div class="field-wrap">
              <label class="field-label">${ICONS.barcode} Código de Barras</label>
              <input class="field-input" id="lote-barcode" placeholder="Ex: 7891234567890">
            </div>
          </div>
        </div>
        <div class="modal-footer">
          <button class="btn btn-secondary" onclick="closeModal('modal-lote')">Cancelar</button>
          <button class="btn btn-primary" id="btn-save-lote" onclick="saveLote()">
            <span class="btn-spin"></span><span class="btn-text-content">${ICONS.check} Guardar</span>
          </button>
        </div>
      </div>
    </div>
  `;
  refocus('search-lotes');
}

function openLoteModal(id=null) {
  editingId=id;
  document.getElementById('modal-lote-title').textContent=id?'Editar Lote':'Novo Lote';
  if (id) {
    const l=db.getById('lotes',id);
    if(l){
      document.getElementById('lote-numero').value=l.numero_lote||'';
      document.getElementById('lote-produto').value=l.produto_id||'';
      document.getElementById('lote-fornecedor').value=l.fornecedor_id||'';
      document.getElementById('lote-quantidade').value=l.quantidade||'';
      document.getElementById('lote-validade').value=l.validade||'';
      document.getElementById('lote-barcode').value=l.codigo_barra||'';
      document.getElementById('lote-preco').value=l.preco||'';
    }
  } else {
    ['lote-numero','lote-quantidade','lote-validade','lote-barcode','lote-preco'].forEach(i=>document.getElementById(i).value='');
    document.getElementById('lote-produto').value='';
    document.getElementById('lote-fornecedor').value='';
  }
  document.getElementById('modal-lote').classList.add('open');
}

async function saveLote() {
  const numero=document.getElementById('lote-numero').value.trim();
  const prodId=parseInt(document.getElementById('lote-produto').value);
  if(!numero){toast('error','Número do lote obrigatório');return;}
  if(!prodId){toast('error','Produto obrigatório');return;}
  const btn=document.getElementById('btn-save-lote');
  setLoading(btn,true);
  await new Promise(r=>setTimeout(r,400));
  const qtd=parseInt(document.getElementById('lote-quantidade').value)||0;
  const lotePreco=parseFloat(document.getElementById('lote-preco').value)||null;
  const data={
    numero_lote:numero, produto_id:prodId,
    fornecedor_id:parseInt(document.getElementById('lote-fornecedor').value)||null,
    quantidade:qtd,
    validade:document.getElementById('lote-validade').value,
    codigo_barra:document.getElementById('lote-barcode').value,
    preco:lotePreco,
  };
  if(editingId){
    const oldLote=db.getById('lotes',editingId);
    db.update('lotes',editingId,data);
    toast('success','Lote actualizado');
    // If quantity changed (new stock added), create auto entrada
    const oldQtd=oldLote?.quantidade||0;
    if(qtd > oldQtd && qtd - oldQtd > 0){
      const diff=qtd-oldQtd;
      const forn=db.getById('fornecedores',data.fornecedor_id);
      db.insert('movimentacoes',{
        produto_id:prodId, produto_nome: db.getById('produtos',prodId)?.nome||`Produto #${prodId}`, tipo:'ENTRADA',
        lote_id:editingId, quantidade:diff,
        destino:`Entrada automática — Adição ao Lote ${numero}${forn?' (Forn: '+forn.nome+')':''}`,
        data:today(), preco:lotePreco, auto:true,
        usuario_nome: currentUser?.nome || '',
        usuario_id: currentUser?.id || null,
      });
      toast('info','Movimentação criada',`Entrada automática de ${diff} unid. registada`);
    }
  } else {
    const novoLote=db.insert('lotes',data);
    toast('success','Lote cadastrado');
    // Auto-create entrada movimentação if quantity > 0
    if(qtd > 0){
      const forn=db.getById('fornecedores',data.fornecedor_id);
      db.insert('movimentacoes',{
        produto_id:prodId, produto_nome: db.getById('produtos',prodId)?.nome||`Produto #${prodId}`, tipo:'ENTRADA',
        lote_id:novoLote.id, quantidade:qtd,
        destino:`Entrada automática — Novo Lote ${numero}${forn?' | Forn: '+forn.nome:''}`,
        data:today(), preco:lotePreco, auto:true,
        usuario_nome: currentUser?.nome || '',
        usuario_id: currentUser?.id || null,
      });
      toast('info','Movimentação criada',`Entrada automática de ${qtd} unid. registada automaticamente`);
    }
  }
  setLoading(btn,false); closeModal('modal-lote'); renderLotes(); updateAlertBadge();
}

async function deleteLote(id) {
  const l=db.getById('lotes',id);
  const ok=await confirm('Eliminar Lote',`Deseja eliminar o lote "${l?.numero_lote}"?`);
  if(ok){db.remove('lotes',id);toast('success','Lote eliminado');renderLotes();updateAlertBadge();}
}

async function toggleLoteBloqueado(id) {
  const l = db.getById('lotes', id);
  if (!l) return;
  const bloqueado = !l.bloqueado;
  const acao = bloqueado ? 'bloquear' : 'desbloquear';
  const ok = await confirm(
    bloqueado ? 'Bloquear Lote' : 'Desbloquear Lote',
    `Deseja ${acao} o lote "${l.numero_lote}"? ${bloqueado ? 'Nenhuma movimentação poderá ser feita com este lote enquanto estiver bloqueado.' : 'O lote voltará a estar disponível para movimentações.'}`
  );
  if (ok) {
    db.update('lotes', id, { bloqueado });
    toast(bloqueado ? 'warning' : 'success',
      bloqueado ? 'Lote bloqueado' : 'Lote desbloqueado',
      `O lote "${l.numero_lote}" foi ${bloqueado ? 'bloqueado' : 'desbloqueado'} com sucesso.`
    );
    renderLotes();
  }
}

// ===================== MOVIMENTACOES PAGE =====================
let movSearch='', movFilter='todos', movDateFrom='', movDateTo='';
let movPage=0;             // página actual (0-indexed)
const MOV_PAGE_SIZE=100;   // desktop: 100 linhas por página

function renderMovimentacoes() {
  movPage = 0;
  _renderMovimentacoesUI();
}

function _getMovsFiltrados() {
  const toISO = d => { if(!d) return ''; const p=String(d).split('-'); if(p.length===3&&p[2].length===4)return p[2]+'-'+p[1]+'-'+p[0]; return String(d); };
  let movs = db.getAll('movimentacoes');
  if (movSearch) {
    const q = movSearch.toLowerCase();
    movs = movs.filter(m=>{
      const nome=(m.produto_nome||(db.getById('produtos',m.produto_id)?.nome)||'').toLowerCase();
      return nome.includes(q)||(m.destino||'').toLowerCase().includes(q);
    });
  }
  if (movFilter!=='todos') movs = movs.filter(m=>(m.tipo||'').toUpperCase()===movFilter.toUpperCase());
  if (movDateFrom) movs = movs.filter(m=>toISO(m.data)>=movDateFrom);
  if (movDateTo)   movs = movs.filter(m=>toISO(m.data)<=movDateTo);
  movs.sort((a,b)=>toISO(b.data).localeCompare(toISO(a.data)));
  return movs;
}

function _buildMovRow(m) {
  const prodNome = m.produto_nome||(db.getById('produtos',m.produto_id)?.nome)||'—';
  const lot = db.getById('lotes', m.lote_id);
  const isEnt = (m.tipo||'').toUpperCase()==='ENTRADA';
  return `<tr>
    <td class="td-name">${prodNome}</td>
    <td><span class="badge ${isEnt?'badge-success':'badge-danger'}">${isEnt?ICONS.arrow_up:ICONS.arrow_down} ${m.tipo||'—'}</span></td>
    <td class="font-mono text-muted">${lot?lot.numero_lote:'—'}</td>
    <td class="font-bold ${isEnt?'text-accent':'text-danger'}">${m.quantidade}</td>
    <td>${m.destino||'—'}</td>
    <td>${m.preco?formatMoney(m.preco):'—'}</td>
    <td>${formatDate(m.data)}</td>
    <td><div style="display:flex;gap:5px;">
      <button class="btn btn-secondary btn-icon" onclick="openMovModal(${m.id})">${ICONS.edit}</button>
      <button class="btn btn-danger btn-icon" onclick="deleteMov(${m.id})">${ICONS.trash}</button>
    </div></td>
  </tr>`;
}

function _renderMovPagination(total, page) {
  const totalPages = Math.max(1, Math.ceil(total / MOV_PAGE_SIZE));
  if (totalPages <= 1) return '';
  const from = page * MOV_PAGE_SIZE + 1;
  const to   = Math.min((page+1)*MOV_PAGE_SIZE, total);
  const pages = [];
  // Mostrar até 7 botões de página em desktop
  let pStart = Math.max(0, page-3), pEnd = Math.min(totalPages-1, page+3);
  if (pEnd - pStart < 6) { pStart = Math.max(0, pEnd-6); pEnd = Math.min(totalPages-1, pStart+6); }
  for (let i=pStart; i<=pEnd; i++) {
    pages.push(`<button class="btn ${i===page?'btn-primary':'btn-secondary'} mov-pag-num" onclick="movGoPage(${i})">${i+1}</button>`);
  }
  return `<div class="mov-pagination">
    <button class="btn btn-secondary" onclick="movGoPage(0)" ${page===0?'disabled':''} title="Primeira">«</button>
    <button class="btn btn-secondary" onclick="movGoPage(${page-1})" ${page===0?'disabled':''}>‹</button>
    ${pages.join('')}
    <button class="btn btn-secondary" onclick="movGoPage(${page+1})" ${page>=totalPages-1?'disabled':''}>›</button>
    <button class="btn btn-secondary" onclick="movGoPage(${totalPages-1})" ${page>=totalPages-1?'disabled':''} title="Última">»</button>
    <span class="mov-pag-info">${from}–${to} de ${total} registos</span>
  </div>`;
}

function movGoPage(p) {
  const total = _getMovsFiltrados().length;
  const totalPages = Math.max(1, Math.ceil(total / MOV_PAGE_SIZE));
  movPage = Math.max(0, Math.min(p, totalPages-1));
  _updateMovTbody();
  // scroll suave para o topo da tabela em desktop
  const wrap = document.getElementById('page-movimentacoes');
  if (wrap) wrap.scrollTo({top:0, behavior:'smooth'});
}

function _updateMovTbody() {
  const allMovs   = _getMovsFiltrados();
  const total     = allMovs.length;
  const totalPages= Math.max(1, Math.ceil(total / MOV_PAGE_SIZE));
  if (movPage >= totalPages) movPage = totalPages-1;
  const page = allMovs.slice(movPage*MOV_PAGE_SIZE, (movPage+1)*MOV_PAGE_SIZE);

  const tbody = document.getElementById('tbody-movimentacoes');
  if (tbody) tbody.innerHTML = page.length
    ? page.map(_buildMovRow).join('')
    : `<tr><td colspan="8"><div class="table-empty">${ICONS.movement}<p>Nenhuma movimentação encontrada</p></div></td></tr>`;

  const counter = document.querySelector('#page-movimentacoes .table-title .chip');
  if (counter) counter.textContent = total;

  ['mov-pagination-bar','mov-pagination-bar-bottom'].forEach(id=>{
    const el=document.getElementById(id);
    if(el) el.innerHTML=_renderMovPagination(total, movPage);
  });
}

function _renderMovimentacoesUI() {
  const produtos = db.getAll('produtos');
  const allMovs  = _getMovsFiltrados();
  const total    = allMovs.length;
  const page     = allMovs.slice(movPage*MOV_PAGE_SIZE, (movPage+1)*MOV_PAGE_SIZE);

  document.getElementById('page-movimentacoes').innerHTML = `
    <div class="page-header">
      <div>
        <div class="page-title">${ICONS.movement} Entradas / Saídas</div>
        <div class="page-title-sub">Registar e consultar movimentações de stock</div>
      </div>
      <div class="page-actions">
        <button class="btn btn-primary" onclick="openMovModal()">${ICONS.plus}<span class="btn-text-content">Nova Movimentação</span></button>
      </div>
    </div>
    <div class="table-wrap">
      <div class="table-header">
        <div class="table-title">${ICONS.list} Movimentações <span class="chip">${total}</span></div>
        <div class="table-actions" style="flex-wrap:wrap;gap:8px;">
          <div class="search-wrap">
            <input class="search-input" id="search-movimentacoes" placeholder="Pesquisar produto/destino..." value="${movSearch}" oninput="movSearch=this.value;movPage=0;filterMovimentacoesTable()">
          </div>
          <select class="select-filter" onchange="movFilter=this.value;movPage=0;filterMovimentacoesTable()">
            <option value="todos" ${movFilter==='todos'?'selected':''}>Todos os tipos</option>
            <option value="ENTRADA" ${movFilter==='ENTRADA'?'selected':''}>Entradas</option>
            <option value="SAÍDA" ${movFilter==='SAÍDA'?'selected':''}>Saídas</option>
          </select>
          <div class="date-filter-wrap">
            ${ICONS.calendar}
            <input type="date" value="${movDateFrom}" onchange="movDateFrom=this.value;movPage=0;filterMovimentacoesTable()" title="Data de início">
            <span>—</span>
            <input type="date" value="${movDateTo}" onchange="movDateTo=this.value;movPage=0;filterMovimentacoesTable()" title="Data de fim">
          </div>
          ${(movDateFrom||movDateTo)?`<button class="btn btn-secondary" onclick="movDateFrom='';movDateTo='';movPage=0;filterMovimentacoesTable()" style="padding:6px 10px;font-size:11px;">${ICONS.x} Limpar datas</button>`:''}
        </div>
      </div>
      <div id="mov-pagination-bar">${_renderMovPagination(total, movPage)}</div>
      <div class="tbl-scroll">
      <table>
        <thead><tr>
          <th>Produto</th><th>Tipo</th><th>Lote</th><th>Quantidade</th><th>Destino/Origem</th><th>Preço Unit.</th><th>Data</th><th>Acções</th>
        </tr></thead>
        <tbody id="tbody-movimentacoes">
          ${page.length ? page.map(_buildMovRow).join('') : `<tr><td colspan="8"><div class="table-empty">${ICONS.movement}<p>Nenhuma movimentação encontrada</p></div></td></tr>`}
        </tbody>
      </table>
      </div>
      <div id="mov-pagination-bar-bottom">${_renderMovPagination(total, movPage)}</div>
    </div>

    <div class="modal-overlay" id="modal-mov">
      <div class="modal">
        <div class="modal-header">
          <div class="modal-title">${ICONS.movement} <span id="modal-mov-title">Nova Movimentação</span></div>
          <button class="modal-close" onclick="closeModal('modal-mov')">${ICONS.x}</button>
        </div>
        <div class="modal-body">
          <div class="form-grid form-grid-2">
            <div class="field-wrap">
              <label class="field-label">${ICONS.pill} Produto <span class="field-req">*</span></label>
              <div class="combo-wrap" id="combo-mov-produto-wrap">
                <input class="field-input" id="combo-mov-produto-input" autocomplete="off"
                  placeholder="Escrever ou seleccionar produto..."
                  oninput="filterMovCombo(this.value)"
                  onfocus="openMovCombo()"
                  onblur="setTimeout(()=>closeMovCombo(),220)">
                <input type="hidden" id="mov-produto">
                <div class="combo-dropdown" id="combo-mov-produto-list">
                  ${produtos.map(p=>{ const {stock}=db.getStock(p.id); return `<div class="combo-option" data-id="${p.id}" data-nome="${p.nome.replace(/"/g,'&quot;')}" onmousedown="selectMovCombo(${p.id},this.dataset.nome)"><span class="combo-opt-nome">${p.nome}</span><span class="combo-opt-stock ${stock<=0?'combo-stock-zero':stock<=(p.stock_minimo||0)?'combo-stock-low':'combo-stock-ok'}">${stock} un.</span></div>`; }).join('')}
                </div>
              </div>
            </div>
            <div class="field-wrap">
              <label class="field-label">${ICONS.movement} Tipo <span class="field-req">*</span></label>
              <select class="field-select" id="mov-tipo">
                <option value="Entrada">Entrada</option>
                <option value="Saída">Saída</option>
              </select>
            </div>
            <div class="field-wrap">
              <label class="field-label">${ICONS.lot} Lote</label>
              <select class="field-select" id="mov-lote">
                <option value="">Seleccionar lote...</option>
              </select>
            </div>
            <div class="field-wrap">
              <label class="field-label">${ICONS.package} Quantidade <span class="field-req">*</span></label>
              <input class="field-input" id="mov-quantidade" type="number" min="1" placeholder="Ex: 50">
            </div>
            <div class="field-wrap">
              <label class="field-label">${ICONS.map_pin} Destino/Origem</label>
              <input class="field-input" id="mov-destino" placeholder="Ex: Enfermaria A">
            </div>
            <div class="field-wrap">
              <label class="field-label">${ICONS.calendar} Data</label>
              <input class="field-input" id="mov-data" type="date" value="${new Date().toISOString().split('T')[0]}">
            </div>
            <div class="field-wrap form-grid-full">
              <label class="field-label">${ICONS.money} Preço Unitário (AOA)</label>
              <input class="field-input" id="mov-preco" type="number" min="0" step="0.01" placeholder="Opcional">
            </div>
          </div>
        </div>
        <div class="modal-footer">
          <button class="btn btn-secondary" onclick="closeModal('modal-mov')">Cancelar</button>
          <button class="btn btn-primary" id="btn-save-mov" onclick="saveMov()">
            <span class="btn-spin"></span><span class="btn-text-content">${ICONS.check} Registar</span>
          </button>
        </div>
      </div>
    </div>
  `;
  refocus('search-movimentacoes');
}

function updateMovLotes() {
  const prodId = parseInt(document.getElementById('mov-produto').value);
  const loteSelect = document.getElementById('mov-lote');
  if (!loteSelect) return;
  const prodLotes = db.getAll('lotes').filter(l=>l.produto_id===prodId).sort((a,b)=>new Date(a.validade)-new Date(b.validade));
  loteSelect.innerHTML = `<option value="" data-preco="">Seleccionar lote...</option>` +
    prodLotes.map(l=>{
      const st=getLotStatus(l.validade, !!l.bloqueado);
      return `<option value="${l.id}" data-preco="${l.preco||''}">${l.numero_lote} — Val: ${formatDate(l.validade)} (${st.label})</option>`;
    }).join('');
  // Reset preco when lotes list changes
  loteSelect.onchange = function() {
    const sel = this.options[this.selectedIndex];
    const precoEl = document.getElementById('mov-preco');
    if (!precoEl) return;
    const precoLote = sel ? sel.getAttribute('data-preco') : '';
    if (precoLote) {
      precoEl.value = precoLote;
    } else {
      // Try to fill from produto preco if no lote preco
      const produtoId = parseInt(document.getElementById('mov-produto').value);
      const prod = produtoId ? db.getById('produtos', produtoId) : null;
      precoEl.value = (prod && prod.preco) ? prod.preco : '';
    }
  };
}

function openMovModal(id=null) {
  // Garantir que a combobox está fechada e no lugar certo antes de abrir
  closeMovCombo();

  editingId=id;
  document.getElementById('modal-mov-title').textContent=id?'Editar Movimentação':'Nova Movimentação';

  // Repopular a lista da combobox com produtos actualizados
  const wrap = document.getElementById('combo-mov-produto-wrap');
  let list = document.getElementById('combo-mov-produto-list');
  if (list && wrap) {
    // Garantir que está no wrap (não no body)
    if (list.parentElement !== wrap) wrap.appendChild(list);
    // Repopular
    const produtos = db.getAll('produtos');
    list.innerHTML = produtos.map(p => {
      const {stock} = db.getStock(p.id);
      const stockClass = stock<=0?'combo-stock-zero':stock<=(p.stock_minimo||0)?'combo-stock-low':'combo-stock-ok';
      return `<div class="combo-option" data-id="${p.id}" data-nome="${p.nome.replace(/"/g,'&quot;')}" onmousedown="selectMovCombo(${p.id},this.dataset.nome)"><span class="combo-opt-nome">${p.nome}</span><span class="combo-opt-stock ${stockClass}">${stock} un.</span></div>`;
    }).join('');
    list.style.display = 'none';
    list.style.position = '';
    list.style.top = '';
    list.style.left = '';
    list.style.width = '';
    list.style.zIndex = '';
  }

  if(id){
    const m=db.getById('movimentacoes',id);
    if(m){
      const prod = db.getById('produtos', m.produto_id);
      document.getElementById('mov-produto').value=m.produto_id||'';
      document.getElementById('combo-mov-produto-input').value = prod ? prod.nome : '';
      updateMovLotes();
      document.getElementById('mov-tipo').value=(m.tipo||'ENTRADA').toUpperCase()==='ENTRADA'?'Entrada':'Saída';
      document.getElementById('mov-lote').value=m.lote_id||'';
      document.getElementById('mov-quantidade').value=m.quantidade||'';
      document.getElementById('mov-destino').value=m.destino||'';
      document.getElementById('mov-data').value=storageToDateInput(m.data)||new Date().toISOString().split('T')[0];
      document.getElementById('mov-preco').value=m.preco||'';
    }
  } else {
    document.getElementById('mov-produto').value='';
    document.getElementById('combo-mov-produto-input').value='';
    document.getElementById('mov-tipo').value='Entrada';
    document.getElementById('mov-lote').value='';
    document.getElementById('mov-quantidade').value='';
    document.getElementById('mov-destino').value='';
    document.getElementById('mov-data').value=new Date().toISOString().split('T')[0];
    document.getElementById('mov-preco').value='';
  }
  document.getElementById('modal-mov').classList.add('open');
}

async function saveMov() {
  const prodId=parseInt(document.getElementById('mov-produto').value);
  const tipo=document.getElementById('mov-tipo').value;
  const qtd=parseInt(document.getElementById('mov-quantidade').value);
  if(!prodId){toast('error','Produto obrigatório');return;}
  if(!qtd||qtd<1){toast('error','Quantidade inválida');return;}
  // Verificar se o lote está bloqueado
  let loteIdSel = parseInt(document.getElementById('mov-lote').value)||null;

  // FIX 3: Auto-selecionar lote com validade mais próxima em Saídas
  if (!loteIdSel && tipo.toUpperCase() === 'SAÍDA') {
    const lotesDisponiveis = db.getAll('lotes')
      .filter(l => l.produto_id === prodId && !l.bloqueado && daysUntil(l.validade) >= 0 && db.getLoteStock(l.id) > 0)
      .sort((a, b) => new Date(a.validade) - new Date(b.validade));
    if (lotesDisponiveis.length > 0) {
      loteIdSel = lotesDisponiveis[0].id;
      // Actualizar visualmente o select
      const loteSelectEl = document.getElementById('mov-lote');
      if (loteSelectEl) loteSelectEl.value = loteIdSel;
      toast('info', 'Lote auto-seleccionado', `Lote "${lotesDisponiveis[0].numero_lote}" seleccionado automaticamente (validade mais próxima).`);
    }
  }

  if (loteIdSel) {
    const loteSel = db.getById('lotes', loteIdSel);
    if (loteSel && loteSel.bloqueado) {
      toast('error', 'Lote bloqueado', `O lote "${loteSel.numero_lote}" está bloqueado e não pode ser movimentado. Desbloqueie-o primeiro na secção de Lotes.`);
      return;
    }
  }
  if(tipo.toUpperCase()==='SAÍDA'){
    const {stock}=db.getStock(prodId);
    const prod=db.getById('produtos',prodId);
    if(stock-qtd<0){toast('error','Stock insuficiente',`Stock actual: ${stock}. Saída bloqueada.`);return;}
  }
  const btn=document.getElementById('btn-save-mov');
  setLoading(btn,true);
  await new Promise(r=>setTimeout(r,400));
  const prodObj = db.getById('produtos', prodId);
  const data={
    produto_id:prodId,
    produto_nome: prodObj ? prodObj.nome : `Produto #${prodId}`,
    tipo: tipo.toUpperCase(),
    lote_id:parseInt(document.getElementById('mov-lote').value)||null,
    quantidade:qtd,
    destino:document.getElementById('mov-destino').value,
    data: dateInputToStorage(document.getElementById('mov-data').value),
    preco:parseFloat(document.getElementById('mov-preco').value)||null,
    usuario_nome: currentUser?.nome || '',
    usuario_id: currentUser?.id || null,
  };
  if(editingId){db.update('movimentacoes',editingId,data);toast('success','Movimentação actualizada');}
  else{db.insert('movimentacoes',data);toast('success',`${tipo} registada com sucesso`);}
  setLoading(btn,false); closeModal('modal-mov'); renderMovimentacoes(); updateAlertBadge();
}

async function deleteMov(id) {
  const ok=await confirm('Eliminar Movimentação','Deseja eliminar esta movimentação?');
  if(ok){db.remove('movimentacoes',id);toast('success','Movimentação eliminada');renderMovimentacoes();updateAlertBadge();}
}

// ===================== ALERTAS PAGE =====================
function renderAlertas() {
  const alerts = getAlerts();
  document.getElementById('page-alertas').innerHTML = `
    <div class="page-header">
      <div>
        <div class="page-title">${ICONS.alert} Alertas do Sistema</div>
        <div class="page-title-sub">Notificações e avisos importantes</div>
      </div>
      <button class="btn btn-secondary" onclick="renderAlertas()">${ICONS.refresh} Actualizar</button>
    </div>
    <div class="grid-2" style="gap:20px;margin-bottom:20px;">
      ${[
        ['Lotes Vencidos',alerts.filter(a=>a.type==='err').length,'err'],
        ['A Vencer (90d)',alerts.filter(a=>a.title.includes('vencer')).length,'warn'],
        ['Stock Mínimo',alerts.filter(a=>a.title.includes('Stock')).length,'warn'],
        ['Total Alertas',alerts.length,alerts.length>0?'err':'ok'],
      ].map(([l,v,t])=>`
        <div class="card" style="display:flex;align-items:center;gap:16px;">
          <div class="alert-icon ${t}">${ICONS[t==='err'?'alert':t==='warn'?'clock':t==='ok'?'check':'info']}</div>
          <div>
            <div style="font-size:24px;font-weight:700;color:var(--text-primary)">${v}</div>
            <div style="font-size:12px;color:var(--text-muted)">${l}</div>
          </div>
        </div>
      `).join('')}
    </div>
    <div class="table-wrap">
      <div class="table-header">
        <div class="table-title">${ICONS.bell} Todas as Notificações <span class="chip">${alerts.length}</span></div>
      </div>
      ${alerts.length ? alerts.map(a=>`
        <div class="alert-item">
          <div class="alert-icon ${a.type}">${ICONS[a.icon]||ICONS.alert}</div>
          <div class="alert-content">
            <div class="alert-title">${a.title}</div>
            <div class="alert-desc">${a.desc}</div>
          </div>
          <div class="alert-time">${a.time}</div>
        </div>
      `).join('') : `<div class="table-empty" style="padding:48px;">${ICONS.check}<p style="color:var(--success)">Sistema sem alertas activos</p></div>`}
    </div>
  `;
}

// ===================== RELATORIOS PAGE (XLSX ONLY) =====================
function renderRelatorios() {
  document.getElementById('page-relatorios').innerHTML = `
    <div class="page-header">
      <div>
        <div class="page-title">${ICONS.report} Relatórios</div>
        <div class="page-title-sub">Exportar dados do sistema em formato <strong>.XLSX</strong> (Excel)</div>
      </div>
    </div>
    <div style="display:grid;grid-template-columns:repeat(auto-fill,minmax(240px,1fr));gap:16px;">
      ${[
        ['Produtos','pill','Lista completa de medicamentos com stocks','produtos'],
        ['Fornecedores','supplier','Lista de fornecedores cadastrados','fornecedores'],
        ['Prateleiras','shelf','Localização e ocupação das prateleiras','prateleiras'],
        ['Lotes','lot','Lotes com validades e estados','lotes'],
        ['Movimentações','movement','Histórico de entradas e saídas','movimentacoes'],
        ['Alertas','alert','Relatório de alertas activos','alertas'],
        ['Stock Actual','package','Snapshot do stock por produto','stock'],
        ['Relatório Geral','report','Todos os dados em múltiplas folhas XLSX','geral'],
      ].map(([title,ico,desc,key])=>`
        <div class="report-card">
          <div class="report-card-icon">${ICONS[ico]||''}</div>
          <div class="report-card-title">${title}</div>
          <div class="report-card-desc">${desc}</div>
          <div style="display:flex;gap:6px;margin-top:10px;align-items:center;font-size:10px;color:var(--text-muted);">
            ${ICONS.download} Formato: <strong style="color:var(--accent);">.XLSX</strong>
          </div>
          <button class="btn btn-primary" style="margin-top:10px;width:100%;" onclick="exportReportXLSX('${key}')">
            ${ICONS.download}<span class="btn-text-content">Exportar XLSX</span>
          </button>
        </div>
      `).join('')}
    </div>
  `;
}

function exportReportXLSX(key) {
  toast('info','A gerar relatório XLSX...','Por favor aguarde');
  setTimeout(() => {
    try {
      let sheets, filename;

      if (key === 'produtos') {
        sheets = [{ name: 'Produtos', data: db.getAll('produtos').map(p => {
          const {entradas,saidas,stock} = db.getStock(p.id);
          const prat = db.getById('prateleiras',p.prateleira_id);
          return {'ID':p.id,'Nome':p.nome,'Forma Farmacêutica':p.forma||'','Grupo Farmacológico':p.grupo_farmacologico||'','Prateleira':prat?prat.nome:'','Stock Mínimo':p.stock_minimo||0,'Preco AOA':p.preco||0,'Entradas':entradas,'Saidas':saidas,'Stock Actual':stock,'Status':p.stock_minimo&&stock<=p.stock_minimo?'Stock Baixo':'Normal'};
        })}];
        filename = `relatorio_produtos_${today()}.xlsx`;
      } else if (key === 'fornecedores') {
        sheets = [{ name: 'Fornecedores', data: db.getAll('fornecedores').map(f=>({'ID':f.id,'Nome':f.nome,'Contacto':f.contacto||'','Email':f.email||'','Telefone':f.telefone||'','Endereco':f.endereco||''})) }];
        filename = `relatorio_fornecedores_${today()}.xlsx`;
      } else if (key === 'prateleiras') {
        sheets = [{ name: 'Prateleiras', data: db.getAll('prateleiras').map(p=>({'ID':p.id,'Nome':p.nome,'Seccao':p.seccao||'','Capacidade':p.capacidade||0,'N Produtos':db.getShelfCount(p.id)})) }];
        filename = `relatorio_prateleiras_${today()}.xlsx`;
      } else if (key === 'lotes') {
        sheets = [{ name: 'Lotes', data: db.getAll('lotes').map(l=>{
          const p=db.getById('produtos',l.produto_id), f=db.getById('fornecedores',l.fornecedor_id), st=getLotStatus(l.validade);
          return {'ID':l.id,'N Lote':l.numero_lote,'Produto':p?p.nome:'','Fornecedor':f?f.nome:'','Quantidade':l.quantidade||0,'Validade':l.validade,'Cod Barras':l.codigo_barra||'','Status':st.label,'Dias Restantes':daysUntil(l.validade)};
        })}];
        filename = `relatorio_lotes_${today()}.xlsx`;
      } else if (key === 'movimentacoes') {
        const _ri = d => { if(!d) return ''; const p = String(d).split('-'); if(p.length===3&&p[2].length===4) return `${p[2]}-${p[1]}-${p[0]}`; return d; };
        const rows = db.getAll('movimentacoes').map(m=>{
          const p=m.produto_nome||(db.getById('produtos',m.produto_id)?.nome)||'', l=db.getById('lotes',m.lote_id);
          return {'ID':m.id,'Produto':p,'Tipo':m.tipo,'Lote':l?l.numero_lote:'','Quantidade':m.quantidade,'Destino Origem':m.destino||'','Preco Unit AOA':m.preco||0,'Data':m.data};
        }).sort((a,b)=>_ri(b.Data).localeCompare(_ri(a.Data)));
        sheets = [{ name: 'Movimentacoes', data: rows }];
        filename = `relatorio_movimentacoes_${today()}.xlsx`;
      } else if (key === 'stock') {
        sheets = [{ name: 'Stock Actual', data: db.getAll('produtos').map(p=>{
          const {entradas,saidas,stock}=db.getStock(p.id), prat=db.getById('prateleiras',p.prateleira_id);
          return {'ID':p.id,'Produto':p.nome,'Grupo':p.grupo_farmacologico||'','Prateleira':prat?prat.nome:'','Stock Minimo':p.stock_minimo||0,'Entradas':entradas,'Saidas':saidas,'Stock Actual':stock,'Status':p.stock_minimo&&stock<=p.stock_minimo?'STOCK BAIXO':'Normal'};
        })}];
        filename = `relatorio_stock_${today()}.xlsx`;
      } else if (key === 'alertas') {
        const rows = getAlerts().map(a=>({'Tipo':a.type==='err'?'Critico':'Aviso','Titulo':a.title,'Descricao':a.desc,'Data Hora':a.time}));
        sheets = [{ name: 'Alertas', data: rows.length ? rows : [{'Mensagem':'Sem alertas activos'}] }];
        filename = `relatorio_alertas_${today()}.xlsx`;
      } else {
        // Relatório Geral — múltiplas abas
        const prodRows = db.getAll('produtos').map(p=>{
          const {entradas,saidas,stock}=db.getStock(p.id), prat=db.getById('prateleiras',p.prateleira_id);
          return {'Nome':p.nome,'Forma':p.forma||'','Grupo':p.grupo_farmacologico||'','Prateleira':prat?prat.nome:'','Stock Min':p.stock_minimo||0,'Entradas':entradas,'Saidas':saidas,'Stock Actual':stock};
        });
        const movRows = db.getAll('movimentacoes').map(m=>{
          const p=m.produto_nome||(db.getById('produtos',m.produto_id)?.nome)||'', l=db.getById('lotes',m.lote_id);
          return {'Produto':p,'Tipo':m.tipo,'Lote':l?l.numero_lote:'','Qtd':m.quantidade,'Destino':m.destino||'','Data':m.data};
        });
        const loteRows = db.getAll('lotes').map(l=>{
          const p=db.getById('produtos',l.produto_id), st=getLotStatus(l.validade);
          return {'N Lote':l.numero_lote,'Produto':p?p.nome:'','Qtd':l.quantidade,'Validade':l.validade,'Status':st.label};
        });
        const alertRows = getAlerts().map(a=>({'Tipo':a.type==='err'?'Critico':'Aviso','Titulo':a.title,'Descricao':a.desc}));
        sheets = [
          { name: 'Produtos',       data: prodRows.length  ? prodRows  : [{'info':'Sem dados'}] },
          { name: 'Movimentacoes',  data: movRows.length   ? movRows   : [{'info':'Sem dados'}] },
          { name: 'Lotes',          data: loteRows.length  ? loteRows  : [{'info':'Sem dados'}] },
          { name: 'Alertas',        data: alertRows.length ? alertRows : [{'Mensagem':'Sem alertas'}] }
        ];
        filename = `relatorio_geral_HMM_${today()}.xlsx`;
      }

      if (XLSXio.download(sheets, filename)) {
        toast('success','Relatório XLSX exportado','Ficheiro Excel gerado com sucesso');
      }
    } catch(e) {
      toast('error','Erro ao exportar',e.message);
    }
  }, 300);
}

// ===================== BASE DE DADOS PAGE (XLSX) =====================
function renderBaseDados() {
  const mode = getDbMode();
  const dirName = localStorage.getItem('hmm_xlsx_dir_name') || '(não seleccionada)';
  const totals = {
    produtos: db.getAll('produtos').length,
    fornecedores: db.getAll('fornecedores').length,
    prateleiras: db.getAll('prateleiras').length,
    lotes: db.getAll('lotes').length,
    movimentacoes: db.getAll('movimentacoes').length,
    kits: db.getAll('kits').length,
  };
  const totalRecs = Object.values(totals).reduce((a,b)=>a+b,0);

  document.getElementById('page-basedados').innerHTML = `
    <div class="page-header">
      <div>
        <div class="page-title">${ICONS.database} Base de Dados</div>
        <div class="page-title-sub">Gestão, armazenamento, exportação e importação da base de dados</div>
      </div>
    </div>

    <!-- STORAGE MODE -->
    <div class="card" style="margin-bottom:18px;">
      <div class="card-header"><div class="card-title">${ICONS.settings} Modo de Armazenamento</div></div>
      <div style="display:grid;grid-template-columns:1fr 1fr;gap:10px;margin-top:4px;">
        <div class="db-mode-option ${mode==='localstorage'?'active':''}" onclick="switchDbMode('localstorage')" style="cursor:pointer;flex-direction:column;gap:8px;">
          <div style="display:flex;align-items:center;gap:10px;">
            <div style="width:34px;height:34px;border-radius:8px;background:rgba(52,152,219,0.15);display:flex;align-items:center;justify-content:center;color:var(--info);flex-shrink:0;">${ICONS.shield}</div>
            <div style="flex:1;">
              <div style="display:flex;align-items:center;gap:6px;flex-wrap:wrap;">
                <div style="font-weight:700;font-size:12px;">LocalStorage</div>
                ${mode==='localstorage'?`<span class="badge badge-success" style="font-size:9px;">Activo</span>`:''}
              </div>
              <div style="font-size:10px;color:var(--text-muted);">Browser interno</div>
            </div>
          </div>
          <div style="font-size:11px;color:var(--text-secondary);padding:6px 8px;background:rgba(52,152,219,0.05);border-radius:6px;">Rápido, offline. Limite ~5MB (~500 movimentações).</div>
        </div>
        <div class="db-mode-option ${mode==='xlsx'?'active':''}" onclick="switchDbMode('xlsx')" style="cursor:pointer;flex-direction:column;gap:8px;">
          <div style="display:flex;align-items:center;gap:10px;">
            <div style="width:34px;height:34px;border-radius:8px;background:rgba(0,184,148,0.15);display:flex;align-items:center;justify-content:center;color:var(--accent);flex-shrink:0;">${ICONS.download}</div>
            <div style="flex:1;">
              <div style="display:flex;align-items:center;gap:6px;flex-wrap:wrap;">
                <div style="font-weight:700;font-size:12px;">Ficheiro .XLSX</div>
                ${mode==='xlsx'?`<span class="badge badge-success" style="font-size:9px;">Activo</span>`:''}
              </div>
              <div style="font-size:10px;color:var(--text-muted);">Excel externo</div>
            </div>
          </div>
          <div style="font-size:11px;color:var(--text-secondary);padding:6px 8px;background:rgba(0,184,148,0.05);border-radius:6px;">Portável, editável no Excel. Limite do sistema de ficheiros.</div>
          ${mode==='xlsx'&&!xlsxDirHandle?`<button class="btn btn-warning" style="margin-top:4px;font-size:11px;padding:5px 8px;width:100%;" onclick="doReconnectXlsx(event)">Reconectar Pasta</button>`:''}
        </div>
        <div class="db-mode-option ${mode==='indexeddb'?'active':''}" onclick="switchDbMode('indexeddb')" style="cursor:pointer;flex-direction:column;gap:8px;border:${mode==='indexeddb'?'2px solid var(--accent)':'1px solid rgba(155,89,182,0.3)'};">
          <div style="display:flex;align-items:center;gap:10px;">
            <div style="width:34px;height:34px;border-radius:8px;background:rgba(155,89,182,0.15);display:flex;align-items:center;justify-content:center;color:#9B59B6;flex-shrink:0;">${ICONS.database}</div>
            <div style="flex:1;">
              <div style="display:flex;align-items:center;gap:6px;flex-wrap:wrap;">
                <div style="font-weight:700;font-size:12px;">SQLite / IndexedDB</div>
                ${mode==='indexeddb'?`<span class="badge badge-success" style="font-size:9px;">Activo</span>`:`<span style="font-size:9px;background:rgba(155,89,182,0.2);color:#9B59B6;padding:2px 6px;border-radius:10px;font-weight:600;">RECOMENDADO</span>`}
              </div>
              <div style="font-size:10px;color:var(--text-muted);">Browser nativo</div>
            </div>
          </div>
          <div style="font-size:11px;color:var(--text-secondary);padding:6px 8px;background:rgba(155,89,182,0.05);border-radius:6px;">⚡ Sem limites — suporta 12.000+ movimentações. Offline, rápido, persistente.</div>
        </div>
        <div class="db-mode-option ${mode==='firebase'?'active':''}" onclick="switchDbMode('firebase')" style="cursor:pointer;flex-direction:column;gap:8px;border:${mode==='firebase'?'2px solid var(--accent)':'1px solid rgba(255,160,0,0.3)'};">
          <div style="display:flex;align-items:center;gap:10px;">
            <div style="width:34px;height:34px;border-radius:8px;background:rgba(255,160,0,0.15);display:flex;align-items:center;justify-content:center;color:#FFA000;flex-shrink:0;">${ICONS.alert}</div>
            <div style="flex:1;">
              <div style="display:flex;align-items:center;gap:6px;flex-wrap:wrap;">
                <div style="font-weight:700;font-size:12px;">Firebase RTDB</div>
                ${mode==='firebase'?`<span class="badge badge-success" style="font-size:9px;">Activo</span>`:`<span style="font-size:9px;background:rgba(255,160,0,0.2);color:#FFA000;padding:2px 6px;border-radius:10px;font-weight:600;">CLOUD</span>`}
              </div>
              <div style="font-size:10px;color:var(--text-muted);">Google Firebase</div>
            </div>
          </div>
          <div style="font-size:11px;color:var(--text-secondary);padding:6px 8px;background:rgba(255,160,0,0.05);border-radius:6px;">☁️ Firebase Realtime Database — Google Cloud.</div>
        </div>
        <div class="db-mode-option ${mode==='cloud'?'active':''}" onclick="switchDbMode('cloud')" style="cursor:pointer;flex-direction:column;gap:8px;border:${mode==='cloud'?'2px solid #3DD598':'1px solid rgba(61,213,152,0.3)'};grid-column:1/-1;">
          <div style="display:flex;align-items:center;gap:10px;">
            <div style="width:34px;height:34px;border-radius:8px;background:rgba(61,213,152,0.15);display:flex;align-items:center;justify-content:center;font-size:16px;flex-shrink:0;">☁️</div>
            <div style="flex:1;">
              <div style="display:flex;align-items:center;gap:6px;flex-wrap:wrap;">
                <div style="font-weight:700;font-size:12px;">Nuvem Universal</div>
                ${mode==='cloud'?`<span class="badge badge-success" style="font-size:9px;">Activo</span>`:`<span style="font-size:9px;background:rgba(61,213,152,0.2);color:#3DD598;padding:2px 6px;border-radius:10px;font-weight:600;">RECOMENDADO</span>`}
                ${mode==='cloud'&&getCloudCfg()?`<span style="font-size:9px;color:var(--text-muted);">${cloudDbTypeName(getCloudCfg().type)}</span>`:''}
              </div>
              <div style="font-size:10px;color:var(--text-muted);">Firebase · Supabase · REST API</div>
            </div>
          </div>
          <div style="font-size:11px;color:var(--text-secondary);padding:6px 8px;background:rgba(61,213,152,0.05);border-radius:6px;">🌐 Login via nuvem · Dados sincronizados entre dispositivos · Utilizadores geridos pelo administrador · Logs em tempo real.</div>
          ${mode==='cloud'&&getCloudCfg()?`<div style="display:flex;gap:8px;margin-top:2px;"><button class="btn btn-secondary" style="font-size:11px;padding:4px 10px;" onclick="testCloudConnectionUI()">Testar Ligação</button><button class="btn btn-secondary" style="font-size:11px;padding:4px 10px;color:#e74c3c;" onclick="disconnectCloud()">Desligar</button></div>`:''}
        </div>
      </div>
      ${(mode==='firebase'||localStorage.getItem('hmm_show_firebase_config'))?`
      <div id="firebase-store-config" style="margin-top:14px;padding:14px;background:rgba(255,160,0,0.06);border:1px solid rgba(255,160,0,0.2);border-radius:10px;">
        <div style="font-weight:700;font-size:12px;color:#FFA000;margin-bottom:10px;">${ICONS.settings} Configuração Firebase Realtime Database</div>
        <div class="field-wrap" style="margin-bottom:8px;">
          <label class="field-label" style="font-size:11px;">URL da Base de Dados <span class="field-req">*</span></label>
          <input class="field-input" id="fb-store-url" placeholder="https://seu-projecto-default-rtdb.firebaseio.com" value="${getFirebaseStoreCfg()?.url||''}" style="font-size:12px;">
        </div>
        <div class="field-wrap" style="margin-bottom:10px;">
          <label class="field-label" style="font-size:11px;">Chave de Autenticação (opcional)</label>
          <input class="field-input" id="fb-store-key" type="password" placeholder="Token de acesso..." value="${getFirebaseStoreCfg()?.key||''}" style="font-size:12px;">
        </div>
        <div style="display:flex;gap:8px;">
          <button class="btn btn-primary" style="flex:1;font-size:12px;" onclick="saveFirebaseStoreCfgUI()">Guardar Configuração</button>
          <button class="btn btn-secondary" style="font-size:12px;" onclick="testFirebaseStoreUI()">Testar Ligação</button>
        </div>
      </div>`:''}
      ${mode==='indexeddb'?`
      <div style="margin-top:10px;padding:10px 14px;background:rgba(155,89,182,0.07);border:1px solid rgba(155,89,182,0.2);border-radius:8px;font-size:11px;color:var(--text-secondary);">
        ${ICONS.info} <strong>IndexedDB activo.</strong> Os dados são guardados localmente sem limite de tamanho. Para migrar os dados para outro dispositivo, use <strong>Exportar → .JSON</strong>.
      </div>`:''}
      ${mode==='cloud'&&getCloudCfg()?`
      <div style="margin-top:10px;padding:12px 14px;background:rgba(61,213,152,0.07);border:1px solid rgba(61,213,152,0.25);border-radius:8px;font-size:11px;color:var(--text-secondary);">
        <div style="display:flex;align-items:center;justify-content:space-between;flex-wrap:wrap;gap:8px;margin-bottom:8px;">
          <span>☁️ <strong>Nuvem activa:</strong> ${cloudDbTypeName(getCloudCfg().type)}</span>
          <span style="display:inline-flex;align-items:center;gap:5px;">
            <span style="width:8px;height:8px;border-radius:50%;background:${_isOnline?'#00b894':'#e74c3c'};display:inline-block;box-shadow:0 0 5px ${_isOnline?'#00b894':'#e74c3c'}88;"></span>
            <span style="color:${_isOnline?'#00b894':'#e74c3c'};font-weight:600;">${_isOnline?'Online':'Offline'}</span>
          </span>
        </div>
        <code style="font-size:10px;opacity:.7;display:block;margin-bottom:8px;word-break:break-all;">${getCloudCfg().url}</code>
        <div style="display:flex;gap:7px;flex-wrap:wrap;">
          <button class="btn btn-secondary" style="font-size:11px;padding:4px 10px;" onclick="syncLocalToCloud()">⬆ Merge → Nuvem</button>
          <button class="btn btn-secondary" style="font-size:11px;padding:4px 10px;" onclick="syncCloudToLocal()">⬇ Merge ← Nuvem</button>
          <button class="btn btn-secondary" style="font-size:11px;padding:4px 10px;" onclick="testCloudConnectionUI()">Testar Ligação</button>
          <button class="btn btn-secondary" style="font-size:11px;padding:4px 10px;color:#e74c3c;" onclick="disconnectCloud()">Desligar</button>
        </div>
      </div>`:''}
      ${mode!=='cloud'&&!getCloudCfg()&&navigator.onLine?`
      <div style="margin-top:10px;padding:10px 14px;background:rgba(61,213,152,0.05);border:1px dashed rgba(61,213,152,0.3);border-radius:8px;font-size:11px;color:var(--text-muted);cursor:pointer;" onclick="switchDbMode('cloud')">
        💡 <strong>Dica:</strong> Internet detectada! Clique em <strong>Nuvem Universal</strong> para configurar acesso em múltiplos dispositivos.
      </div>`:''}
    </div>

    <div class="grid-2" style="gap:18px;">
      <div>
        <!-- ESTATÍSTICAS -->
        <div class="card" style="margin-bottom:18px;">
          <div class="card-header"><div class="card-title">${ICONS.info} Estatísticas</div></div>
          <div class="db-stats-grid">
            ${Object.entries(totals).map(([k,v])=>`<div class="db-stat"><div class="db-stat-val">${v}</div><div class="db-stat-lbl">${k.charAt(0).toUpperCase()+k.slice(1)}</div></div>`).join('')}
            <div class="db-stat"><div class="db-stat-val" style="font-size:11px;">${totalRecs}</div><div class="db-stat-lbl">Total Registos</div></div>
          </div>
        </div>

        <!-- EXPORTAR -->
        <div class="card" style="margin-bottom:18px;">
          <div class="card-header"><div class="card-title">${ICONS.download} Exportar Base de Dados</div></div>
          <div style="font-size:12px;color:var(--text-muted);margin-bottom:10px;">Escolha o formato de exportação:</div>
          <div style="display:flex;flex-direction:column;gap:8px;">
            <button class="db-action-btn" onclick="exportDB('xlsx')" style="flex-direction:row;align-items:center;gap:12px;padding:12px;">
              <div class="db-action-icon" style="background:rgba(0,184,148,0.1);color:var(--accent);flex-shrink:0;">${ICONS.download}</div>
              <div style="flex:1;text-align:left;">
                <div class="db-action-title" style="margin-bottom:2px;">Exportar como .XLSX <span class="chip" style="font-size:9px;margin-left:4px;">Excel</span></div>
                <div class="db-action-desc">Compatível com Microsoft Excel — editável externamente</div>
              </div>
            </button>
            <button class="db-action-btn" onclick="exportDB('json')" style="flex-direction:row;align-items:center;gap:12px;padding:12px;">
              <div class="db-action-icon" style="background:rgba(243,156,18,0.1);color:var(--warning);flex-shrink:0;">${ICONS.settings}</div>
              <div style="flex:1;text-align:left;">
                <div class="db-action-title" style="margin-bottom:2px;">Exportar como .JSON <span class="chip" style="font-size:9px;margin-left:4px;">Portável</span></div>
                <div class="db-action-desc">Formato universal — funciona em qualquer sistema ou dispositivo</div>
              </div>
            </button>
            <button class="db-action-btn" onclick="exportDB('db')" style="flex-direction:row;align-items:center;gap:12px;padding:12px;">
              <div class="db-action-icon" style="background:rgba(155,89,182,0.1);color:#9B59B6;flex-shrink:0;">${ICONS.database}</div>
              <div style="flex:1;text-align:left;">
                <div class="db-action-title" style="margin-bottom:2px;">Exportar como .db <span class="chip" style="font-size:9px;margin-left:4px;">Backup</span></div>
                <div class="db-action-desc">Ficheiro de base de dados — ideal para backup e restauro</div>
              </div>
            </button>
            <button class="db-action-btn" onclick="exportDB('csv')" style="flex-direction:row;align-items:center;gap:12px;padding:12px;">
              <div class="db-action-icon" style="background:rgba(62,207,142,0.1);color:#3ECF8E;flex-shrink:0;">${ICONS.download}</div>
              <div style="flex:1;text-align:left;">
                <div class="db-action-title" style="margin-bottom:2px;">Exportar como .CSV <span class="chip" style="font-size:9px;margin-left:4px;background:rgba(62,207,142,0.2);color:#3ECF8E;">Supabase</span></div>
                <div class="db-action-desc">ZIP com tabelas em CSV — compatível com Supabase e PostgreSQL</div>
              </div>
            </button>
          </div>
        </div>
      </div>

      <div>
        <!-- IMPORTAR -->
        <div class="card" style="margin-bottom:18px;">
          <div class="card-header"><div class="card-title">${ICONS.upload} Importar Base de Dados</div></div>
          <div style="font-size:12px;color:var(--text-muted);margin-bottom:10px;">Seleccione um ficheiro para importar:</div>
          <div style="display:flex;flex-direction:column;gap:8px;">
            <label class="db-action-btn" style="flex-direction:row;align-items:center;gap:12px;padding:12px;cursor:pointer;">
              <div class="db-action-icon" style="background:rgba(0,184,148,0.1);color:var(--accent);flex-shrink:0;">${ICONS.upload}</div>
              <div style="flex:1;text-align:left;">
                <div class="db-action-title" style="margin-bottom:2px;">Importar .XLSX</div>
                <div class="db-action-desc">Ficheiro Excel editado externamente</div>
              </div>
              <input type="file" accept=".xlsx,.xls" style="display:none;" onchange="importDB(event,'xlsx')">
            </label>
            <label class="db-action-btn" style="flex-direction:row;align-items:center;gap:12px;padding:12px;cursor:pointer;">
              <div class="db-action-icon" style="background:rgba(243,156,18,0.1);color:var(--warning);flex-shrink:0;">${ICONS.settings}</div>
              <div style="flex:1;text-align:left;">
                <div class="db-action-title" style="margin-bottom:2px;">Importar .JSON</div>
                <div class="db-action-desc">Ficheiro JSON exportado anteriormente</div>
              </div>
              <input type="file" accept=".json" style="display:none;" onchange="importDB(event,'json')">
            </label>
            <label class="db-action-btn" style="flex-direction:row;align-items:center;gap:12px;padding:12px;cursor:pointer;">
              <div class="db-action-icon" style="background:rgba(155,89,182,0.1);color:#9B59B6;flex-shrink:0;">${ICONS.database}</div>
              <div style="flex:1;text-align:left;">
                <div class="db-action-title" style="margin-bottom:2px;">Importar .db</div>
                <div class="db-action-desc">Ficheiro de base de dados (.db) exportado anteriormente</div>
              </div>
              <input type="file" accept=".db,.bandmed" style="display:none;" onchange="importDB(event,'db')">
            </label>
            <label class="db-action-btn" style="flex-direction:row;align-items:center;gap:12px;padding:12px;cursor:pointer;">
              <div class="db-action-icon" style="background:rgba(62,207,142,0.1);color:#3ECF8E;flex-shrink:0;">${ICONS.upload}</div>
              <div style="flex:1;text-align:left;">
                <div class="db-action-title" style="margin-bottom:2px;">Importar .CSV <span class="chip" style="font-size:9px;margin-left:4px;background:rgba(62,207,142,0.2);color:#3ECF8E;">Supabase</span></div>
                <div class="db-action-desc">Ficheiro CSV individual (uma tabela de cada vez)</div>
              </div>
              <input type="file" accept=".csv" style="display:none;" onchange="importDB(event,'csv')">
            </label>
          </div>
        </div>

        <!-- MANUTENÇÃO -->
        <div class="card">
          <div class="card-header"><div class="card-title">${ICONS.settings} Manutenção</div></div>
          <div style="display:flex;flex-direction:column;gap:8px;">
            <button class="db-action-btn danger" onclick="clearDB()" style="flex-direction:row;align-items:center;gap:12px;padding:12px;">
              <div class="db-action-icon" style="background:rgba(231,76,60,0.1);color:var(--danger);flex-shrink:0;">${ICONS.trash}</div>
              <div style="text-align:left;"><div class="db-action-title" style="margin-bottom:2px;">Limpar Base de Dados</div><div class="db-action-desc">Eliminar todos os dados (mantém utilizadores)</div></div>
            </button>
          </div>
          <div style="margin-top:14px;display:flex;flex-direction:column;gap:6px;">
            ${[['Versão','v3.0.0'],['Armazenamento',mode==='xlsx'?`XLSX (${dirName})`:mode==='indexeddb'?'IndexedDB (SQLite)':mode==='firebase'?'Firebase RTDB':mode==='cloud'?('Nuvem — '+(getCloudCfg()?cloudDbTypeName(getCloudCfg().type):'n/a')):'localStorage'],['Segurança','SHA-256'],['Utilizador',currentUser?.nome||'—'],['Sessão',new Date().toLocaleString('pt-AO')]].map(([l,v])=>`
              <div style="display:flex;justify-content:space-between;padding:6px 0;border-bottom:1px solid var(--border);font-size:12px;">
                <span style="color:var(--text-muted)">${l}</span>
                <span style="font-weight:500;color:var(--text-secondary);text-align:right;max-width:55%;word-break:break-all;">${v}</span>
              </div>`).join('')}
          </div>
        </div>
      </div>
    </div>
  `;
}

async function switchDbMode(mode) {
  if (mode === getDbMode()) return;
  if (mode === 'xlsx') {
    const ok = await confirm('Mudar para Ficheiro .XLSX',
      'Os dados serão guardados num ficheiro Excel na pasta que escolher. Os utilizadores mantêm-se sempre no browser por segurança. Continuar?');
    if (!ok) return;
    const picked = await pickXlsxFolder();
    if (picked) {
      await writeXlsxDb(db.data);
      toast('success','Modo XLSX activo','Base de dados será guardada no ficheiro Excel seleccionado');
      renderBaseDados();
    }
  } else if (mode === 'indexeddb') {
    const ok = await confirm('Mudar para IndexedDB',
      'Os dados serão guardados no IndexedDB do browser — suporta 100.000+ registos sem limite de tamanho. Continuar?');
    if (!ok) return;
    setDbMode('indexeddb');
    xlsxDirHandle = null;
    toast('info','A migrar dados para IndexedDB...');
    try {
      await saveToIDB(db.data);
      toast('success','Modo IndexedDB activo','Suporta grandes volumes de dados (12.000+ movimentações)');
    } catch(e) {
      toast('error','Erro ao migrar para IndexedDB', e.message);
      setDbMode('localstorage');
    }
    renderBaseDados();
  } else if (mode === 'firebase') {
    const cfg = getFirebaseStoreCfg();
    if (!cfg || !cfg.url) {
      // Sem config: mostrar painel de configuração expandido na UI
      localStorage.setItem('hmm_show_firebase_config', '1');
      renderBaseDados();
      setTimeout(() => {
        const el = document.getElementById('firebase-store-config');
        if (el) { el.scrollIntoView({ behavior: 'smooth', block: 'center' }); el.style.boxShadow = '0 0 0 3px #FFA000'; }
      }, 200);
      toast('warning', 'Configure o Firebase', 'Preencha o URL e a chave abaixo e clique "Guardar Config", depois clique Firebase novamente.');
      return;
    }
    const ok = await confirm('Mudar para Firebase',
      `Os dados serão guardados na base de dados Firebase:\n${cfg.url}\nRequer ligação à internet. Continuar?`);
    if (!ok) return;
    setDbMode('firebase');
    xlsxDirHandle = null;
    localStorage.removeItem('hmm_show_firebase_config');
    toast('info', 'A enviar dados para Firebase...');
    try {
      await saveToFirebaseStore(db.data);
      toast('success', 'Modo Firebase activo', 'Dados sincronizados com Firebase Realtime Database');
    } catch(e) {
      toast('error', 'Erro ao conectar ao Firebase', e.message);
      setDbMode('localstorage');
    }
    renderBaseDados();
  } else if (mode === 'cloud') {
    // Se já tem config: reconectar / se não tem: mostrar wizard
    const cfg = getCloudCfg();
    if (cfg) {
      const ok = await confirm('Reconectar Nuvem', `Reconectar a ${cloudDbTypeName(cfg.type)}?
${cfg.url}`);
      if (!ok) return;
      _cloudCfg = cfg;
      setDbMode('cloud');
      toast('info','A reconectar à nuvem...');
      const hasUsers = await CloudDB.checkUsersExist();
      if (hasUsers) {
        const cloudUsers = await CloudDB.loadUsers();
        if (cloudUsers && cloudUsers.length) db.data.usuarios = cloudUsers;
        toast('success', cloudDbTypeName(cfg.type) + ' activo', 'Dados sincronizados da nuvem.');
      }
      renderBaseDados();
    } else {
      // Abrir wizard de configuração
      showCloudSetupWizard();
    }
  } else {
    const ok = await confirm('Mudar para LocalStorage','Os dados voltarão a ser guardados no browser. Continuar?');
    if (!ok) return;
    setDbMode('localstorage');
    xlsxDirHandle = null;
    db.save();
    toast('success','Modo localStorage activo');
    renderBaseDados();
  }
}

async function doReconnectXlsx(e) {
  e.stopPropagation();
  const ok = await reconnectXlsxFolder();
  if (ok) {
    // Read from xlsx and merge
    await db.loadFromXlsx();
    renderBaseDados();
    updateAlertBadge();
  }
}

// ===================== FIREBASE STORE CONFIG UI =====================
async function saveFirebaseStoreCfgUI() {
  const url = (document.getElementById('fb-store-url')?.value || '').trim().replace(/[/]+$/, '');
  const key = (document.getElementById('fb-store-key')?.value || '').trim();
  if (!url) { toast('error', 'URL obrigatório', 'Introduza o URL do Firebase Realtime Database'); return; }
  saveFirebaseStoreCfg({ url, key });
  localStorage.removeItem('hmm_show_firebase_config');
  toast('success', 'Configuração guardada!', 'A activar modo Firebase...');
  // Activar Firebase automaticamente após guardar config
  await switchDbMode('firebase');
}

async function testFirebaseStoreUI() {
  const url = (document.getElementById('fb-store-url')?.value || '').trim().replace(/\/+$/, '');
  const key = (document.getElementById('fb-store-key')?.value || '').trim();
  if (!url) { toast('error', 'URL obrigatório'); return; }
  toast('info', 'A testar ligação Firebase...');
  try {
    const r = await fetch(`${url}/bandmed_ping.json${key ? '?auth=' + key : ''}`);
    if (r.ok) {
      toast('success', 'Firebase acessível!', 'Ligação estabelecida com sucesso.');
    } else {
      toast('error', 'Erro HTTP ' + r.status, 'Verifique o URL e as regras de segurança do Firebase.');
    }
  } catch(e) {
    toast('error', 'Falha na ligação', e.message);
  }
}

// ── Dados para exportar (tabelas + meta) ──
function getExportData() {
  const tables = ['produtos','fornecedores','prateleiras','lotes','movimentacoes','kits'];
  const data = {};
  tables.forEach(t => {
    // includeDeleted=false — registos eliminados não são exportados
    data[t] = db.getAll(t, false).map(r => {
      const obj = {...r};
      if (t === 'kits' && Array.isArray(obj.componentes)) obj.componentes = JSON.stringify(obj.componentes);
      return obj;
    });
  });
  return data;
}

// ── Normalizar registo importado ──
function normalizeImportRow(key, r) {
  const obj = {...r};
  if (obj.id) obj.id = Number(obj.id);
  if (key === 'kits' && typeof obj.componentes === 'string') {
    try { obj.componentes = JSON.parse(obj.componentes); } catch { obj.componentes = []; }
  }
  if (obj.ativo === undefined || obj.ativo === '') obj.ativo = true;
  else if (obj.ativo === 'TRUE'  || obj.ativo === 'true'  || obj.ativo === 1 || obj.ativo === '1') obj.ativo = true;
  else if (obj.ativo === 'FALSE' || obj.ativo === 'false' || obj.ativo === 0 || obj.ativo === '0') obj.ativo = false;
  else if (typeof obj.ativo === 'number') obj.ativo = obj.ativo !== 0;

  if (key === 'movimentacoes') {
    // Normalise tipo to uppercase
    if (obj.tipo) obj.tipo = String(obj.tipo).toUpperCase();

    // Normalise date to DD-MM-YYYY
    if (obj.data) {
      const d = String(obj.data);
      // If YYYY-MM-DD convert to DD-MM-YYYY
      if (/^\d{4}-\d{2}-\d{2}$/.test(d)) {
        const [y,m,day] = d.split('-');
        obj.data = `${day}-${m}-${y}`;
      }
      // If Excel serial number (number)
      else if (/^\d{4,6}$/.test(d)) {
        try {
          const epoch = new Date(1899,11,30);
          epoch.setDate(epoch.getDate() + Number(d));
          const dd = String(epoch.getDate()).padStart(2,'0');
          const mm = String(epoch.getMonth()+1).padStart(2,'0');
          const yyyy = epoch.getFullYear();
          obj.data = `${dd}-${mm}-${yyyy}`;
        } catch(e) {}
      }
    }

    // If produto_nome is stored as a name string in produto_id column, resolve to id
    if (obj.produto_id && isNaN(Number(obj.produto_id))) {
      // produto_id contains a name — try to find the product
      const nomeBusca = String(obj.produto_id).trim().toLowerCase();
      const prod = db.getAll('produtos').find(p => p.nome.trim().toLowerCase() === nomeBusca);
      if (prod) {
        obj.produto_nome = prod.nome;
        obj.produto_id = prod.id;
      } else {
        obj.produto_nome = String(obj.produto_id);
        obj.produto_id = null;
      }
    } else if (obj.produto_id) {
      obj.produto_id = Number(obj.produto_id);
      // Enrich with nome if not present
      if (!obj.produto_nome) {
        const prod = db.getById('produtos', obj.produto_id);
        if (prod) obj.produto_nome = prod.nome;
      }
    }

    // Ensure quantidade is a number
    if (obj.quantidade) obj.quantidade = Number(obj.quantidade) || 0;
    if (obj.preco && obj.preco !== '') obj.preco = parseFloat(obj.preco) || null;
    else obj.preco = null;
  }

  return obj;
}

// ── Download de blob ──
function downloadBlob(content, filename, mime) {
  const blob = new Blob([content], {type: mime});
  const url = URL.createObjectURL(blob);
  const a = document.createElement('a');
  a.href = url; a.download = filename;
  document.body.appendChild(a); a.click();
  document.body.removeChild(a);
  setTimeout(() => URL.revokeObjectURL(url), 3000);
}

// ── EXPORTAR (3 formatos) ──
function exportDB(format) {
  try {
    const data = getExportData();
    const ts = today();
    if (format === 'xlsx') {
      const tables = ['produtos','fornecedores','prateleiras','lotes','movimentacoes','kits'];
      const sheets = tables.map(t => ({ name: t.charAt(0).toUpperCase()+t.slice(1), data: data[t] }));
      sheets.push({ name:'Meta', data:[{'Sistema':'HMM Deposito','Versao':'3.0','Exportado':new Date().toLocaleString('pt-AO'),'Utilizador':currentUser?.nome||'—'}] });
      if (XLSXio.download(sheets, `hmm_database_${ts}.xlsx`)) toast('success','Exportado como .XLSX');
    } else if (format === 'json') {
      const payload = { __bandmed_version: '3.0', exportado: new Date().toISOString(), ...data };
      downloadBlob(JSON.stringify(payload, null, 2), `hmm_database_${ts}.json`, 'application/json');
      toast('success','Exportado como .JSON','Ficheiro JSON gerado com sucesso');
    } else if (format === 'db') {
      // .db = JSON com assinatura BANDMED para identificação na importação
      const payload = { __type: 'BANDMED_DB', __version: '3.0', exportado: new Date().toISOString(), data };
      downloadBlob(JSON.stringify(payload), `hmm_database_${ts}.db`, 'application/octet-stream');
      toast('success','Exportado como .db','Ficheiro de base de dados gerado com sucesso');
    } else if (format === 'csv') {
      // Exportar cada tabela como CSV individual num ZIP (compatível com Supabase/PostgreSQL)
      exportCSVZip(data, ts);
    }
  } catch(e) { toast('error','Erro ao exportar', e.message); }
}

// ── CSV helpers ──
function objectsToCSV(rows) {
  if (!rows || !rows.length) return '';
  // Collect all headers from all rows (some may have extra fields)
  const headers = [];
  rows.forEach(r => Object.keys(r).forEach(k => { if (!headers.includes(k)) headers.push(k); }));
  const escape = v => {
    if (v === null || v === undefined) return '';
    const s = typeof v === 'object' ? JSON.stringify(v) : String(v);
    if (s.includes('"') || s.includes(',') || s.includes('\n')) return '"' + s.replace(/"/g, '""') + '"';
    return s;
  };
  const lines = [headers.join(',')];
  rows.forEach(r => lines.push(headers.map(h => escape(r[h])).join(',')));
  return lines.join('\r\n');
}

function csvToObjects(csvText) {
  const lines = csvText.split(/\r?\n/).filter(l => l.trim());
  if (!lines.length) return [];
  const parseCSVLine = line => {
    const result = []; let cur = ''; let inQ = false;
    for (let i = 0; i < line.length; i++) {
      const c = line[i];
      if (inQ) {
        if (c === '"' && line[i+1] === '"') { cur += '"'; i++; }
        else if (c === '"') inQ = false;
        else cur += c;
      } else {
        if (c === '"') inQ = true;
        else if (c === ',') { result.push(cur); cur = ''; }
        else cur += c;
      }
    }
    result.push(cur);
    return result;
  };
  const headers = parseCSVLine(lines[0]);
  return lines.slice(1).map(line => {
    const vals = parseCSVLine(line);
    const obj = {};
    headers.forEach((h, i) => { obj[h] = vals[i] !== undefined ? vals[i] : ''; });
    return obj;
  });
}

async function exportCSVZip(data, ts) {
  try {
    // Use JSZip if available, otherwise download CSVs one by one
    const tables = ['produtos','fornecedores','prateleiras','lotes','movimentacoes','kits'];
    if (typeof JSZip !== 'undefined') {
      const zip = new JSZip();
      tables.forEach(t => {
        const rows = data[t] || [];
        zip.file(`${t}.csv`, objectsToCSV(rows));
      });
      zip.file('_meta.csv', objectsToCSV([{sistema:'BandMedGest',versao:'3.0',exportado:new Date().toISOString(),utilizador:currentUser?.nome||'—'}]));
      const blob = await zip.generateAsync({type:'blob'});
      const url = URL.createObjectURL(blob);
      const a = document.createElement('a'); a.href = url;
      a.download = `bandmed_csv_${ts}.zip`;
      document.body.appendChild(a); a.click();
      document.body.removeChild(a);
      setTimeout(() => URL.revokeObjectURL(url), 3000);
      toast('success','Exportado como CSV (ZIP)', `${tables.length} tabelas exportadas — compatível com Supabase`);
    } else {
      // Download each table individually
      tables.forEach(t => {
        const rows = data[t] || [];
        if (rows.length) downloadBlob(objectsToCSV(rows), `${t}_${ts}.csv`, 'text/csv;charset=utf-8;');
      });
      toast('success','CSVs exportados', 'Cada tabela foi exportada como ficheiro CSV separado');
    }
  } catch(e) { toast('error','Erro ao exportar CSV', e.message); }
}

// ── Manter compatibilidade com chamadas antigas ──
function exportDBXLSX() { exportDB('xlsx'); }



// ===================== PARSER XLSX ROBUSTO =====================
// Auto-contido, sem Web Worker, sem dependências externas.
// SAX character-by-character: suporta 100k+ linhas sem travar.
// Regra: linha 1 = cabeçalho; lê até à última linha com dados.
async function parseXLSXRobust(arrayBuffer, onProgress) {
  const dec = new TextDecoder();
  const prog = typeof onProgress === 'function' ? onProgress : ()=>{};

  // ── A. DEFLATE puro JS (fallback sem DecompressionStream) ──
  function inflatePureJS(data) {
    const src = data instanceof Uint8Array ? data : new Uint8Array(data);
    let pos=0,bits=0,buf=0;
    function rb(n){while(bits<n){buf|=src[pos++]<<bits;bits+=8;}const v=buf&((1<<n)-1);buf>>>=n;bits-=n;return v;}
    function align(){bits=0;buf=0;}
    function buildTree(lens){
      const mx=Math.max(0,...lens);if(!mx)return{lut:new Int32Array(0),mx:0};
      const cnt=new Int32Array(mx+1);for(const l of lens)if(l)cnt[l]++;
      const nc=new Int32Array(mx+2);for(let i=1;i<=mx;i++)nc[i+1]=(nc[i]+cnt[i])<<1;
      const lut=new Int32Array(1<<mx).fill(-1);
      for(let s=0;s<lens.length;s++){const l=lens[s];if(!l)continue;const c=nc[l]++;let rc=0;for(let i=0;i<l;i++)rc=(rc<<1)|((c>>i)&1);for(let k=rc;k<(1<<mx);k+=(1<<l))lut[k]=(l<<16)|s;}
      return{lut,mx};
    }
    function rSym(t){while(bits<t.mx){buf|=src[pos++]<<bits;bits+=8;}const e=t.lut[buf&((1<<t.mx)-1)];if(e<0)throw new Error('bad sym');buf>>>=(e>>>16);bits-=(e>>>16);return e&0xFFFF;}
    const FLL=new Uint8Array(288);
    for(let i=0;i<144;i++)FLL[i]=8;for(let i=144;i<256;i++)FLL[i]=9;for(let i=256;i<280;i++)FLL[i]=7;for(let i=280;i<288;i++)FLL[i]=8;
    const fL=buildTree([...FLL]),fD=buildTree([...new Uint8Array(32).fill(5)]);
    const LB=[3,4,5,6,7,8,9,10,11,13,15,17,19,23,27,31,35,43,51,59,67,83,99,115,131,163,195,227,258];
    const LE=[0,0,0,0,0,0,0,0,1,1,1,1,2,2,2,2,3,3,3,3,4,4,4,4,5,5,5,5,0];
    const DB=[1,2,3,4,5,7,9,13,17,25,33,49,65,97,129,193,257,385,513,769,1025,1537,2049,3073,4097,6145,8193,12289,16385,24577];
    const DE=[0,0,0,0,1,1,2,2,3,3,4,4,5,5,6,6,7,7,8,8,9,9,10,10,11,11,12,12,13,13];
    let ob=new Uint8Array(Math.max(src.length*6,65536)),op=0;
    function grow(){const b=new Uint8Array(ob.length*2);b.set(ob);ob=b;}
    let bfinal=0;
    do{
      bfinal=rb(1);const bt=rb(2);
      if(bt===0){align();const len=rb(16);rb(16);while(op+len>ob.length)grow();for(let i=0;i<len;i++)ob[op++]=rb(8);}
      else{
        let lT,dT;
        if(bt===1){lT=fL;dT=fD;}
        else{
          const hl=rb(5)+257,hd=rb(5)+1,hc=rb(4)+4;
          const co=[16,17,18,0,8,7,9,6,10,5,11,4,12,3,13,2,14,1,15];
          const cl=new Array(19).fill(0);for(let i=0;i<hc;i++)cl[co[i]]=rb(3);
          const ct=buildTree(cl);const al=[];
          while(al.length<hl+hd){const s=rSym(ct);if(s<16)al.push(s);else if(s===16){const r=al[al.length-1];for(let i=rb(2)+3;i--;)al.push(r);}else if(s===17){for(let i=rb(3)+3;i--;)al.push(0);}else{for(let i=rb(7)+11;i--;)al.push(0);}}
          lT=buildTree(al.slice(0,hl));dT=buildTree(al.slice(hl));
        }
        while(true){
          const sym=rSym(lT);if(sym===256)break;
          if(sym<256){if(op>=ob.length)grow();ob[op++]=sym;}
          else{const li=sym-257,len=LB[li]+rb(LE[li]);const di=rSym(dT),dist=DB[di]+rb(DE[di]);const st=op-dist;while(op+len>ob.length)grow();for(let i=0;i<len;i++)ob[op++]=ob[st+i];}
        }
      }
    }while(!bfinal);
    return ob.subarray(0,op);
  }

  // ── B. Descompressão (DecompressionStream ou fallback JS) ──
  async function decompress(data) {
    const src = data instanceof Uint8Array ? data : new Uint8Array(data);
    if (typeof DecompressionStream !== 'undefined') {
      for (const fmt of ['deflate-raw','deflate']) {
        try {
          const blob = new Blob([src]);
          const stream = blob.stream().pipeThrough(new DecompressionStream(fmt));
          const buf = await new Response(stream).arrayBuffer();
          const out = new Uint8Array(buf);
          if (out.length > 0) return out;
        } catch(e) {}
      }
    }
    return inflatePureJS(src);
  }

  // ── C. Ler ZIP ──
  prog('A descompactar ficheiro ZIP...');
  await new Promise(r=>setTimeout(r,0));
  const b = new Uint8Array(arrayBuffer);
  const dv = new DataView(b.buffer, b.byteOffset, b.byteLength);
  let eocd = -1;
  for (let i = b.length-22; i >= 0; i--) {
    if (b[i]===0x50&&b[i+1]===0x4B&&b[i+2]===0x05&&b[i+3]===0x06) { eocd=i; break; }
  }
  if (eocd < 0) throw new Error('Ficheiro ZIP inválido — não é um .xlsx válido');
  const cnt   = dv.getUint16(eocd+8,  true);
  const cdOff = dv.getUint32(eocd+16, true);
  const files  = new Map();
  let p = cdOff;
  for (let i = 0; i < cnt; i++) {
    if (b[p]!==0x50||b[p+1]!==0x4B||b[p+2]!==0x01||b[p+3]!==0x02) break;
    const nl = dv.getUint16(p+28,true), el=dv.getUint16(p+30,true), cl=dv.getUint16(p+32,true);
    const method = dv.getUint16(p+10,true), lo=dv.getUint32(p+42,true);
    const name = dec.decode(b.slice(p+46, p+46+nl));
    const lnl  = dv.getUint16(lo+26,true), lel=dv.getUint16(lo+28,true);
    const csz  = dv.getUint32(lo+20,true), ds=lo+30+lnl+lel;
    const raw  = b.slice(ds, ds+csz);
    files.set(name, method===8 ? await decompress(raw) : raw);
    p += 46+nl+el+cl;
  }

  function getFile(path) {
    if (files.has(path)) return files.get(path);
    const lc = path.toLowerCase();
    for (const [k,v] of files.entries()) if (k.toLowerCase()===lc) return v;
    const fn = path.split('/').pop().toLowerCase();
    for (const [k,v] of files.entries()) if (k.split('/').pop().toLowerCase()===fn) return v;
    return null;
  }
  function getText(bytes) {
    if (!bytes||!bytes.length) return '';
    try { return dec.decode(bytes); } catch(e) { return ''; }
  }
  function xmlAttr(tag, name) {
    const re = new RegExp('(?:^|\\s)(?:[\\w]+:)?' + name + '\\s*=\\s*"([^"]*)"','i');
    const m = tag.match(re);
    return m ? m[1] : '';
  }
  function unescXml(s) {
    return String(s||'').replace(/&amp;/g,'&').replace(/&lt;/g,'<').replace(/&gt;/g,'>')
      .replace(/&quot;/g,'"').replace(/&apos;/g,"'").replace(/&#(\d+);/g,(_,n)=>String.fromCharCode(+n));
  }
  function colIdx(ref) {
    const s=ref.replace(/[0-9]/g,'').toUpperCase(); let ci=0;
    for(let k=0;k<s.length;k++) ci=ci*26+(s.charCodeAt(k)-64);
    return ci-1;
  }

  // ── D. Shared Strings (tokenizer char-by-char) ──
  prog('A ler strings partilhadas...');
  await new Promise(r=>setTimeout(r,0));
  const ssList = [];
  const ssXml = getText(getFile('xl/sharedStrings.xml'));
  if (ssXml) {
    let inSi=false,inT=false,parts=[],i=0,len=ssXml.length;
    while (i<len) {
      if (ssXml[i]==='<') {
        let e=i+1; while(e<len&&ssXml[e]!=='>') { if(ssXml[e]==='"'||ssXml[e]==="'"){const q=ssXml[e++];while(e<len&&ssXml[e]!==q)e++;} e++; }
        const raw=ssXml.slice(i+1,e).trim(); i=e+1;
        if(raw[0]==='!'||raw[0]==='?') continue;
        const isCl=raw[0]==='/';
        const tn=(isCl?raw.slice(1):raw).split(/[\s/]/)[0].toLowerCase().replace(/^[\w]+:/,'');
        if(!isCl&&tn==='si')  {inSi=true;parts=[];}
        else if(isCl&&tn==='si') {if(inSi)ssList.push(parts.join(''));inSi=false;inT=false;}
        else if(!isCl&&tn==='t'&&inSi) inT=true;
        else if(isCl&&tn==='t') inT=false;
      } else {
        const e2=ssXml.indexOf('<',i);
        if(inSi&&inT) parts.push(e2===-1?ssXml.slice(i):ssXml.slice(i,e2));
        i=e2===-1?len:e2;
      }
    }
  }

  // ── E. Relações workbook → ficheiros de folha ──
  const sheetFiles = new Map();
  const relXml = getText(getFile('xl/_rels/workbook.xml.rels'));
  if (relXml) {
    const rRe=/<[Rr]elationship\s([^/?>]+)/gi; let rm;
    while((rm=rRe.exec(relXml))!==null) {
      const tag=rm[1];
      const id=xmlAttr(tag,'Id'), tgt=xmlAttr(tag,'Target'), type=xmlAttr(tag,'Type');
      if(!id||!tgt) continue;
      if(type.includes('worksheet')||tgt.toLowerCase().includes('sheet')) {
        let path=tgt;
        if(path.startsWith('/')) path=path.slice(1);
        else if(!path.startsWith('xl/')) path='xl/'+path;
        sheetFiles.set(id, path);
      }
    }
  }

  // ── F. Nomes das folhas ──
  const wbXml = getText(getFile('xl/workbook.xml'));
  if (!wbXml) throw new Error('workbook.xml não encontrado — ficheiro XLSX corrompido');
  const sheetDefs = [];
  const stRe=/<[Ss]heet\s([^/?>]+)/gi; let stm;
  while((stm=stRe.exec(wbXml))!==null) {
    const tag=stm[1];
    const name = xmlAttr(tag,'name') || ('Sheet'+(sheetDefs.length+1));
    const rId  = xmlAttr(tag,'r:id') || xmlAttr(tag,'id') || ('rId'+(sheetDefs.length+1));
    sheetDefs.push({name,rId});
  }
  if (!sheetDefs.length) throw new Error('Nenhuma folha encontrada no workbook — ficheiro inválido');

  // ── G. Parser SAX de folha (streaming, sem array de tokens) ──
  // Regra: 1ª linha com dados = cabeçalho; restantes = registos.
  // Cede ao UI a cada 3000 linhas para não travar o browser.
  async function parseFolha(wsXml, nomeFolha, totalFolhas, idxFolha) {
    if (!wsXml) return [];
    const xml=wsXml, len=xml.length;
    let i=0;

    // Helpers de avanço inline
    function skipToGt() { while(i<len&&xml[i]!=='>'){if(xml[i]==='"'||xml[i]==="'"){const q=xml[i++];while(i<len&&xml[i]!==q)i++;}i++;} i++; }
    function readUntilLt() { const s=i; while(i<len&&xml[i]!=='<')i++; return xml.slice(s,i); }

    // Estado
    let inSD=false, inRow=false, inC=false, inV=false, inIs=false, inT=false;
    let cRef='',cType='',cText='',cVal=null;
    let rowCells={};

    let headers=null;
    const rows=[];
    let rowCount=0;
    const YIELD=3000;

    while (i<len) {
      // Texto fora de tags — apenas relevante dentro de <v> ou <is><t>
      if (xml[i]!=='<') {
        if (inV||(inIs&&inT)) { const s=i; while(i<len&&xml[i]!=='<')i++; cText+=xml.slice(s,i); }
        else i++;
        continue;
      }
      // Início de tag
      i++; // saltar '<'
      if (i>=len) break;
      if (xml[i]==='!'||xml[i]==='?') { skipToGt(); continue; } // PI / comentário

      const isCl = xml[i]==='/';
      if (isCl) i++;

      // Nome da tag (sem namespace)
      const ns=i; while(i<len&&xml[i]!==' '&&xml[i]!=='/'&&xml[i]!=='>') i++;
      const tn=xml.slice(ns,i).toLowerCase().replace(/^[\w]+:/,'');

      // Atributos (só necessários em <row> e <c>)
      let aStr='';
      if (!isCl) {
        while(i<len&&xml[i]===' ') i++;
        const as=i;
        while(i<len&&xml[i]!=='/'&&xml[i]!=='>') { if(xml[i]==='"'||xml[i]==="'"){const q=xml[i++];while(i<len&&xml[i]!==q)i++;} i++; }
        aStr=xml.slice(as,i);
      }
      const isSelf=(i<len&&xml[i]==='/');
      while(i<len&&xml[i]!=='>') i++; i++; // fechar '>'

      // ── Máquina de estados ──
      if (!inSD) { if(!isCl&&tn==='sheetdata') inSD=true; continue; }
      if (isCl&&tn==='sheetdata') break;

      if (!isCl&&tn==='row') {
        inRow=true; rowCells={}; cRef=''; cType=''; cVal=null; cText='';
      }
      else if (isCl&&tn==='row') {
        // Processar linha completa
        if (headers===null) {
          // Linha 1: construir cabeçalhos
          const maxC=Object.keys(rowCells).reduce((m,k)=>Math.max(m,+k+1),0);
          headers=[]; for(let c=0;c<maxC;c++) headers.push(String(rowCells[c]??'').trim());
        } else {
          // Linhas de dados
          const obj={}; let hasVal=false;
          for(let c=0;c<headers.length;c++) {
            const h=headers[c], val=rowCells[c]??'';
            if(h!==''){obj[h]=val; if(val!==''&&val!==null&&val!==undefined)hasVal=true;}
          }
          if(hasVal) rows.push(obj);
        }
        rowCount++;
        if(rowCount%YIELD===0) {
          const pct=Math.round((i/len)*100);
          prog('Folha "'+nomeFolha+'" ('+(idxFolha+1)+'/'+totalFolhas+'): '+rowCount+' linhas — '+pct+'%');
          await new Promise(r=>setTimeout(r,0));
        }
        inRow=false;
      }
      else if (inRow&&!isCl&&tn==='c') {
        // Extrair r= e t= dos atributos
        const rM=aStr.match(/\br\s*=\s*"([^"]*)"/i), tM=aStr.match(/\bt\s*=\s*"([^"]*)"/i);
        inC=true; cRef=rM?rM[1]:''; cType=tM?tM[1]:''; cText=''; cVal=null; inV=false; inIs=false;
        if(isSelf&&cRef){const ci=colIdx(cRef);if(ci>=0)rowCells[ci]='';inC=false;cRef='';}
      }
      else if (inRow&&isCl&&tn==='c') {
        if(cRef){const ci=colIdx(cRef);if(ci>=0)rowCells[ci]=cVal!==null?cVal:'';}
        inC=false; inV=false; inIs=false; cRef=''; cType=''; cText=''; cVal=null;
      }
      else if (inC&&!isCl&&tn==='v') { inV=true; cText=''; }
      else if (inC&&isCl&&tn==='v') {
        inV=false;
        if(cType==='s'){const idx=parseInt(cText,10);cVal=isNaN(idx)?'':(ssList[idx]??'');}
        else if(cType==='b'){cVal=cText.trim()==='1';}
        else if(cType==='str'){cVal=unescXml(cText);}
        else{const n=Number(cText.trim());cVal=(cText.trim()!==''&&!isNaN(n))?n:unescXml(cText);}
      }
      else if (inC&&!isCl&&tn==='is') { inIs=true; cText=''; }
      else if (inC&&isCl&&tn==='is')  { inIs=false; cVal=unescXml(cText); }
      else if (inC&&!isCl&&tn==='t')  { inT=true; /* cText already accumulating */ }
      else if (inC&&isCl&&tn==='t')   { inT=false; }
    }
    return rows;
  }

  // ── H. Processar todas as folhas ──
  const result={};
  for(let si=0;si<sheetDefs.length;si++) {
    const {name,rId}=sheetDefs[si];
    const wsPath=sheetFiles.get(rId)||('xl/worksheets/sheet'+(si+1)+'.xml');
    const wsXml=getText(getFile(wsPath)||getFile('xl/worksheets/sheet'+(si+1)+'.xml'));
    prog('A processar folha "'+name+'" ('+(si+1)+'/'+sheetDefs.length+')...');
    await new Promise(r=>setTimeout(r,0));
    result[name]=await parseFolha(wsXml,name,sheetDefs.length,si);
  }
  return result;
}
// ===================== FIM PARSER XLSX ROBUSTO =====================

// ── IMPORTAR (3 formatos) ──
function importDB(event, format) {
  const file = event.target.files[0];
  if (!file) return;
  event.target.value = '';

  const tableMap = {
    'Produtos':'produtos','Fornecedores':'fornecedores','Prateleiras':'prateleiras',
    'Lotes':'lotes','Movimentacoes':'movimentacoes','Movimentações':'movimentacoes','Kits':'kits',
    'produtos':'produtos','fornecedores':'fornecedores','prateleiras':'prateleiras',
    'lotes':'lotes','movimentacoes':'movimentacoes','kits':'kits',
    'movimentações':'movimentacoes','Medicamentos':'produtos','medicamentos':'produtos',
    'Sheet1':'produtos','Sheet2':'movimentacoes','sheet1':'produtos','sheet2':'movimentacoes',
  };

  async function applyImport(dataObj) {
    // dataObj: { produtos:[...], fornecedores:[...], ... }
    const keys = Object.keys(dataObj).filter(k => Array.isArray(dataObj[k]));
    if (!keys.length) { toast('error','Sem dados reconhecidos','Nenhuma tabela válida encontrada no ficheiro.'); return; }
    const names = keys.map(k => k.charAt(0).toUpperCase()+k.slice(1));
    const totalRows = keys.reduce((s,k) => s + (dataObj[k]||[]).length, 0);
    const mode = getDbMode();

    // Aviso se modo LocalStorage e muitos registos
    let extraWarn = '';
    if (mode === 'localstorage' && totalRows > 400) {
      extraWarn = `\n\n⚠️ ATENÇÃO: O ficheiro tem ${totalRows} registos. O LocalStorage suporta ~500. Recomenda-se mudar para IndexedDB na secção "Modo de Armazenamento".`;
    }

    const ok = await confirm('Importar Base de Dados',
      `Encontradas: ${names.join(', ')} (${totalRows} registos total).\nImportar substituirá os dados actuais (exceto utilizadores). Continuar?${extraWarn}`);
    if (!ok) return;

    toast('info', 'A importar dados...', 'Por favor aguarde, pode demorar alguns segundos.');

    let imported = 0;
    // Processar em chunks para não travar o browser com grandes datasets
    for (const key of keys) {
      const data = dataObj[key];
      if (Array.isArray(data) && data.length && !data[0].info) {
        // Normalizar em batches de 500
        const normalized = [];
        const BATCH = 500;
        for (let i = 0; i < data.length; i += BATCH) {
          const batch = data.slice(i, i + BATCH).map(r => normalizeImportRow(key, r));
          normalized.push(...batch);
          // Yield para não bloquear o UI em datasets grandes
          if (i + BATCH < data.length) await new Promise(r => setTimeout(r, 0));
        }
        db.data[key] = normalized;
        imported++;
      }
    }

    // Salvar — se IndexedDB, aguardar a escrita
    if (mode === 'indexeddb') {
      try {
        await saveToIDB(db.data);
        // Guardar só users no localStorage
        try { localStorage.setItem(DB_KEY, JSON.stringify({ usuarios: db.data.usuarios })); } catch(e) {}
        toast('success','Base de dados importada', `${imported} tabela(s) importada(s) — ${totalRows} registos guardados no IndexedDB.`);
      } catch(e) {
        toast('error','Erro ao guardar no IndexedDB', e.message);
        return;
      }
    } else if (mode === 'firebase') {
      try {
        await saveToFirebaseStore(db.data);
        toast('success','Base de dados importada', `${imported} tabela(s) enviadas para Firebase — ${totalRows} registos.`);
      } catch(e) {
        toast('error','Erro ao enviar para Firebase', e.message);
        return;
      }
    } else {
      // LocalStorage — tenta salvar, avisa se falhar
      try {
        localStorage.setItem(DB_KEY, JSON.stringify(db.data));
        toast('success','Base de dados importada', `${imported} tabela(s) importada(s) com sucesso.`);
      } catch(lsErr) {
        // Dados demasiado grandes para localStorage — migrar automaticamente para IDB
        toast('warning','LocalStorage cheio!', 'A migrar automaticamente para IndexedDB...');
        try {
          await saveToIDB(db.data);
          setDbMode('indexeddb');
          try { localStorage.setItem(DB_KEY, JSON.stringify({ usuarios: db.data.usuarios })); } catch(e) {}
          toast('success','Migrado para IndexedDB!', `${totalRows} registos guardados sem limites de tamanho.`);
        } catch(idbErr) {
          toast('error','Erro ao guardar dados', 'LocalStorage cheio e IndexedDB falhou: ' + idbErr.message);
          return;
        }
      }
    }

    renderBaseDados();
    updateAlertBadge();
  }

  if (format === 'json' || format === 'db') {
    // JSON e .db são idênticos internamente — leitura de texto
    const reader = new FileReader();
    reader.onerror = () => toast('error','Erro ao ler ficheiro');
    reader.onload = async (e) => {
      try {
        const parsed = JSON.parse(e.target.result);
        let dataObj = {};
        if (parsed.__type === 'BANDMED_DB' && parsed.data) {
          // Formato .db
          dataObj = parsed.data;
        } else if (parsed.__bandmed_version) {
          // Formato .json
          const skip = new Set(['__bandmed_version','exportado']);
          Object.keys(parsed).forEach(k => { if(!skip.has(k)) dataObj[k] = parsed[k]; });
        } else {
          // JSON genérico — tentar usar directamente
          dataObj = parsed;
        }
        // Normalizar tableMap para chaves minúsculas
        const normalized = {};
        Object.entries(dataObj).forEach(([k, v]) => {
          const mapped = tableMap[k] || tableMap[k.toLowerCase()];
          if (mapped) normalized[mapped] = v;
        });
        if (!Object.keys(normalized).length) { toast('error','Ficheiro não reconhecido','Não foram encontradas tabelas válidas no ficheiro.'); return; }
        await applyImport(normalized);
      } catch(err) { toast('error','Erro ao importar', err.message || 'Ficheiro inválido'); }
    };
    reader.readAsText(file);

  } else if (format === 'csv') {
    // CSV individual — detectar nome da tabela pelo nome do ficheiro
    const reader = new FileReader();
    reader.onerror = () => toast('error','Erro ao ler ficheiro CSV');
    reader.onload = async (e) => {
      try {
        const rows = csvToObjects(e.target.result);
        if (!rows.length) { toast('error','CSV vazio','O ficheiro CSV não contém dados.'); return; }
        // Detect table from filename: e.g. "movimentacoes_2024-10-22.csv" → "movimentacoes"
        const nameLower = file.name.toLowerCase().replace(/\.csv$/,'').split('_')[0];
        const tableName = tableMap[nameLower] || tableMap[file.name.toLowerCase().replace(/\.csv$/,'')];
        if (!tableName) {
          toast('error','Tabela não reconhecida',
            `O ficheiro "${file.name}" não corresponde a nenhuma tabela. Use nomes como: produtos, fornecedores, lotes, movimentacoes, kits.`);
          return;
        }
        const mode = getDbMode();
        let extraWarnCsv = '';
        if (mode === 'localstorage' && rows.length > 400) {
          extraWarnCsv = `\n\n⚠️ ${rows.length} registos podem exceder o limite do LocalStorage. Recomenda-se IndexedDB.`;
        }
        const ok = await confirm('Importar CSV',
          `Importar ${rows.length} registos para "${tableName}" a partir de "${file.name}"?\nOs dados actuais de "${tableName}" serão substituídos.${extraWarnCsv}`);
        if (!ok) return;
        toast('info', 'A importar CSV...', 'Por favor aguarde...');
        // Normalizar em batches
        const normalizedCsv = [];
        const BATCH_CSV = 500;
        for (let i = 0; i < rows.length; i += BATCH_CSV) {
          const batch = rows.slice(i, i + BATCH_CSV).map(r => normalizeImportRow(tableName, r));
          normalizedCsv.push(...batch);
          if (i + BATCH_CSV < rows.length) await new Promise(r => setTimeout(r, 0));
        }
        db.data[tableName] = normalizedCsv;
        if (mode === 'indexeddb') {
          try { await saveToIDB(db.data); } catch(e) { toast('error','Erro IDB', e.message); return; }
        } else if (mode === 'firebase') {
          try { await saveToFirebaseStore(db.data); } catch(e) { toast('error','Erro Firebase', e.message); return; }
        } else {
          try {
            localStorage.setItem(DB_KEY, JSON.stringify(db.data));
          } catch(lsErr) {
            toast('warning','LocalStorage cheio! A migrar para IndexedDB...');
            try {
              await saveToIDB(db.data);
              setDbMode('indexeddb');
              localStorage.setItem(DB_KEY, JSON.stringify({ usuarios: db.data.usuarios }));
            } catch(idbErr) { toast('error','Erro ao guardar', idbErr.message); return; }
          }
        }
        toast('success','CSV importado', `${rows.length} registos importados para "${tableName}"`);
        renderBaseDados(); updateAlertBadge();
      } catch(err) { toast('error','Erro ao importar CSV', err.message || 'Ficheiro inválido'); }
    };
    reader.readAsText(file, 'UTF-8');

  } else {
    // XLSX: parser directo sem Web Worker — funciona em qualquer browser/mobile
    const reader = new FileReader();
    reader.onerror = () => toast('error','Erro ao ler ficheiro');
    reader.onload = async (e) => {
      let sheets = {};
      try {
        sheets = await parseXLSXRobust(e.target.result, (msg) => {
          toast('info', 'A importar XLSX...', msg);
        });
      } catch(err) {
        toast('error','Erro ao importar XLSX', err.message || 'Ficheiro inválido');
        return;
      }
      const sheetNames = Object.keys(sheets);
      if (!sheetNames.length) {
        toast('error','Ficheiro vazio','Nenhuma folha encontrada. Use .xlsx (Excel 2007+).');
        return;
      }
      const normalized = {};
      Object.entries(sheets).forEach(([k, v]) => {
        const trimmed = k.trim();
        const noAccent = trimmed.normalize('NFD').replace(/[\u0300-\u036f]/g,'');
        const mapped = tableMap[trimmed] || tableMap[trimmed.toLowerCase()]
                    || tableMap[noAccent] || tableMap[noAccent.toLowerCase()];
        if (mapped) normalized[mapped] = v;
      });
      if (!Object.keys(normalized).length) {
        const found = sheetNames.join(', ');
        toast('error','Folhas não reconhecidas',
          'Folhas no ficheiro: "' + found + '".\nEsperados: Produtos, Movimentacoes, Fornecedores, Prateleiras, Lotes, Kits.');
        return;
      }
      await applyImport(normalized);
    };
    reader.readAsArrayBuffer(file);
  }
}

// ── Compatibilidade com chamadas antigas ──
function importDBXLSX(event) { importDB(event, 'xlsx'); }

async function clearDB() {
  const ok = await confirm('Limpar Base de Dados','Todos os dados serão eliminados permanentemente (utilizadores mantidos). Esta acção não pode ser revertida!');
  if (ok) {
    db.clear();
    toast('success','Base de dados limpa');
    renderBaseDados();
    updateAlertBadge();
  }
}

// ===================== UTILIZADORES PAGE =====================
let userEditingId = null;

function renderUsuarios() {
  if (currentUser?.funcao !== 'Administrador') {
    document.getElementById('page-usuarios').innerHTML = `
      <div class="table-empty" style="padding:80px 0;">
        ${ICONS.shield}
        <p>Acesso Restrito</p>
        <p style="font-size:12px;color:var(--text-muted)">Apenas administradores podem gerir utilizadores.</p>
      </div>`;
    return;
  }

  const usuarios = db.getAll('usuarios');
  document.getElementById('page-usuarios').innerHTML = `
    <div class="page-header">
      <div>
        <div class="page-title">${ICONS.users} Gestão de Utilizadores</div>
        <div class="page-title-sub">Cadastrar e gerir contas de acesso ao sistema</div>
      </div>
      <div class="page-actions">
        <button class="btn btn-primary" onclick="openUserModal()">${ICONS.plus}<span class="btn-text-content">Novo Utilizador</span></button>
      </div>
    </div>

    <div style="margin-bottom:16px;">
      <div style="background:rgba(0,184,148,0.08);border:1px solid rgba(0,184,148,0.2);border-radius:var(--radius-sm);padding:12px 16px;font-size:12px;color:var(--text-secondary);display:flex;align-items:center;gap:10px;">
        ${ICONS.shield}
        <span>As senhas são armazenadas com encriptação <strong>SHA-256</strong>. Nunca são guardadas em texto simples no código fonte ou base de dados.</span>
      </div>
    </div>

    <div class="table-wrap">
      <div class="table-header">
        <div class="table-title">${ICONS.list} Utilizadores <span class="chip">${usuarios.length}</span></div>
      </div>
      <div style="padding:16px;">
        ${usuarios.length ? usuarios.map(u=>`
          <div class="user-card">
            <div class="user-card-avatar">${initials(u.nome)}</div>
            <div class="user-card-info">
              <div class="user-card-name">${u.nome} ${u.id===currentUser.id?'<span style="font-size:10px;color:var(--accent);">(você)</span>':''}</div>
              <div class="user-card-meta">
                @${u.username}
                <span class="role-badge ${u.funcao==='Administrador'?'role-admin':u.funcao==='Técnico'?'role-tecnico':'role-user'}" style="margin-left:8px;">${u.funcao}</span>
              </div>
            </div>
            <div class="user-card-actions">
              <button class="btn btn-secondary btn-icon" title="Alterar senha" onclick="openChangePasswordModal(${u.id})">${ICONS.lock}</button>
              <button class="btn btn-secondary btn-icon" title="Editar" onclick="openUserModal(${u.id})">${ICONS.edit}</button>
              ${u.id !== currentUser.id ? `<button class="btn btn-danger btn-icon" title="Eliminar" onclick="deleteUser(${u.id})">${ICONS.trash}</button>` : ''}
            </div>
          </div>
        `).join('') : `<div class="table-empty">${ICONS.users}<p>Nenhum utilizador encontrado</p></div>`}
      </div>
    </div>

    <!-- Modal Novo/Editar Utilizador -->
    <div class="modal-overlay" id="modal-user">
      <div class="modal">
        <div class="modal-header">
          <div class="modal-title">${ICONS.user} <span id="modal-user-title">Novo Utilizador</span></div>
          <button class="modal-close" onclick="closeModal('modal-user')">${ICONS.x}</button>
        </div>
        <div class="modal-body">
          <div class="form-grid form-grid-2">
            <div class="field-wrap form-grid-full">
              <label class="field-label">Nome Completo <span class="field-req">*</span></label>
              <input class="field-input" id="user-nome" placeholder="Ex: Maria dos Santos">
            </div>
            <div class="field-wrap">
              <label class="field-label">Nome de Utilizador <span class="field-req">*</span></label>
              <input class="field-input" id="user-username" placeholder="Ex: maria.santos">
            </div>
            <div class="field-wrap">
              <label class="field-label">Função</label>
              <select class="field-select" id="user-funcao">
                <option value="Farmacêutico">Farmacêutico</option>
                <option value="Técnico">Técnico</option>
                <option value="Enfermeiro">Enfermeiro</option>
                ${(() => {
                  const adminExists = db.getAll('usuarios').find(u => u.funcao === 'Administrador');
                  // Show Administrator option only if no admin exists yet, or we are editing the current admin
                  const showAdmin = !adminExists || (adminExists && userEditingId === adminExists.id);
                  return showAdmin ? '<option value="Administrador">Administrador</option>' : '';
                })()}
              </select>
            </div>
            <div class="field-wrap" id="user-pwd-wrap">
              <label class="field-label">Senha <span class="field-req">*</span></label>
              <input class="field-input" id="user-pwd" type="password" placeholder="Mínimo 6 caracteres">
            </div>
          </div>
          <div style="font-size:11px;color:var(--text-muted);margin-top:8px;">
            ${ICONS.shield} A senha será armazenada de forma segura com SHA-256.
          </div>
        </div>
        <div class="modal-footer">
          <button class="btn btn-secondary" onclick="closeModal('modal-user')">Cancelar</button>
          <button class="btn btn-primary" id="btn-save-user" onclick="saveUser()">
            <span class="btn-spin"></span><span class="btn-text-content">${ICONS.check} Guardar</span>
          </button>
        </div>
      </div>
    </div>

    <!-- Modal Alterar Senha -->
    <div class="modal-overlay" id="modal-change-pwd">
      <div class="modal">
        <div class="modal-header">
          <div class="modal-title">${ICONS.lock} Alterar Senha</div>
          <button class="modal-close" onclick="closeModal('modal-change-pwd')">${ICONS.x}</button>
        </div>
        <div class="modal-body">
          <div class="form-grid">
            <div class="field-wrap">
              <label class="field-label">Nova Senha <span class="field-req">*</span></label>
              <input class="field-input" id="new-pwd" type="password" placeholder="Mínimo 6 caracteres">
            </div>
            <div class="field-wrap">
              <label class="field-label">Confirmar Senha <span class="field-req">*</span></label>
              <input class="field-input" id="new-pwd2" type="password" placeholder="Repetir senha">
            </div>
          </div>
        </div>
        <div class="modal-footer">
          <button class="btn btn-secondary" onclick="closeModal('modal-change-pwd')">Cancelar</button>
          <button class="btn btn-primary" id="btn-save-pwd" onclick="saveNewPassword()">
            <span class="btn-spin"></span><span class="btn-text-content">${ICONS.check} Alterar Senha</span>
          </button>
        </div>
      </div>
    </div>
  `;
}

function openUserModal(id=null) {
  userEditingId = id;
  document.getElementById('modal-user-title').textContent = id?'Editar Utilizador':'Novo Utilizador';
  const pwdWrap = document.getElementById('user-pwd-wrap');
  if (id) {
    const u = db.getById('usuarios',id);
    if (u) {
      document.getElementById('user-nome').value=u.nome||'';
      document.getElementById('user-username').value=u.username||'';
      document.getElementById('user-funcao').value=u.funcao||'Farmacêutico';
    }
    if (pwdWrap) pwdWrap.style.display='none';
  } else {
    ['user-nome','user-username','user-pwd'].forEach(i=>{ const el=document.getElementById(i); if(el)el.value=''; });
    document.getElementById('user-funcao').value='Farmacêutico';
    if (pwdWrap) pwdWrap.style.display='';
  }
  document.getElementById('modal-user').classList.add('open');
}

function openChangePasswordModal(id) {
  userEditingId = id;
  document.getElementById('new-pwd').value='';
  document.getElementById('new-pwd2').value='';
  document.getElementById('modal-change-pwd').classList.add('open');
}

async function saveUser() {
  const nome = document.getElementById('user-nome').value.trim();
  const username = document.getElementById('user-username').value.trim();
  const funcao = document.getElementById('user-funcao').value;
  if (!nome||!username) { toast('error','Campos obrigatórios','Preencha nome e utilizador'); return; }

  const btn = document.getElementById('btn-save-user');
  setLoading(btn,true);

  // ── Regra: apenas um Administrador no sistema ───────────────────
  if (funcao === 'Administrador') {
    const adminExists = db.getAll('usuarios').find(u => u.funcao === 'Administrador' && u.id !== userEditingId);
    if (adminExists) {
      toast('error', 'Administrador já existe', `O sistema só pode ter um Administrador. "${adminExists.nome}" já tem essa função.`);
      setLoading(btn, false);
      return;
    }
  }
  // ───────────────────────────────────────────────────────────────

  if (userEditingId) {
    // Check username uniqueness
    const exists = db.data.usuarios.find(u=>u.username===username && u.id!==userEditingId);
    if (exists) { toast('error','Nome de utilizador já existe'); setLoading(btn,false); return; }
    db.update('usuarios', userEditingId, { nome, username, funcao });
    toast('success','Utilizador actualizado');
  } else {
    const exists = db.data.usuarios.find(u=>u.username===username);
    if (exists) { toast('error','Nome de utilizador já existe'); setLoading(btn,false); return; }
    const pwd = document.getElementById('user-pwd').value;
    if (pwd.length < 6) { toast('error','Senha muito curta','Mínimo 6 caracteres'); setLoading(btn,false); return; }
    const hashed = await hashPassword(pwd);
    db.insert('usuarios',{ username, senha:hashed, nome, funcao });
    toast('success','Utilizador criado com sucesso');
  }

  setLoading(btn,false);
  closeModal('modal-user');
  renderUsuarios();
}

async function saveNewPassword() {
  const pwd = document.getElementById('new-pwd').value;
  const pwd2 = document.getElementById('new-pwd2').value;
  if (pwd.length < 6) { toast('error','Senha muito curta','Mínimo 6 caracteres'); return; }
  if (pwd !== pwd2) { toast('error','Senhas não coincidem'); return; }
  const btn = document.getElementById('btn-save-pwd');
  setLoading(btn,true);
  const hashed = await hashPassword(pwd);
  db.update('usuarios', userEditingId, { senha: hashed });
  toast('success','Senha alterada com sucesso');
  setLoading(btn,false);
  closeModal('modal-change-pwd');
}

async function deleteUser(id) {
  const u = db.getById('usuarios',id);
  if (db.data.usuarios.filter(u=>u.ativo!==false).length <= 1) {
    toast('error','Operação inválida','Não pode eliminar o último utilizador activo.');
    return;
  }
  const ok = await confirm('Eliminar Utilizador',`Deseja eliminar "${u?.nome}"? Esta acção não pode ser revertida.`);
  if (ok) { db.remove('usuarios',id); toast('success','Utilizador eliminado'); renderUsuarios(); }
}

// ===================== SINCRONIZAÇÃO PAGE =====================
let syncConfig = JSON.parse(localStorage.getItem('hmm_sync_config')||'{"webAppUrl":"","lastSync":"","status":"disconnected","supabaseUrl":"","supabaseKey":"","firebaseUrl":"","firebaseKey":"","cloudProvider":"none"}');

function saveSyncConfig() {
  localStorage.setItem('hmm_sync_config', JSON.stringify(syncConfig));
}

function renderSincronizacao() {
  const GAS_SCRIPT = `// Google Apps Script — Cole no editor de Apps Script
function doPost(e){
  const data=JSON.parse(e.postData.contents);
  const ss=SpreadsheetApp.getActiveSpreadsheet();
  ['produtos','fornecedores','prateleiras','lotes','movimentacoes','kits'].forEach(t=>{
    let s=ss.getSheetByName(t)||ss.insertSheet(t);
    s.clearContents();
    if(data[t]&&data[t].length>0){
      const h=Object.keys(data[t][0]);
      s.getRange(1,1,1,h.length).setValues([h]);
      s.getRange(2,1,data[t].length,h.length).setValues(data[t].map(r=>h.map(k=>r[k]??'')));
    }
  });
  return ContentService.createTextOutput(JSON.stringify({ok:true})).setMimeType(ContentService.MimeType.JSON);
}
function doGet(e){
  const ss=SpreadsheetApp.getActiveSpreadsheet();
  const result={};
  ['produtos','fornecedores','prateleiras','lotes','movimentacoes','kits'].forEach(t=>{
    const s=ss.getSheetByName(t);
    if(s){const v=s.getDataRange().getValues();if(v.length>1){const h=v[0];result[t]=v.slice(1).map(r=>{const o={};h.forEach((k,i)=>{o[k]=r[i];});return o;});}else result[t]=[];}else result[t]=[];
  });
  return ContentService.createTextOutput(JSON.stringify(result)).setMimeType(ContentService.MimeType.JSON);
}`;

  const prov = syncConfig.cloudProvider || 'none';
  const statusColor = syncConfig.status==='connected'?'ok':syncConfig.status==='syncing'?'pulse':'err';
  const statusLabel = {connected:'Ligado',syncing:'A sincronizar...',disconnected:'Desligado',never:'Nunca sincronizado'}[syncConfig.status]||'Desligado';

  document.getElementById('page-sincronizacao').innerHTML = `
    <div class="page-header">
      <div>
        <div class="page-title">${ICONS.sync} Sincronização Cloud</div>
        <div class="page-title-sub">Sincronizar dados com Google Sheets, Supabase ou Firebase</div>
      </div>
    </div>

    <!-- ESTADO -->
    <div class="sync-card">
      <div class="sync-card-header">
        <div class="sync-icon" style="background:rgba(0,184,148,0.1);color:var(--accent);">${ICONS.cloud}</div>
        <div style="flex:1">
          <div class="sync-card-title">Estado da Sincronização</div>
          <div class="sync-card-desc">Última sincronização: ${syncConfig.lastSync||'Nunca'} — Serviço: ${prov==='supabase'?'Supabase':prov==='firebase'?'Firebase':'Google Sheets'}</div>
        </div>
      </div>
      <div class="sync-status-row">
        <div class="sync-dot ${statusColor}"></div>
        <span>${statusLabel}</span>
      </div>
    </div>

    <!-- SELETOR DE SERVIÇO -->
    <div class="sync-card">
      <div class="sync-card-header">
        <div class="sync-icon" style="background:rgba(52,152,219,0.1);color:var(--info);">${ICONS.settings}</div>
        <div><div class="sync-card-title">Escolher Serviço Cloud</div></div>
      </div>
      <div style="display:grid;grid-template-columns:1fr 1fr;gap:10px;margin-top:4px;">
        ${[
          ['none','Nenhum','Sem sincronização cloud','rgba(90,122,155,0.15)','var(--text-muted)'],
          ['sheets','Google Sheets','Via Google Apps Script','rgba(52,152,219,0.15)','var(--info)'],
          ['supabase','Supabase','Base de dados PostgreSQL','rgba(62,207,142,0.15)','#3ECF8E'],
          ['firebase','Firebase','Google Firebase RTDB','rgba(255,160,0,0.15)','#FFA000'],
        ].map(([id,label,desc,bg,col])=>`
          <div onclick="selectSyncProvider('${id}')" style="cursor:pointer;padding:12px;border-radius:var(--radius-sm);border:2px solid ${prov===id?col:'var(--border)'};background:${prov===id?bg:'var(--bg-card)'};transition:var(--transition);text-align:center;min-height:80px;display:flex;flex-direction:column;align-items:center;justify-content:center;">
            <div style="font-weight:700;font-size:13px;color:${prov===id?col:'var(--text-secondary)'};">${label}</div>
            <div style="font-size:10px;color:var(--text-muted);margin-top:3px;line-height:1.3;">${desc}</div>
            ${prov===id?`<div style="margin-top:6px;"><span class="badge badge-success" style="font-size:9px;">Activo</span></div>`:''}
          </div>
        `).join('')}
      </div>
    </div>

    <!-- CONFIGURAÇÃO GOOGLE SHEETS (só se activo) -->
    ${prov==='sheets'?`
    <div class="sync-card">
      <div class="sync-card-header">
        <div class="sync-icon" style="background:rgba(52,152,219,0.15);color:var(--info);">
          <svg viewBox="0 0 24 24" width="18" height="18" fill="none" stroke="currentColor" stroke-width="2"><rect x="3" y="3" width="18" height="18" rx="2"/><line x1="3" y1="9" x2="21" y2="9"/><line x1="3" y1="15" x2="21" y2="15"/><line x1="9" y1="3" x2="9" y2="21"/></svg>
        </div>
        <div><div class="sync-card-title">Configuração Google Sheets</div></div>
      </div>
      <div class="config-field">
        <label>URL da Web App (Google Apps Script) <span style="color:var(--danger)">*</span></label>
        <input id="sync-webapp-url" placeholder="https://script.google.com/macros/s/.../exec" value="${syncConfig.webAppUrl||''}">
      </div>
      <div class="sync-actions">
        <button class="btn btn-primary" onclick="saveSyncSettings()">${ICONS.check} Guardar</button>
        <button class="btn btn-secondary" onclick="testSyncConnection()" ${syncConfig.webAppUrl?'':'disabled'}>${ICONS.refresh} Testar</button>
      </div>
      <div class="sync-actions" style="margin-top:10px;">
        <button class="btn btn-danger" onclick="syncLocalToSheets()" ${syncConfig.webAppUrl?'':'disabled'} style="flex:1;">${ICONS.upload} Enviar → Sheets</button>
        <button class="btn btn-primary" onclick="syncSheetsToLocal()" ${syncConfig.webAppUrl?'':'disabled'} style="flex:1;">${ICONS.download} Receber ← Sheets</button>
      </div>
      <details style="margin-top:14px;">
        <summary style="cursor:pointer;font-size:12px;color:var(--accent);font-weight:600;">Como configurar o Google Apps Script</summary>
        <ol class="setup-steps-list" style="margin-top:8px;">
          <li>Abra <a href="https://sheets.google.com" target="_blank" style="color:var(--accent)">Google Sheets</a> e crie uma nova folha</li>
          <li>Clique <code>Extensões → Apps Script</code></li>
          <li>Cole o código abaixo, guarde e clique <code>Implementar → Nova implementação</code></li>
          <li>Tipo: <strong>Aplicação Web</strong>, Acesso: <strong>Qualquer pessoa</strong></li>
          <li>Copie o URL e cole acima</li>
        </ol>
        <div class="script-code">${GAS_SCRIPT.replace(/</g,'&lt;').replace(/>/g,'&gt;')}</div>
        <button class="btn btn-secondary" style="margin-top:8px;" onclick="copyGASScript()">${ICONS.check} Copiar Código</button>
      </details>
    </div>`:''}

    <!-- CONFIGURAÇÃO SUPABASE (só se activo) -->
    ${prov==='supabase'?`
    <div class="sync-card">
      <div class="sync-card-header">
        <div class="sync-icon" style="background:rgba(62,207,142,0.15);color:#3ECF8E;">
          <svg viewBox="0 0 24 24" width="18" height="18" fill="none" stroke="currentColor" stroke-width="2"><ellipse cx="12" cy="5" rx="9" ry="3"/><path d="M21 12c0 1.66-4 3-9 3s-9-1.34-9-3"/><path d="M3 5v14c0 1.66 4 3 9 3s9-1.34 9-3V5"/></svg>
        </div>
        <div><div class="sync-card-title">Configuração Supabase</div><div class="sync-card-desc">Conectar com base de dados PostgreSQL na cloud</div></div>
      </div>
      <div style="background:rgba(62,207,142,0.08);border:1px solid rgba(62,207,142,0.25);border-radius:var(--radius-sm);padding:10px 14px;font-size:12px;color:var(--text-secondary);margin-bottom:12px;">
        ${ICONS.info} <strong>Supabase</strong> é uma alternativa open-source ao Firebase, gratuita até 500MB. Crie uma conta em <a href="https://supabase.com" target="_blank" style="color:#3ECF8E;">supabase.com</a>
      </div>
      <div class="form-grid form-grid-2">
        <div class="field-wrap form-grid-full">
          <label class="field-label">URL do Projecto Supabase <span class="field-req">*</span></label>
          <input class="field-input" id="sb-url" placeholder="https://xxxx.supabase.co" value="${syncConfig.supabaseUrl||''}">
        </div>
        <div class="field-wrap form-grid-full">
          <label class="field-label">Chave API (anon/public) <span class="field-req">*</span></label>
          <input class="field-input" id="sb-key" type="password" placeholder="eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9..." value="${syncConfig.supabaseKey||''}">
        </div>
      </div>
      <div class="sync-actions" style="margin-top:10px;">
        <button class="btn btn-primary" onclick="saveSupabaseSettings()">${ICONS.check} Guardar</button>
        <button class="btn btn-secondary" onclick="testSupabase()" ${syncConfig.supabaseUrl&&syncConfig.supabaseKey?'':'disabled'}>${ICONS.refresh} Testar</button>
      </div>
      <div class="sync-actions" style="margin-top:10px;">
        <button class="btn btn-danger" onclick="syncToSupabase()" ${syncConfig.supabaseUrl&&syncConfig.supabaseKey?'':'disabled'} style="flex:1;">${ICONS.upload} Merge → Supabase</button>
        <button class="btn btn-primary" onclick="syncFromSupabase()" ${syncConfig.supabaseUrl&&syncConfig.supabaseKey?'':'disabled'} style="flex:1;">${ICONS.download} Merge ← Supabase</button>
      </div>
      <details style="margin-top:14px;">
        <summary style="cursor:pointer;font-size:12px;color:#3ECF8E;font-weight:600;">Como configurar o Supabase</summary>
        <ol class="setup-steps-list" style="margin-top:8px;">
          <li>Crie conta gratuita em <a href="https://supabase.com" target="_blank" style="color:#3ECF8E">supabase.com</a></li>
          <li>Crie um novo projecto</li>
          <li>Vá a <code>Project Settings → API</code></li>
          <li>Copie o <strong>Project URL</strong> e a chave <strong>anon/public</strong></li>
          <li>No editor SQL do Supabase, execute este comando para criar a tabela de sincronização:</li>
        </ol>
        <div class="script-code">CREATE TABLE IF NOT EXISTS bandmed_sync (
  id TEXT PRIMARY KEY DEFAULT 'main',
  data JSONB NOT NULL,
  updated_at TIMESTAMPTZ DEFAULT NOW()
);
-- Permitir acesso anónimo:
ALTER TABLE bandmed_sync ENABLE ROW LEVEL SECURITY;
CREATE POLICY "allow_all" ON bandmed_sync FOR ALL USING (true) WITH CHECK (true);</div>
      </details>
    </div>`:''}

    <!-- CONFIGURAÇÃO FIREBASE (só se activo) -->
    ${prov==='firebase'?`
    <div class="sync-card">
      <div class="sync-card-header">
        <div class="sync-icon" style="background:rgba(255,160,0,0.15);color:#FFA000;">
          <svg viewBox="0 0 24 24" width="18" height="18" fill="none" stroke="currentColor" stroke-width="2"><path d="M12 2L2 19h20L12 2z"/><path d="M12 6l-5 13h10L12 6z"/></svg>
        </div>
        <div><div class="sync-card-title">Configuração Firebase</div><div class="sync-card-desc">Conectar com Google Firebase Realtime Database</div></div>
      </div>
      <div style="background:rgba(255,160,0,0.08);border:1px solid rgba(255,160,0,0.25);border-radius:var(--radius-sm);padding:10px 14px;font-size:12px;color:var(--text-secondary);margin-bottom:12px;">
        ${ICONS.info} <strong>Firebase RTDB</strong> é gratuito até 1GB de armazenamento. Aceda a <a href="https://console.firebase.google.com" target="_blank" style="color:#FFA000;">console.firebase.google.com</a>
      </div>
      <div class="form-grid form-grid-2">
        <div class="field-wrap form-grid-full">
          <label class="field-label">URL da Base de Dados Firebase <span class="field-req">*</span></label>
          <input class="field-input" id="fb-url" placeholder="https://seu-projeto.firebaseio.com" value="${syncConfig.firebaseUrl||''}">
        </div>
        <div class="field-wrap form-grid-full">
          <label class="field-label">Chave API Web <span class="field-req">*</span></label>
          <input class="field-input" id="fb-key" type="password" placeholder="AIzaSy..." value="${syncConfig.firebaseKey||''}">
        </div>
      </div>
      <div class="sync-actions" style="margin-top:10px;">
        <button class="btn btn-primary" onclick="saveFirebaseSettings()">${ICONS.check} Guardar</button>
        <button class="btn btn-secondary" onclick="testFirebase()" ${syncConfig.firebaseUrl?'':'disabled'}>${ICONS.refresh} Testar</button>
      </div>
      <div class="sync-actions" style="margin-top:10px;">
        <button class="btn btn-danger" onclick="syncToFirebase()" ${syncConfig.firebaseUrl?'':'disabled'} style="flex:1;">${ICONS.upload} Merge → Firebase</button>
        <button class="btn btn-primary" onclick="syncFromFirebase()" ${syncConfig.firebaseUrl?'':'disabled'} style="flex:1;">${ICONS.download} Merge ← Firebase</button>
      </div>
      <details style="margin-top:14px;">
        <summary style="cursor:pointer;font-size:12px;color:#FFA000;font-weight:600;">Como configurar o Firebase</summary>
        <ol class="setup-steps-list" style="margin-top:8px;">
          <li>Aceda a <a href="https://console.firebase.google.com" target="_blank" style="color:#FFA000">console.firebase.google.com</a> e crie um projecto</li>
          <li>Vá a <code>Build → Realtime Database → Create Database</code></li>
          <li>Escolha a região e seleccione <strong>modo de teste</strong> (para desenvolvimento)</li>
          <li>Copie o URL da base de dados (ex: <code>https://xxx.firebaseio.com</code>)</li>
          <li>Vá a <code>Project Settings → General</code> e copie a <strong>Web API Key</strong></li>
          <li>Cole ambos os valores acima e clique Guardar</li>
        </ol>
      </details>
    </div>`:''}

    <!-- EXPORTAÇÃO LOCAL -->
    <div class="sync-card">
      <div class="sync-card-header">
        <div class="sync-icon" style="background:rgba(243,156,18,0.1);color:var(--warning);">${ICONS.download}</div>
        <div><div class="sync-card-title">Backup Local Rápido</div><div class="sync-card-desc">Exportar dados localmente sem precisar de internet</div></div>
      </div>
      <div style="display:flex;gap:8px;flex-wrap:wrap;">
        <button class="btn btn-secondary" onclick="exportDB('xlsx')">${ICONS.download} .XLSX</button>
        <button class="btn btn-secondary" onclick="exportDB('json')">${ICONS.download} .JSON</button>
        <button class="btn btn-secondary" onclick="exportDB('db')">${ICONS.download} .db</button>
      </div>
    </div>
  `;
}

function selectSyncProvider(prov) {
  syncConfig.cloudProvider = prov;
  saveSyncConfig();
  renderSincronizacao();
}

function saveSyncSettings() {
  syncConfig.webAppUrl = document.getElementById('sync-webapp-url').value.trim();
  saveSyncConfig();
  toast('success','Configuração Google Sheets guardada');
  renderSincronizacao();
}

// ── SUPABASE ──
function saveSupabaseSettings() {
  syncConfig.supabaseUrl = document.getElementById('sb-url').value.trim().replace(/\/+$/,'');
  syncConfig.supabaseKey = document.getElementById('sb-key').value.trim();
  saveSyncConfig();
  toast('success','Configuração Supabase guardada');
  renderSincronizacao();
}

async function testSupabase() {
  if (!syncConfig.supabaseUrl || !syncConfig.supabaseKey) { toast('error','Configuração incompleta'); return; }
  toast('info','A testar ligação Supabase...');
  try {
    const r = await fetch(`${syncConfig.supabaseUrl}/rest/v1/bandmed_sync?select=id&limit=1`, {
      headers: { 'apikey': syncConfig.supabaseKey, 'Authorization': `Bearer ${syncConfig.supabaseKey}` }
    });
    if (r.ok || r.status === 406) { // 406 = tabela existe mas query inválida — ligação funciona
      syncConfig.status = 'connected'; saveSyncConfig();
      toast('success','Supabase ligado!','A ligação foi bem-sucedida');
    } else throw new Error(`HTTP ${r.status}`);
  } catch(e) { syncConfig.status='disconnected'; saveSyncConfig(); toast('error','Falha na ligação Supabase', e.message); }
  renderSincronizacao();
}

async function syncToSupabase() {
  if (!syncConfig.supabaseUrl || !syncConfig.supabaseKey) { toast('error','Configure o Supabase primeiro'); return; }
  const ok = await confirm('Enviar para Supabase (Merge Inteligente)',
    'Os dados serão mesclados: novos registos locais serão inseridos, registos mais recentes serão actualizados. Os dados existentes na nuvem com data mais recente NÃO serão sobrescritos. Continuar?');
  if (!ok) return;
  toast('info','A fazer merge inteligente com Supabase...','A ler dados actuais...');
  syncConfig.status='syncing'; saveSyncConfig();
  try {
    // 1. Ler dados actuais do Supabase
    const rGet = await fetch(`${syncConfig.supabaseUrl}/rest/v1/bandmed_sync?id=eq.main&select=data`, {
      headers:{ 'apikey':syncConfig.supabaseKey, 'Authorization':`Bearer ${syncConfig.supabaseKey}` }
    });
    let cloudData = null;
    if (rGet.ok) {
      const rows = await rGet.json();
      if (rows && rows.length && rows[0].data) {
        try { cloudData = JSON.parse(rows[0].data); } catch(e) { cloudData = rows[0].data; }
      }
    }
    // 2. Merge: local + nuvem
    const mergedPayload = buildMergedCloudPayload(cloudData);
    // 3. Estatísticas
    let totalInserted = 0, totalUpdated = 0;
    SYNC_TABLES.forEach(table => {
      const cloudArr = (cloudData && Array.isArray(cloudData[table])) ? cloudData[table] : [];
      const { stats } = mergeRecords(db.data[table] || [], cloudArr);
      totalInserted += stats.inserted; totalUpdated += stats.updated;
    });
    // 4. Guardar merged na nuvem
    const payload = { id:'main', data: JSON.stringify(mergedPayload), updated_at: new Date().toISOString() };
    const r = await fetch(`${syncConfig.supabaseUrl}/rest/v1/bandmed_sync`, {
      method:'POST',
      headers:{ 'apikey':syncConfig.supabaseKey, 'Authorization':`Bearer ${syncConfig.supabaseKey}`, 'Content-Type':'application/json', 'Prefer':'resolution=merge-duplicates' },
      body: JSON.stringify(payload)
    });
    if (!r.ok) throw new Error(`HTTP ${r.status}: ${await r.text()}`);
    // 5. Actualizar dados locais com o estado mesclado
    db.data = { ...JSON.parse(JSON.stringify(DEFAULT_DB)), ...mergedPayload };
    db.save();
    syncConfig.status='connected'; syncConfig.lastSync=new Date().toLocaleString('pt-AO'); saveSyncConfig();
    toast('success',`Merge com Supabase concluído!`, `Inseridos: ${totalInserted} | Actualizados: ${totalUpdated}`);
    updateAlertBadge();
  } catch(e) { syncConfig.status='disconnected'; saveSyncConfig(); toast('error','Erro no merge Supabase', e.message); }
  renderSincronizacao();
}

async function syncFromSupabase() {
  if (!syncConfig.supabaseUrl || !syncConfig.supabaseKey) { toast('error','Configure o Supabase primeiro'); return; }
  const ok = await confirm('Receber do Supabase (Merge Inteligente)',
    'Os dados da nuvem serão mesclados com os dados locais. Registos mais recentes vencem. Nenhum dado local recente será perdido. Continuar?');
  if (!ok) return;
  toast('info','A receber do Supabase (merge inteligente)...');
  syncConfig.status='syncing'; saveSyncConfig();
  try {
    const r = await fetch(`${syncConfig.supabaseUrl}/rest/v1/bandmed_sync?id=eq.main&select=data`, {
      headers:{ 'apikey':syncConfig.supabaseKey, 'Authorization':`Bearer ${syncConfig.supabaseKey}` }
    });
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    const rows = await r.json();
    if (!rows.length) throw new Error('Sem dados no Supabase. Envie os dados primeiro.');
    let cloudData = rows[0].data;
    if (typeof cloudData === 'string') { try { cloudData = JSON.parse(cloudData); } catch(e) {} }
    const stats = mergeCloudIntoLocal(cloudData);
    db.save();
    syncConfig.status='connected'; syncConfig.lastSync=new Date().toLocaleString('pt-AO'); saveSyncConfig();
    toast('success','Merge do Supabase concluído!',
      `Novos: ${stats.inserted} | Actualizados: ${stats.updated} | Mantidos locais: ${stats.skipped}`);
    updateAlertBadge();
  } catch(e) { syncConfig.status='disconnected'; saveSyncConfig(); toast('error','Erro ao receber do Supabase', e.message); }
  renderSincronizacao();
}

// ── FIREBASE ──
function saveFirebaseSettings() {
  syncConfig.firebaseUrl = document.getElementById('fb-url').value.trim().replace(/\/+$/,'');
  syncConfig.firebaseKey = document.getElementById('fb-key').value.trim();
  saveSyncConfig();
  toast('success','Configuração Firebase guardada');
  renderSincronizacao();
}

async function testFirebase() {
  if (!syncConfig.firebaseUrl) { toast('error','URL Firebase não configurado'); return; }
  toast('info','A testar ligação Firebase...');
  try {
    const r = await fetch(`${syncConfig.firebaseUrl}/bandmed_sync/ping.json?auth=${syncConfig.firebaseKey||''}`);
    if (r.ok) { syncConfig.status='connected'; saveSyncConfig(); toast('success','Firebase ligado!'); }
    else throw new Error(`HTTP ${r.status}`);
  } catch(e) { syncConfig.status='disconnected'; saveSyncConfig(); toast('error','Falha Firebase', e.message); }
  renderSincronizacao();
}

async function syncToFirebase() {
  if (!syncConfig.firebaseUrl) { toast('error','Configure o Firebase primeiro'); return; }
  const ok = await confirm('Enviar para Firebase (Merge Inteligente)',
    'Os dados serão mesclados: novos registos locais serão inseridos, registos mais recentes actualizados. Os dados existentes na nuvem com data mais recente NÃO serão sobrescritos. Continuar?');
  if (!ok) return;
  toast('info','A fazer merge inteligente com Firebase...','A ler dados actuais...');
  syncConfig.status='syncing'; saveSyncConfig();
  try {
    // 1. Ler dados actuais do Firebase
    const urlGet = `${syncConfig.firebaseUrl}/bandmed_sync.json${syncConfig.firebaseKey?'?auth='+syncConfig.firebaseKey:''}`;
    const rGet = await fetch(urlGet);
    let cloudData = null;
    if (rGet.ok) {
      const payload = await rGet.json();
      if (payload && payload.data) cloudData = payload.data;
    }
    // 2. Merge: local + nuvem
    const mergedPayload = buildMergedCloudPayload(cloudData);
    // 3. Estatísticas
    let totalInserted = 0, totalUpdated = 0;
    SYNC_TABLES.forEach(table => {
      const cloudArr = (cloudData && Array.isArray(cloudData[table])) ? cloudData[table] : [];
      const { stats } = mergeRecords(db.data[table] || [], cloudArr);
      totalInserted += stats.inserted; totalUpdated += stats.updated;
    });
    // 4. Guardar merged na nuvem
    const urlPut = `${syncConfig.firebaseUrl}/bandmed_sync.json${syncConfig.firebaseKey?'?auth='+syncConfig.firebaseKey:''}`;
    const r = await fetch(urlPut, { method:'PUT', headers:{'Content-Type':'application/json'}, body:JSON.stringify({ data: mergedPayload, updated_at: Date.now() }) });
    if (!r.ok) throw new Error(`HTTP ${r.status}: ${await r.text()}`);
    // 5. Actualizar dados locais com o estado mesclado
    db.data = { ...JSON.parse(JSON.stringify(DEFAULT_DB)), ...mergedPayload };
    db.save();
    syncConfig.status='connected'; syncConfig.lastSync=new Date().toLocaleString('pt-AO'); saveSyncConfig();
    toast('success',`Merge com Firebase concluído!`, `Inseridos: ${totalInserted} | Actualizados: ${totalUpdated}`);
    updateAlertBadge();
  } catch(e) { syncConfig.status='disconnected'; saveSyncConfig(); toast('error','Erro no merge Firebase', e.message); }
  renderSincronizacao();
}

async function syncFromFirebase() {
  if (!syncConfig.firebaseUrl) { toast('error','Configure o Firebase primeiro'); return; }
  const ok = await confirm('Receber do Firebase (Merge Inteligente)',
    'Os dados da nuvem serão mesclados com os dados locais. Registos mais recentes vencem. Nenhum dado local recente será perdido. Continuar?');
  if (!ok) return;
  toast('info','A receber do Firebase (merge inteligente)...');
  syncConfig.status='syncing'; saveSyncConfig();
  try {
    const url = `${syncConfig.firebaseUrl}/bandmed_sync.json${syncConfig.firebaseKey?'?auth='+syncConfig.firebaseKey:''}`;
    const r = await fetch(url);
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    const payload = await r.json();
    if (!payload || !payload.data) throw new Error('Sem dados no Firebase. Envie os dados primeiro.');
    const cloudData = payload.data;
    const stats = mergeCloudIntoLocal(cloudData);
    db.save();
    syncConfig.status='connected'; syncConfig.lastSync=new Date().toLocaleString('pt-AO'); saveSyncConfig();
    toast('success','Merge do Firebase concluído!',
      `Novos: ${stats.inserted} | Actualizados: ${stats.updated} | Mantidos locais: ${stats.skipped}`);
    updateAlertBadge();
  } catch(e) { syncConfig.status='disconnected'; saveSyncConfig(); toast('error','Erro ao receber do Firebase', e.message); }
  renderSincronizacao();
}

async function testSyncConnection() {
  if (!syncConfig.webAppUrl) { toast('error','URL não configurado'); return; }
  toast('info','A testar ligação...','Por favor aguarde');
  try {
    const resp = await fetch(syncConfig.webAppUrl, { method:'GET', mode:'cors' });
    if (resp.ok) {
      syncConfig.status = 'connected';
      saveSyncConfig();
      toast('success','Ligação bem sucedida!','Google Sheets acessível');
    } else {
      throw new Error('HTTP '+resp.status);
    }
  } catch(e) {
    syncConfig.status = 'disconnected';
    saveSyncConfig();
    toast('error','Falha na ligação',e.message+'. Verifique o URL e as permissões.');
  }
  renderSincronizacao();
}

async function syncLocalToSheets() {
  if (!syncConfig.webAppUrl) { toast('error','URL não configurado'); return; }
  const ok = await confirm('Exportar para Google Sheets','Os dados online serão substituídos pelos dados locais actuais. Continuar?');
  if (!ok) return;
  toast('info','A enviar dados para Google Sheets...','Por favor aguarde');
  syncConfig.status = 'syncing';
  saveSyncConfig();
  renderSincronizacao();
  try {
    const payload = {
      produtos: db.getAll('produtos',true),
      fornecedores: db.getAll('fornecedores',true),
      prateleiras: db.getAll('prateleiras',true),
      lotes: db.getAll('lotes',true),
      movimentacoes: db.getAll('movimentacoes',true),
    };
    const resp = await fetch(syncConfig.webAppUrl, {
      method:'POST',
      mode:'cors',
      headers:{'Content-Type':'application/json'},
      body: JSON.stringify(payload),
    });
    if (resp.ok) {
      syncConfig.status = 'connected';
      syncConfig.lastSync = new Date().toLocaleString('pt-AO');
      saveSyncConfig();
      toast('success','Dados enviados com sucesso!','Google Sheets actualizado');
    } else { throw new Error('HTTP '+resp.status); }
  } catch(e) {
    syncConfig.status = 'disconnected';
    saveSyncConfig();
    toast('error','Erro ao enviar dados',e.message);
  }
  renderSincronizacao();
}

async function syncSheetsToLocal() {
  if (!syncConfig.webAppUrl) { toast('error','URL não configurado'); return; }
  const ok = await confirm('Importar do Google Sheets','Os dados locais actuais serão substituídos pelos dados do Google Sheets. Continuar?');
  if (!ok) return;
  toast('info','A receber dados do Google Sheets...','Por favor aguarde');
  syncConfig.status = 'syncing';
  saveSyncConfig();
  renderSincronizacao();
  try {
    const resp = await fetch(syncConfig.webAppUrl+'?t='+Date.now(), { method:'GET', mode:'cors' });
    if (!resp.ok) throw new Error('HTTP '+resp.status);
    const data = await resp.json();
    const tables = ['produtos','fornecedores','prateleiras','lotes','movimentacoes'];
    tables.forEach(t => { if (Array.isArray(data[t])) db.data[t] = data[t]; });
    db.save();
    syncConfig.status = 'connected';
    syncConfig.lastSync = new Date().toLocaleString('pt-AO');
    saveSyncConfig();
    toast('success','Dados recebidos com sucesso!','Base de dados local actualizada');
    updateAlertBadge();
  } catch(e) {
    syncConfig.status = 'disconnected';
    saveSyncConfig();
    toast('error','Erro ao receber dados',e.message);
  }
  renderSincronizacao();
}

function copyGASScript() {
  const script = `// Google Apps Script — Cole este código em Extensions > Apps Script
function doPost(e) {
  const data = JSON.parse(e.postData.contents);
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const tables = ['produtos','fornecedores','prateleiras','lotes','movimentacoes'];
  tables.forEach(t => {
    let sheet = ss.getSheetByName(t);
    if (!sheet) sheet = ss.insertSheet(t);
    sheet.clearContents();
    if (data[t] && data[t].length > 0) {
      const headers = Object.keys(data[t][0]);
      sheet.getRange(1, 1, 1, headers.length).setValues([headers]);
      const rows = data[t].map(r => headers.map(h => r[h] ?? ''));
      sheet.getRange(2, 1, rows.length, headers.length).setValues(rows);
    }
  });
  return ContentService.createTextOutput(JSON.stringify({ok:true})).setMimeType(ContentService.MimeType.JSON);
}
function doGet(e) {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const tables = ['produtos','fornecedores','prateleiras','lotes','movimentacoes'];
  const result = {};
  tables.forEach(t => {
    const sheet = ss.getSheetByName(t);
    if (sheet) {
      const vals = sheet.getDataRange().getValues();
      if (vals.length > 1) {
        const headers = vals[0];
        result[t] = vals.slice(1).map(row => {
          const obj = {};
          headers.forEach((h,i) => { obj[h] = row[i]; });
          return obj;
        });
      } else { result[t] = []; }
    } else { result[t] = []; }
  });
  return ContentService.createTextOutput(JSON.stringify(result)).setMimeType(ContentService.MimeType.JSON);
}`;
  navigator.clipboard.writeText(script).then(()=>toast('success','Código copiado!','Cole no editor do Apps Script')).catch(()=>toast('error','Erro ao copiar'));
}

// ===================== FICHA DE STOCK PAGE =====================
function renderFichaStock() {
  const produtos = db.getAll('produtos');
  document.getElementById('page-fichastock').innerHTML = `
    <div class="page-header">
      <div>
        <div class="page-title">${ICONS.report} Gerar Ficha de Stock</div>
        <div class="page-title-sub">Ficha oficial DNME/PNME — República de Angola, Ministério da Saúde</div>
      </div>
    </div>

    <div style="background:rgba(0,184,148,0.07);border:1px solid rgba(0,184,148,0.2);border-radius:var(--radius-sm);padding:12px 16px;font-size:12px;color:var(--text-secondary);display:flex;align-items:center;gap:10px;margin-bottom:20px;">
      ${ICONS.info}
      <span>A ficha será gerada em formato imprimível, conforme o modelo oficial da <strong>Direcção Nacional de Medicamentos e Equipamentos</strong> do Ministério da Saúde de Angola. Funciona completamente <strong>offline</strong>.</span>
    </div>

    <div class="grid-2" style="gap:20px;">
      <div class="card">
        <div class="card-header"><div class="card-title">${ICONS.settings} Parâmetros da Ficha</div></div>

        <div class="form-grid form-grid-2" style="padding:4px 0 16px;">
          <div class="field-wrap form-grid-full">
            <label class="field-label">${ICONS.map_pin} Unidade de Saúde <span class="field-req">*</span></label>
            <input class="field-input" id="ficha-unidade" placeholder="Ex: Hospital Municipal de Malanje — Depósito" value="Hospital Municipal de Malanje — Depósito">
          </div>
          <div class="field-wrap">
            <label class="field-label">Município <span class="field-req">*</span></label>
            <input class="field-input" id="ficha-municipio" placeholder="Ex: Malanje" value="Malanje">
          </div>
          <div class="field-wrap">
            <label class="field-label">Província <span class="field-req">*</span></label>
            <input class="field-input" id="ficha-provincia" placeholder="Ex: Malanje" value="Malanje">
          </div>
          <div class="field-wrap form-grid-full">
            <label class="field-label">${ICONS.pill} Produto / Medicamento <span class="field-req">*</span></label>
            <select class="field-select" id="ficha-produto" onchange="updateFichaPreview()">
              <option value="">Seleccionar produto...</option>
              ${produtos.map(p=>`<option value="${p.id}">${p.nome}${p.forma?' — '+p.forma:''}</option>`).join('')}
            </select>
          </div>
        </div>

        <div style="display:flex;gap:10px;">
          <button class="btn btn-primary" style="flex:1;" onclick="gerarFichaStock()">
            ${ICONS.report} <span class="btn-text-content">Gerar e Imprimir Ficha</span>
          </button>
        </div>
      </div>

      <div class="card">
        <div class="card-header"><div class="card-title">${ICONS.info} Pré-visualização do Produto</div></div>
        <div id="ficha-preview">
          <div class="table-empty" style="padding:32px;">
            ${ICONS.pill}
            <p style="font-size:13px;">Seleccione um produto para ver o resumo</p>
          </div>
        </div>
      </div>
    </div>
  `;
}

function updateFichaPreview() {
  const prodId = parseInt(document.getElementById('ficha-produto').value);
  const el = document.getElementById('ficha-preview');
  if (!prodId || !el) return;
  const prod = db.getById('produtos', prodId);
  if (!prod) return;
  const lotes = db.getAll('lotes').filter(l=>Number(l.produto_id)===prodId);
  const movs = db.getAll('movimentacoes').filter(m=>Number(m.produto_id)===prodId);
  const {entradas, saidas, stock} = db.getStock(prodId);
  const prat = prod.prateleira_id ? db.getById('prateleiras', prod.prateleira_id) : null;

  el.innerHTML = `
    <div style="display:flex;flex-direction:column;gap:10px;padding:4px 0;">
      <div style="display:flex;justify-content:space-between;padding:8px 0;border-bottom:1px solid var(--border);font-size:13px;">
        <span style="color:var(--text-muted)">Produto</span>
        <span style="font-weight:600;color:var(--text-primary)">${prod.nome}</span>
      </div>
      <div style="display:flex;justify-content:space-between;padding:8px 0;border-bottom:1px solid var(--border);font-size:13px;">
        <span style="color:var(--text-muted)">Dosagem/Forma</span>
        <span style="color:var(--text-secondary)">${prod.forma||'—'}</span>
      </div>
      <div style="display:flex;justify-content:space-between;padding:8px 0;border-bottom:1px solid var(--border);font-size:13px;">
        <span style="color:var(--text-muted)">Grupo Farmacológico</span>
        <span style="color:var(--text-secondary)">${prod.grupo_farmacologico||'—'}</span>
      </div>
      <div style="display:flex;justify-content:space-between;padding:8px 0;border-bottom:1px solid var(--border);font-size:13px;">
        <span style="color:var(--text-muted)">Prateleira</span>
        <span style="color:var(--text-secondary)">${prat?prat.nome:'—'}</span>
      </div>
      <div style="display:flex;gap:10px;margin-top:4px;">
        <div style="flex:1;background:rgba(39,174,96,0.1);border-radius:8px;padding:10px;text-align:center;">
          <div style="font-size:20px;font-weight:700;color:var(--success)">${entradas}</div>
          <div style="font-size:11px;color:var(--text-muted)">Entradas</div>
        </div>
        <div style="flex:1;background:rgba(231,76,60,0.1);border-radius:8px;padding:10px;text-align:center;">
          <div style="font-size:20px;font-weight:700;color:var(--danger)">${saidas}</div>
          <div style="font-size:11px;color:var(--text-muted)">Saídas</div>
        </div>
        <div style="flex:1;background:rgba(0,184,148,0.1);border-radius:8px;padding:10px;text-align:center;">
          <div style="font-size:20px;font-weight:700;color:var(--accent)">${stock}</div>
          <div style="font-size:11px;color:var(--text-muted)">Stock Actual</div>
        </div>
      </div>
      <div style="font-size:12px;color:var(--text-muted);margin-top:4px;">
        ${lotes.length} lote(s) • ${movs.length} movimentação(ões)
      </div>
    </div>
  `;
}

function gerarFichaStock() {
  const prodId = parseInt(document.getElementById('ficha-produto').value);
  const unidade = document.getElementById('ficha-unidade').value.trim();
  const municipio = document.getElementById('ficha-municipio').value.trim();
  const provincia = document.getElementById('ficha-provincia').value.trim();

  if (!prodId) { toast('error', 'Seleccione um produto'); return; }
  if (!unidade) { toast('error', 'Indique a Unidade de Saúde'); return; }

  const prod = db.getById('produtos', prodId);
  if (!prod) { toast('error', 'Produto não encontrado'); return; }

  // Collect all lots for this product (sorted by expiry date)
  const lotes = db.getAll('lotes')
    .filter(l => Number(l.produto_id) === prodId)
    .sort((a,b) => (a.validade||'').localeCompare(b.validade||''));

  // Collect all movements sorted by date ascending
  const movs = db.getAll('movimentacoes')
    .filter(m => Number(m.produto_id) === prodId)
    .sort((a,b) => {
      const toISO = d => {
        if (!d) return '';
        const s = String(d);
        // DD-MM-YYYY → YYYY-MM-DD para ordenação cronológica correcta
        if (/^\d{2}-\d{2}-\d{4}$/.test(s)) { const [day,m,y]=s.split('-'); return `${y}-${m}-${day}`; }
        return s;
      };
      return toISO(a.data).localeCompare(toISO(b.data)) || Number(a.id) - Number(b.id);
    });

  // Calculate running stock for each movement
  let runningStock = 0;
  const movsWithStock = movs.map(m => {
    const q = Number(m.quantidade) || 0;
    if ((m.tipo||'').toUpperCase() === 'ENTRADA') runningStock += q;
    else runningStock -= q;
    return { ...m, stockApos: runningStock };
  });

  // Build rows HTML
  const fmtDate = d => {
    if (!d) return '';
    const s = String(d);
    if (/^\d{2}-\d{2}-\d{4}$/.test(s)) return s;
    if (/^\d{4}-\d{2}-\d{2}$/.test(s)) { const [y,m,day]=s.split('-'); return `${day}-${m}-${y}`; }
    try { return new Date(d).toLocaleDateString('pt-AO', {day:'2-digit',month:'2-digit',year:'numeric'}); }
    catch { return s; }
  };

  // Up to 8 lots shown in header
  const loteCols = lotes.slice(0, 8);
  const numCols = Math.max(loteCols.length, 3);

  const lotNumRow = Array.from({length: numCols}, (_,i) => {
    const l = loteCols[i];
    return `<td style="text-align:center;font-size:9px;">${l ? l.numero_lote : ''}</td>`;
  }).join('');

  const lotValRow = Array.from({length: numCols}, (_,i) => {
    const l = loteCols[i];
    return `<td style="text-align:center;font-size:9px;">${l ? fmtDate(l.validade) : ''}</td>`;
  }).join('');

  const lotValUnitRow = Array.from({length: numCols}, (_,i) => {
    const l = loteCols[i];
    const forn = l?.fornecedor_id ? db.getById('fornecedores', l.fornecedor_id) : null;
    return `<td style="text-align:center;font-size:9px;">${l?.preco || (forn ? '' : '') || ''}</td>`;
  }).join('');

  const lotHeader = Array.from({length: numCols}, (_,i) => {
    const l = loteCols[i];
    return `<th style="text-align:center;font-size:9px;font-weight:600;">${l ? fmtDate(l.validade) : ''}</th>`;
  }).join('');

  // Movement rows
  const movRows = movsWithStock.map((m, idx) => {
    const isEntrada = (m.tipo||'').toUpperCase() === 'ENTRADA';
    const lot = m.lote_id ? db.getById('lotes', m.lote_id) : null;
    const valor = m.preco ? (Number(m.quantidade) * Number(m.preco)).toFixed(2) : '';
    const stockValor = m.preco ? (m.stockApos * Number(m.preco)).toFixed(2) : '';
    return `<tr style="height:24px;">
      <td style="text-align:center;font-size:9px;">${fmtDate(m.data)}</td>
      <td style="font-size:9px;padding-left:3px;">${m.destino||'—'}</td>
      <td style="text-align:center;font-size:9px;">${m.id}</td>
      <td style="text-align:center;font-size:9px;color:${isEntrada?'#155724':''};">${isEntrada ? (m.quantidade||'') : ''}</td>
      <td style="text-align:center;font-size:9px;">${isEntrada && m.preco ? valor : ''}</td>
      <td style="text-align:center;font-size:9px;color:${!isEntrada?'#721c24':''};">${!isEntrada ? (m.quantidade||'') : ''}</td>
      <td style="text-align:center;font-size:9px;">${!isEntrada && m.preco ? valor : ''}</td>
      <td style="text-align:center;font-size:9px;font-weight:600;">${m.stockApos >= 0 ? m.stockApos : 0}</td>
      <td style="text-align:center;font-size:9px;">${m.preco ? stockValor : ''}</td>
      <td style="font-size:8px;text-align:center;line-height:1.2;vertical-align:middle;">${m.usuario_nome||''}</td>
    </tr>`;
  }).join('');

  // Empty rows to fill the page
  const emptyRowCount = Math.max(0, 20 - movsWithStock.length);
  const emptyRows = Array.from({length: emptyRowCount}, () =>
    `<tr style="height:24px;"><td></td><td></td><td></td><td></td><td></td><td></td><td></td><td></td><td></td><td></td></tr>`
  ).join('');

  // Transporte row every 5 rows (after 5, 10, 15, 20)
  const transporteRow = `<tr style="height:20px;background:#f8f8f8;">
    <td colspan="3" style="font-size:8px;padding-left:3px;font-style:italic;">Transporte...</td>
    <td style="text-align:center;font-size:8px;border-top:2px solid #333;font-weight:600;"></td>
    <td style="text-align:center;font-size:8px;border-top:2px solid #333;"></td>
    <td style="text-align:center;font-size:8px;border-top:2px solid #333;font-weight:600;"></td>
    <td style="text-align:center;font-size:8px;border-top:2px solid #333;"></td>
    <td style="text-align:center;font-size:8px;border-top:2px solid #333;font-weight:700;"></td>
    <td style="text-align:center;font-size:8px;border-top:2px solid #333;"></td>
    <td></td>
  </tr>`;

  // Angola coat of arms — embedded base64 image (works 100% offline)
  const angolaEmblema = `<img src="data:image/jpeg;base64,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" alt="República de Angola" style="width:70px;height:80px;object-fit:contain;">`;

  // MINSA logo (text-based profissional, funciona offline)
  const minsaLogo = `<div style="width:65px;height:75px;border:2px solid #000;border-radius:3px;display:flex;flex-direction:column;align-items:center;justify-content:center;text-align:center;padding:4px;gap:2px;">
    <div style="font-size:11px;font-weight:900;letter-spacing:1px;color:#000;line-height:1;">MINSA</div>
    <div style="width:40px;height:1px;background:#000;margin:2px 0;"></div>
    <div style="font-size:6px;color:#000;line-height:1.3;font-weight:600;">Ministério<br/>da Saúde</div>
    <div style="font-size:5.5px;color:#444;line-height:1.3;">República<br/>de Angola</div>
  </div>`;

  const htmlContent = `<!DOCTYPE html>
<html lang="pt">
<head>
<meta charset="UTF-8">
<title>Ficha de Stock — ${prod.nome}</title>
<style>
  * { margin:0; padding:0; box-sizing:border-box; }
  body { font-family:'Times New Roman',Times,serif; font-size:10px; color:#000; background:#fff; }
  .page { width:210mm; min-height:297mm; padding:8mm 10mm; margin:0 auto; }
  table { width:100%; border-collapse:collapse; }
  td, th { border:1px solid #000; padding:2px 3px; vertical-align:middle; }
  .header-table td, .header-table th { border:none; }
  .no-border td, .no-border th { border:none; }
  h2 { font-size:16px; font-weight:bold; letter-spacing:1px; }
  .section-label { font-weight:bold; font-size:10px; }
  @media print {
    body { margin:0; }
    .page { padding:6mm 8mm; margin:0; width:100%; min-height:auto; }
    @page { size:A4 portrait; margin:0; }
  }
</style>
</head>
<body>
<div class="page">

  <!-- CABEÇALHO PRINCIPAL -->
  <table style="border:1px solid #000;margin-bottom:0;">
    <tr>
      <td style="width:65px;border:none;text-align:center;vertical-align:middle;padding:4px;">
        ${angolaEmblema}
      </td>
      <td style="border:none;vertical-align:top;padding:4px 6px;">
        <div style="font-size:9px;font-weight:bold;line-height:1.6;">REPÚBLICA DE ANGOLA</div>
        <div style="font-size:9px;font-weight:bold;line-height:1.6;">MINISTÉRIO DA SAÚDE</div>
        <div style="font-size:8px;line-height:1.5;">DIRECÇÃO NACIONAL DE MEDICAMENTOS E EQUIPAMENTOS</div>
        <div style="font-size:8px;line-height:1.5;">PROGRAMA NACIONAL DE MEDICAMENTOS ESSENCIAIS</div>
      </td>
      <td style="border-left:1px solid #000;border-top:none;border-right:none;border-bottom:none;text-align:center;vertical-align:middle;padding:6px;width:140px;">
        <h2>FICHA DE STOCK</h2>
      </td>
      <td style="width:65px;border:none;text-align:center;vertical-align:middle;padding:4px;">
        ${minsaLogo}
      </td>
    </tr>
  </table>

  <!-- UNIDADE / MUNICÍPIO / PROVÍNCIA -->
  <table style="border:1px solid #000;border-top:none;">
    <tr>
      <td style="width:45%;font-size:9px;"><span style="font-weight:bold;">Unidade de Saúde:</span> ${unidade}</td>
      <td style="width:28%;font-size:9px;"><span style="font-weight:bold;">Município:</span> ${municipio}</td>
      <td style="font-size:9px;"><span style="font-weight:bold;">Província:</span> ${provincia}</td>
    </tr>
  </table>

  <!-- DESIGNAÇÃO E QUANTIDADE -->
  <table style="border:1px solid #000;border-top:none;">
    <tr>
      <td style="width:50%;text-align:center;font-weight:bold;font-size:10px;border-right:1px solid #000;">DESIGNAÇÃO — DOSAGEM — FORMA</td>
      <td style="text-align:center;font-weight:bold;font-size:10px;">QUANTIDADE POR EMBALAGEM</td>
    </tr>
    <tr>
      <td style="font-size:10px;padding:4px 6px;border-right:1px solid #000;font-weight:600;">
        ${prod.nome}${prod.forma ? ' — ' + prod.forma : ''}${prod.grupo_farmacologico ? ' (' + prod.grupo_farmacologico + ')' : ''}
      </td>
      <td style="font-size:10px;padding:4px 6px;text-align:center;"></td>
    </tr>
  </table>

  <!-- LOTES: DATAS EXPIRAÇÃO / Nº LOTE / VALOR UNIT -->
  <table style="border:1px solid #000;border-top:none;">
    <thead>
      <tr>
        <th style="width:90px;font-size:9px;font-weight:bold;border-right:1px solid #000;">Datas de Expiração:</th>
        ${lotHeader}
      </tr>
      <tr>
        <td style="font-size:9px;font-weight:bold;border-right:1px solid #000;">Nº de Lote:</td>
        ${lotNumRow}
      </tr>
      <tr>
        <td style="font-size:9px;font-weight:bold;border-right:1px solid #000;">Valor Unit.:</td>
        ${lotValUnitRow}
      </tr>
    </thead>
  </table>

  <!-- TABELA DE MOVIMENTAÇÕES -->
  <table style="border:1px solid #000;border-top:none;margin-top:0;">
    <thead>
      <tr style="background:#f0f0f0;">
        <th rowspan="2" style="width:60px;font-size:8px;text-align:center;vertical-align:middle;">Data do<br/>Movimento</th>
        <th rowspan="2" style="font-size:8px;text-align:center;vertical-align:middle;">Origem/Destino do Produto</th>
        <th rowspan="2" style="width:45px;font-size:8px;text-align:center;vertical-align:middle;">Nº do<br/>Documento</th>
        <th colspan="2" style="font-size:8px;text-align:center;">Entrada</th>
        <th colspan="2" style="font-size:8px;text-align:center;">Saída</th>
        <th colspan="2" style="font-size:8px;text-align:center;">Stock Existente</th>
        <th rowspan="2" style="width:55px;font-size:8px;text-align:center;vertical-align:middle;">Assinatura</th>
      </tr>
      <tr style="background:#f0f0f0;">
        <th style="width:40px;font-size:8px;text-align:center;">Quant.</th>
        <th style="width:45px;font-size:8px;text-align:center;">Valor</th>
        <th style="width:40px;font-size:8px;text-align:center;">Quant.</th>
        <th style="width:45px;font-size:8px;text-align:center;">Valor</th>
        <th style="width:40px;font-size:8px;text-align:center;">Quant.</th>
        <th style="width:45px;font-size:8px;text-align:center;">Valor</th>
      </tr>
      ${transporteRow}
    </thead>
    <tbody>
      ${movRows}
      ${emptyRows}
    </tbody>
  </table>

  <!-- RODAPÉ -->
  <div style="margin-top:8px;font-size:8px;color:#555;text-align:right;">
    Ficha gerada em ${new Date().toLocaleString('pt-AO')} &nbsp;|&nbsp; BANDMED v3 — Hospital Municipal de Malanje
  </div>

</div>
<script>
  window.onload = function() {
    setTimeout(function() { window.print(); }, 400);
  };
<\/script>
</body>
</html>`;

  // Open in new window (works offline)
  const win = window.open('', '_blank', 'width=900,height=700,scrollbars=yes');
  if (!win) {
    toast('error', 'Janela bloqueada', 'Permita pop-ups para este site para gerar a ficha.');
    return;
  }
  win.document.open();
  win.document.write(htmlContent);
  win.document.close();
  toast('success', 'Ficha gerada!', 'A ficha de stock foi aberta numa nova janela para impressão.');
}

// ===================== MODAL HELPERS =====================
function closeModal(id) {
  const el = document.getElementById(id);
  if (el) el.classList.remove('open');
  editingId = null;
  userEditingId = null;
  // Se é o modal de movimentações, fechar e remover a combobox do body
  if (id === 'modal-mov') {
    closeMovCombo();
  }
}

document.addEventListener('click', (e) => {
  if (e.target.classList.contains('modal-overlay')) {
    e.target.classList.remove('open');
    editingId = null;
    userEditingId = null;
    // Fechar combobox se estiver aberta
    closeMovCombo();
  }
});

// ===================== CONFIRM DIALOG =====================
document.getElementById('confirm-yes').addEventListener('click', () => {
  document.getElementById('confirm-overlay').classList.remove('open');
  if (confirmResolve) { confirmResolve(true); confirmResolve = null; }
});
document.getElementById('confirm-no').addEventListener('click', () => {
  document.getElementById('confirm-overlay').classList.remove('open');
  if (confirmResolve) { confirmResolve(false); confirmResolve = null; }
});

// ===================== HEADER BUTTONS =====================
document.getElementById('header-notif-btn').addEventListener('click', () => navigateTo('alertas'));

// ===================== BOOTSTRAP =====================
// Aviso quando o browser fecha com gravação IDB em curso
window.addEventListener('beforeunload', (e) => {
  if (_idbPending > 0) {
    e.preventDefault();
    e.returnValue = 'Uma gravação na base de dados está em curso. Fechar agora pode causar perda de dados.';
  }
});

document.addEventListener('DOMContentLoaded', () => {
  // ── IndexedDB: pré-carregar dados antes de mostrar o ecrã de login ──────────
  // Quando o modo é IndexedDB, os dados completos residem no IDB, não no localStorage.
  // É necessário carregar do IDB ainda antes de mostrar o login para que os utilizadores
  // (e outros dados em memória) estejam disponíveis imediatamente.
  const _preloadIDB = getDbMode() === 'indexeddb'
    ? loadFromIDB().then(idbData => {
        if (idbData) {
          // Mesclar com a cópia em memória: preferir utilizadores do localStorage (mais seguros)
          const lsUsers = (() => {
            try { const r = localStorage.getItem(DB_KEY); return r ? (JSON.parse(r).usuarios || []) : []; } catch(e) { return []; }
          })();
          db.data = { ...JSON.parse(JSON.stringify(DEFAULT_DB)), ...idbData };
          if (lsUsers.length) db.data.usuarios = lsUsers;
          console.log('[IDB] Pré-carregamento na inicialização concluído — ' + db.data.usuarios.length + ' utilizador(es).');
        } else {
          console.info('[IDB] IndexedDB vazio no arranque — a aguardar primeiro save ou migração.');
        }
      }).catch(e => console.warn('[IDB] Erro no pré-carregamento:', e))
    : Promise.resolve();

  setupLogin();

  const _cloudCfgSaved = getCloudCfg();
  const _mode = getDbMode();

  if (_cloudCfgSaved && _mode === 'cloud') {
    // Configuração de nuvem existente → reconectar automaticamente
    showScreen('splash');
    startSplash(async () => {
      await _preloadIDB; // Aguardar IDB (no-op para modo cloud)
      await cloudAutoReconnect(() => {});
      showScreen('login');
    });
  } else {
    // Sem configuração de nuvem → fluxo normal
    showScreen('splash');
    startSplash(async () => {
      // Garantir que o IDB foi pré-carregado antes de decidir o ecrã a mostrar
      await _preloadIDB;

      // Verificar conectividade real (navigator.onLine pode ser falso positivo)
      let _isReallyOnline = navigator.onLine;
      if (_isReallyOnline) {
        try {
          await fetch('https://www.google.com/generate_204', { method: 'HEAD', mode: 'no-cors', cache: 'no-store', signal: AbortSignal.timeout(3000) });
          _isReallyOnline = true;
        } catch { _isReallyOnline = false; }
      }

      // Online sem configuração de nuvem: mostrar wizard SEMPRE (mesmo com utilizadores locais)
      // Excepto se o utilizador já ignorou o wizard nesta sessão
      const _wizardSkipped = sessionStorage.getItem('hmm_cloud_wizard_skipped');
      if (_isReallyOnline && !getCloudCfg() && !_wizardSkipped) {
        if (db.data.usuarios.length === 0) {
          showScreen('setup'); // por baixo do wizard
          setupFirstRun();
        } else {
          showScreen('login');
        }
        showCloudSetupWizard();
      } else if (db.data.usuarios.length === 0) {
        showScreen('setup');
        setupFirstRun();
      } else {
        showScreen('login');
      }
      // Atualizar badge de rede após splash
      updateNetworkStatusUI();
    });
  }
});

// ===================== KITS PAGE =====================
let kitSearch = '';
let kitEditingId = null;
let kitComponents = []; // [{produto_id, quantidade}]

function renderKits() {
  const kits = db.getAll('kits');
  const filtered = kitSearch ? kits.filter(k=>k.nome.toLowerCase().includes(kitSearch.toLowerCase())) : kits;
  const produtos = db.getAll('produtos');

  document.getElementById('page-kits').innerHTML = `
    <div class="page-header">
      <div>
        <div class="page-title">${ICONS.layers} Kits de Produtos</div>
        <div class="page-title-sub">Cadastrar combinações de medicamentos vendidos/dispensados como um único kit</div>
      </div>
      <div class="page-actions">
        <button class="btn btn-primary" onclick="openKitModal()">${ICONS.plus}<span class="btn-text-content">Novo Kit</span></button>
      </div>
    </div>

    <!-- INFO BOX -->
    <div style="background:rgba(0,184,148,0.07);border:1px solid rgba(0,184,148,0.2);border-radius:var(--radius-sm);padding:12px 16px;font-size:12px;color:var(--text-secondary);display:flex;align-items:center;gap:10px;margin-bottom:16px;">
      ${ICONS.info}
      <span>Um <strong>Kit</strong> agrupa dois ou mais medicamentos num único produto combinado. Ao registar uma saída de um Kit, o sistema debita automaticamente cada componente individualmente nas movimentações.</span>
    </div>

    <div class="table-wrap">
      <div class="table-header">
        <div class="table-title">${ICONS.list} Kits Cadastrados <span class="chip">${filtered.length}</span></div>
        <div class="table-actions">
          <div class="search-wrap">
            <input class="search-input" id="search-kits" placeholder="Pesquisar kit..." value="${kitSearch}" oninput="kitSearch=this.value;filterKitsTable()">
          </div>
        </div>
      </div>
      <div class="tbl-scroll">
      <table>
        <thead><tr><th>Nome do Kit</th><th>Componentes</th><th>Descrição</th><th>Saídas Registadas</th><th>Acções</th></tr></thead>
        <tbody id="tbody-kits">
          ${filtered.length ? filtered.map(k=>{
            const comps = Array.isArray(k.componentes) ? k.componentes : [];
            const saidas = db.getAll('movimentacoes').filter(m=>m.kit_id===k.id&&(m.tipo||'').toUpperCase()==='SAÍDA').length;
            return `<tr>
              <td class="td-name">
                <div style="display:flex;align-items:center;gap:8px;">
                  <div style="width:30px;height:30px;border-radius:8px;background:rgba(155,89,182,0.2);display:flex;align-items:center;justify-content:center;color:#9B59B6;flex-shrink:0;">${ICONS.layers}</div>
                  ${k.nome}
                </div>
              </td>
              <td>
                ${comps.map(c=>{
                  const p=db.getById('produtos',c.produto_id);
                  return `<span class="chip" style="margin:2px;">${p?p.nome:'?'} (${c.quantidade})</span>`;
                }).join('')}
              </td>
              <td style="color:var(--text-muted);font-size:12px;">${k.descricao||'—'}</td>
              <td class="font-bold text-danger">${saidas}</td>
              <td>
                <div style="display:flex;gap:5px;">
                  <button class="btn btn-primary btn-icon" title="Registar Saída de Kit" onclick="openKitSaidaModal(${k.id})">${ICONS.arrow_down}</button>
                  <button class="btn btn-secondary btn-icon" onclick="openKitModal(${k.id})">${ICONS.edit}</button>
                  <button class="btn btn-danger btn-icon" onclick="deleteKit(${k.id})">${ICONS.trash}</button>
                </div>
              </td>
            </tr>`;
          }).join('') : `<tr><td colspan="5"><div class="table-empty">${ICONS.layers}<p>Nenhum kit cadastrado</p></div></td></tr>`}
        </tbody>
      </table>
      </div>
    </div>

    <!-- MODAL CRIAR/EDITAR KIT -->
    <div class="modal-overlay" id="modal-kit">
      <div class="modal modal-lg">
        <div class="modal-header">
          <div class="modal-title">${ICONS.layers} <span id="modal-kit-title">Novo Kit</span></div>
          <button class="modal-close" onclick="closeModal('modal-kit')">${ICONS.x}</button>
        </div>
        <div class="modal-body">
          <div class="form-grid form-grid-2" style="margin-bottom:16px;">
            <div class="field-wrap form-grid-full">
              <label class="field-label">Nome do Kit <span class="field-req">*</span></label>
              <input class="field-input" id="kit-nome" placeholder="Ex: Kit Amoxicilina + Ampicilina">
            </div>
            <div class="field-wrap form-grid-full">
              <label class="field-label">Descrição</label>
              <input class="field-input" id="kit-descricao" placeholder="Ex: Tratamento combinado de infecções bacterianas">
            </div>
          </div>

          <div style="margin-bottom:12px;">
            <div style="font-size:13px;font-weight:600;color:var(--text-secondary);margin-bottom:10px;display:flex;align-items:center;gap:6px;">${ICONS.pill} Componentes do Kit <span class="field-req">*</span></div>
            <div id="kit-components-list" style="display:flex;flex-direction:column;gap:8px;margin-bottom:10px;"></div>
            <div style="display:flex;gap:8px;align-items:flex-end;">
              <div class="field-wrap" style="flex:1;margin:0;">
                <label class="field-label" style="font-size:11px;">Produto</label>
                <select class="field-select" id="kit-add-prod">
                  <option value="">Seleccionar produto...</option>
                  ${produtos.map(p=>`<option value="${p.id}">${p.nome}</option>`).join('')}
                </select>
              </div>
              <div class="field-wrap" style="width:100px;margin:0;">
                <label class="field-label" style="font-size:11px;">Qtd</label>
                <input class="field-input" id="kit-add-qtd" type="number" min="1" value="1" placeholder="1">
              </div>
              <button class="btn btn-secondary" style="height:38px;white-space:nowrap;" onclick="addKitComponent()">${ICONS.plus} Adicionar</button>
            </div>
          </div>
        </div>
        <div class="modal-footer">
          <button class="btn btn-secondary" onclick="closeModal('modal-kit')">Cancelar</button>
          <button class="btn btn-primary" id="btn-save-kit" onclick="saveKit()">
            <span class="btn-spin"></span><span class="btn-text-content">${ICONS.check} Guardar Kit</span>
          </button>
        </div>
      </div>
    </div>

    <!-- MODAL SAÍDA DE KIT -->
    <div class="modal-overlay" id="modal-kit-saida">
      <div class="modal">
        <div class="modal-header">
          <div class="modal-title">${ICONS.arrow_down} Saída de Kit</div>
          <button class="modal-close" onclick="closeModal('modal-kit-saida')">${ICONS.x}</button>
        </div>
        <div class="modal-body">
          <div id="modal-kit-saida-info" style="background:rgba(155,89,182,0.08);border:1px solid rgba(155,89,182,0.2);border-radius:var(--radius-sm);padding:12px;margin-bottom:16px;font-size:13px;color:var(--text-secondary);"></div>
          <div class="form-grid form-grid-2">
            <div class="field-wrap">
              <label class="field-label">${ICONS.package} Quantidade de Kits <span class="field-req">*</span></label>
              <input class="field-input" id="kit-saida-qtd" type="number" min="1" value="1" placeholder="Ex: 1">
            </div>
            <div class="field-wrap">
              <label class="field-label">${ICONS.calendar} Data</label>
              <input class="field-input" id="kit-saida-data" type="date" value="${new Date().toISOString().split('T')[0]}">
            </div>
            <div class="field-wrap form-grid-full">
              <label class="field-label">${ICONS.map_pin} Destino</label>
              <input class="field-input" id="kit-saida-destino" placeholder="Ex: Enfermaria A, Paciente...">
            </div>
          </div>
        </div>
        <div class="modal-footer">
          <button class="btn btn-secondary" onclick="closeModal('modal-kit-saida')">Cancelar</button>
          <button class="btn btn-primary" id="btn-save-kit-saida" onclick="saveKitSaida()">
            <span class="btn-spin"></span><span class="btn-text-content">${ICONS.check} Registar Saída</span>
          </button>
        </div>
      </div>
    </div>
  `;
  refocus('search-kits');
}

function renderKitComponentsList() {
  const produtos = db.getAll('produtos');
  const el = document.getElementById('kit-components-list');
  if (!el) return;
  if (!kitComponents.length) {
    el.innerHTML = `<div style="font-size:12px;color:var(--text-muted);padding:8px;border:1px dashed var(--border);border-radius:6px;text-align:center;">Nenhum componente adicionado. Adicione pelo menos 2 produtos.</div>`;
    return;
  }
  el.innerHTML = kitComponents.map((c,i)=>{
    const p = db.getById('produtos', c.produto_id) || produtos.find(x=>Number(x.id)===Number(c.produto_id));
    const {stock} = db.getStock(c.produto_id);
    return `<div style="display:flex;align-items:center;gap:10px;padding:8px 12px;background:var(--bg-input);border-radius:var(--radius-xs);border:1px solid var(--border);">
      <div style="color:#9B59B6;">${ICONS.pill}</div>
      <span style="flex:1;font-weight:600;font-size:13px;">${p?p.nome:`Produto #${c.produto_id}`}</span>
      <span class="chip">Qtd: ${c.quantidade}</span>
      <span style="font-size:11px;color:${stock>0?'var(--accent)':'var(--danger)'}">Stock: ${stock}</span>
      <button class="btn btn-danger btn-icon" style="width:28px;height:28px;" onclick="removeKitComponent(${i})">${ICONS.x}</button>
    </div>`;
  }).join('');
}

function addKitComponent() {
  const prodId = parseInt(document.getElementById('kit-add-prod').value);
  const qtd = parseInt(document.getElementById('kit-add-qtd').value)||1;
  if (!prodId) { toast('error','Seleccione um produto'); return; }
  if (kitComponents.find(c=>Number(c.produto_id)===prodId)) { toast('warning','Produto já adicionado','Este produto já está no kit. Edite a quantidade existente.'); return; }
  kitComponents.push({produto_id:prodId, quantidade:qtd});
  renderKitComponentsList();
  document.getElementById('kit-add-prod').value='';
  document.getElementById('kit-add-qtd').value='1';
}

function removeKitComponent(idx) {
  kitComponents.splice(idx,1);
  renderKitComponentsList();
}

function openKitModal(id=null) {
  kitEditingId = id;
  kitComponents = [];
  document.getElementById('modal-kit-title').textContent = id ? 'Editar Kit' : 'Novo Kit';
  if (id) {
    const k = db.getById('kits', id);
    if (k) {
      document.getElementById('kit-nome').value = k.nome||'';
      document.getElementById('kit-descricao').value = k.descricao||'';
      kitComponents = Array.isArray(k.componentes) ? [...k.componentes.map(c=>({...c}))] : [];
    }
  } else {
    document.getElementById('kit-nome').value='';
    document.getElementById('kit-descricao').value='';
  }
  document.getElementById('modal-kit').classList.add('open');
  renderKitComponentsList();
}

async function saveKit() {
  const nome = document.getElementById('kit-nome').value.trim();
  if (!nome) { toast('error','Nome obrigatório'); return; }
  if (kitComponents.length < 1) { toast('error','Adicione pelo menos 1 componente ao kit'); return; }
  const btn = document.getElementById('btn-save-kit');
  setLoading(btn,true);
  await new Promise(r=>setTimeout(r,300));
  const data = {
    nome,
    descricao: document.getElementById('kit-descricao').value,
    componentes: kitComponents.map(c=>({produto_id:Number(c.produto_id),quantidade:Number(c.quantidade)}))
  };
  if (kitEditingId) {
    db.update('kits', kitEditingId, data);
    toast('success','Kit actualizado');
  } else {
    db.insert('kits', data);
    toast('success','Kit cadastrado com sucesso');
  }
  setLoading(btn,false);
  closeModal('modal-kit');
  renderKits();
}

async function deleteKit(id) {
  const k = db.getById('kits',id);
  const ok = await confirm('Eliminar Kit',`Deseja eliminar o kit "${k?.nome}"?`);
  if (ok) { db.remove('kits',id); toast('success','Kit eliminado'); renderKits(); }
}

let kitSaidaId = null;
function openKitSaidaModal(kitId) {
  kitSaidaId = kitId;
  const k = db.getById('kits', kitId);
  if (!k) return;
  const comps = Array.isArray(k.componentes) ? k.componentes : [];
  const infoEl = document.getElementById('modal-kit-saida-info');
  if (infoEl) {
    infoEl.innerHTML = `
      <div style="font-weight:700;color:var(--text-primary);margin-bottom:8px;">${ICONS.layers} ${k.nome}</div>
      <div style="font-size:12px;">Componentes que serão debitados individualmente:</div>
      <div style="display:flex;flex-wrap:wrap;gap:6px;margin-top:6px;">
        ${comps.map(c=>{
          const p=db.getById('produtos',c.produto_id);
          const {stock}=db.getStock(c.produto_id);
          return `<span class="chip" style="background:rgba(155,89,182,0.15);color:#9B59B6;">${p?p.nome:'?'} × ${c.quantidade} <span style="color:${stock>=c.quantidade?'var(--accent)':'var(--danger)'};">(stock: ${stock})</span></span>`;
        }).join('')}
      </div>`;
  }
  document.getElementById('kit-saida-qtd').value='1';
  document.getElementById('kit-saida-destino').value='';
  document.getElementById('kit-saida-data').value=new Date().toISOString().split('T')[0];
  document.getElementById('modal-kit-saida').classList.add('open');
}

async function saveKitSaida() {
  const k = db.getById('kits', kitSaidaId);
  if (!k) return;
  const qtdKits = parseInt(document.getElementById('kit-saida-qtd').value)||1;
  const destino = document.getElementById('kit-saida-destino').value;
  const data = dateInputToStorage(document.getElementById('kit-saida-data').value) || today();
  const comps = Array.isArray(k.componentes) ? k.componentes : [];

  // Stock check
  for (const c of comps) {
    const {stock} = db.getStock(c.produto_id);
    const p = db.getById('produtos', c.produto_id);
    const needed = c.quantidade * qtdKits;
    if (stock < needed) {
      toast('error','Stock insuficiente',`${p?p.nome:`Produto #${c.produto_id}`}: Necessário ${needed}, disponível ${stock}`);
      return;
    }
  }

  const btn = document.getElementById('btn-save-kit-saida');
  setLoading(btn,true);
  await new Promise(r=>setTimeout(r,400));

  // Create individual saída for each component
  for (const c of comps) {
    const p = db.getById('produtos', c.produto_id);
    db.insert('movimentacoes', {
      produto_id: c.produto_id,
      produto_nome: db.getById('produtos', c.produto_id)?.nome || `Produto #${c.produto_id}`,
      tipo: 'SAÍDA',
      lote_id: null,
      quantidade: c.quantidade * qtdKits,
      destino: `Kit: ${k.nome}${destino?' → '+destino:''}`,
      data,
      preco: null,
      kit_id: kitSaidaId,
      auto: true,
      usuario_nome: currentUser?.nome || '',
      usuario_id: currentUser?.id || null,
    });
  }

  toast('success','Saída de Kit registada',
    `${qtdKits} kit(s) de "${k.nome}" debitados. ${comps.length} movimentações criadas automaticamente.`);

  setLoading(btn,false);
  closeModal('modal-kit-saida');
  renderKits();
  updateAlertBadge();
}

// ===================== LOGS PAGE =====================
let logsFilter = { action: '', module: '', search: '', date: '' };
let logsPage = 1;
const LOGS_PER_PAGE = 50;

function renderLogs() {
  const allLogs = (db.data.logs || []).slice().reverse(); // most recent first
  const modules = [...new Set(allLogs.map(l => l.module).filter(Boolean))];
  const actions = [...new Set(allLogs.map(l => l.action).filter(Boolean))];

  // Apply filters
  let filtered = allLogs.filter(l => {
    const matchAction = !logsFilter.action || l.action === logsFilter.action;
    const matchModule = !logsFilter.module || l.module === logsFilter.module;
    const matchDate   = !logsFilter.date   || l.date === logsFilter.date;
    const matchSearch = !logsFilter.search || 
      (l.description || '').toLowerCase().includes(logsFilter.search.toLowerCase()) ||
      (l.user_name || '').toLowerCase().includes(logsFilter.search.toLowerCase());
    return matchAction && matchModule && matchDate && matchSearch;
  });

  const total = filtered.length;
  const totalPages = Math.max(1, Math.ceil(total / LOGS_PER_PAGE));
  if (logsPage > totalPages) logsPage = totalPages;
  const start = (logsPage - 1) * LOGS_PER_PAGE;
  const paginated = filtered.slice(start, start + LOGS_PER_PAGE);

  // Stats
  const today = new Date().toLocaleDateString('pt-PT');
  const todayCount   = allLogs.filter(l => l.date === today).length;
  const loginCount   = allLogs.filter(l => l.action === 'login').length;
  const insertCount  = allLogs.filter(l => l.action === 'insert').length;
  const deleteCount  = allLogs.filter(l => l.action === 'update' && l.details && l.details.includes('"ativo":false')).length;

  const actionBadgeColor = a => ({
    insert:'#22c55e', update:'#3b82f6', remove:'#ef4444',
    login:'#8b5cf6', logout:'#f59e0b', view:'#6b7280', export:'#06b6d4', clear:'#f97316'
  }[a] || '#6b7280');

  const actionIcon = a => ({
    insert: ICONS.plus,
    update: ICONS.edit,
    remove: ICONS.trash,
    login:  ICONS.lock,
    logout: ICONS.logout,
    export: ICONS.download,
    clear:  ICONS.trash,
    view:   ICONS.eye,
  }[a] || ICONS.info);

  const moduleIcon = m => ({
    produtos: ICONS.pill, fornecedores: ICONS.supplier, prateleiras: ICONS.shelf,
    lotes: ICONS.lot, movimentacoes: ICONS.movement, kits: ICONS.layers,
    usuarios: ICONS.users
  }[m] || ICONS.database);

  document.getElementById('page-logs').innerHTML = `
    <div class="page-header">
      <div>
        <div class="page-title">${ICONS.list} Registos do Sistema (Logs)</div>
        <div class="page-title-sub">Histórico completo de todas as acções realizadas no sistema</div>
      </div>
      <div style="display:flex;gap:10px;flex-wrap:wrap;">
        <button class="btn btn-secondary" onclick="exportLogs()">${ICONS.download} Exportar CSV</button>
        ${isPrivileged() ? `<button class="btn btn-secondary" style="color:var(--danger);border-color:var(--danger);" onclick="clearLogs()">${ICONS.trash} Limpar Logs</button>` : ''}
        <button class="btn btn-secondary" onclick="renderLogs()">${ICONS.refresh} Actualizar</button>
      </div>
    </div>

    <!-- STAT CARDS -->
    <div class="grid-4" style="gap:16px;margin-bottom:20px;">
      <div class="card" style="display:flex;align-items:center;gap:14px;padding:16px 20px;">
        <div class="stat-icon" style="background:rgba(99,102,241,0.15);color:#6366f1;">${ICONS.list}</div>
        <div><div style="font-size:22px;font-weight:700;color:var(--text-primary)">${allLogs.length}</div><div style="font-size:12px;color:var(--text-muted)">Total de Registos</div></div>
      </div>
      <div class="card" style="display:flex;align-items:center;gap:14px;padding:16px 20px;">
        <div class="stat-icon" style="background:rgba(34,197,94,0.15);color:#22c55e;">${ICONS.calendar}</div>
        <div><div style="font-size:22px;font-weight:700;color:var(--text-primary)">${todayCount}</div><div style="font-size:12px;color:var(--text-muted)">Acções Hoje</div></div>
      </div>
      <div class="card" style="display:flex;align-items:center;gap:14px;padding:16px 20px;">
        <div class="stat-icon" style="background:rgba(139,92,246,0.15);color:#8b5cf6;">${ICONS.users}</div>
        <div><div style="font-size:22px;font-weight:700;color:var(--text-primary)">${loginCount}</div><div style="font-size:12px;color:var(--text-muted)">Total de Logins</div></div>
      </div>
      <div class="card" style="display:flex;align-items:center;gap:14px;padding:16px 20px;">
        <div class="stat-icon" style="background:rgba(239,68,68,0.15);color:#ef4444;">${ICONS.trash}</div>
        <div><div style="font-size:22px;font-weight:700;color:var(--text-primary)">${deleteCount}</div><div style="font-size:12px;color:var(--text-muted)">Eliminações</div></div>
      </div>
    </div>

    <!-- FILTERS -->
    <div class="card" style="padding:16px 20px;margin-bottom:16px;">
      <div style="display:flex;gap:12px;flex-wrap:wrap;align-items:flex-end;">
        <div style="flex:1;min-width:180px;">
          <div style="font-size:11px;font-weight:600;color:var(--text-muted);margin-bottom:6px;text-transform:uppercase;letter-spacing:.05em;">Pesquisar</div>
          <div style="position:relative;">
            <input class="field-input" id="search-logs" style="padding-left:10px;" placeholder="Descrição ou utilizador…" value="${logsFilter.search}" oninput="logsFilter.search=this.value;logsPage=1;filterLogsTable()">
          </div>
        </div>
        <div style="min-width:150px;">
          <div style="font-size:11px;font-weight:600;color:var(--text-muted);margin-bottom:6px;text-transform:uppercase;letter-spacing:.05em;">Acção</div>
          <select class="field-select" onchange="logsFilter.action=this.value;logsPage=1;renderLogs()">
            <option value="">Todas as Acções</option>
            ${Object.entries(ACTION_LABELS).map(([k,v]) => `<option value="${k}" ${logsFilter.action===k?'selected':''}>${v}</option>`).join('')}
          </select>
        </div>
        <div style="min-width:150px;">
          <div style="font-size:11px;font-weight:600;color:var(--text-muted);margin-bottom:6px;text-transform:uppercase;letter-spacing:.05em;">Módulo</div>
          <select class="field-select" onchange="logsFilter.module=this.value;logsPage=1;renderLogs()">
            <option value="">Todos os Módulos</option>
            ${Object.entries(TABLE_LABELS).map(([k,v]) => `<option value="${k}" ${logsFilter.module===k?'selected':''}>${v}</option>`).join('')}
          </select>
        </div>
        <div style="min-width:140px;">
          <div style="font-size:11px;font-weight:600;color:var(--text-muted);margin-bottom:6px;text-transform:uppercase;letter-spacing:.05em;">Data</div>
          <input type="date" class="field-input" value="${logsFilter.date ? new Date(logsFilter.date.split('/').reverse().join('-')).toISOString().split('T')[0] : ''}" oninput="const d=this.value?new Date(this.value).toLocaleDateString('pt-PT'):'';logsFilter.date=d;logsPage=1;renderLogs()">
        </div>
        <button class="btn btn-secondary" onclick="logsFilter={action:'',module:'',search:'',date:''};logsPage=1;renderLogs()" style="height:38px;">${ICONS.x} Limpar Filtros</button>
      </div>
    </div>

    <!-- TABLE -->
    <div class="table-wrap">
      <div class="table-header">
        <div class="table-title">${ICONS.activity} Registos <span class="chip">${filtered.length}</span></div>
        <div class="logs-page-info" style="font-size:13px;color:var(--text-muted);">Página ${logsPage} de ${totalPages} &nbsp;·&nbsp; A mostrar ${paginated.length} de ${total}</div>
      </div>
      ${paginated.length ? `
      <table class="data-table">
        <thead><tr>
          <th style="width:140px;">Data / Hora</th>
          <th style="width:110px;">Acção</th>
          <th style="width:120px;">Módulo</th>
          <th>Descrição</th>
          <th style="width:130px;">Utilizador</th>
        </tr></thead>
        <tbody>
          ${paginated.map(l => {
            const isDelete = l.action === 'update' && l.details && l.details.includes('"ativo":false');
            const actionKey = isDelete ? 'remove' : l.action;
            const color = actionBadgeColor(actionKey);
            const aLabel = isDelete ? 'Eliminação' : (ACTION_LABELS[l.action] || l.action);
            return `<tr>
              <td><span style="font-size:12px;color:var(--text-muted);">${l.date}</span><br><span style="font-size:11px;color:var(--text-muted);opacity:.7;">${l.time}</span></td>
              <td>
                <span style="display:inline-flex;align-items:center;gap:5px;padding:3px 9px;border-radius:20px;font-size:11px;font-weight:600;background:${color}22;color:${color};white-space:nowrap;">
                  <span style="width:12px;height:12px;display:inline-flex;">${actionIcon(actionKey)}</span>${aLabel}
                </span>
              </td>
              <td>
                ${l.module ? `<span style="display:inline-flex;align-items:center;gap:5px;font-size:12px;color:var(--text-secondary);">
                  <span style="width:14px;height:14px;display:inline-flex;opacity:.6;">${moduleIcon(l.module)}</span>${l.module_label||l.module}
                </span>` : '<span style="color:var(--text-muted);font-size:12px;">—</span>'}
              </td>
              <td style="font-size:13px;color:var(--text-primary);max-width:320px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;" title="${(l.description||'').replace(/"/g,'&quot;')}">${l.description || '—'}</td>
              <td>
                <div style="display:flex;align-items:center;gap:8px;">
                  <div style="width:28px;height:28px;border-radius:50%;background:var(--primary);color:#fff;display:flex;align-items:center;justify-content:center;font-size:10px;font-weight:700;flex-shrink:0;">${(l.user_name||'S').split(' ').map(w=>w[0]).slice(0,2).join('').toUpperCase()}</div>
                  <div><div style="font-size:12px;font-weight:500;color:var(--text-primary)">${l.user_name||'Sistema'}</div>${l.user_role?`<div style="font-size:10px;color:var(--text-muted)">${l.user_role}</div>`:''}</div>
                </div>
              </td>
            </tr>`;
          }).join('')}
        </tbody>
      </table>
      <!-- PAGINATION -->
      <div class="logs-pagination" style="display:flex;justify-content:center;align-items:center;gap:8px;padding:16px;">
        <button class="btn btn-secondary" style="padding:6px 12px;font-size:12px;" onclick="logsPage=${logsPage-1};renderLogs()" ${logsPage<=1?'disabled':''}>← Anterior</button>
        ${Array.from({length:Math.min(totalPages,7)},(_,i)=>{
          let p; if(totalPages<=7){p=i+1;}
          else if(logsPage<=4){p=i+1;}
          else if(logsPage>=totalPages-3){p=totalPages-6+i;}
          else{p=logsPage-3+i;}
          return `<button class="btn ${p===logsPage?'btn-primary':'btn-secondary'}" style="padding:6px 12px;font-size:12px;min-width:36px;" onclick="logsPage=${p};renderLogs()">${p}</button>`;
        }).join('')}
        <button class="btn btn-secondary" style="padding:6px 12px;font-size:12px;" onclick="logsPage=${logsPage+1};renderLogs()" ${logsPage>=totalPages?'disabled':''}>Seguinte →</button>
      </div>
      ` : `<div class="table-empty" style="padding:48px;">${ICONS.list}<p style="color:var(--text-muted)">Nenhum registo encontrado com os filtros seleccionados</p></div>`}
    </div>
  `;
  refocus('search-logs');
  const _lp = document.getElementById('page-logs');
  if (_lp) _lp.dataset.shellReady = '1';
}

function exportLogs() {
  const logs = (db.data.logs || []).slice().reverse();
  if (!logs.length) { toast('warning','Sem dados','Não há registos para exportar.'); return; }
  const headers = ['ID','Data','Hora','Acção','Módulo','Descrição','Utilizador','Função','ID Registo'];
  const rows = logs.map(l => [
    l.id, l.date, l.time,
    ACTION_LABELS[l.action]||l.action,
    TABLE_LABELS[l.module]||l.module||'',
    (l.description||'').replace(/"/g,'""'),
    l.user_name||'Sistema',
    l.user_role||'',
    l.record_id||''
  ]);
  const csv = [headers, ...rows].map(r => r.map(c => `"${c}"`).join(',')).join('\n');
  const blob = new Blob(['\uFEFF' + csv], {type:'text/csv;charset=utf-8;'});
  const url = URL.createObjectURL(blob);
  const a = document.createElement('a'); a.href = url;
  a.download = `logs_sistema_${new Date().toISOString().split('T')[0]}.csv`;
  document.body.appendChild(a); a.click(); document.body.removeChild(a);
  URL.revokeObjectURL(url);
  addLog('export', 'logs', null, { total: logs.length });
  toast('success','Exportação concluída',`${logs.length} registos exportados para CSV.`);
}

function clearLogs() {
  if (!isPrivileged()) { toast('error','Sem permissão','Apenas Técnicos e Administradores podem limpar os logs.'); return; }
  confirm('Limpar todos os Logs','Esta acção apaga permanentemente todos os registos de actividade. Confirmar?').then(ok => {
    if (!ok) return;
    addLog('clear', 'logs', null, { cleared_count: (db.data.logs||[]).length });
    db.data.logs = db.data.logs.slice(-1); // keep the clear action itself
    db.save();
    toast('success','Logs limpos','Todos os registos foram removidos.');
    renderLogs();
  });
}

// ===================== SEARCHABLE COMBOBOX — MOV PRODUTO =====================
function openMovCombo() {
  const input = document.getElementById('combo-mov-produto-input');
  let list = document.getElementById('combo-mov-produto-list');
  if (!input || !list) return;

  // Always repopulate with fresh product list
  const produtos = db.getAll('produtos');
  list.innerHTML = produtos.map(p => {
    const {stock} = db.getStock(p.id);
    return `<div class="combo-option" data-id="${p.id}" data-nome="${p.nome.replace(/"/g,'&quot;')}" onmousedown="selectMovCombo(${p.id},this.dataset.nome)"><span class="combo-opt-nome">${p.nome}</span><span class="combo-opt-stock ${stock<=0?'combo-stock-zero':stock<=(p.stock_minimo||0)?'combo-stock-low':'combo-stock-ok'}">${stock} un.</span></div>`;
  }).join('');

  // Move the dropdown to <body> to escape modal's stacking context
  if (list.parentElement !== document.body) {
    document.body.appendChild(list);
  }

  // Position it precisely below the input using fixed coords
  const rect = input.getBoundingClientRect();
  list.style.position = 'fixed';
  list.style.top   = (rect.bottom + 2) + 'px';
  list.style.left  = rect.left + 'px';
  list.style.width = rect.width + 'px';
  list.style.display = 'block';
  list.style.zIndex = '99999';

  // Filter with current input value
  filterMovCombo(input.value);

  // Reposition on scroll/resize
  if (list._repositionFn) window.removeEventListener('scroll', list._repositionFn, true);
  list._repositionFn = () => {
    const r = input.getBoundingClientRect();
    list.style.top  = (r.bottom + 2) + 'px';
    list.style.left = r.left + 'px';
  };
  window.addEventListener('scroll', list._repositionFn, true);
}

function closeMovCombo() {
  const list = document.getElementById('combo-mov-produto-list');
  if (!list) return;
  list.style.display = 'none';
  if (list._repositionFn) {
    window.removeEventListener('scroll', list._repositionFn, true);
    list._repositionFn = null;
  }
  // Se o dropdown foi movido para o body, devolvê-lo ao wrap original
  const wrap = document.getElementById('combo-mov-produto-wrap');
  if (wrap && list.parentElement === document.body) {
    list.style.position = '';
    list.style.top = '';
    list.style.left = '';
    list.style.width = '';
    list.style.zIndex = '';
    wrap.appendChild(list);
  }
}

function filterMovCombo(query) {
  const list = document.getElementById('combo-mov-produto-list');
  const input = document.getElementById('combo-mov-produto-input');
  if (!list || !input) return;

  // Se dropdown não está visível, abrir (sem recursão: openMovCombo chama filterMovCombo no final)
  if (list.style.display === 'none' || list.style.display === '') {
    openMovCombo();
    return;
  }

  list.style.display = 'block';
  const q = (query || '').toLowerCase().trim();
  let anyVisible = false;
  list.querySelectorAll('.combo-option').forEach(opt => {
    const nome = (opt.dataset.nome || '').toLowerCase();
    const match = !q || nome.includes(q);
    opt.style.display = match ? 'flex' : 'none';
    if (match) anyVisible = true;
  });
  let emptyEl = list.querySelector('.combo-empty');
  if (!anyVisible) {
    if (!emptyEl) {
      emptyEl = document.createElement('div');
      emptyEl.className = 'combo-empty';
      emptyEl.textContent = 'Nenhum produto encontrado';
      list.appendChild(emptyEl);
    }
    emptyEl.style.display = 'block';
  } else if (emptyEl) {
    emptyEl.style.display = 'none';
  }
}

function selectMovCombo(id, nome) {
  const hiddenInput = document.getElementById('mov-produto');
  const textInput = document.getElementById('combo-mov-produto-input');
  if (hiddenInput) hiddenInput.value = id;
  if (textInput) textInput.value = nome;
  closeMovCombo();
  updateMovLotes();
  // Auto-fill preco from produto if available (optional — clears on lote selection)
  const precoEl = document.getElementById('mov-preco');
  if (precoEl) {
    const prod = id ? db.getById('produtos', parseInt(id)) : null;
    precoEl.value = (prod && prod.preco) ? prod.preco : '';
  }
}

// ===================== TABLE-ONLY FILTER FUNCTIONS (no full re-render) =====================

function filterProdutosTable() {
  // Update only the tbody — the search input stays focused
  const tbody = document.getElementById('tbody-produtos');
  const counter = document.querySelector('#page-produtos .table-title .chip');
  if (!tbody) { renderProdutos(); return; }

  const prateleiras = db.getAll('prateleiras');
  let produtos = db.getAll('produtos');
  if (prodSearch) produtos = produtos.filter(p =>
    p.nome.toLowerCase().includes(prodSearch.toLowerCase()) ||
    (p.grupo_farmacologico||'').toLowerCase().includes(prodSearch.toLowerCase()) ||
    (p.forma||'').toLowerCase().includes(prodSearch.toLowerCase())
  );
  produtos.sort((a, b) => (a.nome||'').localeCompare(b.nome||'', 'pt', {sensitivity:'base'}));

  if (counter) counter.textContent = produtos.length;

  tbody.innerHTML = produtos.length ? produtos.map(p => {
    const {entradas, saidas, stock} = db.getStock(p.id);
    const prat = prateleiras.find(s => s.id === p.prateleira_id);
    const belowMin = p.stock_minimo && stock <= p.stock_minimo;
    return `<tr>
      <td class="td-name">${p.nome}</td>
      <td>${p.forma||'—'}</td>
      <td>${p.grupo_farmacologico||'—'}</td>
      <td>${prat ? prat.nome : '—'}</td>
      <td>${p.stock_minimo||'—'}</td>
      <td>${p.preco ? formatMoney(p.preco) : '—'}</td>
      <td class="text-accent font-bold">${entradas}</td>
      <td class="text-danger font-bold">${saidas}</td>
      <td class="font-bold ${belowMin?'text-danger':'text-info'}">${stock}</td>
      <td><span class="badge ${belowMin?'badge-danger':'badge-success'}">${belowMin?'Stock Baixo':p.status||'Ativo'}</span></td>
      <td>
        <div style="display:flex;gap:5px;">
          <button class="btn btn-secondary btn-icon" title="Editar" onclick="openProdutoModal(${p.id})">${ICONS.edit}</button>
          <button class="btn btn-danger btn-icon" title="Eliminar" onclick="deleteProduto(${p.id})">${ICONS.trash}</button>
        </div>
      </td>
    </tr>`;
  }).join('') : `<tr><td colspan="11"><div class="table-empty">${ICONS.pill}<p>Nenhum produto encontrado para "<strong>${prodSearch}</strong>"</p></div></td></tr>`;

  // Restore focus at end of text
  const inp = document.getElementById('search-produtos');
  if (inp) { const l = inp.value.length; inp.setSelectionRange(l, l); }
}

function renderLotesFiltered(fromSelect) {
  filterLotesTable(fromSelect);
}

function filterLotesTable(fromSelect) {
  // Update only the tbody — the search input stays focused
  const tbody = document.getElementById('tbody-lotes');
  const counter = document.querySelector('#page-lotes .table-title .chip');
  if (!tbody) { renderLotes(); return; }

  let lotes = db.getAll('lotes');
  if (loteSearch) lotes = lotes.filter(l => {
    const _p = db.getById('produtos', l.produto_id);
    const _f = db.getById('fornecedores', l.fornecedor_id);
    const _q = loteSearch.toLowerCase();
    return l.numero_lote.toLowerCase().includes(_q) ||
      (_p?.nome||'').toLowerCase().includes(_q) ||
      (_f?.nome||'').toLowerCase().includes(_q) ||
      (_p?.grupo_farmacologico||'').toLowerCase().includes(_q) ||
      (_p?.forma||'').toLowerCase().includes(_q);
  });
  if (loteFilter === 'ativos')   lotes = lotes.filter(l => daysUntil(l.validade) >= 0);
  if (loteFilter === 'avencer')  lotes = lotes.filter(l => { const d = daysUntil(l.validade); return d >= 0 && d <= 90; });
  if (loteFilter === 'vencidos') lotes = lotes.filter(l => daysUntil(l.validade) < 0);
  lotes.sort((a, b) => {
    const nA = (db.getById('produtos', a.produto_id)?.nome || '').toLowerCase();
    const nB = (db.getById('produtos', b.produto_id)?.nome || '').toLowerCase();
    return nA.localeCompare(nB, 'pt', {sensitivity:'base'});
  });

  if (counter) counter.textContent = lotes.length;

  tbody.innerHTML = lotes.length ? lotes.map(l => {
    const prod = db.getById('produtos', l.produto_id);
    const forn = db.getById('fornecedores', l.fornecedor_id);
    const isBloqueado = !!l.bloqueado;
    const st   = getLotStatus(l.validade, isBloqueado);
    const dias = daysUntil(l.validade);
    const stockActual = db.getLoteStock(l.id);
    return `<tr style="${isBloqueado?'opacity:0.65;background:var(--bg-tertiary,#f5f5f5);':''}">
      <td class="font-mono text-accent">${l.numero_lote}${isBloqueado?` <span style="color:var(--danger,#dc3545);font-size:10px;">🔒</span>`:''}</td>
      <td class="td-name">${prod ? prod.nome : '—'}</td>
      <td>${forn ? forn.nome : '—'}</td>
      <td class="font-bold">${l.quantidade||0}</td>
      <td class="font-bold ${stockActual<0?'text-danger':stockActual===0?'text-muted':'text-accent'}">${stockActual}</td>
      <td class="font-mono">${l.preco?formatMoney(l.preco):'—'}</td>
      <td>${formatDate(l.validade)}</td>
      <td class="${dias<0?'text-danger':dias<=90?'text-warning':'text-accent'}">${dias<0?`Há ${Math.abs(dias)}d`:dias===Infinity?'—':`${dias}d`}</td>
      <td class="font-mono text-muted">${l.codigo_barra||'—'}</td>
      <td><span class="badge ${st.cls}">${st.label}</span></td>
      <td>
        <div style="display:flex;gap:5px;">
          <button class="btn btn-secondary btn-icon" title="${isBloqueado?'Desbloquear lote':'Bloquear lote'}" onclick="toggleLoteBloqueado(${l.id})" style="${isBloqueado?'color:var(--warning,#ffc107);':''}">${isBloqueado?ICONS.unlock:ICONS.lock}</button>
          <button class="btn btn-secondary btn-icon" onclick="openLoteModal(${l.id})">${ICONS.edit}</button>
          <button class="btn btn-danger btn-icon" onclick="deleteLote(${l.id})">${ICONS.trash}</button>
        </div>
      </td>
    </tr>`;
  }).join('') : `<tr><td colspan="11"><div class="table-empty">${ICONS.lot}<p>Nenhum lote encontrado${loteSearch?' para "<strong>'+loteSearch+'</strong>"':''}</p></div></td></tr>`;

  // Restore focus to search input only when triggered by typing (not by select)
  if (!fromSelect) {
    const inp = document.getElementById('search-lotes');
    if (inp) { inp.focus(); const l = inp.value.length; inp.setSelectionRange(l, l); }
  }
}

// ===================== TABLE-ONLY FILTER: FORNECEDORES =====================
function filterFornecedoresTable() {
  const tbody = document.getElementById('tbody-fornecedores');
  const counter = document.querySelector('#page-fornecedores .table-title .chip');
  if (!tbody) { renderFornecedores(); return; }

  let forns = db.getAll('fornecedores');
  if (fornSearch) forns = forns.filter(f =>
    f.nome.toLowerCase().includes(fornSearch.toLowerCase()) ||
    (f.contacto||'').toLowerCase().includes(fornSearch.toLowerCase()) ||
    (f.email||'').toLowerCase().includes(fornSearch.toLowerCase())
  );

  if (counter) counter.textContent = forns.length;
  tbody.innerHTML = forns.length ? forns.map(f => `<tr>
    <td class="td-name">${f.nome}</td>
    <td>${f.contacto||'—'}</td>
    <td>${f.email||'—'}</td>
    <td>${f.telefone||'—'}</td>
    <td>${f.endereco||'—'}</td>
    <td>
      <div style="display:flex;gap:5px;">
        <button class="btn btn-secondary btn-icon" onclick="openFornecedorModal(${f.id})">${ICONS.edit}</button>
        <button class="btn btn-danger btn-icon" onclick="deleteFornecedor(${f.id})">${ICONS.trash}</button>
      </div>
    </td>
  </tr>`).join('')
  : `<tr><td colspan="6"><div class="table-empty">${ICONS.supplier}<p>Nenhum fornecedor encontrado${fornSearch?' para "<strong>'+fornSearch+'</strong>"':''}</p></div></td></tr>`;

  const inp = document.getElementById('search-fornecedores');
  if (inp) { const l = inp.value.length; inp.setSelectionRange(l, l); }
}

// ===================== TABLE-ONLY FILTER: MOVIMENTAÇÕES =====================
function filterMovimentacoesTable() {
  const tbody = document.getElementById('tbody-movimentacoes');
  if (!tbody) { movPage=0; _renderMovimentacoesUI(); return; }
  _updateMovTbody();
}


// ===================== TABLE-ONLY FILTER: KITS =====================
function filterKitsTable() {
  const tbody = document.getElementById('tbody-kits');
  const counter = document.querySelector('#page-kits .table-title .chip');
  if (!tbody) { renderKits(); return; }

  const kits = db.getAll('kits');
  const filtered = kitSearch ? kits.filter(k => k.nome.toLowerCase().includes(kitSearch.toLowerCase())) : kits;

  if (counter) counter.textContent = filtered.length;
  tbody.innerHTML = filtered.length ? filtered.map(k => {
    const comps = Array.isArray(k.componentes) ? k.componentes : [];
    const saidas = db.getAll('movimentacoes').filter(m => m.kit_id === k.id && (m.tipo||'').toUpperCase() === 'SAÍDA').length;
    return `<tr>
      <td class="td-name">
        <div style="display:flex;align-items:center;gap:8px;">
          <div style="width:30px;height:30px;border-radius:8px;background:rgba(155,89,182,0.2);display:flex;align-items:center;justify-content:center;color:#9B59B6;flex-shrink:0;">${ICONS.layers}</div>
          ${k.nome}
        </div>
      </td>
      <td>${comps.map(comp => { const p = db.getById('produtos', comp.produto_id); return `<span class="chip" style="margin:2px;">${p?p.nome:'?'} (${comp.quantidade})</span>`; }).join('')}</td>
      <td style="color:var(--text-muted);font-size:12px;">${k.descricao||'—'}</td>
      <td class="font-bold text-danger">${saidas}</td>
      <td>
        <div style="display:flex;gap:5px;">
          <button class="btn btn-primary btn-icon" title="Registar Saída de Kit" onclick="openKitSaidaModal(${k.id})">${ICONS.arrow_down}</button>
          <button class="btn btn-secondary btn-icon" onclick="openKitModal(${k.id})">${ICONS.edit}</button>
          <button class="btn btn-danger btn-icon" onclick="deleteKit(${k.id})">${ICONS.trash}</button>
        </div>
      </td>
    </tr>`;
  }).join('')
  : `<tr><td colspan="5"><div class="table-empty">${ICONS.layers}<p>Nenhum kit encontrado${kitSearch?' para "<strong>'+kitSearch+'</strong>"':''}</p></div></td></tr>`;

  const inp = document.getElementById('search-kits');
  if (inp) { const l = inp.value.length; inp.setSelectionRange(l, l); }
}

// ===================== TABLE-ONLY FILTER: LOGS =====================
function filterLogsTable() {
  const page = document.getElementById('page-logs');
  // Logs uses pagination — if shell not ready, do full render
  if (!page || !page.dataset.shellReady) { renderLogs(); return; }

  const allLogs = (db.data.logs || []).slice().reverse();
  let filtered = allLogs.filter(l => {
    const matchAction = !logsFilter.action || l.action === logsFilter.action;
    const matchModule = !logsFilter.module || l.module === logsFilter.module;
    const matchDate   = !logsFilter.date   || l.date === logsFilter.date;
    const matchSearch = !logsFilter.search ||
      (l.description||'').toLowerCase().includes(logsFilter.search.toLowerCase()) ||
      (l.user_name||'').toLowerCase().includes(logsFilter.search.toLowerCase());
    return matchAction && matchModule && matchDate && matchSearch;
  });

  const total = filtered.length;
  const totalPages = Math.max(1, Math.ceil(total / LOGS_PER_PAGE));
  if (logsPage > totalPages) logsPage = totalPages;
  const start = (logsPage - 1) * LOGS_PER_PAGE;
  const paginated = filtered.slice(start, start + LOGS_PER_PAGE);

  const actionBadgeColor = a => ({insert:'#22c55e',update:'#3b82f6',remove:'#ef4444',login:'#8b5cf6',logout:'#f59e0b',view:'#6b7280',export:'#06b6d4',clear:'#f97316'}[a]||'#6b7280');
  const actionIcon = a => ({insert:ICONS.plus,update:ICONS.edit,remove:ICONS.trash,login:ICONS.lock,logout:ICONS.logout,export:ICONS.download,clear:ICONS.trash,view:ICONS.eye}[a]||ICONS.info);
  const moduleIcon = m => ({produtos:ICONS.pill,fornecedores:ICONS.supplier,prateleiras:ICONS.shelf,lotes:ICONS.lot,movimentacoes:ICONS.movement,kits:ICONS.layers,usuarios:ICONS.users}[m]||ICONS.database);

  // Update counter chip
  const chip = page.querySelector('.table-title .chip');
  if (chip) chip.textContent = filtered.length;

  // Update pagination info
  const pageInfo = page.querySelector('.logs-page-info');
  if (pageInfo) pageInfo.textContent = `Página ${logsPage} de ${totalPages} · A mostrar ${paginated.length} de ${total}`;

  // Update table
  const tbody = page.querySelector('.data-table tbody');
  if (tbody) {
    tbody.innerHTML = paginated.length ? paginated.map(l => {
      const isDelete = l.action === 'update' && l.details && l.details.includes('"ativo":false');
      const actionKey = isDelete ? 'remove' : l.action;
      const color = actionBadgeColor(actionKey);
      const aLabel = isDelete ? 'Eliminação' : (ACTION_LABELS[l.action]||l.action);
      return `<tr>
        <td><span style="font-size:12px;color:var(--text-muted);">${l.date}</span><br><span style="font-size:11px;color:var(--text-muted);opacity:.7;">${l.time}</span></td>
        <td><span style="display:inline-flex;align-items:center;gap:5px;padding:3px 9px;border-radius:20px;font-size:11px;font-weight:600;background:${color}22;color:${color};white-space:nowrap;"><span style="width:12px;height:12px;display:inline-flex;">${actionIcon(actionKey)}</span>${aLabel}</span></td>
        <td>${l.module?`<span style="display:inline-flex;align-items:center;gap:5px;font-size:12px;color:var(--text-secondary);"><span style="width:14px;height:14px;display:inline-flex;opacity:.6;">${moduleIcon(l.module)}</span>${l.module_label||l.module}</span>`:'<span style="color:var(--text-muted);font-size:12px;">—</span>'}</td>
        <td style="font-size:13px;color:var(--text-primary);max-width:320px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;" title="${(l.description||'').replace(/"/g,'&quot;')}">${l.description||'—'}</td>
        <td>
          <div style="display:flex;align-items:center;gap:8px;">
            <div style="width:28px;height:28px;border-radius:50%;background:var(--primary);color:#fff;display:flex;align-items:center;justify-content:center;font-size:10px;font-weight:700;flex-shrink:0;">${(l.user_name||'S').split(' ').map(w=>w[0]).slice(0,2).join('').toUpperCase()}</div>
            <div><div style="font-size:12px;font-weight:500;color:var(--text-primary)">${l.user_name||'Sistema'}</div>${l.user_role?`<div style="font-size:10px;color:var(--text-muted)">${l.user_role}</div>`:''}</div>
          </div>
        </td>
      </tr>`;
    }).join('') : `<tr><td colspan="5" style="text-align:center;padding:32px;color:var(--text-muted);">Nenhum registo encontrado</td></tr>`;
  }

  // Update pagination buttons
  const pag = page.querySelector('.logs-pagination');
  if (pag) {
    pag.innerHTML = `
      <button class="btn btn-secondary" style="padding:6px 12px;font-size:12px;" onclick="logsPage=${logsPage-1};filterLogsTable()" ${logsPage<=1?'disabled':''}>← Anterior</button>
      ${Array.from({length:Math.min(totalPages,7)},(_,i)=>{let p=totalPages<=7?i+1:logsPage<=4?i+1:logsPage>=totalPages-3?totalPages-6+i:logsPage-3+i;return`<button class="btn ${p===logsPage?'btn-primary':'btn-secondary'}" style="padding:6px 12px;font-size:12px;min-width:36px;" onclick="logsPage=${p};filterLogsTable()">${p}</button>`;}).join('')}
      <button class="btn btn-secondary" style="padding:6px 12px;font-size:12px;" onclick="logsPage=${logsPage+1};filterLogsTable()" ${logsPage>=totalPages?'disabled':''}>Seguinte →</button>`;
  }

  const inp = document.getElementById('search-logs');
  if (inp) { const l = inp.value.length; inp.setSelectionRange(l, l); }
}

// ===================== OUTRO FIELD TOGGLE =====================
function toggleOutroField(selectId, inputId) {
  const sel = document.getElementById(selectId);
  const inp = document.getElementById(inputId);
  if (!sel || !inp) return;
  if (sel.value === 'Outro') {
    inp.style.display = 'block';
    inp.focus();
  } else {
    inp.style.display = 'none';
    inp.value = '';
  }
}
