/* Four statistics views. Existing app globals provide navigation and shared periods. */
const Stats = (() => {
  const $ = id => document.getElementById(id);
  const money = value => value == null ? '—' : fmt(value);
  const number = value => new Intl.NumberFormat('de-DE',{maximumFractionDigits:1}).format(value);
  const percent = value => value == null ? 'Nicht berechenbar' : number(value)+' %';
  const dateLabel = value => value ? value.split('-').reverse().join('.') : '—';
  const endDay = end => { const d=new Date(end+'T12:00:00'); d.setDate(d.getDate()-1); return d.toISOString().slice(0,10); };
  const rangeLabel = p => p?.start ? `${dateLabel(p.start)}–${dateLabel(endDay(p.end))}` : 'Alle vorhandenen Buchungen';
  const safe = value => esc(String(value ?? '')).replace(/"/g,'&quot;').replace(/'/g,'&#39;');
  const key = value => encodeURIComponent(value);
  const colors = ['#2563eb','#0d9488','#7c3aed','#b45309','#be185d','#0369a1'];
  const categoryColor = name => colors[[...name].reduce((a,c)=>a+c.charCodeAt(0),0)%colors.length];
  let section='overview', perspective='categories', data=null, recurring=null, upcoming=null;
  let sequence=0, detailSequence=0, allCategories=false, allUpcoming=false, days=30, recurringMode='current', showExcluded=false;
  let trigger=null, currentCategory=null, compare=true, aggregation='month';
  const charts = new Map();
  const changed = value => value===0 ? 'Unverändert' : `${money(Math.abs(value))} ${value>0?'mehr':'weniger'}`;
  const empty = text => `<p class="sx-empty">${text}</p>`;
  const card = (title, subtitle, body) => `<section class="card sx-card"><h2>${title}</h2>${subtitle?`<p class="sx-muted">${subtitle}</p>`:''}${body}</section>`;
  const table = (headers, rows) => `<div class="table-wrap sx-table"><table><thead><tr>${headers.map(h=>`<th scope="col">${h}</th>`).join('')}</tr></thead><tbody>${rows.map(row=>`<tr>${row.map(v=>`<td>${v}</td>`).join('')}</tr>`).join('')}</tbody></table></div>`;
  const chartSlot = (id, summary, headers, rows) => `<div class="sx-series"><div class="sx-chart-scroll"><div class="sx-chart"><canvas id="${id}" role="img" aria-label="${safe(summary)}"></canvas></div></div><details><summary>Datentabelle</summary>${table(headers,rows)}</details></div>`;
  const kpiIcons = {
    'Einnahmen':['income','lime'], 'Ausgaben':['expense','red'],
    'Sparüberweisungen':['savings','teal'], 'Verbleibender Betrag':['balance','blue'],
    'Netto in Sparanlagen überwiesen':['savings','teal'], 'Sparüberweisungsquote':['rate','lime'],
    'Zielquote':['target','blue'], 'Abstand zum Ziel':['trend','blue'],
    'Notwendig':['essential','teal'], 'Optional':['optional','orange'], 'Optional-Anteil':['rate','orange'],
    'Fixkosten / Monat':['fixed','red'], 'Variabel regelmäßig / Monat':['recurring','orange'],
    'Regelmäßige Einnahmen / Monat':['income','lime'], 'Sparpläne / Monat':['savings','teal']
  };
  const kpi = (title,value,sub='') => {
    const [icon,color]=kpiIcons[title]||['balance','blue'];
    return `<div class="card sx-kpi"><div class="sx-kpi-heading"><div class="sx-muted">${title}</div><div class="kpi-icon ${color}" aria-hidden="true"><svg viewBox="0 0 24 24"><use href="/static/statistics-icons.svg#${icon}"></use></svg></div></div><div class="sx-number">${value}</div>${sub?`<div class="sx-muted">${sub}</div>`:''}</div>`;
  };
  function destroyCharts() { charts.forEach(c=>c.destroy()); charts.clear(); }
  const chartValue = (value,percentage) => new Intl.NumberFormat('de-DE',{maximumFractionDigits:percentage?1:2}).format(value)+(percentage?' %':' €');
  // Draw values on the actual chart, including exports, without a tooltip dependency.
  const directValues = {
    id:'statisticsValues',
    afterDatasetsDraw(chart, args, options) {
      const {ctx,chartArea}=chart, occupied=[];
      const horizontal=chart.options.indexAxis==='y';
      const surface=getComputedStyle(chart.canvas).getPropertyValue('--surface').trim()||'#ffffff';
      const textColor=getComputedStyle(chart.canvas).getPropertyValue('--text').trim()||'#0f172a';
      const fontSize=11, lineHeight=16, padding=3;
      ctx.save(); ctx.font=`600 ${fontSize}px Inter, sans-serif`; ctx.textAlign='center'; ctx.textBaseline='middle';
      chart.data.datasets.forEach((dataset,index)=>{
        if(dataset.directValues===false || !chart.isDatasetVisible(index)) return;
        const meta=chart.getDatasetMeta(index), isLine=meta.type==='line';
        const count=dataset.data.filter(v=>v!=null&&Number.isFinite(v)).length;
        if(isLine&&count>8) return;
        meta.data.forEach((element,i)=>{
          const value=dataset.data[i];
          if(value==null||!Number.isFinite(value)||(!isLine&&value===0)) return;
          const label=chartValue(value,options.percentage), width=ctx.measureText(label).width+padding*2;
          const base=element.base??element.y;
          let x=element.x, y=element.y;
          // Stacked segments carry their own amount rather than a misleading total.
          if(!isLine&&options.stacked) {
            if(horizontal) x=(element.x+base)/2;
            else y=(element.y+base)/2;
          }
          const endpoint={x:element.x,y:element.y};
          const offsets=!isLine&&options.stacked?[0,-lineHeight,lineHeight,-2*lineHeight,2*lineHeight]
            :horizontal?[0,-lineHeight,lineHeight,-2*lineHeight,2*lineHeight]
            :value>=0?[-12,-28,-44,12,28,44]:[12,28,44,-12,-28,-44];
          if(horizontal&&!options.stacked) x+=value>=0?width/2+7:-width/2-7;
          const candidates=offsets.map(offset=>{
            const cx=Math.min(chart.width-width/2-5,Math.max(width/2+5,x));
            const cy=Math.max(fontSize/2+5,Math.min(chartArea.bottom+18,y+offset));
            const box={x:cx-width/2,y:cy-lineHeight/2,w:width,h:lineHeight};
            const collisions=occupied.filter(b=>box.x<b.x+b.w+3&&box.x+box.w+3>b.x&&box.y<b.y+b.h+2&&box.y+box.h+2>b.y).length;
            return {cx,cy,box,collisions};
          });
          const position=candidates.find(c=>c.collisions===0)||candidates.sort((a,b)=>a.collisions-b.collisions)[0];
          occupied.push(position.box);
          if(Math.abs(position.cy-endpoint.y)>22 || (horizontal&&Math.abs(position.cx-endpoint.x)>width/2+16)) {
            ctx.strokeStyle=dataset.borderColor||'#64748b'; ctx.lineWidth=1;
            ctx.beginPath(); ctx.moveTo(endpoint.x,endpoint.y); ctx.lineTo(position.cx,position.cy); ctx.stroke();
          }
          ctx.fillStyle=surface; ctx.globalAlpha=.94;
          ctx.fillRect(position.box.x,position.box.y,position.box.w,position.box.h); ctx.globalAlpha=1;
          ctx.fillStyle=textColor; ctx.fillText(label,position.cx,position.cy);
        });
      });
      ctx.restore();
    }
  };
  function draw(id,labels,datasets,{type='bar',percentage=false,horizontal=false,stacked=false}={}) {
    const el=$(id); if(!el) return;
    charts.get(id)?.destroy();
    const plot=el.parentElement;
    plot.style.minWidth=horizontal||type==='line'&&labels.length<=8?'0':`${Math.max(0,labels.length*52+80)}px`;
    if(horizontal) plot.style.height=Math.max(250,labels.length*32+85)+'px';
    const axis={beginAtZero:true,grace:'15%',ticks:{color:'#64748b',font:{family:'Inter',size:11},callback:v=>number(v)+(percentage?' %':' €')},grid:{color:'rgba(148,163,184,.15)'}};
    const categoryAxis={ticks:{color:'#64748b',font:{family:'Inter',size:11},maxRotation:0,autoSkip:true,maxTicksLimit:12},grid:{display:false}};
    charts.set(id,new Chart(el,{type,plugins:[directValues],data:{labels,datasets:datasets.map(d=>({borderWidth:2,borderRadius:4,pointRadius:3,tension:0,spanGaps:false,...d}))},options:{responsive:true,maintainAspectRatio:false,animation:false,indexAxis:horizontal?'y':'x',layout:{padding:{top:34,right:horizontal?65:12,left:horizontal?12:12,bottom:12}},
      plugins:{statisticsValues:{percentage,stacked},legend:{position:'bottom',labels:{color:'#475569',boxWidth:12,font:{family:'Inter',size:11}}},tooltip:{callbacks:{label:c=>`${c.dataset.label}: ${percentage?percent(horizontal?c.parsed.x:c.parsed.y):money(horizontal?c.parsed.x:c.parsed.y)}`}}},
      scales:{x:{...(horizontal?axis:categoryAxis),stacked},y:{...(horizontal?categoryAxis:axis),stacked}}}}));
  }
  function qualityHTML() {
    const q=data.quality, label={complete:'Kontenabdeckung bestätigt',partial:'Kontenabdeckung teilweise bestätigt',unverified:'Kontenabdeckung ungeprüft'}[q.status];
    return `<div class="sx-status-line"><details><summary><span class="sx-badge">${label}</span> Details</summary>${data.count===0?'<p class="sx-muted">Keine gebuchten Umsätze</p>':''}${q.accounts.length?q.accounts.map(a=>`<p class="sx-muted">${safe(a.account_name)} · ${safe(a.source)}: ${{complete:'Zeitraum bestätigt',partial:'teilweise bestätigt',unverified:'nicht bestätigt'}[a.status]} · letzte Buchung ${dateLabel(a.last_booking)}</p>`).join(''): '<p class="sx-muted">Keine Bankkonten mit bestätigbarer Abdeckung vorhanden.</p>'}${data.previous_quality?`<p class="sx-muted">Vergleichszeitraum: ${data.previous_quality.complete?'Kontenabdeckung bestätigt':'Kontenabdeckung nicht vollständig bestätigt'}.</p>`:''}<p class="sx-muted">Nur gebuchte Umsätze. Erstattungen mindern Ausgaben. Letzte Buchung ≠ bestätigte Abdeckung.</p></details></div>`;
  }
  function comparisonText() {
    if(!data.previous_period || !compare) return 'Vergleich ausgeschaltet.';
    const text=`Vergleich mit ${rangeLabel(data.previous_period)}.`;
    if(!data.comparison_available) return text+' Keine Buchungen im Vorzeitraum – Vergleich nicht verfügbar.';
    return text+(data.comparison_verified?' Abdeckung bestätigt.':' Vorläufig.');
  }
  function comparisonControl() {
    return `<div class="sx-toolbar"><p class="sx-muted">${comparisonText()}</p>${data.previous_period?`<label><input type="checkbox" id="sx-compare" ${compare?'checked':''}> Vorzeitraum vergleichen</label>`:''}</div>`;
  }
  const canCompare = () => compare && data.comparison_available;
  function categoryButton(r) {
    const total=data.summary.consumption, share=total>0&&r.total>=0?percent(r.total/total*100):'Nettoerstattungen enthalten';
    const max=Math.max(1,...data.categories.map(c=>Math.abs(c.total)));
    return `<button class="sx-category" data-category="${key(r.category)}"><span class="sx-category-name">${safe(r.category)}</span><span class="sx-category-amount">${money(r.total)}</span><span class="sx-muted">${share}</span><span class="sx-muted">${canCompare()?changed(r.diff):'Buchungen ansehen'}</span><progress style="--bar-color:${categoryColor(r.category)}" max="${max}" value="${Math.abs(r.total)}" aria-label="${safe(r.category)} ${money(r.total)}"></progress><span class="sx-muted">Details öffnen</span></button>`;
  }
  function monthlyTable() {
    return table(['Monat','Einnahmen','Ausgaben','Sparüberweisungen','Verbleibend','Sparquote'],data.monthly.map(r=>[r.label,money(r.income),money(r.consumption),money(r.savings),money(r.balance),r.savings_rate==null?'—':percent(r.savings_rate)]));
  }
  function renderOverview() {
    const s=data.summary, prev=canCompare()?data.previous:null;
    const insights=canCompare()?[...data.categories].filter(r=>r.diff!==0).sort((a,b)=>Math.abs(b.diff)-Math.abs(a.diff)).slice(0,3):[];
    const series=aggregation==='quarter'&&data.granularity!=='day'?data.quarterly:data.series;
    const aggregationControl=data.granularity!=='day'&&data.monthly.length>3?'<label>Auflösung<select id="sx-aggregation" aria-label="Auflösung"><option value="month">Monate</option><option value="quarter">Quartale</option></select></label>':'';
    $('sx-content').innerHTML=comparisonControl()+`<div class="sx-toolbar">${aggregationControl}</div><div class="sx-kpis">${[['Einnahmen','income'],['Ausgaben','consumption'],['Sparüberweisungen','savings'],['Verbleibender Betrag','balance']].map(([label,k])=>kpi(label,money(s[k]),prev?changed(s[k]-prev[k]):'')).join('')}</div>`+
      card('Einnahmen, Ausgaben und Sparen',`${rangeLabel(data.period)} · ${data.granularity==='day'?'Tageswerte':aggregation==='quarter'?'Quartalswerte':'Monatswerte'}`, data.count?chartSlot('sx-overview-chart','Gebuchte Nettobeträge; Monate ohne Daten bleiben offen.',['Datum','Einnahmen','Ausgaben','Sparüberweisungen'],series.map(r=>[r.label,money(r.income),money(r.consumption),money(r.savings)])):empty('Keine Buchungen im Zeitraum.'))+
      `<div class="sx-grid">`+card('Das hat sich verändert',data.comparison_verified?'':'Vorläufig',insights.length?insights.map(r=>`<button class="sx-insight" data-category="${key(r.category)}"><strong>${safe(r.category)}</strong><br><span class="sx-muted">${changed(r.diff)}</span></button>`).join(''):empty(canCompare()?'Keine Veränderung gegenüber dem Vorzeitraum.':'Keine Vergleichsdaten.'))+
      card('Einnahmen im Detail','',`<details><summary>Aufschlüsselung</summary>${data.income_categories.length?data.income_categories.map(r=>`<div class="sx-row"><span>${safe(r.category)}</span><strong>${money(r.total)}</strong></div>`).join(''):empty('Keine Einnahmen gebucht.')}</details>`)+`</div>`+
      card('Monatswerte','',`<details><summary>Monatswerte</summary>${monthlyTable()}</details>`);
    if($('sx-aggregation')) $('sx-aggregation').value=aggregation;
    if(data.count) draw('sx-overview-chart',series.map(r=>data.granularity==='day'?r.label.slice(8)+'.'+r.label.slice(5,7)+'.':r.label),[
      {label:'Einnahmen',data:series.map(r=>r.income),backgroundColor:'#2563eb'},
      {label:'Ausgaben',data:series.map(r=>r.consumption),backgroundColor:'#f97316'},
      {label:'Sparüberweisungen',data:series.map(r=>r.savings),backgroundColor:'#0d9488'}]);
  }
  function renderExpenses() {
    const cats=data.categories.filter(r=>r.total!==0 || (canCompare()&&r.previous!==0));
    let body='';
    if(perspective==='categories') body=card('Wofür gibst du Geld aus?','',cats.length?`<div class="sx-list">${(allCategories?cats:cats.slice(0,6)).map(r=>categoryButton(r)).join('')}</div>${cats.length>6?`<button class="sec sx-action" data-action="categories">${allCategories?'Weniger anzeigen':`Alle ${cats.length} Kategorien anzeigen`}</button>`:''}`:empty('Keine gebuchten Ausgaben in diesem Zeitraum.'));
    if(perspective==='changes') {
      const changes=[...cats].sort((a,b)=>Math.abs(b.diff)-Math.abs(a.diff));
      body=card('Woher kommt die Veränderung?','',canCompare()&&changes.length?chartSlot('sx-changes-chart','Balken nach rechts: mehr Ausgaben. Nach links: weniger Ausgaben.',['Kategorie','Aktuell','Vorzeitraum','Veränderung'],changes.map(r=>[safe(r.category),money(r.total),money(r.previous),changed(r.diff)]))+`<div class="sx-list">${changes.slice(0,6).map(r=>categoryButton(r)).join('')}</div><details><summary>Statistische Details</summary><p class="sx-muted">Median, Durchschnitt und Trend basieren auf früheren Monaten. Ohne Vorwert keine Prozentänderung.</p>${table(['Kategorie','Änderung in %','Typischer Monat (Median)','Ø letzte 3 Monate','Trend / Monat'],changes.map(r=>[safe(r.category),r.pct==null?(r.total!==0?'neu / kein positiver Vorwert':'—'):percent(r.pct),money(r.median),money(r.moving_average),money(r.trend)]))}</details>`:empty('Kein Vergleich verfügbar.'));
    }
    if(perspective==='necessity') {
      const n=data.necessity, total=n.essential+n.discretionary, multi=data.necessity_monthly.length>1;
      body=card('Notwendig und optional','',`<div class="sx-kpis">${kpi('Notwendig',money(n.essential))}${kpi('Optional',money(n.discretionary))}${kpi('Optional-Anteil',total>0?percent(n.discretionary/total*100):'Nicht berechenbar')}</div>`+(data.count?chartSlot('sx-necessity-chart',multi?'Nettokonsum pro Monat, aufgeteilt nach Notwendigkeit.':'Nettokonsum des gewählten Zeitraums.',['Zeitraum','Notwendig','Optional'],(multi?data.necessity_monthly:[{label:data.period.label,...n}]).map(r=>[safe(r.label),money(r.essential),money(r.discretionary)])):empty('Keine Buchungen vorhanden.')));
    }
    $('sx-content').innerHTML=comparisonControl()+`<div class="sx-toolbar"><div><p class="sx-muted">Ausgaben im Zeitraum</p><h2>${money(data.summary.consumption)}</h2></div><label>Perspektive<select id="sx-perspective" aria-label="Perspektive"><option value="categories">Kategorien</option><option value="necessity">Notwendig / Optional</option><option value="changes">Veränderungen</option></select></label></div>`+body+
      card('Auffällige Buchungen',`${data.outliers.length} auffällige Kategorie-Buchungen · ${$('sx-nooutlier').checked?'ausgeblendet':'enthalten'}`,data.outliers.length?data.outliers.slice(0,5).map(r=>`<button class="sx-insight" data-category="${key(r.category)}"><strong>${safe(r.merchant_name||r.category)} · ${money(-r.amount)}</strong><br><span class="sx-muted">${dateLabel(r.date)} · ${safe(r.category)} · ${number(r.factor)}× der Erkennungsschwelle von ${money(r.threshold)}</span></button>`).join('')+`<details><summary>Alle Fälle & Erkennung</summary><p class="sx-muted">Die Erkennungsschwelle beträgt mindestens 100 € und basiert auf dem vierfachen typischen Einzelbetrag der Kategorie. Regelmäßige Händler bleiben außen vor. „1× der Schwelle“ bedeutet nicht „1× des typischen Betrags“.</p>${table(['Datum','Kategorie','Betrag','Erkennungsschwelle'],data.outliers.map(r=>[dateLabel(r.date),safe(r.category),money(-r.amount),money(r.threshold)]))}</details>`:empty('Keine auffälligen Buchungen gefunden.'));
    $('sx-perspective').value=perspective;
    if(perspective==='changes'&&canCompare()) {
      const changes=[...cats].sort((a,b)=>Math.abs(b.diff)-Math.abs(a.diff)).slice(0,12);
      draw('sx-changes-chart',changes.map(r=>r.category),[{label:'Veränderung in Euro',data:changes.map(r=>r.diff),backgroundColor:changes.map(r=>r.diff>=0?'#b45309':'#2563eb')}],{horizontal:true});
    }
    if(perspective==='necessity'&&data.count) {
      const multi=data.necessity_monthly.length>1, points=multi?data.necessity_monthly:[{label:data.period.label,...data.necessity}];
      draw('sx-necessity-chart',points.map(r=>r.label),[{label:'Notwendig',data:points.map(r=>r.essential),backgroundColor:'#0d9488'},{label:'Optional',data:points.map(r=>r.discretionary),backgroundColor:'#f97316'}],{stacked:true,horizontal:!multi});
    }
  }
  function renderSavings() {
    const s=data.summary, pts=data.monthly;
    $('sx-content').innerHTML=`<div class="sx-kpis">${kpi('Netto in Sparanlagen überwiesen',money(s.savings))}${kpi('Sparüberweisungsquote',percent(s.savings_rate))}${kpi('Zielquote',data.goal==null?'Kein Ziel gesetzt':percent(data.goal))}${kpi('Abstand zum Ziel',s.savings_rate==null||data.goal==null?'—':number(s.savings_rate-data.goal)+' Prozentpunkte')}</div>`+
      card('Sparüberweisungen pro Monat','',pts.length?chartSlot('sx-savings-month','Monatliche Netto-Sparüberweisungen.',['Monat','Sparüberweisungen'],pts.map(r=>[r.label,money(r.savings)])):empty('Keine Buchungen im Zeitraum.'))+
      card('Kumulierte Sparüberweisungen',`${data.period.start?'Ab '+dateLabel(data.period.start):'Ab erster Buchung'} · kein Vermögensstand`,pts.length>1?chartSlot('sx-savings-total','Kumulierte Netto-Sparüberweisungen; Lücken bleiben offen.',['Monat','Kumulierte Sparüberweisungen'],pts.map(r=>[r.label,money(r.cumulative)])):`<p class="sx-number">${money(s.savings)}</p><p class="sx-muted">Für einen Verlauf mehrere Monate auswählen.</p>`)+
      card('Sparquote und Ziel','',pts.length>1?chartSlot('sx-savings-rate','Sparüberweisungsquote pro Monat und eingestelltes Ziel.',['Monat','Sparquote'],pts.map(r=>[r.label,r.savings_rate==null?'Nicht berechenbar':percent(r.savings_rate)])):`<p>${percent(s.savings_rate)}${data.goal==null?'':` · Ziel ${percent(data.goal)}`}</p>`);
    if(pts.length) draw('sx-savings-month',pts.map(r=>r.label),[{label:'Netto-Sparüberweisungen',data:pts.map(r=>r.savings),backgroundColor:'#0d9488'}]);
    if(pts.length>1) {
      draw('sx-savings-total',pts.map(r=>r.label),[{label:'Kumulierte Sparüberweisungen',data:pts.map(r=>r.cumulative),borderColor:'#0d9488',backgroundColor:'rgba(13,148,136,.08)',fill:true}],{type:'line'});
      const sets=[{label:'Sparüberweisungsquote',data:pts.map(r=>r.savings_rate),backgroundColor:'#2563eb'}];
      if(data.goal!=null) sets.push({type:'line',label:'Ziel '+percent(data.goal),data:pts.map(()=>data.goal),borderColor:'#b45309',borderDash:[5,4],pointRadius:0,directValues:false});
      draw('sx-savings-rate',pts.map(r=>r.label),sets,{percentage:true});
    }
  }
  function seriesItem(item,index) {
    const type=item.kind==='income'?'Einnahme':item.kind==='savings'?'Sparplan':'Ausgabe';
    return `<details class="sx-recurring"><summary><span><strong>${safe(item.merchant_name)}</strong><br><span class="sx-muted">${safe(item.category)} · ${safe(item.account_name)} · ${item.excluded?'Ausgeschlossen':!item.active?'Inaktiv / Daten prüfen':item.type==='fix'?'Fix':'Variabel'}${item.price_changed?' · Preisänderung':''}</span></span><strong>${money(item.last_amount??item.median_amount)}</strong></summary><p class="sx-muted">${type} · ${item.interval_months?`alle ${item.interval_months} Monate`:'unregelmäßig'} · nächster Termin ${dateLabel(item.next_expected)}<br>Normalisiert: ${money(item.monthly_cost)} / Monat · ${money(item.yearly_cost)} / Jahr${item.price_change?`<br>Preisänderung: ${money(item.price_change.from)} → ${money(item.price_change.to)}`:''}</p><div class="sx-actions"><button class="sec" data-recurring-index="${index}" data-recurring-action="exclude">${item.excluded?'Wieder berücksichtigen':'Nicht mehr wiederkehrend'}</button><button class="sec" data-recurring-index="${index}" data-recurring-action="type">Als ${item.type==='fix'?'variabel':'fix'} einstufen</button>${item.type_overridden?`<button class="sec" data-recurring-index="${index}" data-recurring-action="reset">Automatische Einstufung</button>`:''}</div></details>`;
  }
  function renderRecurring() {
    if(!recurring||!upcoming) return;
    const toolbar=`<div class="sx-toolbar"><label>Zeitbezug<select id="sx-recurring-mode" aria-label="Zeitbezug"><option value="current">Aktuell & anstehend</option><option value="booked">Im gewählten Zeitraum gebucht</option></select></label></div>`;
    const keys=new Set(recurring.items.filter(r=>!r.excluded&&r.active).map(r=>[r.source,r.account_name,r.merchant_name,r.category].join('\u001f')));
    if(recurringMode==='booked') {
      const rows=recurring.booked.filter(r=>keys.has([r.source,r.account_name,r.merchant_name,r.category].join('\u001f')));
      $('sx-content').innerHTML=toolbar+card('Regelmäßige Buchungen im Zeitraum',`${rangeLabel(data.period)} · aktuell aktive Serien`,rows.length?table(['Datum','Händler','Kategorie','Gebuchter Betrag'],rows.map(r=>[dateLabel(r.date),safe(r.merchant_name),safe(r.category),money(r.amount)])):empty('Keine Buchungen zu aktuell aktiven Serien in diesem Zeitraum.'));
    } else {
      const income=recurring.items.filter(r=>r.active&&!r.excluded&&r.kind==='income').reduce((s,r)=>s+r.monthly_cost,0);
      const upcomingItems=allUpcoming?upcoming.items:upcoming.items.slice(0,3);
      let groups='';
      for(const [kind,label] of [['consumption','Ausgaben'],['income','Einnahmen'],['savings','Sparpläne']]) {
        const items=recurring.items.map((r,i)=>({r,i})).filter(({r})=>r.kind===kind&&(showExcluded||!r.excluded));
        groups+=card(label,'',items.length?items.map(({r,i})=>seriesItem(r,i)).join(''):empty('Keine passenden Serien.'));
      }
      $('sx-content').innerHTML=toolbar+`<p class="sx-muted sx-status-line">Aktuelle Serien · Monatswerte auf den Monat umgerechnet. Der Zeitraumfilter gilt nur für gebuchte Zahlungen.</p><div class="sx-kpis">${kpi('Fixkosten / Monat',money(recurring.fixed_monthly))}${kpi('Variabel regelmäßig / Monat',money(recurring.variable_monthly))}${kpi('Regelmäßige Einnahmen / Monat',money(income))}${kpi('Sparpläne / Monat',money(recurring.savings_monthly))}</div>`+
        card('Nächste Termine','Geschätzte Termine und Beträge.',`<div class="sx-toolbar"><label>Vorschau<select id="sx-days" aria-label="Vorschau"><option value="30">Nächste 30 Tage</option><option value="60">Nächste 60 Tage</option><option value="90">Nächste 90 Tage</option></select></label></div>${upcomingItems.length?upcomingItems.map(r=>`<div class="sx-row"><div><strong>${dateLabel(r.expected)} · ${safe(r.merchant_name)}</strong><p class="sx-muted">${safe(r.category)} · ${safe(r.account_name)}</p></div><strong>${r.kind==='income'?'+':'−'} ${money(r.amount)}</strong></div>`).join(''):empty('Keine erkannten Termine in diesem Fenster.')}${upcoming.items.length>3?`<button class="sec sx-action" data-action="upcoming">${allUpcoming?'Nur nächste drei Termine':`Alle ${upcoming.items.length} Termine anzeigen`}</button>`:''}`)+
        `<div class="sx-toolbar"><label><input type="checkbox" id="sx-show-excluded" ${showExcluded?'checked':''}> Ausgeschlossene Serien anzeigen</label></div>`+groups+
        card('Wie wird das erkannt?','',`<details><summary>Erkennung und Einstufung erklären</summary><p class="sx-muted">Mindestens drei Zahlungen mit gleichmäßigem Monatsabstand und ähnlichem Buchungstag. Stabile Beträge gelten als fix, schwankende als variabel. Manuelle Einstufungen bleiben erhalten. „Nicht mehr wiederkehrend“ nimmt eine Serie aus Terminen und Hochrechnung; bisherige Buchungen bleiben erhalten. Die automatische Einstufung lässt sich wiederherstellen.</p></details>`);
      $('sx-days').value=String(days);
    }
    $('sx-recurring-mode').value=recurringMode;
  }
  function render() {
    if(!data) return;
    const focused=document.activeElement;
    const restoreId=$('sx-content').contains(focused)?focused.id:null;
    const restoreAction=$('sx-content').contains(focused)?focused.dataset.action:null;
    destroyCharts();
    $('sx-status').innerHTML=section==='recurring'&&recurringMode==='current'?'':qualityHTML();
    $('sx-range').textContent=rangeLabel(data.period)+(data.selection.end!==data.period.end?' · bis heute':'')+(/^r\d+$/.test(currentPeriod)?' · Ende am neuesten Datenmonat':'')+(months[0]&&months[0]<data.today.slice(0,7)?' · Neuester Datenmonat: '+months[0]:'');
    $('sx-excluded').textContent=data.excluded_count?`${data.excluded_count} Kategorie-Buchungen · ${money(data.excluded_total)} ausgeblendet`:'';
    $('sx-content').removeAttribute('aria-busy');
    if(section==='overview') renderOverview();
    if(section==='expenses') renderExpenses();
    if(section==='savings') renderSavings();
    if(section==='recurring') renderRecurring();
    if(restoreId) $(restoreId)?.focus({preventScroll:true});
    else if(restoreAction) document.querySelector(`#sx-content [data-action="${restoreAction}"]`)?.focus({preventScroll:true});
  }
  function syncControls() {
    const value=periodOf('stats-period');
    const kind=/^\d{4}-\d{2}$/.test(value)?'month':/^\d{4}-Q[1-4]$/.test(value)?'quarter':/^\d{4}$/.test(value)?'year':'range';
    $('sx-kind').value=kind;
    const groups=periodGroups(months), pick={month:'Monate',quarter:'Quartale',year:'Jahre'}[kind];
    let options=pick?(groups.find(([label])=>label===pick)?.[1]||[]):[['r3','Letzte 3 Monate'],['r6','Letzte 6 Monate'],['r12','Letzte 12 Monate'],['all','Alle Daten'],['custom','Von–Bis']];
    if(pick&&!options.some(([v])=>v===value)) options=[[value,value],...options];
    if(kind==='month'&&matchMedia('(max-width:700px)').matches) options=options.map(([v,label])=>[v,/^\d{4}-\d{2}$/.test(v)?new Intl.DateTimeFormat('de-DE',{month:'short',year:'numeric'}).format(new Date(v+'-01T12:00:00')):label]);
    $('sx-value').innerHTML=options.map(([v,label])=>`<option value="${v}">${safe(label)}</option>`).join('');
    $('sx-value').value=value.includes('..')?'custom':value;
    $('sx-custom').hidden=!value.includes('..');
    if(value.includes('..')) [$('sx-from').value,$('sx-to').value]=value.split('..');
    $('sx-prev').hidden=kind==='range'; $('sx-next').hidden=kind==='range';
  }
  function applyPeriod(value) {
    const sel=$('stats-period');
    if(![...sel.options].some(o=>o.value===value)) sel.add(new Option(value,value));
    sel.value=value;
    currentCategory=null; detailSequence++; if($('sx-detail').open) $('sx-detail').close();
    onPeriodChange('stats-period');
  }
  function stepPeriod(delta) {
    const p=periodOf('stats-period'), kind=$('sx-kind').value;
    const d=new Date(Number(p.slice(0,4)),kind==='quarter'?(Number(p.slice(-1))-1)*3:kind==='year'?0:Number(p.slice(5,7))-1,1,12);
    if(kind==='year') d.setFullYear(d.getFullYear()+delta); else d.setMonth(d.getMonth()+delta*(kind==='quarter'?3:1));
    const y=d.getFullYear(), m=String(d.getMonth()+1).padStart(2,'0');
    applyPeriod(kind==='year'?String(y):kind==='quarter'?`${y}-Q${Math.floor(d.getMonth()/3)+1}`:`${y}-${m}`);
  }
  async function load() {
    const run=++sequence;
    syncControls();
    $('sx-content').setAttribute('aria-busy','true'); $('sx-status').textContent='Auswertung wird aktualisiert …';
    $('sx-range').textContent='Zeitraum wird aktualisiert …'; $('sx-excluded').textContent='';
    try {
      const q=new URLSearchParams({period:periodOf('stats-period'),exclude_outliers:String($('sx-nooutlier').checked),include_bookings:String(section==='recurring')});
      const requests=[api('/api/stats/explorer?'+q)];
      if(section==='recurring') requests.push(api('/api/stats/recurring'),api('/api/stats/upcoming?days='+days));
      const results=await Promise.all(requests);
      if(run!==sequence) return;
      data=results[0];
      if(section==='recurring') { recurring=results[1]; upcoming=results[2]; recurring.booked=data.bookings; }
      render();
    } catch(e) {
      if(run!==sequence) return;
      $('sx-content').innerHTML=''; $('sx-content').removeAttribute('aria-busy');
      $('sx-status').innerHTML=`<div class="sx-error" role="alert">Die Auswertung konnte nicht geladen werden. ${safe(e.message)} <button class="sec" data-action="retry">Erneut versuchen</button></div>`;
    }
  }
  async function openCategory(name,button) {
    const run=++detailSequence; currentCategory=name; trigger=button;
    const dialog=$('sx-detail'); $('sx-detail-title').textContent=name;
    $('sx-detail-content').textContent='Buchungen werden geladen …';
    if(!dialog.open) dialog.showModal();
    try {
      const q=new URLSearchParams({period:periodOf('stats-period'),exclude_outliers:String($('sx-nooutlier').checked),category:name});
      const response=await api('/api/stats/explorer?'+q);
      if(run!==detailSequence||!dialog.open) return;
      const d=response.detail, cat=response.categories.find(r=>r.category===name);
      $('sx-detail-content').innerHTML=card('Ausgaben im Zeitraum',rangeLabel(response.period),`<p class="sx-number">${money(d.total)}</p>${canCompare()&&cat?`<p class="sx-muted">${changed(cat.diff)} gegenüber ${rangeLabel(response.previous_period)} · ${response.comparison_verified?'bestätigte Abdeckung':'vorläufig'}</p>`:''}`)+
        card('Verlauf','',d.series.length?chartSlot('sx-category-chart','Gebuchte Nettobeträge dieser Kategorie pro Buchungstag.',['Datum','Betrag'],d.series.map(r=>[dateLabel(r.label),money(r.total)])):empty('Keine Buchungen für diese Kategorie im Zeitraum.'))+
        card('Zugehörige Buchungen','Beträge dieser Kategorie, einschließlich Erstattungen.',d.transactions.length?d.transactions.map(r=>`<div class="sx-row"><div><strong>${safe(r.merchant_name||r.description||'Buchung')}</strong><p class="sx-muted">${dateLabel(r.date)} · ${safe(r.account_name)}<br>${safe(r.description||'')}</p></div><strong>${money(r.amount)}</strong></div>`).join(''):empty('Keine zugehörigen Buchungen.'));
      if(d.series.length) draw('sx-category-chart',d.series.map(r=>dateLabel(r.label)),[{label:name,data:d.series.map(r=>r.total),backgroundColor:categoryColor(name)}]);
    } catch(e) { if(run===detailSequence) $('sx-detail-content').innerHTML=`<p role="alert">Buchungen konnten nicht geladen werden: ${safe(e.message)}</p>`; }
  }
  function selectSection(value) {
    section=value;
    document.querySelectorAll('.sx-nav button').forEach(b=>{ if(b.dataset.section===value) b.setAttribute('aria-current','page'); else b.removeAttribute('aria-current'); });
    load();
  }
  async function recurringActionClick(button) {
    const item=recurring.items[Number(button.dataset.recurringIndex)], action=button.dataset.recurringAction;
    button.disabled=true;
    try {
      if(action==='exclude') await api('/api/recurring/exclusion',{method:'PUT',headers:{'Content-Type':'application/json'},body:JSON.stringify({source:item.source,account_name:item.account_name,merchant_name:item.merchant_name,category:item.category,excluded:!item.excluded})});
      else await api(`/api/recurring/${key(item.merchant_name)}/${key(item.category)}/type`,{method:action==='reset'?'DELETE':'PUT',headers:{'Content-Type':'application/json'},...(action==='reset'?{}:{body:JSON.stringify({type:item.type==='fix'?'variabel':'fix'})})});
      await load();
    } catch(e) { button.disabled=false; $('sx-status').innerHTML=`<p role="alert">Änderung fehlgeschlagen: ${safe(e.message)}</p>`; }
  }
  function init() {
    syncControls();
    $('sx-prev').addEventListener('click',()=>stepPeriod(-1)); $('sx-next').addEventListener('click',()=>stepPeriod(1));
    $('sx-kind').addEventListener('change',()=>{
      const kind=$('sx-kind').value, selected=periodOf('stats-period'), latest=months[0]||new Date().toISOString().slice(0,7);
      const m=/^\d{4}-\d{2}/.test(selected)?selected.slice(0,7):/^\d{4}-Q/.test(selected)?selected.slice(0,4)+'-'+String((Number(selected.slice(-1))-1)*3+1).padStart(2,'0'):/^\d{4}$/.test(selected)?selected+'-'+latest.slice(5,7):latest;
      applyPeriod(kind==='month'?m:kind==='quarter'?m.slice(0,4)+'-Q'+Math.ceil(Number(m.slice(5))/3):kind==='year'?m.slice(0,4):'r3');
    });
    $('sx-value').addEventListener('change',()=>{ if($('sx-value').value==='custom') { $('sx-custom').hidden=false; $('sx-from').focus(); } else applyPeriod($('sx-value').value); });
    $('sx-custom').addEventListener('submit',e=>{ e.preventDefault(); const from=$('sx-from'),to=$('sx-to'); to.setCustomValidity(to.value<from.value?'Das Ende muss am oder nach dem Anfang liegen.':''); if(to.reportValidity()) applyPeriod(from.value+'..'+to.value); });
    $('sx-to').addEventListener('input',()=>$('sx-to').setCustomValidity(''));
    $('sx-nooutlier').addEventListener('change',load);
    const panel=$('sx-detail');
    $('sx-close').addEventListener('click',()=>panel.close());
    let backdropPressed=false;
    const outsidePanel=e=>{ const r=panel.getBoundingClientRect(); return e.clientX<r.left||e.clientX>r.right||e.clientY<r.top||e.clientY>r.bottom; };
    panel.addEventListener('pointerdown',e=>{backdropPressed=e.target===panel&&outsidePanel(e);});
    panel.addEventListener('click',e=>{if(backdropPressed&&e.target===panel&&outsidePanel(e)) panel.close(); backdropPressed=false;});
    $('sx-detail').addEventListener('close',()=>{detailSequence++; charts.get('sx-category-chart')?.destroy();charts.delete('sx-category-chart'); trigger?.focus();currentCategory=null;});
    $('page-stats').addEventListener('click',e=>{
      const b=e.target.closest('button'); if(!b) return;
      if(b.dataset.section) selectSection(b.dataset.section);
      if(b.dataset.category) openCategory(decodeURIComponent(b.dataset.category),b);
      if(b.dataset.action==='categories') {allCategories=!allCategories; render();}
      if(b.dataset.action==='upcoming') {allUpcoming=!allUpcoming; render();}
      if(b.dataset.action==='retry') load();
      if(b.dataset.recurringAction) recurringActionClick(b);
    });
    $('page-stats').addEventListener('change',e=>{
      if(e.target.id==='sx-aggregation') {aggregation=e.target.value;render();}
      if(e.target.id==='sx-perspective') {perspective=e.target.value;render();}
      if(e.target.id==='sx-compare') {compare=e.target.checked;render();}
      if(e.target.id==='sx-days') {days=Number(e.target.value);allUpcoming=false;load();}
      if(e.target.id==='sx-recurring-mode') {recurringMode=e.target.value;render();}
      if(e.target.id==='sx-show-excluded') {showExcluded=e.target.checked;render();}
    });
  }
  return {init,load,selectSection};
})();
