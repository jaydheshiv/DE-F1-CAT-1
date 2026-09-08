import { useState, useEffect, useRef } from 'react'
import axios from 'axios'
import { BarChart, Bar, XAxis, YAxis, Tooltip, ResponsiveContainer } from 'recharts'
import './index.css'

const API = 'http://localhost:8000/api'

// ── Helpers ─────────────────────────────────────────────
const fmt   = (n) => (n == null ? '—' : Number(n).toLocaleString('en-US', { maximumFractionDigits: 2 }))
const dt    = (s) => s ? new Date(s).toLocaleString('en-US') : '—'
const flag  = (nat) => {
  const map = {
    'British':'🇬🇧','German':'🇩🇪','Spanish':'🇪🇸','Finnish':'🇫🇮','Brazilian':'🇧🇷',
    'Australian':'🇦🇺','French':'🇫🇷','Dutch':'🇳🇱','Belgian':'🇧🇪','Italian':'🇮🇹',
    'Austrian':'🇦🇹','American':'🇺🇸','Canadian':'🇨🇦','Mexican':'🇲🇽','Japanese':'🇯🇵',
    'Russian':'🇷🇺','Chinese':'🇨🇳','Indian':'🇮🇳','Danish':'🇩🇰','Polish':'🇵🇱',
    'Swiss':'🇨🇭','New Zealander':'🇳🇿','Argentine':'🇦🇷','Colombian':'🇨🇴','Thai':'🇹🇭',
    'Monegasque':'🇲🇨','Swedish':'🇸🇪','Irish':'🇮🇪','Hungarian':'🇭🇺','Czech':'🇨🇿',
  }
  return map[nat] || '🏁'
}

function BarRow({ label, value, max, color = '#6366f1' }) {
  const w = max > 0 ? Math.round((value / max) * 100) : 0
  return (
    <div className="bar-row">
      <span className="bar-label" title={label}>{label}</span>
      <div className="bar-track"><div className="bar-fill" style={{ width: `${w}%`, background: color }} /></div>
      <span className="bar-val">{fmt(value)}</span>
    </div>
  )
}

// ══════════════════════════════════════════════════════
// WEEK 1 — EDA with F1 CSV support
// ══════════════════════════════════════════════════════
function Week1() {
  const [eda, setEda]       = useState(null)
  const [loading, setLoading] = useState(false)
  const [tab, setTab]        = useState('summary')
  const [activeTable, setActiveTable] = useState(null)
  const fileRef              = useRef()

  const F1_TABLES = [
    { id: 'drivers',              label: '🏎️ Drivers',              color: 'color-indigo'  },
    { id: 'races',                label: '🏁 Races',                color: 'color-cyan'    },
    { id: 'results',              label: '🏆 Results',              color: 'color-emerald' },
    { id: 'circuits',             label: '🗺️ Circuits',             color: 'color-amber'   },
    { id: 'constructors',         label: '🔧 Constructors',         color: 'color-purple'  },
    { id: 'driver_standings',     label: '📊 Driver Standings',     color: 'color-rose'    },
    { id: 'constructor_standings',label: '🏗️ Constructor Standings',color: 'color-cyan'    },
  ]

  const loadF1Table = async (tableId) => {
    setLoading(true); setEda(null); setActiveTable(tableId)
    try {
      const r = await axios.get(`${API}/week1/eda/f1/${tableId}`)
      setEda(r.data)
    } catch { alert('API not ready. Start Docker first.') }
    finally { setLoading(false) }
  }

  const handleUpload = async (e) => {
    const file = e.target.files[0]; if (!file) return
    setLoading(true); setEda(null); setActiveTable(null)
    const fd = new FormData(); fd.append('file', file)
    try { const r = await axios.post(`${API}/week1/eda/upload`, fd); setEda(r.data) }
    catch { alert('Upload failed. Check file format.') }
    finally { setLoading(false) }
  }

  const numCols = eda?.summary?.filter(s => s.mean != null) ?? []
  const catCols = eda ? Object.keys(eda.distributions ?? {}) : []

  return (
    <div className="fade-in">
      <div className="section-heading">
        <h2>📊 Week 1 — Data Collection, Preprocessing & EDA</h2>
        <p>Explore any F1DB table or upload your own CSV for automated Exploratory Data Analysis.</p>
      </div>

      {/* F1 Table Picker */}
      <div className="panel" style={{ marginBottom: '1.5rem' }}>
        <div className="panel-title"><span className="panel-icon">🏎️</span> Analyse F1DB Tables</div>
        <div className="f1-table-grid">
          {F1_TABLES.map(t => (
            <button key={t.id}
              id={`eda-btn-${t.id}`}
              className={`f1-table-btn ${activeTable === t.id ? 'active' : ''}`}
              onClick={() => loadF1Table(t.id)} disabled={loading}>
              <span className={t.color} style={{ fontWeight: 700 }}>{t.label}</span>
              <span style={{ fontSize: '0.68rem', color: 'var(--text-3)' }}>Click to analyse</span>
            </button>
          ))}
        </div>
      </div>

      {loading && (
        <div style={{ display: 'flex', justifyContent: 'center', padding: '3rem' }}>
          <span className="spinner" style={{ width: 36, height: 36 }} />
        </div>
      )}

      {eda && (
        <>

          <div className="grid-4" style={{ marginBottom: '1.5rem' }}>
            {[
              { label: 'Total Rows',    value: fmt(eda.rows),    icon: '📋', color: 'color-indigo' },
              { label: 'Total Columns', value: fmt(eda.columns), icon: '🗂️', color: 'color-cyan'   },
              { label: 'Numeric Cols',  value: numCols.length,   icon: '🔢', color: 'color-emerald' },
              { label: 'Categorical',   value: catCols.length,   icon: '🏷️', color: 'color-amber'  },
            ].map(c => (
              <div className="stat-card" key={c.label}>
                <div className="stat-label">{c.icon} {c.label}</div>
                <div className={`stat-value ${c.color}`}>{c.value}</div>
              </div>
            ))}
          </div>

          <div className="flex gap-1 items-center" style={{ marginBottom: '1rem', flexWrap: 'wrap' }}>
            {['summary', 'missing', 'distributions', 'outliers'].map(t => (
              <button key={t} id={`eda-tab-${t}`} className={`btn ${tab===t?'btn-primary':'btn-outline'} text-xs`} onClick={() => setTab(t)}>
                {t.charAt(0).toUpperCase()+t.slice(1)}
              </button>
            ))}
          </div>

          {tab === 'summary' && (
            <div className="panel">
              <div className="panel-title"><span className="panel-icon">📋</span> Column Summary Statistics</div>
              <div className="table-wrap">
                <table>
                  <thead><tr><th>Column</th><th>Type</th><th>Unique</th><th>Nulls</th><th>Min</th><th>Max</th><th>Mean</th><th>Top Value</th></tr></thead>
                  <tbody>
                    {eda.summary.map(col => (
                      <tr key={col.column}>
                        <td><strong>{col.column}</strong></td>
                        <td><span className="badge badge-info">{col.dtype}</span></td>
                        <td>{fmt(col.unique)}</td>
                        <td><span className={`badge ${col.null_count > 0 ? 'badge-warn' : 'badge-valid'}`}>{col.null_count}</span></td>
                        <td className="color-text2">{fmt(col.min)}</td>
                        <td className="color-text2">{fmt(col.max)}</td>
                        <td className="fw-600">{fmt(col.mean)}</td>
                        <td className="color-text2 text-xs">{col.top_value ?? '—'}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>
          )}

          {tab === 'missing' && (
            <div className="panel">
              <div className="panel-title"><span className="panel-icon">🔍</span> Missing Value Analysis</div>
              <div style={{ width: '100%', height: 350, marginTop: '1rem' }}>
                <ResponsiveContainer>
                  <BarChart data={eda.missing_overview} layout="vertical" margin={{ top: 5, right: 20, left: 10, bottom: 5 }}>
                    <XAxis type="number" stroke="#64748b" tick={{ fill: '#94a3b8' }} />
                    <YAxis dataKey="column" type="category" width={100} stroke="#64748b" tick={{ fill: '#94a3b8' }} />
                    <Tooltip contentStyle={{ backgroundColor: '#1e293b', border: '1px solid #334155', borderRadius: '8px', color: '#f8fafc' }} />
                    <Bar dataKey="missing" fill="#f59e0b" radius={[0, 4, 4, 0]} />
                  </BarChart>
                </ResponsiveContainer>
              </div>
              <p className="text-xs color-text2 mt-4 text-center">Missing values across columns (\N in F1DB maps to NULL)</p>
            </div>
          )}

          {tab === 'distributions' && (
            <div className="grid-2">
              {Object.entries(eda.distributions).map(([col, data]) => (
                <div className="panel" key={col}>
                  <div className="panel-title"><span className="panel-icon">📊</span> {col}</div>
                  <div style={{ width: '100%', height: 300, marginTop: '1rem' }}>
                    <ResponsiveContainer>
                      <BarChart data={data} layout="vertical" margin={{ top: 5, right: 20, left: 0, bottom: 5 }}>
                        <XAxis type="number" stroke="#64748b" tick={{ fill: '#94a3b8' }} />
                        <YAxis dataKey="label" type="category" width={100} stroke="#64748b" tick={{ fill: '#94a3b8' }} />
                        <Tooltip contentStyle={{ backgroundColor: '#1e293b', border: '1px solid #334155', borderRadius: '8px', color: '#f8fafc' }} />
                        <Bar dataKey="count" fill="#22d3ee" radius={[0, 4, 4, 0]} />
                      </BarChart>
                    </ResponsiveContainer>
                  </div>
                </div>
              ))}
              {Object.entries(eda.histograms ?? {}).map(([col, bins]) => (
                <div className="panel" key={col}>
                  <div className="panel-title"><span className="panel-icon">📈</span> {col} Histogram</div>
                  <div style={{ width: '100%', height: 300, marginTop: '1rem' }}>
                    <ResponsiveContainer>
                      <BarChart data={bins} margin={{ top: 5, right: 20, left: 0, bottom: 5 }}>
                        <XAxis dataKey="bin" stroke="#64748b" tick={{ fill: '#94a3b8' }} />
                        <YAxis stroke="#64748b" tick={{ fill: '#94a3b8' }} />
                        <Tooltip contentStyle={{ backgroundColor: '#1e293b', border: '1px solid #334155', borderRadius: '8px', color: '#f8fafc' }} />
                        <Bar dataKey="count" fill="#a855f7" radius={[4, 4, 0, 0]} />
                      </BarChart>
                    </ResponsiveContainer>
                  </div>
                </div>
              ))}
            </div>
          )}

          {tab === 'outliers' && (
            <div className="panel">
              <div className="panel-title"><span className="panel-icon">⚠️</span> Outlier Detection (IQR Method)</div>
              
              <div style={{ width: '100%', height: 300, marginTop: '1rem', marginBottom: '2rem' }}>
                <ResponsiveContainer>
                  <BarChart data={eda.summary.filter(c => c.outliers != null)} margin={{ top: 5, right: 20, left: 0, bottom: 5 }}>
                    <XAxis dataKey="column" stroke="#64748b" tick={{ fill: '#94a3b8' }} />
                    <YAxis stroke="#64748b" tick={{ fill: '#94a3b8' }} />
                    <Tooltip contentStyle={{ backgroundColor: '#1e293b', border: '1px solid #334155', borderRadius: '8px', color: '#f8fafc' }} />
                    <Bar dataKey="outliers" fill="#ef4444" radius={[4, 4, 0, 0]} />
                  </BarChart>
                </ResponsiveContainer>
              </div>

              <div className="table-wrap">
                <table>
                  <thead><tr><th>Column</th><th>Outlier Count</th><th>Risk</th></tr></thead>
                  <tbody>
                    {eda.summary.filter(c => c.outliers != null).map(c => (
                      <tr key={c.column}>
                        <td><strong>{c.column}</strong></td>
                        <td><span className={`badge ${c.outliers>3?'badge-error':c.outliers>0?'badge-warn':'badge-valid'}`}>{c.outliers} outliers</span></td>
                        <td>{c.outliers>3?'🔴 High':c.outliers>0?'🟡 Medium':'🟢 None'}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>
          )}
        </>
      )}
    </div>
  )
}

// ══════════════════════════════════════════════════════
// WEEK 2 — ETL + Kafka (F1 Lap Events)
// ══════════════════════════════════════════════════════
function Week2() {
  const [runs, setRuns]     = useState([])
  const [events, setEvents] = useState([])
  const [msg, setMsg]       = useState(null)
  const [busy, setBusy]     = useState(false)

  const load = async () => {
    try {
      const [r, e] = await Promise.all([
        axios.get(`${API}/week2/etl/runs`),
        axios.get(`${API}/week2/kafka/events`),
      ])
      setRuns(r.data); setEvents(e.data)
    } catch {}
  }

  useEffect(() => { load(); const t = setInterval(load, 6000); return () => clearInterval(t) }, [])

  const triggerLoad = async (type) => {
    setBusy(true); setMsg(null)
    try {
      const r = await axios.post(`${API}/week2/etl/${type}-load`)
      setMsg({ ok: true, text: r.data.message }); load()
    } catch(e) { setMsg({ ok: false, text: e.response?.data?.detail || 'Error' }) }
    finally { setBusy(false) }
  }

  const posColor = (pos) => {
    if (pos === 1) return '#f59e0b'
    if (pos <= 3)  return '#10b981'
    return 'var(--text-2)'
  }

  return (
    <div className="fade-in">
      <div className="section-heading">
        <h2>🔄 Week 2 — Building Core Data Pipeline (ETL)</h2>
        <p>Extract → Transform → Load. Full vs Incremental load. Live F1 lap events via Kafka CDC.</p>
      </div>

      <div className="grid-3" style={{ marginBottom: '1.5rem' }}>
        {[
          { icon: '📥', title: 'Extract', desc: 'F1DB CSVs (drivers, races, results, standings) mounted in Docker', color: 'color-cyan' },
          { icon: '🔧', title: 'Transform', desc: 'Map integer IDs to string refs, handle \\N nulls, type coercion', color: 'color-indigo' },
          { icon: '📤', title: 'Load', desc: 'Upsert into PostgreSQL (ON CONFLICT DO UPDATE) — idempotent', color: 'color-emerald' },
        ].map(c => (
          <div className="panel" key={c.title}>
            <div className="panel-title" style={{ fontSize: '1rem' }}><span style={{ fontSize: '1.4rem' }}>{c.icon}</span> <span className={c.color}>{c.title}</span></div>
            <p className="text-sm color-text2">{c.desc}</p>
          </div>
        ))}
      </div>

      <div className="panel" style={{ marginBottom: '1.5rem' }}>
        <div className="panel-title"><span className="panel-icon">⚡</span> Trigger ETL Load</div>
        <div className="flex gap-2 items-center" style={{ flexWrap: 'wrap' }}>
          <button className="btn btn-primary" id="btn-full-load" onClick={() => triggerLoad('full')} disabled={busy}>
            {busy ? <span className="spinner"/> : '🔄'} Full Load (all results)
          </button>
          <button className="btn btn-outline" id="btn-incremental-load" onClick={() => triggerLoad('incremental')} disabled={busy}>
            ⚡ Incremental Load (new rows only)
          </button>
          <button className="btn btn-outline" onClick={load}>↺ Refresh</button>
        </div>
        {msg && <div className={`mt-2 text-sm ${msg.ok?'log-ok':'log-error'}`} style={{ marginTop: '0.85rem' }}>{msg.ok?'✅':'❌'} {msg.text}</div>}
      </div>

      <div className="grid-2">
        <div className="panel">
          <div className="panel-title"><span className="panel-icon">📜</span> ETL Run History</div>
          <div className="table-wrap">
            <table>
              <thead><tr><th>Run ID</th><th>Type</th><th>Rows Loaded</th><th>Status</th><th>Time</th></tr></thead>
              <tbody>
                {runs.length ? runs.map(r => (
                  <tr key={r.run_id}>
                    <td className="color-text2">#{r.run_id}</td>
                    <td><span className="badge badge-info">{r.load_type}</span></td>
                    <td className="fw-600">{fmt(r.rows_loaded)}</td>
                    <td><span className={`badge ${r.status==='SUCCESS'?'badge-valid':'badge-error'}`}>{r.status}</span></td>
                    <td className="text-xs color-text2">{dt(r.started_at)}</td>
                  </tr>
                )) : <tr><td colSpan={5} style={{ textAlign:'center', color:'var(--text-3)' }}>No runs yet. Run DAG in Airflow or trigger above.</td></tr>}
              </tbody>
            </table>
          </div>
        </div>

        <div className="panel">
          <div className="panel-title">
            <span className="panel-icon">🏎️</span> Live F1 Lap Events (Kafka CDC)
            <span className="live-badge" style={{ marginLeft: 'auto' }}><span className="live-dot"/>LIVE</span>
          </div>
          <div className="table-wrap">
            <table>
              <thead><tr><th>Driver</th><th>Lap</th><th>Pos</th><th>Lap Time</th><th>Status</th><th>At</th></tr></thead>
              <tbody>
                {events.length ? events.map(e => (
                  <tr key={e.event_id}>
                    <td><strong>{e.driver_code || e.driver_id || '—'}</strong></td>
                    <td className="fw-600">{e.lap}</td>
                    <td style={{ color: posColor(e.position), fontWeight: 700 }}>P{e.position}</td>
                    <td className="color-text2 text-xs" style={{ fontFamily: 'monospace' }}>{e.lap_time || '—'}</td>
                    <td><span className={`badge ${e.validation_status==='VALID'?'badge-valid':'badge-error'}`}>{e.validation_status}</span></td>
                    <td className="text-xs color-text2">{dt(e.ingested_at)}</td>
                  </tr>
                )) : <tr><td colSpan={6} style={{ textAlign:'center', color:'var(--text-3)' }}>Waiting for Kafka lap events… (start Docker to stream)</td></tr>}
              </tbody>
            </table>
          </div>
        </div>
      </div>

      <div className="panel mt-3">
        <div className="panel-title"><span className="panel-icon">📡</span> CDC Architecture — F1 Lap Streaming</div>
        <div className="grid-3">
          {[
            { step:'1', title:'CSV Source', desc:'lap_times.csv is read by the Kafka Producer. Latest race laps are emitted one per second on topic f1-lap-events.' },
            { step:'2', title:'Consumer Validation', desc:'Consumer validates: no NULL driver_id, no negative milliseconds. Outliers (>5 min) also rejected.' },
            { step:'3', title:'Target / DLQ', desc:'Valid → streaming_lap_events table. Invalid → pipeline_errors (DLQ) with full raw payload preserved.' },
          ].map(s => (
            <div key={s.step} style={{ padding:'1rem', background:'rgba(99,102,241,0.06)', borderRadius:'10px', border:'1px solid var(--border)' }}>
              <div style={{ fontSize:'1.5rem', fontWeight:800, color:'var(--indigo)', marginBottom:'0.35rem' }}>Step {s.step}</div>
              <div className="fw-700 text-sm" style={{ marginBottom:'0.35rem' }}>{s.title}</div>
              <div className="text-xs color-text2">{s.desc}</div>
            </div>
          ))}
        </div>
      </div>
    </div>
  )
}

// ══════════════════════════════════════════════════════
// WEEK 3 — F1 Star Schema
// ══════════════════════════════════════════════════════
function Week3() {
  const [activeTab,  setActiveTab]  = useState('drivers')
  const [data,       setData]       = useState([])
  const [factData,   setFact]       = useState([])
  const [query,      setQuery]      = useState(null)
  const [queryRes,   setQRes]       = useState(null)
  const [yearFilter, setYearFilter] = useState('')
  const [busy,       setBusy]       = useState(false)

  const dimTabs = [
    { id: 'drivers',      label: '🏎️ Drivers' },
    { id: 'circuits',     label: '🗺️ Circuits' },
    { id: 'constructors', label: '🔧 Constructors' },
    { id: 'races',        label: '🏁 Races' },
  ]

  const loadDim = async (t) => {
    setActiveTab(t); setData([])
    try { const r = await axios.get(`${API}/week3/dimensions/${t}?limit=25`); setData(r.data) } catch {}
  }

  const loadFact = async () => {
    try { const r = await axios.get(`${API}/week3/fact/results?limit=25`); setFact(r.data) } catch {}
  }

  const runQuery = async (q) => {
    setBusy(true); setQuery(q); setQRes(null)
    const url = yearFilter ? `${API}/week3/olap/query/${q}?year=${yearFilter}` : `${API}/week3/olap/query/${q}`
    try { const r = await axios.get(url); setQRes(r.data.results) }
    catch {} finally { setBusy(false) }
  }

  useEffect(() => { loadDim('drivers'); loadFact() }, [])

  const queries = [
    { id: 'points_by_driver',    label: '🏆 Points by Driver'    },
    { id: 'wins_by_constructor', label: '🔧 Wins by Constructor'  },
    { id: 'races_by_season',     label: '📅 Races by Season'      },
    { id: 'fastest_drivers',     label: '⚡ Fastest Drivers'      },
    { id: 'results_by_circuit',  label: '🗺️ Results by Circuit'   },
  ]

  const positionBadge = (p) => {
    if (p === 1) return <span className="badge" style={{ background:'rgba(245,158,11,0.2)', color:'#f59e0b', border:'1px solid rgba(245,158,11,0.3)' }}>🥇 P1</span>
    if (p === 2) return <span className="badge badge-info">🥈 P2</span>
    if (p === 3) return <span className="badge badge-valid">🥉 P3</span>
    return <span className="badge badge-warn">P{p}</span>
  }

  return (
    <div className="fade-in">
      <div className="section-heading">
        <h2>🏗️ Week 3 — Data Architecture & Schema Design</h2>
        <p>OLTP vs OLAP · Dimensional Modeling · F1 Star Schema · Data Cubes</p>
      </div>

      {/* OLTP vs OLAP */}
      <div className="compare-grid" style={{ marginBottom: '1.5rem' }}>
        <div className="compare-side compare-oltp">
          <h3 style={{ color:'var(--amber)' }}>⚡ OLTP — Normalized Source</h3>
          <p className="text-xs color-text2" style={{ marginBottom:'0.75rem' }}>Original f1db CSVs — row-by-row, fully normalized</p>
          <ul>
            <li>Primary key: driverId, raceId, resultId (integers)</li>
            <li>14 separate CSV tables — fully normalized</li>
            <li>Optimized for row-level accuracy</li>
            <li>No aggregation or joins pre-computed</li>
          </ul>
        </div>
        <div className="compare-side compare-olap">
          <h3 style={{ color:'var(--indigo)' }}>📊 OLAP — F1 Star Schema</h3>
          <p className="text-xs color-text2" style={{ marginBottom:'0.75rem' }}>PostgreSQL warehouse — denormalized for analysis</p>
          <ul>
            <li>Fact: fact_race_results (positions, points, laps)</li>
            <li>Dims: dim_driver, dim_circuit, dim_constructor, dim_race</li>
            <li>String IDs (e.g. "hamilton", "mercedes") for readability</li>
            <li>Optimized for GROUP BY / aggregation queries</li>
          </ul>
        </div>
      </div>

      {/* Star Schema Diagram */}
      <div className="panel" style={{ marginBottom: '1.5rem' }}>
        <div className="panel-title"><span className="panel-icon">⭐</span> F1 Star Schema</div>
        <div className="schema-diagram">
          <div className="schema-row">
            <div className="schema-box dim">
              <h4 style={{ color:'var(--cyan)' }}>dim_driver</h4>
              <ul><li>driver_id PK</li><li>driver_name</li><li>code, number</li><li>nationality, dob</li></ul>
            </div>
            <div className="schema-box dim">
              <h4 style={{ color:'var(--cyan)' }}>dim_race</h4>
              <ul><li>race_id PK</li><li>year, round</li><li>name, race_date</li><li>circuit_id FK</li></ul>
            </div>
          </div>
          <div className="schema-row">
            <span className="schema-arrow">↓</span>
            <div className="schema-box fact">
              <h4 style={{ color:'var(--indigo)' }}>⭐ fact_race_results</h4>
              <ul><li>result_id PK</li><li>driver_id FK</li><li>constructor_id FK</li><li>race_id FK</li><li>grid, position, points, laps</li></ul>
            </div>
            <span className="schema-arrow">↑</span>
          </div>
          <div className="schema-row">
            <div className="schema-box dim">
              <h4 style={{ color:'var(--cyan)' }}>dim_constructor</h4>
              <ul><li>constructor_id PK</li><li>name</li><li>nationality</li></ul>
            </div>
            <div className="schema-box dim">
              <h4 style={{ color:'var(--cyan)' }}>dim_circuit</h4>
              <ul><li>circuit_id PK</li><li>name, country</li><li>location</li><li>lat, lng</li></ul>
            </div>
          </div>
        </div>
      </div>

      {/* Dimension Tables */}
      <div className="panel" style={{ marginBottom: '1.5rem' }}>
        <div className="flex justify-between items-center" style={{ marginBottom:'1rem', flexWrap:'wrap', gap:'0.5rem' }}>
          <div className="panel-title" style={{ margin:0 }}><span className="panel-icon">🗂️</span> Dimension Tables</div>
          <div className="flex gap-1" style={{ flexWrap:'wrap' }}>
            {dimTabs.map(t => (
              <button key={t.id} id={`dim-tab-${t.id}`}
                className={`btn text-xs ${activeTab===t.id?'btn-primary':'btn-outline'}`}
                onClick={() => loadDim(t.id)}>{t.label}</button>
            ))}
          </div>
        </div>
        <div className="table-wrap">
          <table>
            <thead><tr>{data[0] ? Object.keys(data[0]).map(k => <th key={k}>{k}</th>) : <th>Loading…</th>}</tr></thead>
            <tbody>
              {data.map((row, i) => (
                <tr key={i}>{Object.entries(row).map(([k, v], j) => (
                  <td key={j}>
                    {k === 'nationality' && v ? `${flag(v)} ${v}` :
                     k === 'country'     && v ? `${flag(v)} ${v}` :
                     v == null ? <span className="badge badge-warn">NULL</span> : String(v)}
                  </td>
                ))}</tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>

      {/* Fact Table */}
      <div className="panel" style={{ marginBottom: '1.5rem' }}>
        <div className="panel-title"><span className="panel-icon">⭐</span> Fact Table — fact_race_results (with Joins)</div>
        <div className="table-wrap">
          <table>
            <thead><tr><th>Race</th><th>Year</th><th>Circuit</th><th>Driver</th><th>Constructor</th><th>Grid</th><th>Finish</th><th>Points</th><th>Laps</th><th>Fastest Lap</th></tr></thead>
            <tbody>
              {factData.map((r, i) => (
                <tr key={i}>
                  <td className="text-xs">{r.race_name}</td>
                  <td className="badge badge-info">{r.year}</td>
                  <td className="text-xs color-text2">{flag(r.country)} {r.country}</td>
                  <td><strong>{r.driver_code || r.driver_name}</strong></td>
                  <td className="color-text2 text-xs">{r.constructor_name}</td>
                  <td className="color-text2">{r.grid_position ?? '—'}</td>
                  <td>{r.finish_position != null ? positionBadge(r.finish_position) : <span className="badge badge-error">{r.position_text}</span>}</td>
                  <td className="fw-600 color-amber">{r.points}</td>
                  <td className="color-text2">{r.laps}</td>
                  <td className="text-xs" style={{ fontFamily:'monospace', color:'var(--cyan)' }}>{r.fastest_lap_time || '—'}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>

      {/* OLAP Queries */}
      <div className="panel">
        <div className="panel-title"><span className="panel-icon">🔍</span> F1 Analytical Queries (OLAP Data Cube)</div>
        <div className="flex gap-2 items-center" style={{ marginBottom:'1rem', flexWrap:'wrap' }}>
          <input
            id="year-filter-input"
            type="number" min="2009" max="2024" placeholder="Year filter (optional)"
            value={yearFilter} onChange={e => setYearFilter(e.target.value)}
            style={{ padding:'0.5rem 0.85rem', borderRadius:'8px', border:'1px solid var(--border)',
                     background:'var(--bg-2)', color:'var(--text)', fontSize:'0.82rem', width:'180px' }}
          />
        </div>
        <div className="flex gap-1" style={{ marginBottom:'1rem', flexWrap:'wrap' }}>
          {queries.map(q => (
            <button key={q.id} id={`olap-${q.id}`}
              className={`btn text-xs ${query===q.id?'btn-primary':'btn-outline'}`}
              onClick={() => runQuery(q.id)} disabled={busy}>
              {busy && query===q.id ? <span className="spinner"/> : null} {q.label}
            </button>
          ))}
        </div>
        {queryRes && (
          <div className="table-wrap">
            <table>
              <thead><tr>{Object.keys(queryRes[0]).map(k => <th key={k}>{k}</th>)}</tr></thead>
              <tbody>
                {queryRes.map((row, i) => (
                  <tr key={i}>{Object.entries(row).map(([k, v], j) => (
                    <td key={j}>
                      {k === 'nationality' && v ? `${flag(v)} ${v}` :
                       typeof v === 'number' ? <strong>{fmt(v)}</strong> : String(v ?? '—')}
                    </td>
                  ))}</tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </div>
    </div>
  )
}

// ══════════════════════════════════════════════════════
// WEEK 4 — Batch Pipeline (results.csv → Staging)
// ══════════════════════════════════════════════════════
function Week4() {
  const [steps,   setSteps]   = useState(null)
  const [staging, setStaging] = useState([])
  const [busy,    setBusy]    = useState(false)
  const [forceFail, setForceFail] = useState(false)
  const [airflowStatus, setAirflowStatus] = useState(null)
  const [airflowBusy, setAirflowBusy] = useState(false)

  const runPipeline = async () => {
    setBusy(true); setSteps(null)
    try { const r = await axios.post(`${API}/week4/pipeline/run?force_fail=${forceFail}`); setSteps(r.data.steps) }
    catch(e) { 
        alert(e.response?.data?.detail || 'Pipeline failed');
        if (e.response?.data?.detail) {
            setSteps([{step: "Error", status: "FAILED", detail: e.response.data.detail}]);
        }
    }
    finally { setBusy(false); loadStaging() }
  }

  const triggerAirflow = async () => {
    setAirflowBusy(true)
    try {
      const r = await axios.post(`${API}/week4/airflow/trigger`)
      setAirflowStatus(r.data)
    } catch(e) {
      setAirflowStatus({ status: 'ERROR', message: 'Could not reach Airflow' })
    } finally {
      setAirflowBusy(false)
    }
  }

  const loadStaging = async () => {
    try { const r = await axios.get(`${API}/week4/staging`); setStaging(r.data) } catch {}
  }

  useEffect(() => { loadStaging() }, [])

  return (
    <div className="fade-in">
      <div className="section-heading">
        <h2>🐍 Week 4 & 5 — Resilient API Batch Pipeline</h2>
        <p>Extract live F1 data from Ergast API. Transform & Validate. Load into PostgreSQL staging table via an atomic & idempotent UPSERT.</p>
      </div>

      <div className="panel" style={{ marginBottom: '1.5rem' }}>
        <div className="panel-title"><span className="panel-icon">🔄</span> Pipeline Architecture</div>
        <div className="flex items-center gap-2" style={{ flexWrap:'wrap', justifyContent:'center', gap:'0.5rem' }}>
          {['Ergast API\n(HTTP GET)', '→', 'requests.get()\n(Python)', '→', 'Transform &\nValidate', '→', 'PostgreSQL\n(Atomic UPSERT)'].map((s, i) => (
            s === '→' ? <div key={i} style={{ fontSize:'1.5rem', color:'var(--text-3)' }}>→</div> :
            <div key={i} style={{ background:'rgba(99,102,241,0.1)', border:'1px solid rgba(99,102,241,0.3)', borderRadius:'10px', padding:'0.75rem 1.25rem', textAlign:'center', whiteSpace:'pre', fontSize:'0.8rem', fontWeight:600 }}>{s}</div>
          ))}
        </div>
        <div className="flex items-center mt-2" style={{ justifyContent:'center', marginTop:'1.25rem', gap:'1rem' }}>
          <label style={{ display: 'flex', alignItems: 'center', gap: '0.5rem', fontSize: '0.9rem', cursor: 'pointer' }}>
            <input type="checkbox" checked={forceFail} onChange={(e) => setForceFail(e.target.checked)} />
            💥 Simulate Failure (Test Rollback)
          </label>
          <button id="btn-run-pipeline" className="btn btn-primary" onClick={runPipeline} disabled={busy}>
            {busy ? <span className="spinner"/> : '▶'} Run F1 Batch Pipeline
          </button>
          <button id="btn-trigger-airflow" className="btn btn-outline" onClick={triggerAirflow} disabled={airflowBusy} style={{ borderColor:'#f59e0b', color:'#f59e0b' }}>
            {airflowBusy ? <span className="spinner"/> : '🌬️'} Trigger Airflow DAG
          </button>
        </div>
      </div>

      {airflowStatus && (
        <div className="panel" style={{ marginBottom:'1.5rem', borderColor: airflowStatus.status === 'TRIGGERED' ? 'rgba(16,185,129,0.5)' : 'rgba(239,68,68,0.5)', background: airflowStatus.status === 'TRIGGERED' ? 'linear-gradient(145deg, rgba(16,185,129,0.05), var(--panel))' : 'linear-gradient(145deg, rgba(239,68,68,0.05), var(--panel))' }}>
          <div className="panel-title"><span className="panel-icon">🌬️</span> Airflow DAG — f1_etl_pipeline</div>
          <div className="flex items-center" style={{ gap:'1rem', flexWrap:'wrap' }}>
            <span className={`badge ${airflowStatus.status === 'TRIGGERED' ? 'badge-valid' : 'badge-error'}`}>{airflowStatus.status}</span>
            <span className="text-xs color-text2">{airflowStatus.message}</span>
            {airflowStatus.run_id && <span className="badge badge-info">Run: {airflowStatus.run_id}</span>}
          </div>
          {airflowStatus.airflow_url && (
            <div className="mt-2">
              <a href={airflowStatus.airflow_url} target="_blank" rel="noreferrer"
                style={{ display:'inline-flex', alignItems:'center', gap:'0.4rem', marginTop:'0.75rem', padding:'0.5rem 1rem', background:'rgba(245,158,11,0.15)', border:'1px solid rgba(245,158,11,0.4)', borderRadius:'8px', color:'#f59e0b', fontWeight:600, fontSize:'0.85rem', textDecoration:'none' }}>
                🔗 View Pipeline Graph in Airflow UI →
              </a>
            </div>
          )}
        </div>
      )}

      {steps && (
        <div className="panel" style={{ marginBottom:'1.5rem' }}>
          <div className="panel-title"><span className="panel-icon">📋</span> Pipeline Execution Log</div>
          <div className="log-output">
            {steps.map((s, i) => (
              <div key={i} style={{ marginBottom:'0.75rem' }}>
                <span className={s.status==='SUCCESS'?'log-ok':'log-error'}>[{s.status}] {s.step}</span>
                <br/><span className="log-info">  → {s.detail}</span>
                {s.sample && <><br/><span className="color-text2">  Sample: {JSON.stringify(s.sample[0]).substring(0,120)}…</span></>}
              </div>
            ))}
          </div>
        </div>
      )}

      <div className="panel">
        <div className="flex justify-between items-center" style={{ marginBottom:'1rem', flexWrap:'wrap', gap:'0.5rem' }}>
          <div className="panel-title" style={{ margin:0 }}><span className="panel-icon">📥</span> Staging Table (etl_staging)</div>
          <button className="btn btn-outline text-xs" onClick={loadStaging}>↺ Refresh</button>
        </div>
        <div className="table-wrap">
          <table>
            <thead><tr><th>ID</th><th>Race Data Payload</th><th>Status</th><th>Ingested</th></tr></thead>
            <tbody>
              {staging.length ? staging.map(r => (
                <tr key={r.staging_id}>
                  <td className="color-text2">#{r.staging_id}</td>
                  <td>
                    {(() => {
                      const p = typeof r.raw_payload === 'string' ? JSON.parse(r.raw_payload) : (r.raw_payload || {});
                      return (
                        <div className="flex" style={{ gap: '0.5rem', flexWrap: 'wrap' }}>
                          <span className="badge badge-info">Race: {p.race_id}</span>
                          <span className="badge badge-info">Driver: {p.driver_id}</span>
                          <span className={`badge ${p.position ? 'badge-valid' : 'badge-error'}`}>Pos: {p.position ?? 'DNF'}</span>
                          <span className="badge badge-valid">Pts: {p.points}</span>
                          <span className="badge badge-warn">Laps: {p.laps}</span>
                        </div>
                      )
                    })()}
                  </td>
                  <td><span className={`badge ${r.validation_status==='VALID'?'badge-valid':r.validation_status==='PENDING'?'badge-warn':'badge-error'}`}>{r.validation_status}</span></td>
                  <td className="text-xs color-text2">{dt(r.ingested_at)}</td>
                </tr>
              )) : <tr><td colSpan={4} style={{ textAlign:'center', color:'var(--text-3)' }}>No records. Run the pipeline above.</td></tr>}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  )
}

// ══════════════════════════════════════════════════════
// WEEK 5 — Production Pipelines
// ══════════════════════════════════════════════════════
function Week5() {
  const [idem, setIdem]   = useState(null)
  const [atom, setAtom]   = useState(null)
  const [staging, setStg] = useState([])
  const [errors, setErrs] = useState([])
  const [finalData, setFinal] = useState([])
  const [busy, setBusy]   = useState('')

  const load = async () => {
    try {
      const [s, e, f] = await Promise.all([
        axios.get(`${API}/week5/staging`),
        axios.get(`${API}/week5/errors`),
        axios.get(`${API}/week5/final`),
      ])
      setStg(s.data); setErrs(e.data); setFinal(f.data)
    } catch {}
  }

  useEffect(() => { load(); const t = setInterval(load, 8000); return () => clearInterval(t) }, [])

  const runIdem = async () => {
    setBusy('idem')
    try { const r = await axios.post(`${API}/week5/idempotency/run`); setIdem(r.data) }
    catch(e) { setIdem({ error: e.response?.data?.detail }) }
    finally { setBusy('') }
  }

  const runAtom = async (fail) => {
    setBusy('atom')
    try { const r = await axios.post(`${API}/week5/atomicity/demo?force_fail=${fail}`); setAtom(r.data) }
    catch {} finally { setBusy(''); load() }
  }

  return (
    <div className="fade-in">
      <div className="section-heading">
        <h2>🛡️ Week 5 — Resilient & Production-Ready Pipelines</h2>
        <p>Staging & Validation · Idempotency · Atomicity · Error Handling & Dead Letter Queue</p>
      </div>
      
      <p style={{marginBottom: '1.5rem', color: 'var(--text-2)'}}>
        The concepts of Idempotency (UPSERT) and Atomicity (Rollback) have been seamlessly integrated into the Week 4 API Batch Pipeline above.
      </p>

      <div className="panel" style={{ marginBottom:'1.5rem' }}>
        <div className="flex justify-between items-center" style={{ marginBottom:'1rem', flexWrap:'wrap', gap:'0.5rem' }}>
          <div className="panel-title" style={{ margin:0 }}><span className="panel-icon">✅</span> Staging & Validation Records</div>
          <button className="btn btn-outline text-xs" onClick={load}>↺ Refresh</button>
        </div>
        <div className="table-wrap">
          <table>
            <thead><tr><th>ID</th><th>Race Data Payload</th><th>Status</th><th>Error</th><th>Ingested</th></tr></thead>
            <tbody>
              {staging.length ? staging.map(r => (
                <tr key={r.staging_id}>
                  <td className="color-text2">#{r.staging_id}</td>
                  <td>
                    {(() => {
                      const p = typeof r.raw_payload === 'string' ? JSON.parse(r.raw_payload) : (r.raw_payload || {});
                      return (
                        <div className="flex" style={{ gap: '0.5rem', flexWrap: 'wrap' }}>
                          <span className="badge badge-info">Race: {p.race_id}</span>
                          <span className="badge badge-info">Driver: {p.driver_id}</span>
                          <span className={`badge ${p.position ? 'badge-valid' : 'badge-error'}`}>Pos: {p.position ?? 'DNF'}</span>
                          <span className="badge badge-valid">Pts: {p.points}</span>
                          <span className="badge badge-warn">Laps: {p.laps}</span>
                        </div>
                      )
                    })()}
                  </td>
                  <td><span className={`badge ${r.validation_status==='VALID'?'badge-valid':r.validation_status==='PENDING'?'badge-warn':'badge-error'}`}>{r.validation_status}</span></td>
                  <td className="text-xs color-text2">{r.validation_error ?? '—'}</td>
                  <td className="text-xs color-text2">{dt(r.ingested_at)}</td>
                </tr>
              )) : <tr><td colSpan={5} style={{ textAlign:'center', color:'var(--text-3)' }}>No records yet.</td></tr>}
            </tbody>
          </table>
        </div>
      </div>

      <div className="panel" style={{ background:'linear-gradient(145deg, rgba(244,63,94,0.05), var(--panel))', borderColor:'rgba(244,63,94,0.2)' }}>
        <div className="panel-title"><span className="panel-icon">☠️</span> Dead Letter Queue — F1 Pipeline Errors</div>
        <div className="table-wrap">
          <table>
            <thead><tr><th>Source</th><th>Error Type</th><th>Message</th><th>Failed At</th></tr></thead>
            <tbody>
              {errors.length ? errors.map(r => (
                <tr key={r.id}>
                  <td className="fw-600 text-xs">{r.source_id ?? '—'}</td>
                  <td><span className="badge badge-error">{r.error_type}</span></td>
                  <td className="text-xs color-text2">{r.error_message}</td>
                  <td className="text-xs color-text2">{dt(r.failed_at)}</td>
                </tr>
              )) : <tr><td colSpan={4} style={{ textAlign:'center', color:'var(--text-3)' }}>No errors. All lap events passed validation.</td></tr>}
            </tbody>
          </table>
        </div>
        <p className="text-xs color-text2 mt-2">
          ☝ Invalid Kafka lap events (NULL driver_id, negative ms, outlier lap times) are routed here by the Consumer.
        </p>
      </div>

      <div className="panel" style={{ background:'linear-gradient(145deg, rgba(16,185,129,0.05), var(--panel))', borderColor:'rgba(16,185,129,0.2)' }}>
        <div className="panel-title"><span className="panel-icon">🏆</span> Final Output — Data Warehouse (fact_race_results)</div>
        <div className="table-wrap">
          <table>
            <thead><tr><th>Result ID</th><th>Race ID</th><th>Driver</th><th>Constructor</th><th>Grid</th><th>Pos</th><th>Points</th><th>Laps</th><th>Updated At</th></tr></thead>
            <tbody>
              {finalData.length ? finalData.map(r => (
                <tr key={r.result_id}>
                  <td className="color-text2">#{r.result_id}</td>
                  <td className="fw-600">Race {r.race_id}</td>
                  <td><span className="badge badge-info">{r.driver_id}</span></td>
                  <td><span className="badge badge-info">{r.constructor_id}</span></td>
                  <td className="text-xs">{r.grid_position}</td>
                  <td><span className={`badge ${r.finish_position === 1 ? 'badge-valid' : 'badge-warn'}`}>{r.position_text}</span></td>
                  <td className="fw-600 color-valid">{r.points}</td>
                  <td className="text-xs">{r.laps}</td>
                  <td className="text-xs color-text2">{dt(r.updated_at)}</td>
                </tr>
              )) : <tr><td colSpan={9} style={{ textAlign:'center', color:'var(--text-3)' }}>No finalized data available.</td></tr>}
            </tbody>
          </table>
        </div>
        <p className="text-xs color-text2 mt-2">
          ☝ This is the ultimate output of the Data Engineering pipelines. 26K+ historical F1 race results securely loaded via idempotent UPSERTs.
        </p>
      </div>

      <div className="panel mt-3">
        <div className="panel-title"><span className="panel-icon">🌬️</span> Airflow Orchestration — f1_etl_pipeline DAG</div>
        <div className="grid-3">
          {[
            { icon:'▶',  task:'start_pipeline',            desc:'DAG entry point' },
            { icon:'🏎️', task:'load_drivers',               desc:'drivers.csv → dim_driver (ON CONFLICT UPSERT)' },
            { icon:'🔧', task:'load_constructors',          desc:'constructors.csv → dim_constructor' },
            { icon:'🗺️', task:'load_circuits',              desc:'circuits.csv → dim_circuit (with lat/lng)' },
            { icon:'🏁', task:'load_races',                 desc:'races.csv → dim_race (FK to circuit)' },
            { icon:'🏆', task:'load_results',               desc:'results.csv → fact_race_results (26K rows)' },
            { icon:'📊', task:'load_driver_standings',      desc:'driver_standings.csv → fact_driver_standings' },
            { icon:'🏗️', task:'load_constructor_standings', desc:'constructor_standings.csv → fact_constructor_standings' },
            { icon:'✅', task:'validate_data_quality',      desc:'SQL check: NULL drivers, NULL circuits, result count' },
            { icon:'📝', task:'log_etl_run',                desc:'Write total rows + SUCCESS to etl_run_log' },
          ].map(t => (
            <div key={t.task} style={{ padding:'0.85rem', background:'rgba(99,102,241,0.06)', borderRadius:'10px', border:'1px solid var(--border)' }}>
              <div style={{ fontSize:'1.1rem', marginBottom:'0.3rem' }}>{t.icon}</div>
              <div className="fw-700 text-xs color-indigo">{t.task}</div>
              <div className="text-xs color-text2 mt-1">{t.desc}</div>
            </div>
          ))}
        </div>
        <p className="text-xs color-text2 mt-2">
          🔗 Access Airflow UI at <strong style={{ color:'var(--cyan)' }}>http://localhost:8080</strong> (admin / admin) → Trigger <strong>f1_etl_pipeline</strong> DAG.
        </p>
      </div>
    </div>
  )
}

// ══════════════════════════════════════════════════════
// HERO STATS
// ══════════════════════════════════════════════════════
function HeroStats() {
  const [stats, setStats] = useState(null)
  useEffect(() => {
    axios.get(`${API}/f1/stats`).then(r => setStats(r.data)).catch(() => {})
  }, [])
  if (!stats) return null
  return (
    <div className="hero-stats">
      {[
        { label: 'Drivers',  value: stats.drivers,  icon: '🏎️', color: 'color-indigo'  },
        { label: 'Circuits', value: stats.circuits, icon: '🗺️', color: 'color-cyan'    },
        { label: 'Races',    value: stats.races,    icon: '🏁', color: 'color-emerald' },
        { label: 'Seasons',  value: stats.seasons,  icon: '📅', color: 'color-amber'   },
        { label: 'Results',  value: fmt(stats.results), icon: '📊', color: 'color-purple' },
      ].map(s => (
        <div key={s.label} className="hero-stat">
          <span className="hero-stat-icon">{s.icon}</span>
          <span className={`hero-stat-value ${s.color}`}>{s.value}</span>
          <span className="hero-stat-label">{s.label}</span>
        </div>
      ))}
    </div>
  )
}

// ══════════════════════════════════════════════════════
// ROOT APP
// ══════════════════════════════════════════════════════
const TABS = [
  { id:'w1', week:'W1', label:'EDA & Data Collection', icon:'📊', Component: Week1 },
  { id:'w2', week:'W2', label:'ETL Pipeline',          icon:'🔄', Component: Week2 },
  { id:'w3', week:'W3', label:'Schema Design',         icon:'🏗️', Component: Week3 },
  { id:'w4', week:'W4', label:'Batch Pipeline',        icon:'🐍', Component: Week4 },
  { id:'w5', week:'W5', label:'Production Pipelines',  icon:'🛡️', Component: Week5 },
]

export default function App() {
  const [activeTab, setActiveTab] = useState('w1')
  const Active = TABS.find(t => t.id === activeTab)?.Component

  return (
    <div className="app">
      <header>
        <div className="header-brand">
          <div className="header-logo">F1</div>
          <div>
            <div className="header-title">F1 Data Engineering Lab</div>
            <div className="header-sub">22MDCEL10 · CIT Coimbatore · F1DB Pipeline</div>
          </div>
        </div>
        <HeroStats />
        <div className="live-badge">
          <span className="live-dot"/>Kafka Live
        </div>
      </header>

      <nav className="tab-nav">
        {TABS.map(t => (
          <button key={t.id} id={`tab-${t.id}`}
            className={`tab-btn ${activeTab===t.id?'active':''}`}
            onClick={() => setActiveTab(t.id)}>
            <span>{t.icon}</span>
            <span className="week-badge">{t.week}</span>
            <span>{t.label}</span>
          </button>
        ))}
      </nav>

      <main className="main">
        {Active && <Active />}
      </main>
    </div>
  )
}
