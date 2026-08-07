import { useEffect, useMemo, useState } from "react";
import type { JsonRecord, ResearchReadModelName, ResearchReadModelResult } from "../../types";

const MODELS: ResearchReadModelName[] = [
  "research_questions_and_investigations_v1",
  "research_evidence_coverage_v1",
  "research_checkpoints_v1",
  "research_roadmap_v1",
];

type TabId = "triage" | "investigations" | "evidence" | "checkpoints" | "roadmap";
type InvestigationSort = "investigation_id" | "generated_at" | "confidence";

export function ResearchControlCenterPage(): JSX.Element {
  const [activeTab, setActiveTab] = useState<TabId>("triage");
  const [models, setModels] = useState<Partial<Record<ResearchReadModelName, ResearchReadModelResult>>>({});
  const [loadError, setLoadError] = useState<string | null>(null);

  useEffect(() => {
    let cancelled = false;
    if (!window.researchControlCenter) {
      setLoadError("Research Control Center preload bridge is unavailable.");
      return () => {
        cancelled = true;
      };
    }
    Promise.all(MODELS.map(async (name) => [name, await window.researchControlCenter!.getReadModel(name)] as const))
      .then((entries) => {
        if (!cancelled) {
          setModels(Object.fromEntries(entries) as Partial<Record<ResearchReadModelName, ResearchReadModelResult>>);
        }
      })
      .catch((error) => {
        if (!cancelled) {
          setLoadError(error instanceof Error ? error.message : String(error));
        }
      });
    return () => {
      cancelled = true;
    };
  }, []);

  const investigations = useMemo(() => modelArray(models.research_questions_and_investigations_v1, "investigations"), [models]);
  const coverage = useMemo(() => modelArray(models.research_evidence_coverage_v1, "coverage_items"), [models]);
  const checkpoints = useMemo(() => modelArray(models.research_checkpoints_v1, "checkpoint_history"), [models]);
  const roadmap = models.research_roadmap_v1?.payload ?? null;

  const staleOrBroken = coverage.filter((item) => ["STALE", "MISSING", "INVALID"].includes(String(item.status)));
  const contradictions = investigations.flatMap((item) => asStringArray(item.contradictory_evidence));

  return (
    <main className="page-shell research-control-center-shell">
      <section className="hero-panel compact">
        <div>
          <p className="eyebrow">Read-only research workspace</p>
          <h1>Research Control Center</h1>
          <p className="hero-copy">Read-only research triage over bounded read-models. Evidence Coverage reads prepared research artifacts; investigations, checkpoints, and roadmap remain fixture-backed. No runtime state, broker state, or operational controls are available in this window.</p>
        </div>
      </section>

      {loadError ? <StatusBanner tone="danger" title="Read-model bridge unavailable" detail={loadError} /> : null}
      <nav className="tab-strip" aria-label="Research Control Center sections">
        {[
          ["triage", "Triage"],
          ["investigations", "Investigation Library"],
          ["evidence", "Evidence Coverage"],
          ["checkpoints", "Checkpoint History"],
          ["roadmap", "Roadmap"],
        ].map(([id, label]) => (
          <button key={id} className={`tab-button ${activeTab === id ? "active" : ""}`} onClick={() => setActiveTab(id as TabId)}>{label}</button>
        ))}
      </nav>

      {activeTab === "triage" ? (
        <div className="dashboard-grid two-column">
          <Panel title="What Changed" subtitle="Producer-designated fixture updates only">
            <SimpleList items={investigations.slice(0, 2).map((item) => `${item.investigation_id}: ${item.title}`)} empty="No investigation fixtures loaded." />
            <SimpleList items={checkpoints.slice(-2).map((item) => `${item.cohort_id} checkpoint ${item.checkpoint_trade_count}`)} empty="No checkpoint fixture rows loaded." />
          </Panel>
          <Panel title="What Needs Attention" subtitle="Visible stale, missing, invalid, and contradictory evidence">
            <SimpleList items={staleOrBroken.map((item) => `${item.label}: ${item.status}`)} empty="No fixture coverage issues." />
            <SimpleList items={contradictions.slice(0, 5)} empty="No contradictory evidence in fixture." />
          </Panel>
          <Panel title="What Remains Credible" subtitle="Confidence and conclusion status pass through unchanged">
            <KeyValueTable rows={investigations.map((item) => [String(item.investigation_id), `${item.confidence} / ${item.conclusion_status}`])} />
          </Panel>
          <Panel title="Roadmap" subtitle="Structured roadmap producer is deferred">
            <StatusPill status={String(roadmap?.model_status ?? "NOT_READY")} />
            <p className="muted-copy">{String(roadmap?.reason ?? "GOVERNED_ROADMAP_PRODUCER_NOT_AVAILABLE")}</p>
          </Panel>
        </div>
      ) : null}

      {activeTab === "investigations" ? <InvestigationLibrary investigations={investigations} model={models.research_questions_and_investigations_v1} /> : null}
      {activeTab === "evidence" ? <EvidenceCoverage coverage={coverage} model={models.research_evidence_coverage_v1} /> : null}
      {activeTab === "checkpoints" ? <CheckpointHistory checkpoints={checkpoints} model={models.research_checkpoints_v1} /> : null}
      {activeTab === "roadmap" ? (
        <Panel title="Research Roadmap" subtitle="No UI inference from prose roadmap documents">
          <StatusPill status={String(roadmap?.model_status ?? "NOT_READY")} />
          <p className="muted-copy">{String(roadmap?.reason ?? "GOVERNED_ROADMAP_PRODUCER_NOT_AVAILABLE")}</p>
        </Panel>
      ) : null}
    </main>
  );
}

function InvestigationLibrary(props: { investigations: JsonRecord[]; model?: ResearchReadModelResult }): JSX.Element {
  const [sortBy, setSortBy] = useState<InvestigationSort>("investigation_id");
  const [confidenceFilter, setConfidenceFilter] = useState("ALL");
  const [conclusionFilter, setConclusionFilter] = useState("ALL");
  const [populationFilter, setPopulationFilter] = useState("ALL");
  const [selectedId, setSelectedId] = useState<string | null>(null);
  const confidences = uniqueValues(props.investigations.map((item) => item.confidence));
  const conclusions = uniqueValues(props.investigations.map((item) => item.conclusion_status));
  const populations = uniqueValues(props.investigations.map((item) => item.active_population_view));
  const visibleInvestigations = props.investigations
    .filter((item) => confidenceFilter === "ALL" || String(item.confidence) === confidenceFilter)
    .filter((item) => conclusionFilter === "ALL" || String(item.conclusion_status) === conclusionFilter)
    .filter((item) => populationFilter === "ALL" || String(item.active_population_view) === populationFilter)
    .sort((left, right) => String(left[sortBy] ?? "").localeCompare(String(right[sortBy] ?? "")));
  const selected = visibleInvestigations.find((item) => String(item.investigation_id) === selectedId) ?? visibleInvestigations[0] ?? null;
  return (
    <Panel title="Investigation Library" subtitle="Question remains embedded in Investigation for v1; confidence and conclusion status pass through unchanged">
      <ModelStatus model={props.model} />
      <div className="control-row">
        <label>Sort <select value={sortBy} onChange={(event) => setSortBy(event.target.value as InvestigationSort)}>
          <option value="investigation_id">Investigation ID</option>
          <option value="generated_at">Generated At</option>
          <option value="confidence">Confidence</option>
        </select></label>
        <label>Confidence <select value={confidenceFilter} onChange={(event) => setConfidenceFilter(event.target.value)}>
          <option value="ALL">All</option>
          {confidences.map((value) => <option key={value} value={value}>{value}</option>)}
        </select></label>
        <label>Conclusion <select value={conclusionFilter} onChange={(event) => setConclusionFilter(event.target.value)}>
          <option value="ALL">All</option>
          {conclusions.map((value) => <option key={value} value={value}>{value}</option>)}
        </select></label>
        <label>Population <select value={populationFilter} onChange={(event) => setPopulationFilter(event.target.value)}>
          <option value="ALL">All</option>
          {populations.map((value) => <option key={value} value={value}>{value}</option>)}
        </select></label>
      </div>
      <table className="data-table"><thead><tr><th>Investigation</th><th>Question</th><th>Confidence</th><th>Conclusion</th><th>Contradictions</th></tr></thead><tbody>
        {visibleInvestigations.map((item) => (
          <tr key={String(item.investigation_id)}>
            <td><button className="link-button" onClick={() => setSelectedId(String(item.investigation_id))}>{String(item.investigation_id)}</button><br /><span className="muted-copy">{String(item.title)}</span></td>
            <td>{String(item.question)}</td>
            <td>{String(item.confidence)}</td>
            <td>{String(item.conclusion_status)}</td>
            <td>{asStringArray(item.contradictory_evidence).length}{asStringArray(item.limitations).length ? <span className="muted-copy"> / {asStringArray(item.limitations).length} limits</span> : null}</td>
          </tr>
        ))}
      </tbody></table>
      {selected ? <InvestigationDetail investigation={selected} /> : <p className="muted-copy">No investigations match the current filters.</p>}
    </Panel>
  );
}

function InvestigationDetail(props: { investigation: JsonRecord }): JSX.Element {
  const item = props.investigation;
  const sourceArtifacts = isRecord(item.source_artifacts) ? Object.entries(item.source_artifacts).map(([key, value]) => `${key}: ${String(value)}`) : [];
  const sourceFingerprints = isRecord(item.source_fingerprints) ? Object.entries(item.source_fingerprints).map(([key, value]) => `${key}: ${String(value)}`) : [];
  return (
    <div className="detail-panel">
      <h3 className="subsection-title">{String(item.investigation_id)} Detail</h3>
      <KeyValueTable rows={[
        ["Title", String(item.title)],
        ["Generated", String(item.generated_at ?? "Missing")],
        ["Status", String(item.status)],
        ["Confidence", String(item.confidence)],
        ["Conclusion", String(item.conclusion_status)],
        ["Population View", String(item.active_population_view)],
        ["Fingerprint", String(item.deterministic_fingerprint ?? "Missing")],
      ]} />
      <h3 className="subsection-title">Key Findings</h3>
      <SimpleList items={asStringArray(item.key_findings)} empty="No producer-authored findings." />
      <h3 className="subsection-title">Contradictory Evidence</h3>
      <SimpleList items={asStringArray(item.contradictory_evidence)} empty="No producer-authored contradictory evidence." />
      <h3 className="subsection-title">Limitations</h3>
      <SimpleList items={asStringArray(item.limitations)} empty="No producer-authored limitations." />
      <h3 className="subsection-title">Unresolved Questions</h3>
      <SimpleList items={asStringArray(item.unresolved_questions)} empty="No producer-authored unresolved questions." />
      <h3 className="subsection-title">Follow-Up Candidates</h3>
      <SimpleList items={asStringArray(item.follow_up_candidates)} empty="No producer-authored follow-up candidates." />
      <h3 className="subsection-title">Source Artifacts</h3>
      <SimpleList items={sourceArtifacts} empty="No producer-authored source artifacts." />
      <h3 className="subsection-title">Source Fingerprints</h3>
      <SimpleList items={sourceFingerprints} empty="No producer-authored source fingerprints." />
    </div>
  );
}

function EvidenceCoverage(props: { coverage: JsonRecord[]; model?: ResearchReadModelResult }): JSX.Element {
  const payload = props.model?.payload ?? null;
  const sourceArtifacts = modelArray(props.model, "source_artifacts");
  const warnings = asStringArray(payload?.warnings);
  const generatedAt = payload?.generated_at ? String(payload.generated_at) : "Unknown";
  const evidenceCoverage = isRecord(payload?.evidence_coverage) ? payload.evidence_coverage : null;
  const contextCoverage = Array.isArray(evidenceCoverage?.prospective_market_context)
    ? evidenceCoverage.prospective_market_context.filter(isRecord)
    : [];
  return (
    <Panel title="Evidence Coverage" subtitle="Prepared artifact coverage only; missing producer evidence remains visible">
      <ModelStatus model={props.model} />
      <p className="muted-copy">Generated from source artifacts at {generatedAt}.</p>
      <table className="data-table"><thead><tr><th>Layer</th><th>Status</th><th>Count</th><th>Detail</th></tr></thead><tbody>
        {props.coverage.map((item) => (
          <tr key={String(item.key)}><td>{String(item.label)}</td><td><StatusPill status={String(item.status)} /></td><td>{formatCount(item)}</td><td>{String(item.detail)}</td></tr>
        ))}
      </tbody></table>
      {contextCoverage.length ? (
        <>
          <h3 className="subsection-title">Prospective Context Fields</h3>
          <table className="data-table"><thead><tr><th>Field</th><th>Status</th><th>Coverage</th><th>Source</th></tr></thead><tbody>
            {contextCoverage.map((item) => (
              <tr key={String(item.field)}>
                <td>{String(item.field)}</td>
                <td><StatusPill status={String(item.status)} /></td>
                <td>{formatCoverage(item)}</td>
                <td>{String(item.source_artifact ?? "Missing")}</td>
              </tr>
            ))}
          </tbody></table>
        </>
      ) : null}
      {warnings.length ? (
        <>
          <h3 className="subsection-title">Warnings</h3>
          <SimpleList items={warnings} empty="No warnings." />
        </>
      ) : null}
      {sourceArtifacts.length ? (
        <>
          <h3 className="subsection-title">Source Provenance</h3>
          <table className="data-table"><thead><tr><th>Source</th><th>Status</th><th>Schema</th><th>Generated</th><th>Fingerprint</th></tr></thead><tbody>
            {sourceArtifacts.map((item) => (
              <tr key={String(item.source_id)}>
                <td>{String(item.source_id)}<br /><span className="muted-copy">{String(item.path)}</span></td>
                <td><StatusPill status={String(item.status)} /></td>
                <td>{String(item.schema_version ?? "Missing")}</td>
                <td>{String(item.generated_at ?? "Missing")}</td>
                <td className="mono-cell">{String(item.fingerprint ?? "Missing")}</td>
              </tr>
            ))}
          </tbody></table>
        </>
      ) : null}
    </Panel>
  );
}

function CheckpointHistory(props: { checkpoints: JsonRecord[]; model?: ResearchReadModelResult }): JSX.Element {
  return (
    <Panel title="Checkpoint History" subtitle="NQ-specific checkpoint key is cohort_id + checkpoint_trade_count">
      <ModelStatus model={props.model} />
      <table className="data-table"><thead><tr><th>Cohort</th><th>Checkpoint</th><th>Confidence</th><th>Contradictory Evidence</th></tr></thead><tbody>
        {props.checkpoints.map((item) => (
          <tr key={`${String(item.cohort_id)}-${String(item.checkpoint_trade_count)}`}><td>{String(item.cohort_id)}</td><td>{String(item.checkpoint_trade_count)}</td><td>{String(item.confidence)}</td><td>{asStringArray(item.contradictory_evidence).join("; ")}</td></tr>
        ))}
      </tbody></table>
    </Panel>
  );
}

function ModelStatus(props: { model?: ResearchReadModelResult }): JSX.Element {
  if (!props.model) {
    return <StatusBanner tone="warn" title="Model missing" detail="The fixture read-model has not loaded." />;
  }
  if (!props.model.ok) {
    return <StatusBanner tone="danger" title="Model invalid" detail={props.model.error ?? "Unsupported fixture read-model."} />;
  }
  return <div className="model-status-row"><StatusPill status={props.model.model_status} /><span className="muted-copy">{props.model.model_name}</span></div>;
}

function Panel(props: { title: string; subtitle?: string; children: React.ReactNode }): JSX.Element {
  return <section className="section-card"><div className="section-header"><div><h2 className="section-title">{props.title}</h2>{props.subtitle ? <p className="section-subtitle">{props.subtitle}</p> : null}</div></div>{props.children}</section>;
}

function StatusBanner(props: { tone: "warn" | "danger"; title: string; detail: string }): JSX.Element {
  return <div className={`status-banner ${props.tone}`}><strong>{props.title}</strong><div className="status-banner-body">{props.detail}</div></div>;
}

function StatusPill(props: { status: string }): JSX.Element {
  const tone = props.status === "HEALTHY" ? "good" : props.status === "INVALID" || props.status === "MISSING" ? "danger" : "warn";
  return <span className={`status-pill ${tone}`}>{props.status}</span>;
}

function SimpleList(props: { items: string[]; empty: string }): JSX.Element {
  return props.items.length ? <ul className="compact-list">{props.items.map((item) => <li key={item}>{item}</li>)}</ul> : <p className="muted-copy">{props.empty}</p>;
}

function KeyValueTable(props: { rows: Array<[string, string]> }): JSX.Element {
  return <table className="data-table"><tbody>{props.rows.map(([key, value]) => <tr key={key}><th>{key}</th><td>{value}</td></tr>)}</tbody></table>;
}

function modelArray(result: ResearchReadModelResult | undefined, key: string): JsonRecord[] {
  const value = result?.payload?.[key];
  return Array.isArray(value) ? value.filter(isRecord) : [];
}

function isRecord(value: unknown): value is JsonRecord {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function asStringArray(value: unknown): string[] {
  return Array.isArray(value) ? value.map(String) : [];
}

function formatCount(item: JsonRecord): string {
  const count = item.count === null || item.count === undefined ? "Unknown" : String(item.count);
  return item.total_count === null || item.total_count === undefined ? count : `${count} / ${String(item.total_count)}`;
}

function formatCoverage(item: JsonRecord): string {
  const available = item.available_count === null || item.available_count === undefined ? "Unknown" : String(item.available_count);
  const total = item.total_count === null || item.total_count === undefined ? "Unknown" : String(item.total_count);
  const rate = typeof item.coverage_rate === "number" ? `${Math.round(item.coverage_rate * 1000) / 10}%` : "Unknown";
  return `${available} / ${total} (${rate})`;
}

function uniqueValues(values: unknown[]): string[] {
  return Array.from(new Set(values.map((value) => String(value ?? "Missing")))).sort();
}
