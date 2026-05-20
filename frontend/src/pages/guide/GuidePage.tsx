import { Link } from "react-router-dom";

import { listJobTypeGuides } from "../../lib/job-ui";

export function GuidePage() {
  const guides = listJobTypeGuides();

  return (
    <section className="page page-guide">
      <header className="page-header">
        <p className="eyebrow">Guide</p>
        <h2>How BackToPure Works</h2>
        <p>See what each workflow produces, which files to review, and what to do next.</p>
      </header>

      <section className="panel">
        <p className="eyebrow">Overview</p>
        <h3>Job flow</h3>
        <div className="guide-grid">
          <article className="guide-card">
            <h4>Collect and compare</h4>
            <p>
              BackToPure checks Pure and connected sources, then prepares files for review.
            </p>
          </article>
          <article className="guide-card">
            <h4>Logs and review files</h4>
            <p>
              Each job keeps its own logs and review files together so the run stays traceable.
            </p>
          </article>
          <article className="guide-card">
            <h4>Supported workflows</h4>
            <p>
              <code>internal_persons</code>, <code>external_persons</code>, <code>external_orgs</code>,
              <code>research_outputs</code>, and <code>datasets</code> can be started here.
            </p>
          </article>
        </div>
      </section>

      <section className="panel">
        <p className="eyebrow">User Flow</p>
        <h3>Operator steps</h3>
        <ol className="workflow-steps">
          <li>Create a job.</li>
          <li>Run it and wait for the review files.</li>
          <li>Check the generated CSV and JSON files.</li>
          <li>Apply updates only after review.</li>
          <li>Use the log and stored files as the audit trail.</li>
        </ol>
      </section>

      <section className="panel">
        <div className="panel-header">
          <div>
            <p className="eyebrow">Workflows</p>
            <h3>Job Types</h3>
          </div>
          <div className="inline-actions">
            <Link className="primary-action" to="/dashboard">
              Open Dashboard
            </Link>
          </div>
        </div>

        <div className="guide-list">
          {guides.map((guide) => (
            <article key={guide.jobType} className="guide-workflow-card">
              <div className="guide-workflow-top">
                <div>
                  <h4>{guide.title}</h4>
                </div>
              </div>

              <p>{guide.summary}</p>
              <p className="hint">{guide.currentScope}</p>

              <div className="guide-detail-grid">
                <section className="info-strip">
                  <strong>Review files</strong>
                  <ul className="guide-inline-list">
                    {guide.reviewFiles.map((file) => (
                      <li key={file}>
                        <code>{file}</code>
                      </li>
                    ))}
                  </ul>
                </section>

                <section className="info-strip">
                  <strong>Result counts mean</strong>
                  <ul className="guide-definition-list">
                    {guide.resultDefinitions.map((definition) => (
                      <li key={definition.label}>
                        <strong>{definition.label}</strong>
                        <span>{definition.description}</span>
                      </li>
                    ))}
                  </ul>
                </section>
              </div>

              <section className="info-strip">
                <strong>User actions</strong>
                <ol className="workflow-steps compact-steps">
                  {guide.userSteps.map((step) => (
                    <li key={step}>{step}</li>
                  ))}
                </ol>
              </section>
            </article>
          ))}
        </div>
      </section>
    </section>
  );
}
