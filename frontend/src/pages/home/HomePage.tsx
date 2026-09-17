import { Link } from "react-router-dom";

export function HomePage() {
  return (
    <section className="page page-home">
      <header className="panel page-header home-header">
        <div>
          <p className="eyebrow">Home</p>
          <h2>BackToPure</h2>
          <p className="hero-copy">
            Review data against Pure, prepare files, and apply only the rows that were checked.
          </p>
        </div>
        <div className="inline-actions">
          <Link className="primary-action" to="/dashboard">
            Dashboard
          </Link>
          <Link className="secondary-action" to="/history">
            History
          </Link>
        </div>
      </header>

      <section className="home-grid">
        <article className="panel home-card">
          <p className="eyebrow">Role</p>
          <h3>Review-first updates</h3>
          <p>
            Run controlled workflows for persons, organisations, outputs, and datasets with logs
            and review files stored per job.
          </p>
        </article>

        <article className="panel home-card">
          <p className="eyebrow">Flow</p>
          <h3>Check, then apply</h3>
          <p>
            Compare source data with Pure, review the proposed changes, and apply only the selected
            rows.
          </p>
        </article>

        <article className="panel home-card">
          <p className="eyebrow">Source</p>
          <h3>Ricgraph input</h3>
          <p>
            Ricgraph provides the connected source data that is checked against Pure before any
            update is applied.
          </p>
        </article>
      </section>

      <section className="panel home-flow">
        <p className="eyebrow">Process</p>
        <h3>Three-step flow</h3>
        <div className="home-steps">
          <article>
            <strong>1. Start a job</strong>
            <p>Choose the workflow and scope you want to check.</p>
          </article>
          <article>
            <strong>2. Review the result</strong>
            <p>Inspect the files, logs, and selected rows before anything is changed.</p>
          </article>
          <article>
            <strong>3. Apply or roll back</strong>
            <p>Apply checked updates and use rollback where that workflow supports it.</p>
          </article>
        </div>
      </section>
    </section>
  );
}
