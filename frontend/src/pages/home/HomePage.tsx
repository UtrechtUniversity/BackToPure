import { Link } from "react-router-dom";

export function HomePage() {
  return (
    <section className="page page-home">
      <header className="hero-card home-hero">
        <div>
          <p className="eyebrow">Home</p>
          <h2>BackToPure</h2>
          <p className="hero-copy">
            BackToPure helps teams check data from Ricgraph against Pure, prepare review files,
            and apply only the updates that have been checked first.
          </p>
        </div>
        <div className="inline-actions">
          <Link className="primary-action" to="/dashboard">
            Open Dashboard
          </Link>
        </div>
      </header>

      <section className="home-grid">
        <article className="panel home-card">
          <p className="eyebrow">What It Is</p>
          <h3>A review-first update tool</h3>
          <p>
            The application runs controlled workflows for internal persons, external persons,
            external organisations, research outputs, and datasets. Each run keeps its own log and
            review files together.
          </p>
        </article>

        <article className="panel home-card">
          <p className="eyebrow">What It Does</p>
          <h3>Checks first, updates after</h3>
          <p>
            BackToPure compares source data with what is already in Pure, shows the proposed
            changes in review files, and only applies the selected rows after review.
          </p>
        </article>

        <article className="panel home-card">
          <p className="eyebrow">Ricgraph</p>
          <h3>The connected graph source</h3>
          <p>
            Ricgraph is the graph-based source used here to collect and connect information about
            people, organisations, outputs, and datasets before those results are checked against
            Pure.
          </p>
        </article>
      </section>

      <section className="panel home-flow">
        <p className="eyebrow">How It Works</p>
        <h3>Simple flow</h3>
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
