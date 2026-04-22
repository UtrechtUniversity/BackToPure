import { Link, NavLink, Outlet } from "react-router-dom";

export function AppShell() {
  return (
    <div className="app-shell">
      <aside className="app-sidebar">
        <div className="brand-block">
          <Link to="/" className="brand-link">
            <img
              className="brand-logo"
              src="/static/images/BACK-TO-Pure-7-1-2024.gif"
              alt="BackToPure Logo"
            />
            <p className="eyebrow">BackToPure</p>
            <h1>Jobs</h1>
            <p className="brand-copy">
              Run jobs, review files, and apply updates.
            </p>
          </Link>
        </div>

        <nav className="app-nav">
          <NavLink to="/" end className="nav-link">
            Home
          </NavLink>
          <NavLink to="/dashboard" className="nav-link">
            Dashboard
          </NavLink>
          <NavLink to="/history" className="nav-link">
            History
          </NavLink>
          <NavLink to="/jobs/new/internal-persons" className="nav-link">
            New Internal Persons Job
          </NavLink>
          <NavLink to="/jobs/new/external-persons" className="nav-link">
            New External Persons Job
          </NavLink>
          <NavLink to="/jobs/new/external-orgs" className="nav-link">
            New External Organisations Job
          </NavLink>
          <NavLink to="/jobs/new/research-outputs" className="nav-link">
            New Research Outputs Job
          </NavLink>
          <NavLink to="/jobs/new/datasets" className="nav-link">
            New Datasets Job
          </NavLink>
        </nav>
      </aside>

      <main className="app-main">
        <Outlet />
      </main>
    </div>
  );
}
