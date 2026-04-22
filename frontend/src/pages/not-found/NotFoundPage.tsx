import { Link } from "react-router-dom";

export function NotFoundPage() {
  return (
    <section className="page">
      <div className="panel">
        <p className="eyebrow">404</p>
        <h2>Page not found</h2>
        <p>The requested frontend route does not exist.</p>
        <Link className="primary-action" to="/">
          Back to home
        </Link>
      </div>
    </section>
  );
}
