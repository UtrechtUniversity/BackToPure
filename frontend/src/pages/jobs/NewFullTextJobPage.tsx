import { useMutation, useQuery } from "@tanstack/react-query";
import { FormEvent, useState } from "react";
import { useNavigate } from "react-router-dom";

import { ApiError, api } from "../../lib/api";

const VERSION_POLICY_OPTIONS = [
  {
    value: "published",
    label: "Publisher version only",
    hint: "The final published article. The safest option and the current repository policy.",
  },
  {
    value: "published_accepted",
    label: "Publisher version or accepted manuscript",
    hint: "Also allows the peer-reviewed author manuscript. Each file is labelled with its own version in Pure.",
  },
  {
    value: "any",
    label: "Any version, including preprints",
    hint: "Also allows submitted manuscripts, which have not been peer reviewed.",
  },
];

export function NewFullTextJobPage() {
  const navigate = useNavigate();
  const [facultyChoice, setFacultyChoice] = useState("all");
  const [versionPolicy, setVersionPolicy] = useState("published");
  const [formError, setFormError] = useState<string | null>(null);

  const facultiesQuery = useQuery({
    queryKey: ["faculties"],
    queryFn: () => api.getFaculties(),
  });

  const createJobMutation = useMutation({
    mutationFn: () =>
      api.createJob({
        jobType: "full_text",
        params: { facultyChoice, versionPolicy },
      }),
    onSuccess: (job) => {
      navigate(`/jobs/${job.id}`);
    },
    onError: (error) => {
      if (error instanceof ApiError) {
        setFormError(error.message);
        return;
      }
      setFormError("Could not create the job.");
    },
  });

  function onSubmit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    setFormError(null);
    createJobMutation.mutate();
  }

  const facultyOptions = facultiesQuery.data?.items ?? [];
  const selectedPolicy = VERSION_POLICY_OPTIONS.find((option) => option.value === versionPolicy);

  return (
    <section className="page">
      <header className="page-header">
        <p className="eyebrow">Create Job</p>
        <h2>Open Access Full Texts</h2>
        <p>Report which publications could have an open access PDF attached.</p>
      </header>

      <form className="panel form-panel" onSubmit={onSubmit}>
        <div className="info-strip">
          <strong>Purpose</strong>
          <span>
            Find publications in Pure with no file attached, check OpenAlex for an open access copy, and
            verify each candidate really is a PDF. This job reports only; nothing is uploaded.
          </span>
        </div>

        <label className="field">
          <span>Faculty</span>
          <select value={facultyChoice} onChange={(event) => setFacultyChoice(event.target.value)}>
            <option value="all">All Faculties</option>
            {facultyOptions.map((faculty) => (
              <option key={faculty.value} value={faculty.value}>
                {faculty.label}
              </option>
            ))}
          </select>
        </label>

        <label className="field">
          <span>Which versions may be deposited</span>
          <select value={versionPolicy} onChange={(event) => setVersionPolicy(event.target.value)}>
            {VERSION_POLICY_OPTIONS.map((option) => (
              <option key={option.value} value={option.value}>
                {option.label}
              </option>
            ))}
          </select>
        </label>
        {selectedPolicy ? <p className="hint">{selectedPolicy.hint}</p> : null}

        {facultiesQuery.isLoading ? <p className="hint">Loading faculties...</p> : null}
        {facultiesQuery.isError ? (
          <p className="error-banner">Could not load faculties. You can still submit with All Faculties.</p>
        ) : null}
        {formError ? <p className="error-banner">{formError}</p> : null}

        <div className="form-actions">
          <button className="primary-action" type="submit" disabled={createJobMutation.isPending}>
            {createJobMutation.isPending ? "Creating..." : "Create Job"}
          </button>
          <p className="hint">
            Fetching and checking every candidate takes a while; the report lists a reason for each
            publication it could not use.
          </p>
        </div>
      </form>
    </section>
  );
}
