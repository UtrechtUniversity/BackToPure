import { useMutation, useQuery } from "@tanstack/react-query";
import { FormEvent, useState } from "react";
import { useNavigate } from "react-router-dom";

import { ApiError, api } from "../../lib/api";

export function NewResearchOutputsJobPage() {
  const navigate = useNavigate();
  const [facultyChoice, setFacultyChoice] = useState("all");
  const [formError, setFormError] = useState<string | null>(null);

  const facultiesQuery = useQuery({
    queryKey: ["faculties"],
    queryFn: () => api.getFaculties(),
  });

  const createJobMutation = useMutation({
    mutationFn: () =>
      api.createJob({
        jobType: "research_outputs",
        params: { facultyChoice },
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

  return (
    <section className="page">
      <header className="page-header">
        <p className="eyebrow">Create Job</p>
        <h2>Research Outputs</h2>
        <p>
          Create a research-outputs job for the selected faculty.
        </p>
      </header>

      <form className="panel form-panel" onSubmit={onSubmit}>
        <div className="info-strip">
          <strong>What this job does</strong>
          <span>
            This job collects research outputs linked to the selected faculty, prepares the review
            CSV and JSON files, and lets you apply only the approved outputs afterward.
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
            Review the proposed outputs carefully before importing them into Pure.
          </p>
        </div>
      </form>
    </section>
  );
}
