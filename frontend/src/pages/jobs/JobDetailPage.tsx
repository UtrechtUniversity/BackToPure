import { useDeferredValue, useEffect, useRef, useState } from "react";
import { useMutation, useQuery } from "@tanstack/react-query";
import { useNavigate, useParams } from "react-router-dom";

import { ApiError, api } from "../../lib/api";
import { StatusPill } from "../../components/StatusPill";
import {
  canCancelJobStatus,
  canDeleteJobStatus,
  describeReadiness,
  formatBytes,
  formatJobScope,
  formatJobStatus,
  formatTimestamp,
  getJobTypeGuide,
  isActiveJobStatus,
  nextUserAction,
} from "../../lib/job-ui";
import type { JobType } from "../../lib/types";

function formatReviewCell(column: string, value: string | number | undefined) {
  const rawValue = String(value ?? "");
  if (column !== "FULL_NAME") {
    return rawValue;
  }

  return rawValue
    .split("|")
    .map((part) => part.trim())
    .filter(Boolean)
    .map((part) => part.split("#")[0].trim())
    .filter(Boolean)
    .join(" | ");
}

function getVisibleReviewColumns(columns: string[]) {
  return columns.filter((column) => column !== "person_id" && column !== "uri");
}

const REVIEW_TABLE_MAX_HEIGHT = 400;
const REVIEW_TABLE_ROW_HEIGHT = 42;
const REVIEW_TABLE_OVERSCAN = 6;

const REVIEW_COLUMN_LABELS: Record<string, string> = {
  to_be_updated: "Selected",
  updated: "Updated",
  FULL_NAME: "Name",
  organization_name: "Organisation",
  org_name: "Organisation",
  PURE_UUID_PERS: "Pure UUID",
  PURE_UUID_ORG: "Pure UUID",
  Pure_UUID: "Pure UUID",
  person_uuid: "Person UUID",
  new_id: "New ID",
  new_value: "New Value",
  old_value: "Current Value",
  value: "Value",
  id_type: "Identifier Type",
  identifier_type: "Identifier Type",
  needs_ror_update: "Needs ROR",
  needs_geo_update: "Needs Address/Geo",
  ror: "ROR",
  city: "City",
  country: "Country",
  address: "Address",
  point: "Geo Point",
};

function getReviewColumnLabel(column: string) {
  return REVIEW_COLUMN_LABELS[column] ?? column.replace(/_/g, " ");
}

function getReviewColumnPriority(jobType: JobType, column: string) {
  const basePriority: Record<string, number> = {
    to_be_updated: 0,
    updated: 1,
    FULL_NAME: 2,
    organization_name: 2,
    org_name: 2,
    PURE_UUID_PERS: 3,
    PURE_UUID_ORG: 3,
    Pure_UUID: 3,
    person_uuid: 4,
    id_type: 5,
    identifier_type: 5,
    new_id: 6,
    new_value: 7,
    old_value: 8,
  };

  if (jobType === "external_orgs") {
    const externalOrgPriority: Record<string, number> = {
      to_be_updated: 0,
      updated: 1,
      organization_name: 2,
      org_name: 2,
      Pure_UUID: 3,
      PURE_UUID_ORG: 3,
      needs_ror_update: 4,
      needs_geo_update: 5,
      ror: 6,
      city: 7,
      country: 8,
      address: 9,
      point: 10,
    };
    return externalOrgPriority[column] ?? basePriority[column] ?? 100;
  }

  if (jobType === "external_persons") {
    const externalPersonPriority: Record<string, number> = {
      to_be_updated: 0,
      updated: 1,
      FULL_NAME: 2,
      Pure_UUID: 3,
      id_type: 4,
      identifier_type: 4,
      new_id: 5,
      new_value: 6,
      old_value: 7,
    };
    return externalPersonPriority[column] ?? basePriority[column] ?? 100;
  }

  return basePriority[column] ?? 100;
}

function getReviewColumns(jobType: JobType, columns: string[]) {
  return getVisibleReviewColumns(columns).sort((left, right) => {
    const priorityDiff = getReviewColumnPriority(jobType, left) - getReviewColumnPriority(jobType, right);
    if (priorityDiff !== 0) {
      return priorityDiff;
    }
    return left.localeCompare(right);
  });
}

function formatApplyErrorMessage(rawLine: string) {
  const cleaned = rawLine.replace(/^.*(?:ERROR|WARNING) /, "").trim();
  const personMatch = cleaned.match(/name=([^)'"]+)/);
  const personName = personMatch?.[1]?.trim();
  const hasOrcid = cleaned.toLowerCase().includes("orcid");
  const hasDoi = cleaned.toLowerCase().includes("doi");

  if (cleaned.includes("validation.orcid.invalid.format")) {
    return personName
      ? `Pure rejected the ORCID for ${personName} because the format is invalid. Check the ORCID value in the review table before applying again.`
      : "Pure rejected an ORCID because the format is invalid. Check the ORCID value in the review table before applying again.";
  }

  if (cleaned.includes("validation.text.length") && hasOrcid) {
    return personName
      ? `Pure rejected the ORCID for ${personName} because the value is too long. Check the ORCID value in the review table before applying again.`
      : "Pure rejected an ORCID because the value is too long. Check the ORCID value in the review table before applying again.";
  }

  if (cleaned.includes("validation.text.length") && hasDoi) {
    return "Pure rejected a DOI because the value is too long. Check the DOI in the review table before applying again.";
  }

  if (cleaned.includes("validation.required")) {
    if (hasOrcid) {
      return personName
        ? `Pure rejected the ORCID for ${personName} because a required value is missing. Check the review table before applying again.`
        : "Pure rejected an update because a required value is missing. Check the review table before applying again.";
    }
    if (hasDoi) {
      return "Pure rejected a DOI update because a required value is missing. Check the review table before applying again.";
    }
    return "Pure rejected an update because a required value is missing. Check the review table before applying again.";
  }

  if (cleaned.includes("validation.orcid") && cleaned.includes("invalid")) {
    return personName
      ? `Pure rejected the ORCID for ${personName} because the value is invalid. Check the ORCID value in the review table before applying again.`
      : "Pure rejected an ORCID because the value is invalid. Check the ORCID value in the review table before applying again.";
  }

  if (cleaned.toLowerCase().includes("duplicate") || cleaned.toLowerCase().includes("already exists")) {
    if (hasOrcid) {
      return personName
        ? `Pure rejected the ORCID for ${personName} because that identifier already exists. Check whether this person already has the identifier in Pure.`
        : "Pure rejected an ORCID because that identifier already exists. Check whether it is already present in Pure.";
    }
    if (hasDoi) {
      return "Pure rejected a DOI because it already exists. Check whether this record is already present in Pure.";
    }
    return "Pure rejected an update because the value already exists in Pure.";
  }

  if (hasDoi && cleaned.toLowerCase().includes("invalid")) {
    return "Pure rejected a DOI because the format is invalid. Check the DOI in the review table before applying again.";
  }

  return cleaned.slice(0, 320);
}

export function JobDetailPage() {
  const [showMainLog, setShowMainLog] = useState(false);
  const [showRollbackLog, setShowRollbackLog] = useState(false);
  const [reviewSearch, setReviewSearch] = useState("");
  const [reviewFilter, setReviewFilter] = useState<"all" | "selected" | "unselected">("all");
  const [reviewSelection, setReviewSelection] = useState<Record<number, boolean>>({});
  const [reviewScrollTop, setReviewScrollTop] = useState(0);
  const [jobActionInProgress, setJobActionInProgress] = useState(false);
  const { jobId = "" } = useParams();
  const navigate = useNavigate();
  const deferredReviewSearch = useDeferredValue(reviewSearch);
  const reviewTableRef = useRef<HTMLDivElement | null>(null);

  const jobQuery = useQuery({
    queryKey: ["job", jobId],
    queryFn: () => api.getJob(jobId),
    enabled: Boolean(jobId),
    refetchInterval: (query) =>
      jobActionInProgress || (query.state.data && isActiveJobStatus(query.state.data.status)) ? 3000 : false,
  });
  const shouldPollJobLogs = jobActionInProgress || Boolean(jobQuery.data && isActiveJobStatus(jobQuery.data.status));

  const logsQuery = useQuery({
    queryKey: ["jobLogs", jobId],
    queryFn: () => api.getJobLogs(jobId),
    enabled: Boolean(jobId),
    refetchInterval: () => (shouldPollJobLogs ? 3000 : false),
  });

  const artifactsQuery = useQuery({
    queryKey: ["jobArtifacts", jobId],
    queryFn: () => api.getJobArtifacts(jobId),
    enabled: Boolean(jobId),
    refetchInterval: () => (shouldPollJobLogs ? 3000 : false),
  });

  const changeSetQuery = useQuery({
    queryKey: ["jobChangeSet", jobId],
    queryFn: () => api.getJobChangeSet(jobId),
    enabled: Boolean(jobId),
  });

  const reviewTableQuery = useQuery({
    queryKey: ["jobReviewTable", jobId],
    queryFn: () => api.getJobReviewTable(jobId),
    enabled: Boolean(jobId),
  });

  useEffect(() => {
    const table = reviewTableQuery.data;
    if (!table) {
      setReviewSelection({});
      return;
    }
    const nextSelection: Record<number, boolean> = {};
    if (table.selectionColumn) {
      for (const row of table.rows) {
        nextSelection[row._rowIndex] = String(row[table.selectionColumn] ?? "").trim().toUpperCase() === "X";
      }
    }
    setReviewSelection(nextSelection);
  }, [reviewTableQuery.data]);

  const rollbackJobQuery = useQuery({
    queryKey: ["rollbackJob", jobQuery.data?.rollback_job_id],
    queryFn: async () => {
      const rollbackJobId = jobQuery.data?.rollback_job_id;
      if (!rollbackJobId) {
        return null;
      }
      return (await api.getJob(rollbackJobId)) ?? null;
    },
    enabled: Boolean(jobQuery.data?.rollback_job_id),
    refetchInterval: (query) =>
      query.state.data && isActiveJobStatus(query.state.data.status) ? 3000 : false,
  });

  const rollbackLogsQuery = useQuery({
    queryKey: ["rollbackJobLogs", jobQuery.data?.rollback_job_id],
    queryFn: async () => {
      const rollbackJobId = jobQuery.data?.rollback_job_id;
      if (!rollbackJobId) {
        return null;
      }
      return api.getJobLogs(rollbackJobId);
    },
    enabled: Boolean(jobQuery.data?.rollback_job_id),
    refetchInterval: () =>
      rollbackJobQuery.data && isActiveJobStatus(rollbackJobQuery.data.status) ? 3000 : false,
  });

  const runJobMutation = useMutation({
    mutationFn: () => api.runJob(jobId),
    onMutate: () => {
      setJobActionInProgress(true);
      void logsQuery.refetch();
    },
    onSuccess: () => {
      void jobQuery.refetch();
      void logsQuery.refetch();
      void artifactsQuery.refetch();
    },
    onSettled: () => {
      setJobActionInProgress(false);
    },
  });

  const applyJobMutation = useMutation({
    mutationFn: () => api.applyJob(jobId),
    onMutate: () => {
      setJobActionInProgress(true);
      void logsQuery.refetch();
    },
    onSuccess: () => {
      void jobQuery.refetch();
      void logsQuery.refetch();
      void artifactsQuery.refetch();
      void changeSetQuery.refetch();
    },
    onSettled: () => {
      setJobActionInProgress(false);
    },
  });

  const rollbackJobMutation = useMutation({
    mutationFn: () => api.rollbackJob(jobId),
    onMutate: () => {
      setJobActionInProgress(true);
      void rollbackLogsQuery.refetch();
    },
    onSuccess: () => {
      void jobQuery.refetch();
      void logsQuery.refetch();
      void changeSetQuery.refetch();
      void rollbackJobQuery.refetch();
      void rollbackLogsQuery.refetch();
    },
    onSettled: () => {
      setJobActionInProgress(false);
    },
  });

  const cancelJobMutation = useMutation({
    mutationFn: () => api.cancelJob(jobId),
    onSuccess: () => {
      setJobActionInProgress(false);
      void jobQuery.refetch();
      void logsQuery.refetch();
      void artifactsQuery.refetch();
      void changeSetQuery.refetch();
    },
  });

  const deleteJobMutation = useMutation({
    mutationFn: () => api.deleteJob(jobId),
    onSuccess: () => {
      void navigate("/");
    },
  });

  const saveReviewTableMutation = useMutation({
    mutationFn: () => {
      if (!reviewTableQuery.data) {
        throw new Error("No review table is loaded.");
      }
      const reviewTable = reviewTableQuery.data;
      const selectionColumn = reviewTable.selectionColumn;
      const updates =
        reviewTable.rows.flatMap((row) => {
          if (!selectionColumn) {
            return [];
          }
          const originalSelected = String(row[selectionColumn] ?? "").trim().toUpperCase() === "X";
          const currentSelected = reviewSelection[row._rowIndex] ?? originalSelected;
          if (originalSelected === currentSelected) {
            return [];
          }
          return [{ rowIndex: row._rowIndex, to_be_updated: currentSelected ? "X" : "" }];
        }) ?? [];
      return api.updateJobReviewTable(jobId, {
        fileName: reviewTable.fileName,
        expectedRowCount: reviewTable.rowCount,
        updates,
      });
    },
    onSuccess: () => {
      void reviewTableQuery.refetch();
      void jobQuery.refetch();
    },
  });

  useEffect(() => {
    setReviewScrollTop(0);
    if (reviewTableRef.current) {
      reviewTableRef.current.scrollTop = 0;
    }
  }, [reviewTableQuery.data?.fileName, deferredReviewSearch, reviewFilter]);

  if (jobQuery.isLoading) {
    return <section className="page"><p className="empty-state">Loading job...</p></section>;
  }

  if (jobQuery.isError || !jobQuery.data) {
    const message =
      jobQuery.error instanceof ApiError ? jobQuery.error.message : "Could not load the requested job.";
    return (
      <section className="page">
        <p className="error-banner">{message}</p>
      </section>
    );
  }

  const job = jobQuery.data;
  const supportsRollback =
    job.job_type === "internal_persons" ||
    job.job_type === "external_persons" ||
    job.job_type === "external_orgs" ||
    job.job_type === "research_outputs" ||
    job.job_type === "datasets";
  const canDelete = canDeleteJobStatus(job.status);
  const canCancel = canCancelJobStatus(job.status);
  const guide = getJobTypeGuide(job.job_type);
  const hasSelectedUpdates = job.results.ready_count > 0;
  const effectiveCanApply = job.canApply && hasSelectedUpdates;
  const changeSet = changeSetQuery.data;
  const rollbackJob = rollbackJobQuery.data;
  const rollbackLogContent = rollbackLogsQuery.data?.content ?? "";
  const logContent = logsQuery.data?.content ?? "";
  const logLineCount = logContent ? logContent.split("\n").filter((line) => line.trim().length > 0).length : 0;
  const rollbackLogLineCount = rollbackLogContent
    ? rollbackLogContent.split("\n").filter((line) => line.trim().length > 0).length
    : 0;
  const reviewTable = reviewTableQuery.data;
  const reviewColumns = getReviewColumns(job.job_type, reviewTable?.columns ?? []);
  const isEmptyReviewTable = Boolean(reviewTable) && (reviewTable?.rowCount ?? 0) === 0;
  const selectionColumn = reviewTable?.selectionColumn ?? null;
  const isSelectionEditable = selectionColumn ? Boolean(reviewTable?.editableColumns?.includes(selectionColumn)) : false;
  const reviewRowsWithLocalSelection = reviewTable?.rows.map((row) => {
    if (!selectionColumn) {
      return row;
    }
    const selected = reviewSelection[row._rowIndex] ?? (String(row[selectionColumn] ?? "").trim().toUpperCase() === "X");
    return {
      ...row,
      [selectionColumn]: selected ? "X" : "",
    };
  });
  const normalizedReviewSearch = deferredReviewSearch.trim().toLowerCase();
  const filteredReviewRows = reviewRowsWithLocalSelection?.filter((row) => {
    const selected = selectionColumn ? String(row[selectionColumn] ?? "").trim().toUpperCase() === "X" : false;
    if (reviewFilter === "selected" && !selected) {
      return false;
    }
    if (reviewFilter === "unselected" && selected) {
      return false;
    }
    if (!normalizedReviewSearch) {
      return true;
    }
    return reviewColumns.some((column) =>
      String(row[column] ?? "")
        .toLowerCase()
        .includes(normalizedReviewSearch),
    );
  });
  const totalFilteredReviewRows = filteredReviewRows?.length ?? 0;
  const shouldVirtualizeReviewRows = totalFilteredReviewRows > 100;
  const virtualReviewRowsPerViewport = Math.ceil(REVIEW_TABLE_MAX_HEIGHT / REVIEW_TABLE_ROW_HEIGHT);
  const virtualReviewStartIndex = shouldVirtualizeReviewRows
    ? Math.max(0, Math.floor(reviewScrollTop / REVIEW_TABLE_ROW_HEIGHT) - REVIEW_TABLE_OVERSCAN)
    : 0;
  const virtualReviewEndIndex = shouldVirtualizeReviewRows
    ? Math.min(
        totalFilteredReviewRows,
        virtualReviewStartIndex + virtualReviewRowsPerViewport + REVIEW_TABLE_OVERSCAN * 2,
      )
    : totalFilteredReviewRows;
  const visibleReviewRows = shouldVirtualizeReviewRows
    ? filteredReviewRows?.slice(virtualReviewStartIndex, virtualReviewEndIndex)
    : filteredReviewRows;
  const reviewTopSpacerHeight = shouldVirtualizeReviewRows
    ? virtualReviewStartIndex * REVIEW_TABLE_ROW_HEIGHT
    : 0;
  const reviewBottomSpacerHeight = shouldVirtualizeReviewRows
    ? Math.max(0, (totalFilteredReviewRows - virtualReviewEndIndex) * REVIEW_TABLE_ROW_HEIGHT)
    : 0;
  const reviewSelectionUpdates =
    reviewTable?.rows.flatMap((row) => {
      if (!selectionColumn) {
        return [];
      }
      const originalSelected = String(row[selectionColumn] ?? "").trim().toUpperCase() === "X";
      const currentSelected = reviewSelection[row._rowIndex] ?? originalSelected;
      if (originalSelected === currentSelected) {
        return [];
      }
      return [{ rowIndex: row._rowIndex, to_be_updated: currentSelected ? "X" : "" }];
    }) ?? [];
  const reviewHasUnsavedChanges = reviewSelectionUpdates.length > 0;
  const applyErrorLines = logContent
    .split("\n")
    .filter(
      (line) =>
        line.includes("ERROR Failed to update person UUID:") ||
        line.includes("WARNING Failed to update data for UUID ") ||
        line.includes("ERROR Error updating UUID "),
    );
  const latestApplyError = applyErrorLines.length
    ? formatApplyErrorMessage(applyErrorLines[applyErrorLines.length - 1])
    : null;
  const appliedItems = changeSet?.items.filter((item) => item.apply_status === "applied").length ?? 0;
  const applyFailedItems = changeSet?.items.filter((item) => item.apply_status === "not_applied").length ?? 0;
  const rollbackableItems = changeSet?.items.filter((item) => item.apply_status === "applied").length ?? 0;
  const remainingRollbackItems =
    changeSet?.items.filter(
      (item) =>
        item.apply_status === "applied" && !["rolled_back", "conflict", "skipped"].includes(item.rollback_status ?? ""),
    ).length ?? 0;
  const rolledBackItems = changeSet?.items.filter((item) => item.rollback_status === "rolled_back").length ?? 0;
  const conflictItems = changeSet?.items.filter((item) => item.rollback_status === "conflict").length ?? 0;
  const rollbackUnavailableReason =
    supportsRollback && changeSet && rollbackableItems === 0
      ? "Rollback is not available because 0 changes were applied successfully."
      : supportsRollback && changeSet && rollbackableItems > 0 && remainingRollbackItems === 0
        ? "Rollback is not available because there are no remaining applied changes to roll back."
      : null;
  const canRollback =
    supportsRollback &&
    job.status === "completed" &&
    !!changeSet &&
    ["applied", "rolled_back_with_conflicts"].includes(changeSet.status) &&
    remainingRollbackItems > 0 &&
    (!job.rollback_job_id || rollbackJob?.status === "failed");
  const applyDisabledReason =
    job.status === "needs_review" && job.canApply && !hasSelectedUpdates
      ? `Apply is not possible because ${job.results.ready_label.toLowerCase()} is 0. Review the CSV file and keep at least one row selected with X in to_be_updated.`
      : null;

  return (
    <section className="page page-job-detail">
      <header className="page-header">
        <p className="eyebrow">Job Detail</p>
        <div className="job-header-row">
          <h2>{job.id}</h2>
          <StatusPill status={job.status} />
        </div>
        <p>{formatJobStatus(job.job_type)} · {nextUserAction(job)}</p>
      </header>

      <div className="detail-grid">
        <section className="panel">
          <h3>Run Summary</h3>
          <dl className="summary-grid">
            <div>
              <dt>Status</dt>
              <dd>{formatJobStatus(job.status)}</dd>
            </div>
            <div>
              <dt>Created</dt>
              <dd>{formatTimestamp(job.created_at)}</dd>
            </div>
            <div>
              <dt>Started</dt>
              <dd>{formatTimestamp(job.started_at)}</dd>
            </div>
            <div>
              <dt>Finished</dt>
              <dd>{formatTimestamp(job.finished_at)}</dd>
            </div>
            <div>
              <dt>Faculty</dt>
              <dd>{formatJobScope(job)}</dd>
            </div>
            <div>
              <dt>Review Files Ready</dt>
              <dd>{job.canOpen ? "Yes" : "No"}</dd>
            </div>
            <div>
              <dt>Ready To Apply</dt>
              <dd>{effectiveCanApply ? "Yes" : "No"}</dd>
            </div>
            <div>
              <dt>Exit Code</dt>
              <dd>{job.exit_code ?? "Not available"}</dd>
            </div>
            <div>
              <dt>Current Step</dt>
              <dd>{describeReadiness(job)}</dd>
            </div>
          </dl>

          {job.status === "completed" && changeSet ? (
            <div className="info-strip compact">
              <strong>Apply</strong>
              <span>
                {appliedItems} updated successfully{applyFailedItems ? `, ${applyFailedItems} failed during apply` : "."}
              </span>
            </div>
          ) : null}
          {job.status === "completed" && latestApplyError ? (
            <div className="info-strip compact">
              <strong>Latest Error</strong>
              <span>{latestApplyError}</span>
            </div>
          ) : null}

          <div className="job-actions">
            {job.status === "queued" ? (
              <button
                className="primary-action"
                type="button"
                onClick={() => runJobMutation.mutate()}
                disabled={runJobMutation.isPending}
              >
                {runJobMutation.isPending ? "Running..." : "Run Job"}
              </button>
            ) : null}
            {job.status === "needs_review" && job.canApply ? (
              <button
                className={`primary-action ${!hasSelectedUpdates ? "disabled-action" : ""}`}
                type="button"
                onClick={() => applyJobMutation.mutate()}
                disabled={applyJobMutation.isPending || !hasSelectedUpdates}
              >
                {applyJobMutation.isPending ? "Applying..." : "Apply Updates"}
              </button>
            ) : null}
            {canRollback ? (
              <button
                className="secondary-action"
                type="button"
                onClick={() => rollbackJobMutation.mutate()}
                disabled={rollbackJobMutation.isPending}
              >
                {rollbackJobMutation.isPending ? "Rolling Back..." : "Rollback Updates"}
              </button>
            ) : null}
            {applyDisabledReason ? <p className="hint">{applyDisabledReason}</p> : null}
            {rollbackJobMutation.isError ? (
              <p className="error-banner">
                {rollbackJobMutation.error instanceof ApiError
                  ? rollbackJobMutation.error.message
                  : "Could not start rollback."}
              </p>
            ) : null}
            <button className="secondary-action" type="button" onClick={() => void jobQuery.refetch()}>
              Refresh Job
            </button>
            {canCancel ? (
              <button
                className="danger-action"
                type="button"
                onClick={() => {
                  if (window.confirm("Stop this running job?")) {
                    cancelJobMutation.mutate();
                  }
                }}
                disabled={cancelJobMutation.isPending}
              >
                {cancelJobMutation.isPending ? "Stopping..." : "Stop Job"}
              </button>
            ) : null}
            {canDelete ? (
              <button
                className="danger-action"
                type="button"
                onClick={() => {
                  if (window.confirm("Delete this tracked job and its stored log?")) {
                    deleteJobMutation.mutate();
                  }
                }}
                disabled={deleteJobMutation.isPending}
              >
                {deleteJobMutation.isPending ? "Deleting..." : "Delete Job"}
              </button>
            ) : null}
            {runJobMutation.isError ? (
              <p className="error-banner">
                {runJobMutation.error instanceof ApiError
                  ? runJobMutation.error.message
                  : "Could not run the job."}
              </p>
            ) : null}
            {applyJobMutation.isError ? (
              <p className="error-banner">
                {applyJobMutation.error instanceof ApiError
                  ? applyJobMutation.error.message
                  : "Could not apply the updates."}
              </p>
            ) : null}
            {deleteJobMutation.isError ? (
              <p className="error-banner">
                {deleteJobMutation.error instanceof ApiError
                  ? deleteJobMutation.error.message
                  : "Could not delete the job."}
              </p>
            ) : null}
            {cancelJobMutation.isError ? (
              <p className="error-banner">
                {cancelJobMutation.error instanceof ApiError
                  ? cancelJobMutation.error.message
                  : "Could not stop the job."}
              </p>
            ) : null}
            {job.error_message ? <p className="error-banner">{job.error_message}</p> : null}
          </div>
        </section>

        <section className="panel">
          <h3>Results</h3>
          <div className="result-metrics">
            <article className="result-metric-card">
              <span>{job.results.found_label}</span>
              <strong>{job.results.found_count ?? "Not available"}</strong>
            </article>
            <article className="result-metric-card">
              <span>{job.results.ready_label}</span>
              <strong>{job.results.ready_count}</strong>
            </article>
            <article className="result-metric-card">
              <span>{job.results.updated_label}</span>
              <strong>{job.results.updated_count}</strong>
            </article>
            {job.results.rolled_back_label && (job.rollback_job_id || (job.results.rolled_back_count ?? 0) > 0) ? (
              <article className="result-metric-card">
                <span>{job.results.rolled_back_label}</span>
                <strong>{job.results.rolled_back_count ?? 0}</strong>
              </article>
            ) : null}
          </div>
          <div className="info-strip compact">
            <strong>This run only</strong>
            <span>These counts come from the files generated for this job.</span>
          </div>
          <ul className="guide-definition-list metric-definition-list">
            {guide.resultDefinitions.map((definition) => (
              <li key={definition.label}>
                <strong>{definition.label}</strong>
                <span>{definition.description}</span>
              </li>
            ))}
          </ul>
          <div className="info-strip compact">
            <strong>Review files</strong>
            <span>{guide.reviewFiles.join(" and ")}</span>
          </div>
          {job.job_type === "external_orgs" ? (
            <div className="info-strip compact">
              <strong>Selection reasons</strong>
              <span>
                An organisation appears here only when Pure is still missing the matched ROR, the address or geo data
                differs from the matched source, or both.
              </span>
            </div>
          ) : null}
          {Object.keys(job.params).length === 0 ? (
            <p className="empty-state">No parameters were recorded for this job.</p>
          ) : (
            <ul className="key-value-list">
              {Object.entries(job.params).map(([key, value]) => (
                <li key={key}>
                  <strong>{key}</strong>
                  <span>{String(value)}</span>
                </li>
              ))}
            </ul>
          )}
          {job.sourceConfig ? (
            <div className="info-strip compact">
              <strong>Sources</strong>
              <span>
                Pure: {job.sourceConfig.pureBaseUrl} · Ricgraph: {job.sourceConfig.ricgraphBaseUrl} · Scope:{" "}
                {job.sourceConfig.facultyChoice ?? job.sourceConfig.facultyPrefix}
              </span>
            </div>
          ) : null}
        </section>

        <section className="panel">
          <div className="panel-header">
            <div>
              <h3>Files</h3>
              <p className="hint">Stored in: {job.artifacts.directory}</p>
            </div>
            <button className="secondary-action" type="button" onClick={() => void artifactsQuery.refetch()}>
              Refresh Files
            </button>
          </div>
          <p className="hint">
            Download and check these files before applying updates.
          </p>
          {artifactsQuery.isLoading ? <p className="empty-state">Loading review files...</p> : null}
          {artifactsQuery.isError ? <p className="error-banner">Could not load the review files.</p> : null}
          {!artifactsQuery.isLoading && !artifactsQuery.isError && !artifactsQuery.data?.items.length ? (
            <p className="empty-state">No review files are available yet.</p>
          ) : null}
          {!artifactsQuery.isLoading && !artifactsQuery.isError && artifactsQuery.data?.items.length ? (
            <ul className="artifact-download-list">
              {artifactsQuery.data.items.map((item) => (
                <li key={item.name} className="artifact-download-item">
                  <div>
                    <strong>{item.name}</strong>
                    <p>
                      {item.kind.toUpperCase()} file · {formatBytes(item.size_bytes)}
                    </p>
                  </div>
                  <a className="secondary-action" href={item.download_path}>
                    Download
                  </a>
                </li>
              ))}
            </ul>
          ) : null}
        </section>

        <section className="panel review-table-panel">
          <div className="panel-header">
            <div>
              <h3>Review Table</h3>
              <p className="hint">Adjust which rows should be applied.</p>
            </div>
            <button className="secondary-action" type="button" onClick={() => void reviewTableQuery.refetch()}>
              Refresh Table
            </button>
          </div>
          {reviewTableQuery.isLoading ? <p className="empty-state">Loading review table...</p> : null}
          {reviewTableQuery.isError ? <p className="error-banner">Could not load the review table.</p> : null}
          {!reviewTableQuery.isLoading && !reviewTableQuery.isError && !reviewTable ? (
            <p className="empty-state">No review CSV is available for this job yet.</p>
          ) : null}
          {!reviewTableQuery.isLoading && !reviewTableQuery.isError && reviewTable ? (
            <>
              <div className="info-strip compact">
                <strong>Loaded</strong>
                <span>
                  {reviewTable.fileName} with {reviewTable.rowCount} rows
                </span>
              </div>
              {isEmptyReviewTable ? (
                <p className="empty-state">
                  This review file is empty, so there is nothing to preview or select for this job.
                </p>
              ) : (
                <>
                  <div className="review-table-toolbar">
                    <label className="field">
                      <span>Search rows</span>
                      <input
                        className="review-table-search"
                        type="search"
                        value={reviewSearch}
                        onChange={(event) => setReviewSearch(event.target.value)}
                        placeholder="Search names, UUIDs, identifiers..."
                      />
                    </label>
                    <label className="field">
                      <span>Filter rows</span>
                      <select value={reviewFilter} onChange={(event) => setReviewFilter(event.target.value as typeof reviewFilter)}>
                        <option value="all">All rows</option>
                        <option value="selected">Selected only</option>
                        <option value="unselected">Unselected only</option>
                      </select>
                    </label>
                  </div>
                  <div className="job-actions">
                    {selectionColumn && isSelectionEditable ? (
                      <>
                        <button
                          className="secondary-action"
                          type="button"
                          onClick={() => {
                            if (!filteredReviewRows) {
                              return;
                            }
                            setReviewSelection((current) => {
                              const nextSelection = { ...current };
                              for (const row of filteredReviewRows) {
                                nextSelection[row._rowIndex] = true;
                              }
                              return nextSelection;
                            });
                          }}
                          disabled={!filteredReviewRows?.length || saveReviewTableMutation.isPending}
                        >
                          Select Filtered Rows
                        </button>
                        <button
                          className="secondary-action"
                          type="button"
                          onClick={() => {
                            if (!filteredReviewRows) {
                              return;
                            }
                            setReviewSelection((current) => {
                              const nextSelection = { ...current };
                              for (const row of filteredReviewRows) {
                                nextSelection[row._rowIndex] = false;
                              }
                              return nextSelection;
                            });
                          }}
                          disabled={!filteredReviewRows?.length || saveReviewTableMutation.isPending}
                        >
                          Deselect Filtered Rows
                        </button>
                      </>
                    ) : null}
                    <button
                      className="primary-action"
                      type="button"
                      onClick={() => saveReviewTableMutation.mutate()}
                      disabled={!reviewHasUnsavedChanges || saveReviewTableMutation.isPending}
                    >
                      {saveReviewTableMutation.isPending ? "Saving..." : "Save Review"}
                    </button>
                    <button
                      className="secondary-action"
                      type="button"
                      onClick={() => {
                        if (!reviewTable || !selectionColumn) {
                          return;
                        }
                        const resetSelection: Record<number, boolean> = {};
                        for (const row of reviewTable.rows) {
                          resetSelection[row._rowIndex] =
                            String(row[selectionColumn] ?? "").trim().toUpperCase() === "X";
                        }
                        setReviewSelection(resetSelection);
                      }}
                      disabled={!reviewHasUnsavedChanges || saveReviewTableMutation.isPending}
                    >
                      Reset Changes
                    </button>
                    <p className="hint">
                      {reviewHasUnsavedChanges
                        ? `${reviewSelectionUpdates.length} row${reviewSelectionUpdates.length === 1 ? "" : "s"} changed`
                        : "No unsaved review changes"}
                    </p>
                    {saveReviewTableMutation.isError ? (
                      <p className="error-banner">
                        {saveReviewTableMutation.error instanceof ApiError
                          ? saveReviewTableMutation.error.message
                          : "Could not save the review file."}
                      </p>
                    ) : null}
                  </div>
                  <p className="hint">
                    Showing {filteredReviewRows?.length ?? 0} of {reviewTable.rowCount} rows.
                  </p>
                  <div
                    ref={reviewTableRef}
                    className="review-table-wrapper"
                    onScroll={(event) => setReviewScrollTop(event.currentTarget.scrollTop)}
                  >
                    <table className="review-table">
                      <thead>
                        <tr>
                          {reviewColumns.map((column) => (
                            <th key={column} scope="col">
                              {getReviewColumnLabel(column)}
                            </th>
                          ))}
                        </tr>
                      </thead>
                      <tbody>
                        {reviewTopSpacerHeight > 0 ? (
                          <tr aria-hidden="true" className="review-table-spacer-row">
                            <td colSpan={reviewColumns.length} style={{ height: `${reviewTopSpacerHeight}px` }} />
                          </tr>
                        ) : null}
                        {visibleReviewRows?.map((row) => (
                          <tr key={row._rowIndex}>
                            {reviewColumns.map((column) => {
                              const isEditableSelection = Boolean(
                                selectionColumn &&
                                  isSelectionEditable &&
                                  column === selectionColumn,
                              );
                              return (
                                <td key={`${row._rowIndex}-${column}`}>
                                  {isEditableSelection ? (
                                    <input
                                      type="checkbox"
                                      checked={String(row[column] ?? "").trim().toUpperCase() === "X"}
                                      onChange={(event) => {
                                        const checked = event.target.checked;
                                        setReviewSelection((current) => ({
                                          ...current,
                                          [row._rowIndex]: checked,
                                        }));
                                      }}
                                      aria-label={`Select row ${row._rowIndex + 1}`}
                                    />
                                  ) : (
                                    formatReviewCell(column, row[column])
                                  )}
                                </td>
                              );
                            })}
                          </tr>
                        ))}
                        {reviewBottomSpacerHeight > 0 ? (
                          <tr aria-hidden="true" className="review-table-spacer-row">
                            <td colSpan={reviewColumns.length} style={{ height: `${reviewBottomSpacerHeight}px` }} />
                          </tr>
                        ) : null}
                      </tbody>
                    </table>
                  </div>
                </>
              )}
            </>
          ) : null}
        </section>

        <section className="panel log-panel">
          <div className="panel-header">
            <div>
              <h3>Logs</h3>
              <p className="hint">
                {isActiveJobStatus(job.status)
                  ? "Logs auto-refresh while the job is active."
                  : "Use refresh if you want to reload the persisted log file."}
              </p>
            </div>
            <div className="inline-actions">
              <button className="secondary-action" type="button" onClick={() => void logsQuery.refetch()}>
                Refresh Logs
              </button>
            </div>
          </div>
          {logsQuery.isLoading ? <p className="empty-state">Loading logs...</p> : null}
          {logsQuery.isError ? <p className="error-banner">Could not load logs.</p> : null}
          {!logsQuery.isLoading && !logsQuery.isError ? (
            <div className="log-disclosure">
              <button
                className="log-toggle"
                type="button"
                onClick={() => setShowMainLog((current) => !current)}
                aria-expanded={showMainLog}
              >
                {showMainLog ? "Hide main log" : `Show main log (${logLineCount} lines)`}
              </button>
              {showMainLog ? <pre className="log-output">{logsQuery.data?.content || "No log output yet."}</pre> : null}
            </div>
          ) : null}
        </section>

        {supportsRollback && job.rollback_job_id ? (
          <section className="panel log-panel">
            <div className="panel-header">
              <div>
                <h3>Rollback Log</h3>
                <p className="hint">
                  {rollbackJob && isActiveJobStatus(rollbackJob.status)
                    ? "Rollback logs auto-refresh while the rollback job is active."
                    : "This log shows what was reverted during rollback."}
                </p>
              </div>
              <div className="inline-actions">
                <button
                  className="secondary-action"
                  type="button"
                  onClick={() => void rollbackLogsQuery.refetch()}
                >
                  Refresh Rollback Log
                </button>
              </div>
            </div>
            {rollbackLogsQuery.isLoading ? <p className="empty-state">Loading rollback log...</p> : null}
            {rollbackLogsQuery.isError ? <p className="error-banner">Could not load the rollback log.</p> : null}
            {!rollbackLogsQuery.isLoading && !rollbackLogsQuery.isError ? (
              <div className="log-disclosure">
                <button
                  className="log-toggle"
                  type="button"
                  onClick={() => setShowRollbackLog((current) => !current)}
                  aria-expanded={showRollbackLog}
                >
                  {showRollbackLog ? "Hide rollback log" : `Show rollback log (${rollbackLogLineCount} lines)`}
                </button>
                {showRollbackLog ? (
                  <pre className="log-output">{rollbackLogContent || "No rollback log output yet."}</pre>
                ) : null}
              </div>
            ) : null}
          </section>
        ) : null}

        {supportsRollback ? (
          <section className="panel">
            <h3>Rollback</h3>
            {changeSetQuery.isLoading ? <p className="empty-state">Loading rollback details...</p> : null}
            {!changeSetQuery.isLoading && !changeSet ? (
              <p className="empty-state">No rollback data is available for this job.</p>
            ) : null}
            {changeSet ? (
              <>
                <div className="result-metrics">
                  <article className="result-metric-card">
                    <span>Rollbackable changes</span>
                    <strong>{rollbackableItems}</strong>
                  </article>
                  <article className="result-metric-card">
                    <span>Rolled back</span>
                    <strong>{rolledBackItems}</strong>
                  </article>
                  <article className="result-metric-card">
                    <span>Conflicts skipped</span>
                    <strong>{conflictItems}</strong>
                  </article>
                </div>
                <div className="info-strip compact">
                  <strong>Rollback status</strong>
                  <span>
                    {job.rollback_job_id
                      ? rollbackJob
                        ? rollbackJob.status === "failed" && canRollback
                          ? `Previous rollback failed in ${rollbackJob.id}. You can retry the remaining changes.`
                          : `${formatJobStatus(rollbackJob.status)} in rollback job ${rollbackJob.id}`
                        : `Rollback job created: ${job.rollback_job_id}`
                      : canRollback
                        ? "Rollback is available because this apply run has stored change data."
                        : changeSet.status.replace(/_/g, " ")}
                  </span>
                </div>
                {conflictItems > 0 ? (
                  <p className="hint">
                    Some updates were skipped because the current Pure value no longer matched what this job applied.
                  </p>
                ) : null}
                {rollbackUnavailableReason ? <p className="hint">{rollbackUnavailableReason}</p> : null}
              </>
            ) : null}
          </section>
        ) : null}
      </div>
    </section>
  );
}
