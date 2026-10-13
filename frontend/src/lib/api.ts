import type {
  CreateJobInput,
  FacultyListResponse,
  JobChangeSet,
  JobArtifactsResponse,
  JobListResponse,
  JobLogsResponse,
  JobRecord,
  JobReviewTableUpdate,
  JobReviewTableResponse,
  ResultsDashboardSummary,
} from "./types";

const API_BASE = "/api";

class ApiError extends Error {
  status: number;

  constructor(message: string, status: number) {
    super(message);
    this.name = "ApiError";
    this.status = status;
  }
}

async function request<T>(path: string, init?: RequestInit): Promise<T> {
  const response = await fetch(`${API_BASE}${path}`, {
    headers: {
      "Content-Type": "application/json",
      ...(init?.headers ?? {}),
    },
    ...init,
  });

  if (response.status === 204) {
    return undefined as T;
  }

  const data = await response.json().catch(() => null);
  if (!response.ok) {
    const message = typeof data?.error === "string" ? data.error : `Request failed with status ${response.status}`;
    throw new ApiError(message, response.status);
  }
  return data as T;
}

async function requestJobAction(jobId: string, action: "run" | "apply"): Promise<JobRecord> {
  try {
    return await request<JobRecord>(`/jobs/${jobId}/${action}`, { method: "POST" });
  } catch (error) {
    if (error instanceof ApiError && error.status < 500) throw error;
    // A lost long-running response does not mean the server failed to start.
    // Reconcile with the job, never repeat a potentially accepted POST.
    try {
      const job = await request<JobRecord>(`/jobs/${jobId}`);
      const initialStatus = action === "run" ? "queued" : "needs_review";
      if (job && job.status !== initialStatus) return job;
    } catch {
      // Preserve uncertainty when the status endpoint is also unreachable.
    }
    throw new ApiError(
      action === "run"
        ? "Could not confirm whether the job started. Refresh its status before trying again; it may still be running."
        : "Could not confirm the apply result. Refresh the job status before trying again; updates may still be running.",
      0,
    );
  }
}

export const api = {
  getFaculties(): Promise<FacultyListResponse> {
    return request<FacultyListResponse>("/faculties");
  },

  getJobs(): Promise<JobListResponse> {
    return request<JobListResponse>("/jobs");
  },

  getResultsDashboard(days?: number | null): Promise<ResultsDashboardSummary> {
    const suffix = typeof days === "number" ? `?days=${days}` : "";
    return request<ResultsDashboardSummary>(`/results-dashboard${suffix}`);
  },

  getJob(jobId: string): Promise<JobRecord> {
    return request<JobRecord>(`/jobs/${jobId}`);
  },

  deleteJob(jobId: string): Promise<void> {
    return request<void>(`/jobs/${jobId}`, {
      method: "DELETE",
    });
  },

  createJob(input: CreateJobInput): Promise<JobRecord> {
    return request<JobRecord>("/jobs", {
      method: "POST",
      body: JSON.stringify(input),
    });
  },

  runJob(jobId: string): Promise<JobRecord> {
    return requestJobAction(jobId, "run");
  },

  cancelJob(jobId: string): Promise<JobRecord> {
    return request<JobRecord>(`/jobs/${jobId}/cancel`, {
      method: "POST",
    });
  },

  applyJob(jobId: string): Promise<JobRecord> {
    return requestJobAction(jobId, "apply");
  },

  rollbackJob(jobId: string): Promise<JobRecord> {
    return request<JobRecord>(`/jobs/${jobId}/rollback`, {
      method: "POST",
    });
  },

  getJobLogs(jobId: string): Promise<JobLogsResponse> {
    return request<JobLogsResponse>(`/jobs/${jobId}/logs`);
  },

  getJobArtifacts(jobId: string): Promise<JobArtifactsResponse> {
    return request<JobArtifactsResponse>(`/jobs/${jobId}/artifacts`);
  },

  async getJobReviewTable(jobId: string): Promise<JobReviewTableResponse | null> {
    try {
      return await request<JobReviewTableResponse>(`/jobs/${jobId}/review-table`);
    } catch (error) {
      if (error instanceof ApiError && error.status === 404) {
        return null;
      }
      throw error;
    }
  },

  updateJobReviewTable(
    jobId: string,
    input: { fileName: string; expectedRowCount: number; expectedRevision: string; updates: JobReviewTableUpdate[] },
  ): Promise<JobReviewTableResponse> {
    return request<JobReviewTableResponse>(`/jobs/${jobId}/review-table`, {
      method: "POST",
      body: JSON.stringify(input),
    });
  },

  async getJobChangeSet(jobId: string): Promise<JobChangeSet | null> {
    try {
      return await request<JobChangeSet>(`/jobs/${jobId}/change-set`);
    } catch (error) {
      if (error instanceof ApiError && error.status === 404) {
        return null;
      }
      throw error;
    }
  },
};

export { ApiError };
