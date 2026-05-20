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
    return request<JobRecord>(`/jobs/${jobId}/run`, {
      method: "POST",
    });
  },

  cancelJob(jobId: string): Promise<JobRecord> {
    return request<JobRecord>(`/jobs/${jobId}/cancel`, {
      method: "POST",
    });
  },

  applyJob(jobId: string): Promise<JobRecord> {
    return request<JobRecord>(`/jobs/${jobId}/apply`, {
      method: "POST",
    });
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
    input: { fileName: string; expectedRowCount: number; updates: JobReviewTableUpdate[] },
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
