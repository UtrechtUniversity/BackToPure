export type JobStatus = "queued" | "running" | "needs_review" | "applying" | "completed" | "failed";

export type JobType =
  | "internal_persons"
  | "external_persons"
  | "external_orgs"
  | "research_outputs"
  | "datasets";

export interface FacultyOption {
  value: string;
  label: string;
}

export interface JobArtifacts {
  directory: string;
  csv: string[];
  json: string[];
}

export interface JobArtifactItem {
  name: string;
  kind: "csv" | "json" | "other";
  size_bytes: number;
  download_path: string;
}

export interface JobArtifactsResponse {
  jobId: string;
  directory: string;
  items: JobArtifactItem[];
}

export interface JobReviewTableRow {
  _rowIndex: number;
  [key: string]: string | number;
}

export interface JobReviewTableResponse {
  jobId: string;
  fileName: string;
  columns: string[];
  rows: JobReviewTableRow[];
  rowCount: number;
  selectionColumn: string | null;
  editableColumns: string[];
}

export interface JobReviewTableUpdate {
  rowIndex: number;
  to_be_updated: string;
}

export interface JobResultsSummary {
  entity_label: string;
  found_label: string;
  ready_label: string;
  updated_label: string;
  rolled_back_label?: string;
  found_count: number | null;
  ready_count: number;
  updated_count: number;
  rolled_back_count?: number;
}

export interface JobRecord {
  id: string;
  job_type: JobType;
  status: JobStatus;
  params: Record<string, string | number | boolean | null>;
  created_at: string;
  started_at: string | null;
  finished_at: string | null;
  exit_code: number | null;
  log_path: string | null;
  artifact_dir: string;
  apply_job_id: string | null;
  rollback_job_id: string | null;
  error_message: string | null;
  canOpen: boolean;
  canApply: boolean;
  artifacts: JobArtifacts;
  results: JobResultsSummary;
}

export interface JobChangeSetItem {
  id: number;
  change_set_id: string;
  item_key: string;
  entity_uuid: string;
  entity_label: string | null;
  field_name: string;
  identifier_type: string | null;
  old_value: unknown;
  new_value: unknown;
  apply_status: string;
  rollback_status: string;
  conflict_reason: string | null;
}

export interface JobChangeSet {
  id: string;
  job_id: string;
  job_type: JobType;
  status: string;
  created_at: string;
  applied_at: string | null;
  item_count: number;
  applied_item_count: number;
  items: JobChangeSetItem[];
}

export interface FacultyListResponse {
  items: FacultyOption[];
}

export interface JobListResponse {
  items: JobRecord[];
}

export interface ResultsDashboardBreakdownItem {
  key: string;
  label: string;
  applied_items: number;
  rolled_back_items: number;
  net_items: number;
}

export interface ResultsDashboardWorkflow {
  job_type: JobType;
  label: string;
  applied_items: number;
  rolled_back_items: number;
  net_items: number;
  applied_entities: number;
  rolled_back_entities: number;
  net_entities: number;
  breakdown: ResultsDashboardBreakdownItem[];
}

export interface ResultsDashboardSummary {
  totals: {
    applied_items: number;
    rolled_back_items: number;
    net_items: number;
    applied_entities: number;
    rolled_back_entities: number;
    net_entities: number;
  };
  workflows: ResultsDashboardWorkflow[];
}

export interface JobLogsResponse {
  jobId: string;
  logPath: string | null;
  content: string;
}

export interface CreateJobInput {
  jobType: JobType;
  params?: Record<string, string | number | boolean | null>;
}
