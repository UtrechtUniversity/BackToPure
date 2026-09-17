import { formatJobStatus } from "../lib/job-ui";
import type { JobStatus } from "../lib/types";

interface StatusPillProps {
  status: JobStatus;
}

export function StatusPill({ status }: StatusPillProps) {
  return <span className={`status-pill status-${status}`}>{formatJobStatus(status)}</span>;
}
