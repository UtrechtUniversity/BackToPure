import { screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";

import { AppShell } from "../../components/AppShell";
import { api } from "../../lib/api";
import type { JobRecord } from "../../lib/types";
import { renderWithRouter } from "../../test/test-utils";
import { HistoryPage } from "./HistoryPage";

vi.mock("../../lib/api", () => ({
  api: {
    getJobs: vi.fn(),
  },
}));

const mockedApi = vi.mocked(api);

function makeJob(overrides: Partial<JobRecord> = {}): JobRecord {
  return {
    id: "job-001",
    job_type: "internal_persons",
    status: "completed",
    params: { facultyChoice: "all" },
    created_at: "2026-04-20T13:00:00Z",
    started_at: null,
    finished_at: null,
    exit_code: null,
    log_path: null,
    artifact_dir: "output/internal_persons/job-001",
    apply_job_id: null,
    rollback_job_id: null,
    error_message: null,
    canOpen: true,
    canApply: false,
    artifacts: {
      directory: "output/internal_persons/job-001",
      csv: [],
      json: [],
    },
    results: {
      entity_label: "persons",
      found_label: "Persons found",
      ready_label: "Persons ready to update",
      updated_label: "Persons updated",
      found_count: 0,
      ready_count: 0,
      updated_count: 0,
    },
    ...overrides,
  };
}

describe("HistoryPage", () => {
  afterEach(() => {
    vi.resetAllMocks();
  });

  it("renders jobs and filters by job type", async () => {
    const user = userEvent.setup();
    mockedApi.getJobs.mockResolvedValue({
      items: [
        makeJob(),
        makeJob({ id: "job-002", job_type: "datasets", status: "needs_review" }),
      ],
    });

    renderWithRouter({
      routes: [
        {
          path: "/",
          element: <AppShell />,
          children: [{ path: "history", element: <HistoryPage /> }],
        },
      ],
      initialEntries: ["/history"],
    });

    expect(await screen.findByText("Job History")).toBeInTheDocument();
    expect(await screen.findByRole("link", { name: /job-001/i })).toBeInTheDocument();
    expect(await screen.findByRole("link", { name: /job-002/i })).toBeInTheDocument();

    await user.selectOptions(screen.getByLabelText("Job Type"), "datasets");

    expect(screen.queryByRole("link", { name: /job-001/i })).not.toBeInTheDocument();
    expect(screen.getByRole("link", { name: /job-002/i })).toBeInTheDocument();
  });
});
