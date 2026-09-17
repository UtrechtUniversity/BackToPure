import { screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";

import { AppShell } from "../../components/AppShell";
import { NewDatasetsJobPage } from "./NewDatasetsJobPage";
import { api } from "../../lib/api";
import { renderWithRouter } from "../../test/test-utils";

vi.mock("../../lib/api", () => ({
  ApiError: class ApiError extends Error {
    status: number;

    constructor(message: string, status: number) {
      super(message);
      this.name = "ApiError";
      this.status = status;
    }
  },
  api: {
    getFaculties: vi.fn(),
    createJob: vi.fn(),
  },
}));

const mockedApi = vi.mocked(api);

describe("NewDatasetsJobPage", () => {
  afterEach(() => {
    vi.resetAllMocks();
  });

  it("creates a datasets job and navigates to the detail page", async () => {
    const user = userEvent.setup();
    mockedApi.getFaculties.mockResolvedValue({
      items: [{ value: "law", label: "Law" }],
    });
    mockedApi.createJob.mockResolvedValue({
      id: "job-990",
      job_type: "datasets",
      status: "queued",
      params: { facultyChoice: "law" },
      created_at: "2026-04-21T13:00:00Z",
      started_at: null,
      finished_at: null,
      exit_code: null,
      log_path: null,
      artifact_dir: "output/datasets/job-990",
      apply_job_id: null,
      rollback_job_id: null,
      error_message: null,
      canOpen: false,
      canApply: false,
      artifacts: { directory: "output/datasets/job-990", csv: [], json: [] },
      results: {
        entity_label: "datasets",
        found_label: "Datasets found",
        ready_label: "Datasets ready to update",
        updated_label: "Datasets updated",
        found_count: null,
        ready_count: 0,
        updated_count: 0,
      },
    });

    const { router } = renderWithRouter({
      routes: [
        {
          path: "/",
          element: <AppShell />,
          children: [
            { path: "jobs/new/datasets", element: <NewDatasetsJobPage /> },
            { path: "jobs/:jobId", element: <div>Job detail route</div> },
          ],
        },
      ],
      initialEntries: ["/jobs/new/datasets"],
    });

    await screen.findByRole("heading", { name: "Datasets" });
    await screen.findByRole("option", { name: "Law" });
    await user.selectOptions(screen.getByRole("combobox"), "law");
    await user.click(screen.getByRole("button", { name: "Create Job" }));

    await waitFor(() => expect(router.state.location.pathname).toBe("/jobs/job-990"));
    expect(mockedApi.createJob).toHaveBeenCalledWith({
      jobType: "datasets",
      params: { facultyChoice: "law" },
    });
  });
});
