import { screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";

import { AppShell } from "../../components/AppShell";
import { NewResearchOutputsJobPage } from "./NewResearchOutputsJobPage";
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

describe("NewResearchOutputsJobPage", () => {
  afterEach(() => {
    vi.resetAllMocks();
  });

  it("creates a research outputs job and navigates to the detail page", async () => {
    const user = userEvent.setup();
    mockedApi.getFaculties.mockResolvedValue({
      items: [{ value: "law", label: "Law" }],
    });
    mockedApi.createJob.mockResolvedValue({
      id: "job-789",
      job_type: "research_outputs",
      status: "queued",
      params: { facultyChoice: "law" },
      created_at: "2026-04-21T13:00:00Z",
      started_at: null,
      finished_at: null,
      exit_code: null,
      log_path: null,
      artifact_dir: "output/research_output/job-789",
      apply_job_id: null,
      rollback_job_id: null,
      error_message: null,
      canOpen: false,
      canApply: false,
      artifacts: { directory: "output/research_output/job-789", csv: [], json: [] },
      results: {
        entity_label: "research outputs",
        found_label: "Research outputs found",
        ready_label: "Research outputs ready to update",
        updated_label: "Research outputs updated",
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
            { path: "jobs/new/research-outputs", element: <NewResearchOutputsJobPage /> },
            { path: "jobs/:jobId", element: <div>Job detail route</div> },
          ],
        },
      ],
      initialEntries: ["/jobs/new/research-outputs"],
    });

    await screen.findByText("Research Outputs");
    await screen.findByRole("option", { name: "Law" });
    await user.selectOptions(screen.getByRole("combobox"), "law");
    await user.click(screen.getByRole("button", { name: "Create Job" }));

    await waitFor(() => expect(router.state.location.pathname).toBe("/jobs/job-789"));
    expect(mockedApi.createJob).toHaveBeenCalledWith({
      jobType: "research_outputs",
      params: { facultyChoice: "law" },
    });
  });
});
