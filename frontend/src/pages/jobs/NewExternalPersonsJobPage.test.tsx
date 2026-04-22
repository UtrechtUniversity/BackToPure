import { screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";

import { AppShell } from "../../components/AppShell";
import { NewExternalPersonsJobPage } from "./NewExternalPersonsJobPage";
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

describe("NewExternalPersonsJobPage", () => {
  afterEach(() => {
    vi.resetAllMocks();
  });

  it("creates an external persons job and navigates to the detail page", async () => {
    const user = userEvent.setup();
    mockedApi.getFaculties.mockResolvedValue({
      items: [{ value: "law", label: "Law" }],
    });
    mockedApi.createJob.mockResolvedValue({
      id: "job-456",
      job_type: "external_persons",
      status: "queued",
      params: { facultyChoice: "law" },
      created_at: "2026-04-20T13:00:00Z",
      started_at: null,
      finished_at: null,
      exit_code: null,
      log_path: null,
      artifact_dir: "output/external_persons/job-456",
      apply_job_id: null,
      error_message: null,
      canOpen: false,
      canApply: false,
      artifacts: { directory: "output/external_persons/job-456", csv: [], json: [] },
      results: {
        entity_label: "persons",
        found_label: "Persons found",
        ready_label: "Persons ready to update",
        updated_label: "Persons updated",
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
            { path: "jobs/new/external-persons", element: <NewExternalPersonsJobPage /> },
            { path: "jobs/:jobId", element: <div>Job detail route</div> },
          ],
        },
      ],
      initialEntries: ["/jobs/new/external-persons"],
    });

    await screen.findByText("External Persons");
    await screen.findByRole("option", { name: "Law" });
    await user.selectOptions(screen.getByRole("combobox"), "law");
    await user.click(screen.getByRole("button", { name: "Create Job" }));

    await waitFor(() => expect(router.state.location.pathname).toBe("/jobs/job-456"));
    expect(mockedApi.createJob).toHaveBeenCalledWith({
      jobType: "external_persons",
      params: { facultyChoice: "law" },
    });
  });
});
