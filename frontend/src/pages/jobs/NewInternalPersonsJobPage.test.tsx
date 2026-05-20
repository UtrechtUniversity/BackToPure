import { screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";

import { AppShell } from "../../components/AppShell";
import { NewInternalPersonsJobPage } from "./NewInternalPersonsJobPage";
import { api, ApiError } from "../../lib/api";
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

describe("NewInternalPersonsJobPage", () => {
  afterEach(() => {
    vi.resetAllMocks();
  });

  it("creates a job and navigates to the job detail page", async () => {
    const user = userEvent.setup();
    mockedApi.getFaculties.mockResolvedValue({
      items: [{ value: "sci", label: "Science" }],
    });
    mockedApi.createJob.mockResolvedValue({
      id: "job-123",
      job_type: "internal_persons",
      status: "queued",
      params: { facultyChoice: "sci" },
      created_at: "2026-04-20T13:00:00Z",
      started_at: null,
      finished_at: null,
      exit_code: null,
      log_path: null,
      artifact_dir: "output/internal_persons",
      apply_job_id: null,
      error_message: null,
      canOpen: false,
      canApply: false,
      artifacts: { directory: "output/internal_persons", csv: [], json: [] },
    });

    const { router } = renderWithRouter({
      routes: [
        {
          path: "/",
          element: <AppShell />,
          children: [
            { path: "jobs/new/internal-persons", element: <NewInternalPersonsJobPage /> },
            { path: "jobs/:jobId", element: <div>Job detail route</div> },
          ],
        },
      ],
      initialEntries: ["/jobs/new/internal-persons"],
    });

    await screen.findByRole("heading", { name: "Internal Persons" });
    await screen.findByRole("option", { name: "Science" });
    await user.selectOptions(screen.getByRole("combobox"), "sci");
    await user.click(screen.getByRole("button", { name: "Create Job" }));

    await waitFor(() => expect(router.state.location.pathname).toBe("/jobs/job-123"));
    expect(mockedApi.createJob).toHaveBeenCalledWith({
      jobType: "internal_persons",
      params: { facultyChoice: "sci" },
    });
  });

  it("shows backend validation errors", async () => {
    const user = userEvent.setup();
    mockedApi.getFaculties.mockResolvedValue({ items: [] });
    mockedApi.createJob.mockRejectedValue(new ApiError("An active internal_persons job already exists", 400));

    renderWithRouter({
      routes: [
        {
          path: "/",
          element: <AppShell />,
          children: [{ path: "jobs/new/internal-persons", element: <NewInternalPersonsJobPage /> }],
        },
      ],
      initialEntries: ["/jobs/new/internal-persons"],
    });

    await screen.findByRole("heading", { name: "Internal Persons" });
    await user.click(screen.getByRole("button", { name: "Create Job" }));

    expect(await screen.findByText("An active internal_persons job already exists")).toBeInTheDocument();
  });
});
