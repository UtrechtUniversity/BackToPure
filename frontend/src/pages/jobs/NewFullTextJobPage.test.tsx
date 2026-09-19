import { screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";

import { AppShell } from "../../components/AppShell";
import { NewFullTextJobPage } from "./NewFullTextJobPage";
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

function jobRecord() {
  return {
    id: "job-789",
    job_type: "full_text" as const,
    status: "queued" as const,
    params: { facultyChoice: "law" },
    created_at: "2026-09-19T13:00:00Z",
    started_at: null,
    finished_at: null,
    exit_code: null,
    log_path: null,
    artifact_dir: "output/full_text/job-789",
    apply_job_id: null,
    error_message: null,
    canOpen: false,
    canApply: false,
    artifacts: { directory: "output/full_text/job-789", csv: [], json: [] },
    results: {
      entity_label: "full texts",
      found_label: "Publications found",
      ready_label: "Full texts ready to attach",
      updated_label: "Full texts attached",
      found_count: null,
      ready_count: 0,
      updated_count: 0,
    },
  };
}

function renderPage() {
  return renderWithRouter({
    routes: [
      {
        path: "/",
        element: <AppShell />,
        children: [
          { path: "jobs/new/full-text", element: <NewFullTextJobPage /> },
          { path: "jobs/:jobId", element: <div>Job detail route</div> },
        ],
      },
    ],
    initialEntries: ["/jobs/new/full-text"],
  });
}

describe("NewFullTextJobPage", () => {
  afterEach(() => {
    vi.resetAllMocks();
  });

  it("defaults to the publisher-version policy so a deposit is never widened by accident", async () => {
    const user = userEvent.setup();
    mockedApi.getFaculties.mockResolvedValue({ items: [{ value: "law", label: "Law" }] });
    mockedApi.createJob.mockResolvedValue(jobRecord());

    const { router } = renderPage();

    await screen.findByRole("heading", { name: "Open Access Full Texts" });
    await screen.findByRole("option", { name: "Law" });
    await user.selectOptions(screen.getByLabelText("Faculty"), "law");
    await user.click(screen.getByRole("button", { name: "Create Job" }));

    await waitFor(() => expect(router.state.location.pathname).toBe("/jobs/job-789"));
    expect(mockedApi.createJob).toHaveBeenCalledWith({
      jobType: "full_text",
      params: { facultyChoice: "law", versionPolicy: "published" },
    });
  });

  it("sends the chosen version policy when it is widened", async () => {
    const user = userEvent.setup();
    mockedApi.getFaculties.mockResolvedValue({ items: [{ value: "law", label: "Law" }] });
    mockedApi.createJob.mockResolvedValue(jobRecord());

    renderPage();

    await screen.findByRole("heading", { name: "Open Access Full Texts" });
    await user.selectOptions(
      screen.getByLabelText("Which versions may be deposited"),
      "published_accepted",
    );
    await user.click(screen.getByRole("button", { name: "Create Job" }));

    await waitFor(() =>
      expect(mockedApi.createJob).toHaveBeenCalledWith({
        jobType: "full_text",
        params: { facultyChoice: "all", versionPolicy: "published_accepted" },
      }),
    );
  });

  it("explains what the selected policy means", async () => {
    const user = userEvent.setup();
    mockedApi.getFaculties.mockResolvedValue({ items: [] });

    renderPage();

    await screen.findByRole("heading", { name: "Open Access Full Texts" });
    expect(screen.getByText(/current repository policy/i)).toBeInTheDocument();

    await user.selectOptions(screen.getByLabelText("Which versions may be deposited"), "any");
    expect(screen.getByText(/not been peer reviewed/i)).toBeInTheDocument();
  });

  it("shows an error when the job cannot be created", async () => {
    const user = userEvent.setup();
    mockedApi.getFaculties.mockResolvedValue({ items: [] });
    mockedApi.createJob.mockRejectedValue(new Error("boom"));

    renderPage();

    await screen.findByRole("heading", { name: "Open Access Full Texts" });
    await user.click(screen.getByRole("button", { name: "Create Job" }));

    expect(await screen.findByText("Could not create the job.")).toBeInTheDocument();
  });
});
