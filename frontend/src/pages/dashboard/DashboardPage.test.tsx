import { screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";

import { AppShell } from "../../components/AppShell";
import { DashboardPage } from "./DashboardPage";
import { api } from "../../lib/api";
import { renderWithRouter } from "../../test/test-utils";
import type { JobRecord } from "../../lib/types";

vi.mock("../../lib/api", () => ({
  api: {
    getJobs: vi.fn(),
    getResultsDashboard: vi.fn(),
    deleteJob: vi.fn(),
  },
}));

const mockedApi = vi.mocked(api);

function makeJob(overrides: Partial<JobRecord> = {}): JobRecord {
  return {
    id: "job-001",
    job_type: "internal_persons",
    status: "queued",
    params: { facultyChoice: "all" },
    created_at: "2026-04-20T13:00:00Z",
    started_at: null,
    finished_at: null,
    exit_code: null,
    log_path: null,
    artifact_dir: "output/internal_persons",
    apply_job_id: null,
    error_message: null,
    rollback_job_id: null,
    canOpen: false,
    canApply: false,
    artifacts: {
      directory: "output/internal_persons",
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

describe("DashboardPage", () => {
  afterEach(() => {
    vi.resetAllMocks();
    vi.unstubAllGlobals();
  });

  it("renders recent jobs and the results overview", async () => {
    mockedApi.getJobs.mockResolvedValue({
      items: [
        makeJob(),
        makeJob({ id: "job-002", status: "needs_review", canOpen: true, canApply: true }),
      ],
    });
    mockedApi.getResultsDashboard.mockResolvedValue({
      totals: {
        applied_items: 4,
        rolled_back_items: 1,
        net_items: 3,
        applied_entities: 4,
        rolled_back_entities: 1,
        net_entities: 3,
      },
      workflows: [
        {
          job_type: "internal_persons",
          label: "Internal Persons",
          applied_items: 2,
          rolled_back_items: 1,
          net_items: 1,
          applied_entities: 2,
          rolled_back_entities: 1,
          net_entities: 1,
          breakdown: [
            { key: "orcid", label: "ORCID", applied_items: 1, rolled_back_items: 0, net_items: 1 },
          ],
        },
      ],
    });

    renderWithRouter({
      routes: [
        {
          path: "/",
          element: <AppShell />,
          children: [{ index: true, element: <DashboardPage /> }],
        },
      ],
    });

    expect(await screen.findByText("Tracked Runs")).toBeInTheDocument();
    expect(screen.getByText("Net Results")).toBeInTheDocument();
    expect(await screen.findByRole("heading", { name: "Internal Persons" })).toBeInTheDocument();
    expect(await screen.findByText("Rollback 0")).toBeInTheDocument();
    expect(screen.getByText("Review rows in each job before applying updates.")).toBeInTheDocument();
    expect(screen.queryByRole("link", { name: "Open Guide" })).not.toBeInTheDocument();
    expect(screen.getAllByRole("link", { name: "Datasets" })).toHaveLength(2);
    expect(screen.getAllByRole("link", { name: "Research Outputs" })).toHaveLength(2);
    expect(screen.getAllByRole("link", { name: "External Persons" })).toHaveLength(2);
    expect(screen.queryByRole("link", { name: "New Datasets Job" })).not.toBeInTheDocument();
    expect(screen.queryByRole("link", { name: "New Research Outputs Job" })).not.toBeInTheDocument();
    expect(screen.queryByRole("link", { name: "New External Persons Job" })).not.toBeInTheDocument();
    expect(await screen.findByText("job-001")).toBeInTheDocument();
    expect(await screen.findByText("job-002")).toBeInTheDocument();
    expect(screen.getAllByText("Faculty: All faculties")).toHaveLength(2);
  });

  it("supports manual refresh", async () => {
    const user = userEvent.setup();
    mockedApi.getJobs.mockResolvedValue({ items: [makeJob()] });
    mockedApi.getResultsDashboard.mockResolvedValue({
      totals: {
        applied_items: 0,
        rolled_back_items: 0,
        net_items: 0,
        applied_entities: 0,
        rolled_back_entities: 0,
        net_entities: 0,
      },
      workflows: [],
    });

    renderWithRouter({
      routes: [
        {
          path: "/",
          element: <AppShell />,
          children: [{ index: true, element: <DashboardPage /> }],
        },
      ],
    });

    await screen.findByText("job-001");
    await user.click(screen.getAllByRole("button", { name: "Refresh" })[0]);

    expect(mockedApi.getJobs).toHaveBeenCalledTimes(2);
  });

  it("deletes an inactive job from the dashboard", async () => {
    const user = userEvent.setup();
    vi.stubGlobal("confirm", vi.fn(() => true));
    mockedApi.getJobs
      .mockResolvedValueOnce({
        items: [
          makeJob({ id: "job-001", status: "failed", error_message: "boom" }),
          makeJob({ id: "job-002", status: "running" }),
        ],
      })
      .mockResolvedValueOnce({
        items: [makeJob({ id: "job-002", status: "running" })],
      });
    mockedApi.getResultsDashboard.mockResolvedValue({
      totals: {
        applied_items: 0,
        rolled_back_items: 0,
        net_items: 0,
        applied_entities: 0,
        rolled_back_entities: 0,
        net_entities: 0,
      },
      workflows: [],
    });
    mockedApi.deleteJob.mockResolvedValue(undefined);

    renderWithRouter({
      routes: [
        {
          path: "/",
          element: <AppShell />,
          children: [{ index: true, element: <DashboardPage /> }],
        },
      ],
    });

    await screen.findByText("job-001");
    await user.click(screen.getByRole("button", { name: "Delete" }));

    expect(mockedApi.deleteJob).toHaveBeenCalledWith("job-001");
    expect(await screen.findByText("job-002")).toBeInTheDocument();
  });

  it("allows deleting a queued job from the dashboard", async () => {
    const user = userEvent.setup();
    vi.stubGlobal("confirm", vi.fn(() => true));
    mockedApi.getJobs
      .mockResolvedValueOnce({
        items: [
          makeJob({ id: "job-001", status: "queued" }),
          makeJob({ id: "job-002", status: "running" }),
        ],
      })
      .mockResolvedValueOnce({
        items: [makeJob({ id: "job-002", status: "running" })],
      });
    mockedApi.getResultsDashboard.mockResolvedValue({
      totals: {
        applied_items: 0,
        rolled_back_items: 0,
        net_items: 0,
        applied_entities: 0,
        rolled_back_entities: 0,
        net_entities: 0,
      },
      workflows: [],
    });
    mockedApi.deleteJob.mockResolvedValue(undefined);

    renderWithRouter({
      routes: [
        {
          path: "/",
          element: <AppShell />,
          children: [{ index: true, element: <DashboardPage /> }],
        },
      ],
    });

    await screen.findByText("job-001");
    await user.click(screen.getByRole("button", { name: "Delete" }));

    expect(mockedApi.deleteJob).toHaveBeenCalledWith("job-001");
  });
});
