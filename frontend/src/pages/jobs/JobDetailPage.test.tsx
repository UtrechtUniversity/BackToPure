import { fireEvent, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";

import { AppShell } from "../../components/AppShell";
import { JobDetailPage } from "./JobDetailPage";
import { api } from "../../lib/api";
import { renderWithRouter } from "../../test/test-utils";
import type { JobRecord } from "../../lib/types";

vi.mock("../../lib/api", () => ({
  api: {
    getJob: vi.fn(),
    getJobLogs: vi.fn(),
    getJobArtifacts: vi.fn(),
    getJobReviewTable: vi.fn(),
    updateJobReviewTable: vi.fn(),
    getJobChangeSet: vi.fn(),
    runJob: vi.fn(),
    cancelJob: vi.fn(),
    applyJob: vi.fn(),
    rollbackJob: vi.fn(),
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
    rollback_job_id: null,
    error_message: null,
    canOpen: false,
    canApply: false,
    artifacts: { directory: "output/internal_persons", csv: [], json: [] },
    results: {
      entity_label: "persons",
      found_label: "Persons found",
      ready_label: "Persons ready to update",
      updated_label: "Persons updated",
      found_count: 3,
      ready_count: 2,
      updated_count: 1,
    },
    ...overrides,
  };
}

describe("JobDetailPage", () => {
  beforeEach(() => {
    mockedApi.getJobReviewTable.mockResolvedValue(null);
  });

  afterEach(() => {
    vi.resetAllMocks();
    vi.unstubAllGlobals();
  });

  it("renders job detail and logs", async () => {
    const user = userEvent.setup();
    mockedApi.getJobChangeSet.mockResolvedValue(null);
    mockedApi.getJob.mockResolvedValue(makeJob({ status: "needs_review", canOpen: true, canApply: true }));
    mockedApi.getJobLogs.mockResolvedValue({
      jobId: "job-001",
      logPath: "logs/jobs/job-001.log",
      content: "done",
    });
    mockedApi.getJobArtifacts.mockResolvedValue({
      jobId: "job-001",
      directory: "output/internal_persons",
      items: [
        {
          name: "personstobeupdated_20260420.csv",
          kind: "csv",
          size_bytes: 512,
          download_path: "/api/jobs/job-001/artifacts/personstobeupdated_20260420.csv",
        },
      ],
    });
    mockedApi.getJobReviewTable.mockResolvedValue({
      jobId: "job-001",
      fileName: "personstobeupdated_20260420.csv",
      columns: ["to_be_updated", "updated", "PURE_UUID_PERS", "FULL_NAME"],
      rows: [
        { _rowIndex: 0, to_be_updated: "X", updated: "", PURE_UUID_PERS: "uuid-1", FULL_NAME: "Alpha" },
        { _rowIndex: 1, to_be_updated: "", updated: "X", PURE_UUID_PERS: "uuid-2", FULL_NAME: "Beta" },
      ],
      rowCount: 2,
      selectionColumn: "to_be_updated",
      editableColumns: ["to_be_updated"],
    });

    renderWithRouter({
      routes: [
        {
          path: "/",
          element: <AppShell />,
          children: [{ path: "jobs/:jobId", element: <JobDetailPage /> }],
        },
      ],
      initialEntries: ["/jobs/job-001"],
    });

    expect(await screen.findByText("job-001")).toBeInTheDocument();
    expect(await screen.findByText("Faculty")).toBeInTheDocument();
    expect(await screen.findByText("All faculties")).toBeInTheDocument();
    expect(await screen.findByText("Download and check these files before applying updates.")).toBeInTheDocument();
    expect(await screen.findByText("personstobeupdated_20260420.csv")).toBeInTheDocument();
    expect((await screen.findAllByText("Persons found")).length).toBeGreaterThan(0);
    expect((await screen.findAllByText("Persons ready to update")).length).toBeGreaterThan(0);
    expect((await screen.findAllByText("Persons updated")).length).toBeGreaterThan(0);
    expect(
      await screen.findByText(
        "How many Pure person records from this faculty run were actually returned in the generated JSON snapshot.",
      ),
    ).toBeInTheDocument();
    expect(await screen.findByText("personstobeupdated_<date>.csv and datatotal.json")).toBeInTheDocument();
    expect((await screen.findAllByRole("link", { name: "Download" }))[0]).toHaveAttribute(
      "href",
      "/api/jobs/job-001/artifacts/personstobeupdated_20260420.csv",
    );
    expect(await screen.findByRole("button", { name: "Show main log (1 lines)" })).toBeInTheDocument();
    expect(screen.queryByText("done")).not.toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "Show main log (1 lines)" }));
    expect(await screen.findByText("done")).toBeInTheDocument();
    expect(await screen.findByRole("heading", { name: "Review Table" })).toBeInTheDocument();
    expect(await screen.findByText("Adjust which rows should be applied.")).toBeInTheDocument();
    expect(screen.getByRole("columnheader", { name: "Selected" })).toBeInTheDocument();
    expect(screen.getByRole("columnheader", { name: "Updated" })).toBeInTheDocument();
    expect(screen.getByRole("columnheader", { name: "Name" })).toBeInTheDocument();
    expect(screen.getByRole("columnheader", { name: "Pure UUID" })).toBeInTheDocument();
    expect(await screen.findByText("Showing 2 of 2 rows.")).toBeInTheDocument();
  });

  it("runs a queued job and refreshes job detail and logs", async () => {
    const user = userEvent.setup();
    mockedApi.getJobChangeSet.mockResolvedValue(null);
    mockedApi.getJob
      .mockResolvedValueOnce(makeJob())
      .mockResolvedValueOnce(
        makeJob({
          status: "needs_review",
          canOpen: true,
          canApply: true,
          log_path: "logs/jobs/job-001.log",
          artifacts: {
            directory: "output/internal_persons",
            csv: ["personstobeupdated_20260420.csv"],
            json: ["datatotal.json"],
          },
        }),
      );
    mockedApi.getJobLogs
      .mockResolvedValueOnce({ jobId: "job-001", logPath: null, content: "" })
      .mockResolvedValue({
        jobId: "job-001",
        logPath: "logs/jobs/job-001.log",
        content: "collected",
      });
    mockedApi.getJobArtifacts
      .mockResolvedValueOnce({ jobId: "job-001", directory: "output/internal_persons", items: [] })
      .mockResolvedValueOnce({
        jobId: "job-001",
        directory: "output/internal_persons",
        items: [
          {
            name: "personstobeupdated_20260420.csv",
            kind: "csv",
            size_bytes: 512,
            download_path: "/api/jobs/job-001/artifacts/personstobeupdated_20260420.csv",
          },
        ],
      });
    mockedApi.runJob.mockResolvedValue(
      makeJob({
        status: "needs_review",
        canOpen: true,
        canApply: true,
        log_path: "logs/jobs/job-001.log",
        artifacts: {
          directory: "output/internal_persons",
          csv: ["personstobeupdated_20260420.csv"],
          json: ["datatotal.json"],
        },
      }),
    );

    renderWithRouter({
      routes: [
        {
          path: "/",
          element: <AppShell />,
          children: [{ path: "jobs/:jobId", element: <JobDetailPage /> }],
        },
      ],
      initialEntries: ["/jobs/job-001"],
    });

    await screen.findByText("Run Job");
    await user.click(screen.getByRole("button", { name: "Run Job" }));

    await waitFor(() => expect(mockedApi.runJob).toHaveBeenCalledWith("job-001"));
    await waitFor(() => expect(mockedApi.getJob).toHaveBeenCalledTimes(2));
    await waitFor(() => expect(mockedApi.getJobLogs).toHaveBeenCalledTimes(3));
    await waitFor(() => expect(mockedApi.getJobArtifacts).toHaveBeenCalledTimes(2));
  });

  it("applies a reviewable job and refreshes job detail and logs", async () => {
    const user = userEvent.setup();
    mockedApi.getJobChangeSet.mockResolvedValue(null);
    mockedApi.getJob
      .mockResolvedValueOnce(
        makeJob({
          status: "needs_review",
          canOpen: true,
          canApply: true,
          log_path: "logs/jobs/job-001.log",
          artifacts: {
            directory: "output/internal_persons",
            csv: ["personstobeupdated_20260420.csv"],
            json: ["datatotal.json"],
          },
        }),
      )
      .mockResolvedValueOnce(
        makeJob({
          status: "completed",
          canOpen: true,
          canApply: true,
          log_path: "logs/jobs/job-001.log",
          finished_at: "2026-04-20T13:30:00Z",
          artifacts: {
            directory: "output/internal_persons",
            csv: ["personstobeupdated_20260420.csv"],
            json: ["datatotal.json"],
          },
        }),
      );
    mockedApi.getJobLogs
      .mockResolvedValueOnce({ jobId: "job-001", logPath: "logs/jobs/job-001.log", content: "review ready" })
      .mockResolvedValue({
        jobId: "job-001",
        logPath: "logs/jobs/job-001.log",
        content: "review ready\napply ok",
      });
    mockedApi.getJobArtifacts
      .mockResolvedValueOnce({
        jobId: "job-001",
        directory: "output/internal_persons",
        items: [
          {
            name: "personstobeupdated_20260420.csv",
            kind: "csv",
            size_bytes: 512,
            download_path: "/api/jobs/job-001/artifacts/personstobeupdated_20260420.csv",
          },
          {
            name: "datatotal.json",
            kind: "json",
            size_bytes: 256,
            download_path: "/api/jobs/job-001/artifacts/datatotal.json",
          },
        ],
      })
      .mockResolvedValueOnce({
        jobId: "job-001",
        directory: "output/internal_persons",
        items: [
          {
            name: "personstobeupdated_20260420.csv",
            kind: "csv",
            size_bytes: 512,
            download_path: "/api/jobs/job-001/artifacts/personstobeupdated_20260420.csv",
          },
          {
            name: "datatotal.json",
            kind: "json",
            size_bytes: 256,
            download_path: "/api/jobs/job-001/artifacts/datatotal.json",
          },
        ],
      });
    mockedApi.applyJob.mockResolvedValue(
      makeJob({
        status: "completed",
        canOpen: true,
        canApply: true,
        log_path: "logs/jobs/job-001.log",
        finished_at: "2026-04-20T13:30:00Z",
        artifacts: {
          directory: "output/internal_persons",
          csv: ["personstobeupdated_20260420.csv"],
          json: ["datatotal.json"],
        },
      }),
    );

    renderWithRouter({
      routes: [
        {
          path: "/",
          element: <AppShell />,
          children: [{ path: "jobs/:jobId", element: <JobDetailPage /> }],
        },
      ],
      initialEntries: ["/jobs/job-001"],
    });

    await screen.findByRole("button", { name: "Apply Updates" });
    await user.click(screen.getByRole("button", { name: "Apply Updates" }));

    await waitFor(() => expect(mockedApi.applyJob).toHaveBeenCalledWith("job-001"));
    await waitFor(() => expect(mockedApi.getJob).toHaveBeenCalledTimes(2));
    await waitFor(() => expect(mockedApi.getJobLogs).toHaveBeenCalledTimes(3));
    await waitFor(() => expect(mockedApi.getJobArtifacts).toHaveBeenCalledTimes(2));
  });

  it("disables apply when no reviewed rows are still selected", async () => {
    mockedApi.getJobChangeSet.mockResolvedValue(null);
    mockedApi.getJob.mockResolvedValue(
      makeJob({
        status: "needs_review",
        canOpen: true,
        canApply: true,
        log_path: "logs/jobs/job-001.log",
        results: {
          entity_label: "persons",
          found_label: "Persons found",
          ready_label: "Persons ready to update",
          updated_label: "Persons updated",
          found_count: 10,
          ready_count: 0,
          updated_count: 0,
        },
      }),
    );
    mockedApi.getJobLogs.mockResolvedValue({
      jobId: "job-001",
      logPath: "logs/jobs/job-001.log",
      content: "review ready",
    });
    mockedApi.getJobArtifacts.mockResolvedValue({
      jobId: "job-001",
      directory: "output/internal_persons",
      items: [
        {
          name: "personstobeupdated_20260420.csv",
          kind: "csv",
          size_bytes: 512,
          download_path: "/api/jobs/job-001/artifacts/personstobeupdated_20260420.csv",
        },
      ],
    });

    renderWithRouter({
      routes: [
        {
          path: "/",
          element: <AppShell />,
          children: [{ path: "jobs/:jobId", element: <JobDetailPage /> }],
        },
      ],
      initialEntries: ["/jobs/job-001"],
    });

    const applyButton = await screen.findByRole("button", { name: "Apply Updates" });

    expect(applyButton).toBeDisabled();
    expect(await screen.findByText("Ready To Apply")).toBeInTheDocument();
    expect(await screen.findByText("No")).toBeInTheDocument();
    expect(
      await screen.findByText(
        "Apply is not possible because persons ready to update is 0. Review the CSV file and keep at least one row selected with X in to_be_updated.",
      ),
    ).toBeInTheDocument();
  });

  it("deletes an inactive job after confirmation", async () => {
    const user = userEvent.setup();
    vi.stubGlobal("confirm", vi.fn(() => true));
    mockedApi.getJobChangeSet.mockResolvedValue(null);
    mockedApi.getJob.mockResolvedValue(
      makeJob({
        status: "failed",
        error_message: "boom",
      }),
    );
    mockedApi.getJobLogs.mockResolvedValue({ jobId: "job-001", logPath: "logs/jobs/job-001.log", content: "boom" });
    mockedApi.getJobArtifacts.mockResolvedValue({ jobId: "job-001", directory: "output/internal_persons", items: [] });
    mockedApi.deleteJob.mockResolvedValue(undefined);

    renderWithRouter({
      routes: [
        {
          path: "/",
          element: <AppShell />,
          children: [
            { index: true, element: <div>Dashboard</div> },
            { path: "jobs/:jobId", element: <JobDetailPage /> },
          ],
        },
      ],
      initialEntries: ["/jobs/job-001"],
    });

    await screen.findByRole("button", { name: "Delete Job" });
    await user.click(screen.getByRole("button", { name: "Delete Job" }));

    await waitFor(() => expect(mockedApi.deleteJob).toHaveBeenCalledWith("job-001"));
    expect(await screen.findByText("Dashboard", { selector: "div" })).toBeInTheDocument();
  });

  it("allows deleting a queued job before it starts", async () => {
    const user = userEvent.setup();
    vi.stubGlobal("confirm", vi.fn(() => true));
    mockedApi.getJobChangeSet.mockResolvedValue(null);
    mockedApi.getJob.mockResolvedValue(
      makeJob({
        status: "queued",
      }),
    );
    mockedApi.getJobLogs.mockResolvedValue({ jobId: "job-001", logPath: null, content: "" });
    mockedApi.getJobArtifacts.mockResolvedValue({ jobId: "job-001", directory: "output/internal_persons", items: [] });
    mockedApi.deleteJob.mockResolvedValue(undefined);

    renderWithRouter({
      routes: [
        {
          path: "/",
          element: <AppShell />,
          children: [
            { index: true, element: <div>Dashboard</div> },
            { path: "jobs/:jobId", element: <JobDetailPage /> },
          ],
        },
      ],
      initialEntries: ["/jobs/job-001"],
    });

    await screen.findByRole("button", { name: "Delete Job" });
    await user.click(screen.getByRole("button", { name: "Delete Job" }));

    await waitFor(() => expect(mockedApi.deleteJob).toHaveBeenCalledWith("job-001"));
  });

  it("allows stopping a running job", async () => {
    const user = userEvent.setup();
    vi.stubGlobal("confirm", vi.fn(() => true));
    mockedApi.getJobChangeSet.mockResolvedValue(null);
    mockedApi.getJob
      .mockResolvedValueOnce(
        makeJob({
          status: "running",
          log_path: "logs/jobs/job-001.log",
        }),
      )
      .mockResolvedValueOnce(
        makeJob({
          status: "failed",
          log_path: "logs/jobs/job-001.log",
          error_message: "Job was cancelled by user",
        }),
      );
    mockedApi.getJobLogs.mockResolvedValue({ jobId: "job-001", logPath: "logs/jobs/job-001.log", content: "" });
    mockedApi.getJobArtifacts.mockResolvedValue({ jobId: "job-001", directory: "output/internal_persons", items: [] });
    mockedApi.cancelJob.mockResolvedValue(
      makeJob({
        status: "failed",
        log_path: "logs/jobs/job-001.log",
        error_message: "Job was cancelled by user",
      }),
    );

    renderWithRouter({
      routes: [
        {
          path: "/",
          element: <AppShell />,
          children: [{ path: "jobs/:jobId", element: <JobDetailPage /> }],
        },
      ],
      initialEntries: ["/jobs/job-001"],
    });

    await screen.findByRole("button", { name: "Stop Job" });
    await user.click(screen.getByRole("button", { name: "Stop Job" }));

    await waitFor(() => expect(mockedApi.cancelJob).toHaveBeenCalledWith("job-001"));
  });

  it("shows rollback details and starts rollback for a completed internal persons job", async () => {
    const user = userEvent.setup();
    mockedApi.getJob
      .mockResolvedValueOnce(
        makeJob({
          status: "completed",
          canOpen: true,
          canApply: false,
          log_path: "logs/jobs/job-001.log",
        }),
      )
      .mockResolvedValueOnce(
        makeJob({
          status: "completed",
          canOpen: true,
          canApply: false,
          log_path: "logs/jobs/job-001.log",
          rollback_job_id: "rollback-001",
        }),
      )
      .mockResolvedValueOnce(
        makeJob({
          id: "rollback-001",
          status: "completed",
          canOpen: false,
          canApply: false,
          log_path: "logs/jobs/rollback-001.log",
          results: {
            entity_label: "persons",
            found_label: "Persons found",
            ready_label: "Persons ready to update",
            updated_label: "Persons updated",
            found_count: null,
            ready_count: 0,
            updated_count: 0,
          },
        }),
      );
    mockedApi.getJobLogs.mockImplementation(async (jobId: string) => ({
      jobId,
      logPath: `logs/jobs/${jobId}.log`,
      content: jobId === "rollback-001" ? "ROLLED BACK pers-1 orcid" : "apply ok",
    }));
    mockedApi.getJobArtifacts.mockResolvedValue({
      jobId: "job-001",
      directory: "output/internal_persons",
      items: [],
    });
    mockedApi.getJobChangeSet
      .mockResolvedValueOnce({
        id: "changeset-001",
        job_id: "job-001",
        job_type: "internal_persons",
        status: "applied",
        created_at: "2026-04-20T13:10:00Z",
        applied_at: "2026-04-20T13:30:00Z",
        item_count: 2,
        applied_item_count: 2,
        items: [
          {
            id: 1,
            change_set_id: "changeset-001",
            item_key: "pers-1|orcid|0000-0001",
            entity_uuid: "pers-1",
            entity_label: "Alpha",
            field_name: "orcid",
            identifier_type: "orcid",
            old_value: "0000-0000",
            new_value: { value: "0000-0001" },
            apply_status: "applied",
            rollback_status: "pending",
            conflict_reason: null,
          },
          {
            id: 2,
            change_set_id: "changeset-001",
            item_key: "pers-2|orcid|0000-0002",
            entity_uuid: "pers-2",
            entity_label: "Beta",
            field_name: "orcid",
            identifier_type: "orcid",
            old_value: "0000-0009",
            new_value: { value: "0000-0002" },
            apply_status: "applied",
            rollback_status: "pending",
            conflict_reason: null,
          },
        ],
      })
      .mockResolvedValueOnce({
        id: "changeset-001",
        job_id: "job-001",
        job_type: "internal_persons",
        status: "rolled_back_with_conflicts",
        created_at: "2026-04-20T13:10:00Z",
        applied_at: "2026-04-20T13:30:00Z",
        item_count: 2,
        applied_item_count: 2,
        items: [
          {
            id: 1,
            change_set_id: "changeset-001",
            item_key: "pers-1|orcid|0000-0001",
            entity_uuid: "pers-1",
            entity_label: "Alpha",
            field_name: "orcid",
            identifier_type: "orcid",
            old_value: "0000-0000",
            new_value: { value: "0000-0001" },
            apply_status: "applied",
            rollback_status: "rolled_back",
            conflict_reason: null,
          },
          {
            id: 2,
            change_set_id: "changeset-001",
            item_key: "pers-2|orcid|0000-0002",
            entity_uuid: "pers-2",
            entity_label: "Beta",
            field_name: "orcid",
            identifier_type: "orcid",
            old_value: "0000-0009",
            new_value: { value: "0000-0002" },
            apply_status: "applied",
            rollback_status: "conflict",
            conflict_reason: "changed",
          },
        ],
      });
    mockedApi.rollbackJob.mockResolvedValue(
      makeJob({
        id: "rollback-001",
        status: "completed",
        canOpen: false,
        canApply: false,
        log_path: "logs/jobs/rollback-001.log",
      }),
    );

    renderWithRouter({
      routes: [
        {
          path: "/",
          element: <AppShell />,
          children: [{ path: "jobs/:jobId", element: <JobDetailPage /> }],
        },
      ],
      initialEntries: ["/jobs/job-001"],
    });

    expect(await screen.findByText("Rollbackable changes")).toBeInTheDocument();
    expect(await screen.findByRole("button", { name: "Rollback Updates" })).toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "Rollback Updates" }));

    await waitFor(() => expect(mockedApi.rollbackJob).toHaveBeenCalledWith("job-001"));
    expect(await screen.findByRole("heading", { name: "Rollback Log" })).toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "Show rollback log (1 lines)" }));
    expect(await screen.findByText("ROLLED BACK pers-1 orcid")).toBeInTheDocument();
    expect(await screen.findByText("Conflicts skipped")).toBeInTheDocument();
    expect(await screen.findByText("Some updates were skipped because the current Pure value no longer matched what this job applied.")).toBeInTheDocument();
  });

  it("explains why rollback is unavailable when nothing was applied successfully", async () => {
    mockedApi.getJob.mockResolvedValue(
      makeJob({
        status: "completed",
        canOpen: true,
        canApply: false,
        log_path: "logs/jobs/job-001.log",
      }),
    );
    mockedApi.getJobLogs.mockResolvedValue({
      jobId: "job-001",
      logPath: "logs/jobs/job-001.log",
      content: "apply failed",
    });
    mockedApi.getJobArtifacts.mockResolvedValue({
      jobId: "job-001",
      directory: "output/internal_persons",
      items: [],
    });
    mockedApi.getJobChangeSet.mockResolvedValue({
      id: "changeset-002",
      job_id: "job-001",
      job_type: "internal_persons",
      status: "no_applied_changes",
      created_at: "2026-04-20T13:10:00Z",
      applied_at: "2026-04-20T13:30:00Z",
      item_count: 1,
      applied_item_count: 0,
      items: [
        {
          id: 1,
          change_set_id: "changeset-002",
          item_key: "pers-1|orcid|0000-0001",
          entity_uuid: "pers-1",
          entity_label: "Alpha",
          field_name: "orcid",
          identifier_type: "orcid",
          old_value: null,
          new_value: { value: "0000-0001" },
          apply_status: "not_applied",
          rollback_status: "pending",
          conflict_reason: null,
        },
      ],
    });

    renderWithRouter({
      routes: [
        {
          path: "/",
          element: <AppShell />,
          children: [{ path: "jobs/:jobId", element: <JobDetailPage /> }],
        },
      ],
      initialEntries: ["/jobs/job-001"],
    });

    expect(await screen.findByText("Rollback is not available because 0 changes were applied successfully.")).toBeInTheDocument();
    expect(screen.queryByRole("button", { name: "Rollback Updates" })).not.toBeInTheDocument();
  });

  it("shows rollback section for external persons jobs even when no rollback data exists yet", async () => {
    mockedApi.getJob.mockResolvedValue(
      makeJob({
        job_type: "external_persons",
        artifact_dir: "output/external_persons/job-456",
        artifacts: { directory: "output/external_persons/job-456", csv: ["ext_pers_update.csv"], json: ["to_be_updated.json"] },
        status: "completed",
        canOpen: true,
        canApply: false,
        log_path: "logs/jobs/job-456.log",
      }),
    );
    mockedApi.getJobLogs.mockResolvedValue({
      jobId: "job-456",
      logPath: "logs/jobs/job-456.log",
      content: "apply ok",
    });
    mockedApi.getJobArtifacts.mockResolvedValue({
      jobId: "job-456",
      directory: "output/external_persons/job-456",
      items: [],
    });
    mockedApi.getJobChangeSet.mockResolvedValue(null);

    renderWithRouter({
      routes: [
        {
          path: "/",
          element: <AppShell />,
          children: [{ path: "jobs/:jobId", element: <JobDetailPage /> }],
        },
      ],
      initialEntries: ["/jobs/job-456"],
    });

    expect(await screen.findByText("job-001")).toBeInTheDocument();
    expect(await screen.findByRole("heading", { name: "Rollback" })).toBeInTheDocument();
    expect(await screen.findByText("No rollback data is available for this job.")).toBeInTheDocument();
    expect(screen.queryByRole("button", { name: "Rollback Updates" })).not.toBeInTheDocument();
  });

  it("shows rollback section for external organisations jobs even when no rollback data exists yet", async () => {
    mockedApi.getJob.mockResolvedValue(
      makeJob({
        job_type: "external_orgs",
        artifact_dir: "output/external_orgs/job-789",
        artifacts: {
          directory: "output/external_orgs/job-789",
          csv: ["external_orgs_to_update.csv"],
          json: ["external_orgs_updates.json"],
        },
        results: {
          entity_label: "organisations",
          found_label: "Organisations found",
          ready_label: "Organisations ready to update",
          updated_label: "Organisations updated",
          rolled_back_label: "Organisations rolled back",
          found_count: 2,
          ready_count: 1,
          updated_count: 0,
          rolled_back_count: 0,
        },
        status: "completed",
        canOpen: true,
        canApply: false,
        log_path: "logs/jobs/job-789.log",
      }),
    );
    mockedApi.getJobLogs.mockResolvedValue({
      jobId: "job-789",
      logPath: "logs/jobs/job-789.log",
      content: "apply ok",
    });
    mockedApi.getJobArtifacts.mockResolvedValue({
      jobId: "job-789",
      directory: "output/external_orgs/job-789",
      items: [],
    });
    mockedApi.getJobChangeSet.mockResolvedValue(null);

    renderWithRouter({
      routes: [
        {
          path: "/",
          element: <AppShell />,
          children: [{ path: "jobs/:jobId", element: <JobDetailPage /> }],
        },
      ],
      initialEntries: ["/jobs/job-789"],
    });

    expect(await screen.findByText("job-001")).toBeInTheDocument();
    expect(await screen.findByRole("heading", { name: "Rollback" })).toBeInTheDocument();
    expect(await screen.findByText("No rollback data is available for this job.")).toBeInTheDocument();
    expect(
      await screen.findByText(
        "An organisation appears here only when Pure is still missing the matched ROR, the address or geo data differs from the matched source, or both.",
      ),
    ).toBeInTheDocument();
  });

  it("shows rolled back results on the original job after rollback", async () => {
    mockedApi.getJob.mockResolvedValue(
      makeJob({
        status: "completed",
        canOpen: true,
        canApply: false,
        log_path: "logs/jobs/job-001.log",
        rollback_job_id: "rollback-001",
        results: {
          entity_label: "persons",
          found_label: "Persons found",
          ready_label: "Persons ready to update",
          updated_label: "Persons updated",
          rolled_back_label: "Persons rolled back",
          found_count: 1,
          ready_count: 0,
          updated_count: 0,
          rolled_back_count: 1,
        },
      }),
    );
    mockedApi.getJobLogs.mockResolvedValue({
      jobId: "job-001",
      logPath: "logs/jobs/job-001.log",
      content: "apply ok",
    });
    mockedApi.getJobArtifacts.mockResolvedValue({
      jobId: "job-001",
      directory: "output/internal_persons",
      items: [],
    });
    mockedApi.getJobChangeSet.mockResolvedValue(null);

    renderWithRouter({
      routes: [
        {
          path: "/",
          element: <AppShell />,
          children: [{ path: "jobs/:jobId", element: <JobDetailPage /> }],
        },
      ],
      initialEntries: ["/jobs/job-001"],
    });

    expect(await screen.findByText("Persons rolled back")).toBeInTheDocument();
    expect((await screen.findAllByText("0")).length).toBeGreaterThan(0);
    expect((await screen.findAllByText("1")).length).toBeGreaterThan(0);
  });

  it("allows retrying rollback after a failed rollback job when changes remain", async () => {
    mockedApi.getJob
      .mockResolvedValueOnce(
        makeJob({
          job_type: "external_persons",
          id: "job-456",
          artifact_dir: "output/external_persons/job-456",
          artifacts: { directory: "output/external_persons/job-456", csv: ["ext_pers_update.csv"], json: ["to_be_updated.json"] },
          status: "completed",
          canOpen: true,
          canApply: false,
          log_path: "logs/jobs/job-456.log",
          rollback_job_id: "rollback-456",
        }),
      )
      .mockResolvedValueOnce(
        makeJob({
          job_type: "external_persons",
          id: "rollback-456",
          artifact_dir: "output/external_persons/job-456",
          artifacts: { directory: "output/external_persons/job-456", csv: [], json: [] },
          status: "failed",
          canOpen: false,
          canApply: false,
          log_path: "logs/jobs/rollback-456.log",
        }),
      );
    mockedApi.getJobLogs.mockResolvedValue({
      jobId: "job-456",
      logPath: "logs/jobs/job-456.log",
      content: "apply ok",
    });
    mockedApi.getJobArtifacts.mockResolvedValue({
      jobId: "job-456",
      directory: "output/external_persons/job-456",
      items: [],
    });
    mockedApi.getJobChangeSet.mockResolvedValue({
      id: "changeset-456",
      job_id: "job-456",
      job_type: "external_persons",
      status: "rolled_back_with_conflicts",
      created_at: "2026-04-20T13:10:00Z",
      applied_at: "2026-04-20T13:30:00Z",
      item_count: 2,
      applied_item_count: 2,
      items: [
        {
          id: 1,
          change_set_id: "changeset-456",
          item_key: "ext-1|orcid|0000-0001",
          entity_uuid: "ext-1",
          entity_label: "Alpha",
          field_name: "identifiers",
          identifier_type: "orcid",
          old_value: null,
          new_value: { id: "0000-0001", uri: "orcid-uri" },
          apply_status: "applied",
          rollback_status: "rolled_back",
          conflict_reason: null,
        },
        {
          id: 2,
          change_set_id: "changeset-456",
          item_key: "ext-1|openalex|A1",
          entity_uuid: "ext-1",
          entity_label: "Alpha",
          field_name: "identifiers",
          identifier_type: "openalex",
          old_value: null,
          new_value: { id: "A1", uri: "openalex-uri" },
          apply_status: "applied",
          rollback_status: "failed",
          conflict_reason: "409 conflict",
        },
      ],
    });

    renderWithRouter({
      routes: [
        {
          path: "/",
          element: <AppShell />,
          children: [{ path: "jobs/:jobId", element: <JobDetailPage /> }],
        },
      ],
      initialEntries: ["/jobs/job-456"],
    });

    expect(await screen.findByRole("button", { name: "Rollback Updates" })).toBeInTheDocument();
    expect(
      await screen.findByText("Previous rollback failed in rollback-456. You can retry the remaining changes."),
    ).toBeInTheDocument();
  });

  it("shows latest external persons apply error near the top", async () => {
    mockedApi.getJob.mockResolvedValue(
      makeJob({
        job_type: "external_persons",
        artifact_dir: "output/external_persons/job-456",
        artifacts: { directory: "output/external_persons/job-456", csv: ["ext_pers_update.csv"], json: ["to_be_updated.json"] },
        status: "completed",
        canOpen: true,
        canApply: false,
        log_path: "logs/jobs/job-456.log",
      }),
    );
    mockedApi.getJobLogs.mockResolvedValue({
      jobId: "job-456",
      logPath: "logs/jobs/job-456.log",
      content: "WARNING Failed to update data for UUID ext-1: bad request",
    });
    mockedApi.getJobArtifacts.mockResolvedValue({
      jobId: "job-456",
      directory: "output/external_persons/job-456",
      items: [],
    });
    mockedApi.getJobChangeSet.mockResolvedValue(null);

    renderWithRouter({
      routes: [
        {
          path: "/",
          element: <AppShell />,
          children: [{ path: "jobs/:jobId", element: <JobDetailPage /> }],
        },
      ],
      initialEntries: ["/jobs/job-456"],
    });

    expect(await screen.findByText("Latest Error")).toBeInTheDocument();
    expect(await screen.findByText("Failed to update data for UUID ext-1: bad request")).toBeInTheDocument();
  });

  it("shows internal person ORCID validation errors in plain language", async () => {
    mockedApi.getJobChangeSet.mockResolvedValue(null);
    mockedApi.getJob.mockResolvedValue(makeJob({ status: "completed", canOpen: true, canApply: false, log_path: "logs/jobs/job-456.log" }));
    mockedApi.getJobLogs.mockResolvedValue({
      jobId: "job-456",
      logPath: "logs/jobs/job-456.log",
      content:
        'ERROR Failed to update person UUID: 1f090d3d-6c91-42b7-8606-f981bf271aa3, Response: {"type":"/validation-error","title":"Validation failed","status":400,"detail":"Validation of content \'Person(id=243545795, name=T.K. Košir)\' failed: \\nValidation errors:\\n\\torcid: ValidationResult [code=validation.orcid.invalid.format, args="}',
    });
    mockedApi.getJobArtifacts.mockResolvedValue({
      jobId: "job-456",
      directory: "output/internal_persons/job-456",
      items: [],
    });

    renderWithRouter({
      routes: [
        {
          path: "/",
          element: <AppShell />,
          children: [{ path: "jobs/:jobId", element: <JobDetailPage /> }],
        },
      ],
      initialEntries: ["/jobs/job-456"],
    });

    expect(await screen.findByText("Latest Error")).toBeInTheDocument();
    expect(
      await screen.findByText(
        "Pure rejected the ORCID for T.K. Košir because the format is invalid. Check the ORCID value in the review table before applying again.",
      ),
    ).toBeInTheDocument();
  });

  it("shows duplicate DOI errors in plain language", async () => {
    mockedApi.getJobChangeSet.mockResolvedValue(null);
    mockedApi.getJob.mockResolvedValue(
      makeJob({
        job_type: "research_outputs",
        status: "completed",
        canOpen: true,
        canApply: false,
        log_path: "logs/jobs/job-789.log",
      }),
    );
    mockedApi.getJobLogs.mockResolvedValue({
      jobId: "job-789",
      logPath: "logs/jobs/job-789.log",
      content:
        'ERROR Error updating UUID out-1, Response: {"detail":"Validation failed: doi already exists in Pure for this record"}',
    });
    mockedApi.getJobArtifacts.mockResolvedValue({
      jobId: "job-789",
      directory: "output/research_output/job-789",
      items: [],
    });

    renderWithRouter({
      routes: [
        {
          path: "/",
          element: <AppShell />,
          children: [{ path: "jobs/:jobId", element: <JobDetailPage /> }],
        },
      ],
      initialEntries: ["/jobs/job-789"],
    });

    expect(await screen.findByText("Latest Error")).toBeInTheDocument();
    expect(
      await screen.findByText(
        "Pure rejected a DOI because it already exists. Check whether this record is already present in Pure.",
      ),
    ).toBeInTheDocument();
  });

  it("filters the review table rows in read-only mode", async () => {
    const user = userEvent.setup();
    mockedApi.getJobChangeSet.mockResolvedValue(null);
    mockedApi.getJob.mockResolvedValue(makeJob({ status: "needs_review", canOpen: true, canApply: true }));
    mockedApi.getJobLogs.mockResolvedValue({
      jobId: "job-001",
      logPath: "logs/jobs/job-001.log",
      content: "",
    });
    mockedApi.getJobArtifacts.mockResolvedValue({
      jobId: "job-001",
      directory: "output/internal_persons",
      items: [],
    });
    mockedApi.getJobReviewTable.mockResolvedValue({
      jobId: "job-001",
      fileName: "personstobeupdated_20260420.csv",
      columns: ["to_be_updated", "FULL_NAME"],
      rows: [
        { _rowIndex: 0, to_be_updated: "X", FULL_NAME: "Alpha" },
        { _rowIndex: 1, to_be_updated: "", FULL_NAME: "Beta" },
      ],
      rowCount: 2,
      selectionColumn: "to_be_updated",
      editableColumns: ["to_be_updated"],
    });

    renderWithRouter({
      routes: [
        {
          path: "/",
          element: <AppShell />,
          children: [{ path: "jobs/:jobId", element: <JobDetailPage /> }],
        },
      ],
      initialEntries: ["/jobs/job-001"],
    });

    expect(await screen.findByText("Showing 2 of 2 rows.")).toBeInTheDocument();
    await user.type(screen.getByRole("searchbox"), "Alpha");
    expect(await screen.findByText("Showing 1 of 2 rows.")).toBeInTheDocument();
    expect(screen.getByText("Alpha")).toBeInTheDocument();
    expect(screen.queryByText("Beta")).not.toBeInTheDocument();
  });

  it("shows parsed FULL_NAME values in the review table", async () => {
    mockedApi.getJobChangeSet.mockResolvedValue(null);
    mockedApi.getJob.mockResolvedValue(makeJob({ status: "needs_review", canOpen: true, canApply: true }));
    mockedApi.getJobLogs.mockResolvedValue({
      jobId: "job-001",
      logPath: "logs/jobs/job-001.log",
      content: "",
    });
    mockedApi.getJobArtifacts.mockResolvedValue({
      jobId: "job-001",
      directory: "output/internal_persons",
      items: [],
    });
    mockedApi.getJobReviewTable.mockResolvedValue({
      jobId: "job-001",
      fileName: "personstobeupdated_20260420.csv",
      columns: ["FULL_NAME"],
      rows: [
        {
          _rowIndex: 0,
          FULL_NAME:
            "S. Marjolein van Cappellen#0030d86e-84cb-4655-ae25-f87d6c6af193 | van Cappellen, S.M.#0030d86e-84cb-4655-ae25-f87d6c6af193",
        },
      ],
      rowCount: 1,
      selectionColumn: null,
      editableColumns: [],
    });

    renderWithRouter({
      routes: [
        {
          path: "/",
          element: <AppShell />,
          children: [{ path: "jobs/:jobId", element: <JobDetailPage /> }],
        },
      ],
      initialEntries: ["/jobs/job-001"],
    });

    expect(
      await screen.findByText("S. Marjolein van Cappellen | van Cappellen, S.M."),
    ).toBeInTheDocument();
  });

  it("hides person_id and uri from the review table", async () => {
    mockedApi.getJobChangeSet.mockResolvedValue(null);
    mockedApi.getJob.mockResolvedValue(makeJob({ status: "needs_review", canOpen: true, canApply: true }));
    mockedApi.getJobLogs.mockResolvedValue({
      jobId: "job-001",
      logPath: "logs/jobs/job-001.log",
      content: "",
    });
    mockedApi.getJobArtifacts.mockResolvedValue({
      jobId: "job-001",
      directory: "output/internal_persons",
      items: [],
    });
    mockedApi.getJobReviewTable.mockResolvedValue({
      jobId: "job-001",
      fileName: "personstobeupdated_20260420.csv",
      columns: ["FULL_NAME", "person_id", "uri", "PURE_UUID_PERS"],
      rows: [
        {
          _rowIndex: 0,
          FULL_NAME: "Alpha",
          person_id: "person-1",
          uri: "https://example.org/value",
          PURE_UUID_PERS: "uuid-1",
        },
      ],
      rowCount: 1,
      selectionColumn: null,
      editableColumns: [],
    });

    renderWithRouter({
      routes: [
        {
          path: "/",
          element: <AppShell />,
          children: [{ path: "jobs/:jobId", element: <JobDetailPage /> }],
        },
      ],
      initialEntries: ["/jobs/job-001"],
    });

    expect(await screen.findByText("Alpha")).toBeInTheDocument();
    expect(screen.queryByRole("columnheader", { name: "person_id" })).not.toBeInTheDocument();
    expect(screen.queryByRole("columnheader", { name: "uri" })).not.toBeInTheDocument();
    expect(screen.queryByText("person-1")).not.toBeInTheDocument();
    expect(screen.queryByText("https://example.org/value")).not.toBeInTheDocument();
    expect(screen.getByRole("columnheader", { name: "Name" })).toBeInTheDocument();
    expect(screen.getByRole("columnheader", { name: "Pure UUID" })).toBeInTheDocument();
  });

  it("supports selecting and deselecting the visible filtered rows", async () => {
    const user = userEvent.setup();
    mockedApi.getJobChangeSet.mockResolvedValue(null);
    mockedApi.getJob.mockResolvedValue(makeJob({ status: "needs_review", canOpen: true, canApply: true }));
    mockedApi.getJobLogs.mockResolvedValue({
      jobId: "job-001",
      logPath: "logs/jobs/job-001.log",
      content: "",
    });
    mockedApi.getJobArtifacts.mockResolvedValue({
      jobId: "job-001",
      directory: "output/internal_persons",
      items: [],
    });
    mockedApi.getJobReviewTable.mockResolvedValue({
      jobId: "job-001",
      fileName: "personstobeupdated_20260420.csv",
      columns: ["to_be_updated", "FULL_NAME"],
      rows: [
        { _rowIndex: 0, to_be_updated: "", FULL_NAME: "Alpha" },
        { _rowIndex: 1, to_be_updated: "", FULL_NAME: "Beta" },
      ],
      rowCount: 2,
      selectionColumn: "to_be_updated",
      editableColumns: ["to_be_updated"],
    });

    renderWithRouter({
      routes: [
        {
          path: "/",
          element: <AppShell />,
          children: [{ path: "jobs/:jobId", element: <JobDetailPage /> }],
        },
      ],
      initialEntries: ["/jobs/job-001"],
    });

    await screen.findByText("Showing 2 of 2 rows.");
    await user.type(screen.getByRole("searchbox"), "Alpha");
    expect(await screen.findByText("Showing 1 of 2 rows.")).toBeInTheDocument();

    await user.click(screen.getByRole("button", { name: "Select Filtered Rows" }));
    expect(await screen.findByText("1 row changed")).toBeInTheDocument();
    expect(screen.getByRole("checkbox", { name: "Select row 1" })).toBeChecked();
    expect(screen.queryByRole("checkbox", { name: "Select row 2" })).not.toBeInTheDocument();

    await user.click(screen.getByRole("button", { name: "Deselect Filtered Rows" }));
    expect(await screen.findByText("No unsaved review changes")).toBeInTheDocument();
    expect(screen.getByRole("checkbox", { name: "Select row 1" })).not.toBeChecked();
  });

  it("shows a clear empty state for empty dataset review tables", async () => {
    mockedApi.getJobChangeSet.mockResolvedValue(null);
    mockedApi.getJob.mockResolvedValue(
      makeJob({
        job_type: "datasets",
        status: "needs_review",
        canOpen: true,
        canApply: false,
        artifact_dir: "output/datasets/job-001",
        artifacts: { directory: "output/datasets/job-001", csv: ["to_be_updated.csv"], json: ["datasets_to_be_updated.json"] },
        results: {
          entity_label: "datasets",
          found_label: "Datasets found",
          ready_label: "Datasets ready to update",
          updated_label: "Datasets updated",
          found_count: 0,
          ready_count: 0,
          updated_count: 0,
        },
      }),
    );
    mockedApi.getJobLogs.mockResolvedValue({
      jobId: "job-001",
      logPath: "logs/jobs/job-001.log",
      content: "",
    });
    mockedApi.getJobArtifacts.mockResolvedValue({
      jobId: "job-001",
      directory: "output/datasets/job-001",
      items: [],
    });
    mockedApi.getJobReviewTable.mockResolvedValue({
      jobId: "job-001",
      fileName: "to_be_updated.csv",
      columns: ["to_be_updated", "updated", "doi", "title"],
      rows: [],
      rowCount: 0,
      selectionColumn: "to_be_updated",
      editableColumns: ["to_be_updated"],
    });

    renderWithRouter({
      routes: [
        {
          path: "/",
          element: <AppShell />,
          children: [{ path: "jobs/:jobId", element: <JobDetailPage /> }],
        },
      ],
      initialEntries: ["/jobs/job-001"],
    });

    expect(await screen.findByText("to_be_updated.csv with 0 rows")).toBeInTheDocument();
    expect(
      await screen.findByText("This review file is empty, so there is nothing to preview or select for this job."),
    ).toBeInTheDocument();
    expect(screen.queryByRole("columnheader", { name: "Selected" })).not.toBeInTheDocument();
    expect(screen.queryByRole("button", { name: "Select Filtered Rows" })).not.toBeInTheDocument();
  });

  it("virtualizes large review tables and reveals later rows on scroll", async () => {
    mockedApi.getJobChangeSet.mockResolvedValue(null);
    mockedApi.getJob.mockResolvedValue(makeJob({ status: "needs_review", canOpen: true, canApply: true }));
    mockedApi.getJobLogs.mockResolvedValue({
      jobId: "job-001",
      logPath: "logs/jobs/job-001.log",
      content: "",
    });
    mockedApi.getJobArtifacts.mockResolvedValue({
      jobId: "job-001",
      directory: "output/internal_persons",
      items: [],
    });
    mockedApi.getJobReviewTable.mockResolvedValue({
      jobId: "job-001",
      fileName: "personstobeupdated_20260420.csv",
      columns: ["to_be_updated", "FULL_NAME"],
      rows: Array.from({ length: 160 }, (_, index) => ({
        _rowIndex: index,
        to_be_updated: index % 2 === 0 ? "X" : "",
        FULL_NAME: `Person ${index + 1}`,
      })),
      rowCount: 160,
      selectionColumn: "to_be_updated",
      editableColumns: ["to_be_updated"],
    });

    renderWithRouter({
      routes: [
        {
          path: "/",
          element: <AppShell />,
          children: [{ path: "jobs/:jobId", element: <JobDetailPage /> }],
        },
      ],
      initialEntries: ["/jobs/job-001"],
    });

    expect(await screen.findByText("Showing 160 of 160 rows.")).toBeInTheDocument();
    expect(screen.getByText("Person 1")).toBeInTheDocument();
    expect(screen.queryByText("Person 160")).not.toBeInTheDocument();

    const reviewTableWrapper = document.querySelector(".review-table-wrapper");
    expect(reviewTableWrapper).not.toBeNull();
    fireEvent.scroll(reviewTableWrapper as Element, { target: { scrollTop: 6200 } });

    expect(await screen.findByText("Person 160")).toBeInTheDocument();
    expect(screen.queryByText("Person 1")).not.toBeInTheDocument();
  });

  it("orders review columns for organisations with readable labels", async () => {
    mockedApi.getJobChangeSet.mockResolvedValue(null);
    mockedApi.getJob.mockResolvedValue(
      makeJob({
        job_type: "external_orgs",
        status: "needs_review",
        canOpen: true,
        canApply: true,
      }),
    );
    mockedApi.getJobLogs.mockResolvedValue({
      jobId: "job-001",
      logPath: "logs/jobs/job-001.log",
      content: "",
    });
    mockedApi.getJobArtifacts.mockResolvedValue({
      jobId: "job-001",
      directory: "output/external_orgs",
      items: [],
    });
    mockedApi.getJobReviewTable.mockResolvedValue({
      jobId: "job-001",
      fileName: "external_orgs_to_update.csv",
      columns: ["point", "organization_name", "needs_geo_update", "to_be_updated", "Pure_UUID"],
      rows: [
        {
          _rowIndex: 0,
          point: "52.09,5.12",
          organization_name: "Example Org",
          needs_geo_update: "True",
          to_be_updated: "X",
          Pure_UUID: "uuid-org-1",
        },
      ],
      rowCount: 1,
      selectionColumn: "to_be_updated",
      editableColumns: ["to_be_updated"],
    });

    renderWithRouter({
      routes: [
        {
          path: "/",
          element: <AppShell />,
          children: [{ path: "jobs/:jobId", element: <JobDetailPage /> }],
        },
      ],
      initialEntries: ["/jobs/job-001"],
    });

    await screen.findByText("Example Org");
    const headers = screen.getAllByRole("columnheader").map((header) => header.textContent);
    expect(headers).toEqual(["Selected", "Organisation", "Pure UUID", "Needs Address/Geo", "Geo Point"]);
  });

  it("saves inline review selection changes and refreshes the job", async () => {
    const user = userEvent.setup();
    mockedApi.getJobChangeSet.mockResolvedValue(null);
    mockedApi.getJob
      .mockResolvedValueOnce(
        makeJob({
          status: "needs_review",
          canOpen: true,
          canApply: true,
          results: {
            entity_label: "persons",
            found_label: "Persons found",
            ready_label: "Persons ready to update",
            updated_label: "Persons updated",
            found_count: 2,
            ready_count: 1,
            updated_count: 0,
          },
        }),
      )
      .mockResolvedValueOnce(
        makeJob({
          status: "needs_review",
          canOpen: true,
          canApply: true,
          results: {
            entity_label: "persons",
            found_label: "Persons found",
            ready_label: "Persons ready to update",
            updated_label: "Persons updated",
            found_count: 2,
            ready_count: 2,
            updated_count: 0,
          },
        }),
      );
    mockedApi.getJobLogs.mockResolvedValue({
      jobId: "job-001",
      logPath: "logs/jobs/job-001.log",
      content: "",
    });
    mockedApi.getJobArtifacts.mockResolvedValue({
      jobId: "job-001",
      directory: "output/internal_persons",
      items: [],
    });
    mockedApi.getJobReviewTable
      .mockResolvedValueOnce({
        jobId: "job-001",
        fileName: "personstobeupdated_20260420.csv",
        columns: ["to_be_updated", "FULL_NAME"],
        rows: [
          { _rowIndex: 0, to_be_updated: "X", FULL_NAME: "Alpha" },
          { _rowIndex: 1, to_be_updated: "", FULL_NAME: "Beta" },
        ],
        rowCount: 2,
        selectionColumn: "to_be_updated",
        editableColumns: ["to_be_updated"],
      })
      .mockResolvedValueOnce({
        jobId: "job-001",
        fileName: "personstobeupdated_20260420.csv",
        columns: ["to_be_updated", "FULL_NAME"],
        rows: [
          { _rowIndex: 0, to_be_updated: "X", FULL_NAME: "Alpha" },
          { _rowIndex: 1, to_be_updated: "X", FULL_NAME: "Beta" },
        ],
        rowCount: 2,
        selectionColumn: "to_be_updated",
        editableColumns: ["to_be_updated"],
      });
    mockedApi.updateJobReviewTable.mockResolvedValue({
      jobId: "job-001",
      fileName: "personstobeupdated_20260420.csv",
      columns: ["to_be_updated", "FULL_NAME"],
      rows: [
        { _rowIndex: 0, to_be_updated: "X", FULL_NAME: "Alpha" },
        { _rowIndex: 1, to_be_updated: "X", FULL_NAME: "Beta" },
      ],
      rowCount: 2,
      selectionColumn: "to_be_updated",
      editableColumns: ["to_be_updated"],
    });

    renderWithRouter({
      routes: [
        {
          path: "/",
          element: <AppShell />,
          children: [{ path: "jobs/:jobId", element: <JobDetailPage /> }],
        },
      ],
      initialEntries: ["/jobs/job-001"],
    });

    const betaCheckbox = await screen.findByRole("checkbox", { name: "Select row 2" });
    expect(screen.getByText("No unsaved review changes")).toBeInTheDocument();
    await user.click(betaCheckbox);
    expect(await screen.findByText("1 row changed")).toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "Save Review" }));

    await waitFor(() =>
      expect(mockedApi.updateJobReviewTable).toHaveBeenCalledWith("job-001", {
        fileName: "personstobeupdated_20260420.csv",
        expectedRowCount: 2,
        updates: [{ rowIndex: 1, to_be_updated: "X" }],
      }),
    );
    await waitFor(() => expect(mockedApi.getJob).toHaveBeenCalledTimes(2));
    await waitFor(() => expect(mockedApi.getJobReviewTable).toHaveBeenCalledTimes(2));
  });
});
