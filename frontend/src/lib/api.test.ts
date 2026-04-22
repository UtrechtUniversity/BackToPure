import { ApiError, api } from "./api";

describe("api client", () => {
  afterEach(() => {
    vi.restoreAllMocks();
  });

  it("parses successful JSON responses", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn().mockResolvedValue({
        ok: true,
        json: async () => ({ items: [] }),
      }),
    );

    const result = await api.getJobs();

    expect(result).toEqual({ items: [] });
    expect(fetch).toHaveBeenCalledWith(
      "/api/jobs",
      expect.objectContaining({
        headers: { "Content-Type": "application/json" },
      }),
    );
  });

  it("throws ApiError for backend error responses", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn().mockResolvedValue({
        ok: false,
        status: 400,
        json: async () => ({ error: "bad request" }),
      }),
    );

    await expect(api.createJob({ jobType: "internal_persons", params: {} })).rejects.toEqual(
      expect.objectContaining<ApiError>({
        name: "ApiError",
        message: "bad request",
        status: 400,
      }),
    );
  });

  it("posts to the apply endpoint", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn().mockResolvedValue({
        ok: true,
        json: async () => ({ id: "job-001", status: "completed" }),
      }),
    );

    await api.applyJob("job-001");

    expect(fetch).toHaveBeenCalledWith(
      "/api/jobs/job-001/apply",
      expect.objectContaining({
        method: "POST",
        headers: { "Content-Type": "application/json" },
      }),
    );
  });

  it("loads job artifacts", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn().mockResolvedValue({
        ok: true,
        json: async () => ({ jobId: "job-001", directory: "output/internal_persons", items: [] }),
      }),
    );

    const result = await api.getJobArtifacts("job-001");

    expect(result).toEqual({ jobId: "job-001", directory: "output/internal_persons", items: [] });
    expect(fetch).toHaveBeenCalledWith(
      "/api/jobs/job-001/artifacts",
      expect.objectContaining({
        headers: { "Content-Type": "application/json" },
      }),
    );
  });

  it("loads a job review table and returns null for 404", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn()
        .mockResolvedValueOnce({
          ok: true,
          json: async () => ({ jobId: "job-001", fileName: "review.csv", columns: [], rows: [], rowCount: 0 }),
        })
        .mockResolvedValueOnce({
          ok: false,
          status: 404,
          json: async () => ({ error: "not found" }),
        }),
    );

    await expect(api.getJobReviewTable("job-001")).resolves.toEqual({
      jobId: "job-001",
      fileName: "review.csv",
      columns: [],
      rows: [],
      rowCount: 0,
    });
    await expect(api.getJobReviewTable("job-002")).resolves.toBeNull();
  });

  it("posts review table selection updates", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn().mockResolvedValue({
        ok: true,
        json: async () => ({ jobId: "job-001", fileName: "review.csv", columns: [], rows: [], rowCount: 0 }),
      }),
    );

    await api.updateJobReviewTable("job-001", {
      fileName: "review.csv",
      expectedRowCount: 2,
      updates: [{ rowIndex: 1, to_be_updated: "X" }],
    });

    expect(fetch).toHaveBeenCalledWith(
      "/api/jobs/job-001/review-table",
      expect.objectContaining({
        method: "POST",
        headers: { "Content-Type": "application/json" },
      }),
    );
  });

  it("loads a job change set and returns null for 404", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn()
        .mockResolvedValueOnce({
          ok: true,
          json: async () => ({ id: "changeset-001", items: [] }),
        })
        .mockResolvedValueOnce({
          ok: false,
          status: 404,
          json: async () => ({ error: "not found" }),
        }),
    );

    await expect(api.getJobChangeSet("job-001")).resolves.toEqual({ id: "changeset-001", items: [] });
    await expect(api.getJobChangeSet("job-002")).resolves.toBeNull();
  });

  it("posts to the rollback endpoint", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn().mockResolvedValue({
        ok: true,
        json: async () => ({ id: "rollback-001", status: "completed" }),
      }),
    );

    await api.rollbackJob("job-001");

    expect(fetch).toHaveBeenCalledWith(
      "/api/jobs/job-001/rollback",
      expect.objectContaining({
        method: "POST",
        headers: { "Content-Type": "application/json" },
      }),
    );
  });

  it("deletes a job", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn().mockResolvedValue({
        ok: true,
        status: 204,
      }),
    );

    await expect(api.deleteJob("job-001")).resolves.toBeUndefined();

    expect(fetch).toHaveBeenCalledWith(
      "/api/jobs/job-001",
      expect.objectContaining({
        method: "DELETE",
        headers: { "Content-Type": "application/json" },
      }),
    );
  });
});
