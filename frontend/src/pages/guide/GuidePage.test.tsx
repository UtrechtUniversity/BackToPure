import { screen } from "@testing-library/react";

import { AppShell } from "../../components/AppShell";
import { GuidePage } from "./GuidePage";
import { renderWithRouter } from "../../test/test-utils";

describe("GuidePage", () => {
  it("explains the workflows without migration status labels", async () => {
    renderWithRouter({
      routes: [
        {
          path: "/",
          element: <AppShell />,
          children: [{ path: "guide", element: <GuidePage /> }],
        },
      ],
      initialEntries: ["/guide"],
    });

    expect(await screen.findByText("How BackToPure Works")).toBeInTheDocument();
    expect(screen.getByText("Internal Persons")).toBeInTheDocument();
    expect(screen.getByText("External Persons")).toBeInTheDocument();
    expect(screen.getByText("External Organisations")).toBeInTheDocument();
    expect(screen.getByText("Research Outputs")).toBeInTheDocument();
    expect(screen.getByText("Datasets")).toBeInTheDocument();
    expect(screen.queryByText("Live in new UI")).not.toBeInTheDocument();
    expect(screen.queryByText("Legacy only")).not.toBeInTheDocument();
    expect(screen.queryByText("Planned")).not.toBeInTheDocument();
    expect(screen.getByRole("link", { name: "Start Internal Persons" })).toHaveAttribute(
      "href",
      "/jobs/new/internal-persons",
    );
    expect(screen.getByRole("link", { name: "Start External Persons" })).toHaveAttribute(
      "href",
      "/jobs/new/external-persons",
    );
    expect(screen.getByRole("link", { name: "Start External Organisations" })).toHaveAttribute(
      "href",
      "/jobs/new/external-orgs",
    );
    expect(screen.getByRole("link", { name: "Start Research Outputs" })).toHaveAttribute(
      "href",
      "/jobs/new/research-outputs",
    );
    expect(screen.getByRole("link", { name: "Start Datasets" })).toHaveAttribute(
      "href",
      "/jobs/new/datasets",
    );
  });
});
