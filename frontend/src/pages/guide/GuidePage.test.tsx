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

    expect(await screen.findByRole("heading", { name: "How BackToPure Works" })).toBeInTheDocument();
    expect(screen.getByRole("heading", { name: "Internal Persons" })).toBeInTheDocument();
    expect(screen.getByRole("heading", { name: "External Persons" })).toBeInTheDocument();
    expect(screen.getByRole("heading", { name: "External Organisations" })).toBeInTheDocument();
    expect(screen.getByRole("heading", { name: "Research Outputs" })).toBeInTheDocument();
    expect(screen.getByRole("heading", { name: "Datasets" })).toBeInTheDocument();
    expect(screen.queryByText("Live in new UI")).not.toBeInTheDocument();
    expect(screen.queryByText("Legacy only")).not.toBeInTheDocument();
    expect(screen.queryByText("Planned")).not.toBeInTheDocument();
    expect(screen.getByRole("link", { name: "Open Dashboard" })).toHaveAttribute("href", "/dashboard");
  });
});
