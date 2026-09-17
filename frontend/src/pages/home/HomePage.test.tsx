import { screen } from "@testing-library/react";

import { AppShell } from "../../components/AppShell";
import { renderWithRouter } from "../../test/test-utils";
import { HomePage } from "./HomePage";

describe("HomePage", () => {
  it("renders the calm landing page and links the logo back home", async () => {
    renderWithRouter({
      routes: [
        {
          path: "/",
          element: <AppShell />,
          children: [{ index: true, element: <HomePage /> }],
        },
      ],
    });

    expect(await screen.findByRole("heading", { name: "BackToPure" })).toBeInTheDocument();
    expect(screen.getByText("Review-first updates")).toBeInTheDocument();
    expect(screen.getByText("Ricgraph input")).toBeInTheDocument();
    expect(screen.getAllByRole("link", { name: "Dashboard" })[1]).toHaveAttribute("href", "/dashboard");
    expect(screen.getAllByRole("link", { name: "History" })[1]).toHaveAttribute("href", "/history");
    expect(screen.getByAltText("BackToPure Logo").closest("a")).toHaveAttribute("href", "/");
  });
});
