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
    expect(screen.getByText("A review-first update tool")).toBeInTheDocument();
    expect(screen.getByText("The connected graph source")).toBeInTheDocument();
    expect(screen.getByRole("link", { name: "Open Dashboard" })).toHaveAttribute("href", "/dashboard");
    expect(screen.getByAltText("BackToPure Logo").closest("a")).toHaveAttribute("href", "/");
  });
});
