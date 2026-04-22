import { createBrowserRouter } from "react-router-dom";

import { AppShell } from "../components/AppShell";
import { DashboardPage } from "../pages/dashboard/DashboardPage";
import { HistoryPage } from "../pages/history/HistoryPage";
import { HomePage } from "../pages/home/HomePage";
import { JobDetailPage } from "../pages/jobs/JobDetailPage";
import { NewDatasetsJobPage } from "../pages/jobs/NewDatasetsJobPage";
import { NewExternalOrgsJobPage } from "../pages/jobs/NewExternalOrgsJobPage";
import { NewExternalPersonsJobPage } from "../pages/jobs/NewExternalPersonsJobPage";
import { NewInternalPersonsJobPage } from "../pages/jobs/NewInternalPersonsJobPage";
import { NewResearchOutputsJobPage } from "../pages/jobs/NewResearchOutputsJobPage";
import { NotFoundPage } from "../pages/not-found/NotFoundPage";

export const router = createBrowserRouter(
  [
    {
      path: "/",
      element: <AppShell />,
      errorElement: <NotFoundPage />,
      children: [
        {
          index: true,
          element: <HomePage />,
        },
        {
          path: "dashboard",
          element: <DashboardPage />,
        },
        {
          path: "history",
          element: <HistoryPage />,
        },
        {
          path: "jobs/new/internal-persons",
          element: <NewInternalPersonsJobPage />,
        },
        {
          path: "jobs/new/external-persons",
          element: <NewExternalPersonsJobPage />,
        },
        {
          path: "jobs/new/external-orgs",
          element: <NewExternalOrgsJobPage />,
        },
        {
          path: "jobs/new/research-outputs",
          element: <NewResearchOutputsJobPage />,
        },
        {
          path: "jobs/new/datasets",
          element: <NewDatasetsJobPage />,
        },
        {
          path: "jobs/:jobId",
          element: <JobDetailPage />,
        },
      ],
    },
  ],
  {
    basename: "/app",
  },
);
