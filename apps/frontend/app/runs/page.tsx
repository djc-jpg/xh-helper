import { Suspense } from "react";

import { PageSkeleton } from "../../components/skeleton";

import { RunsPageClient } from "./runs-client";

export default function RunsPage() {
  return (
    <Suspense fallback={<PageSkeleton />}>
      <RunsPageClient />
    </Suspense>
  );
}
